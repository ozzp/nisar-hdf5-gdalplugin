/*
 * Copyright 2025, California Institute of Technology.
 * All rights reserved. U.S. Government sponsorship acknowledged.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cmath>
#include <cctype>
#include <cstdio>
#include <algorithm>
#include <limits>

#include "nisarinterpolated.h"
#include "nisarinterpolatedrasterband.h"
#include "nisardataset.h"
#include "gdalwarper.h"
#include "gdal_utils.h"
#include "vrtdataset.h"
#include "cpl_time.h"

// Split "NISAR:<file>[:<hdf5-path>]" into its parts (same rules as NisarDataset::Open):
// the last colon separates file and path unless it is followed by "//" (URL scheme).
static bool NisarSplitConnectionString(const char *pszInput, std::string &sFile,
                                       std::string &sPath)
{
    std::string s(pszInput);
    if (STARTS_WITH_CI(s.c_str(), "NISAR:"))
        s = s.substr(6);
    if (s.empty())
        return false;

    sPath.clear();
    const size_t nColon = s.rfind(':');
    if (nColon != std::string::npos && s.compare(nColon, 3, "://") != 0)
    {
        sPath = s.substr(nColon + 1);
        sFile = s.substr(0, nColon);
    }
    else
    {
        sFile = s;
    }
    if (sFile.size() >= 2 && sFile.front() == '"' && sFile.back() == '"')
    {
        sFile = sFile.substr(1, sFile.size() - 2);
    }
    return !sFile.empty();
}

static std::string NisarUpper(const char *psz)
{
    std::string s(psz);
    for (auto &c : s)
        c = static_cast<char>(toupper(static_cast<unsigned char>(c)));
    return s;
}

// Open a NISAR dataset with the driver forced; returns the concrete NisarDataset.
static NisarDataset *NisarOpenInternal(const std::string &sConn,
                                       char **papszOptions)
{
    const char *const apszAllowedDrivers[] = {"NISAR", nullptr};
    GDALDataset *poDS = static_cast<GDALDataset *>(GDALOpenEx(
        sConn.c_str(), GDAL_OF_RASTER | GDAL_OF_READONLY | GDAL_OF_INTERNAL,
        apszAllowedDrivers, papszOptions, nullptr));
    if (poDS == nullptr)
        return nullptr;
    NisarDataset *poNisar = dynamic_cast<NisarDataset *>(poDS);
    if (poNisar == nullptr)
        GDALClose(poDS);
    return poNisar;
}

// Read a whole numeric HDF5 dataset (scalar, 1D or 3D) as doubles.
static bool NisarReadDoubles(hid_t hFile, const std::string &sPath,
                             std::vector<double> &vec,
                             std::vector<hsize_t> *pDims = nullptr)
{
    hid_t hDset = H5Dopen2(hFile, sPath.c_str(), H5P_DEFAULT);
    if (hDset < 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "Interpolation: missing dataset %s", sPath.c_str());
        return false;
    }
    hid_t hSpace = H5Dget_space(hDset);
    const int nDims = H5Sget_simple_extent_ndims(hSpace);
    std::vector<hsize_t> dims(std::max(nDims, 0));
    if (nDims > 0)
        H5Sget_simple_extent_dims(hSpace, dims.data(), nullptr);
    hssize_t nCount = H5Sget_simple_extent_npoints(hSpace);
    H5Sclose(hSpace);
    if (nCount <= 0)
    {
        H5Dclose(hDset);
        return false;
    }
    vec.resize(static_cast<size_t>(nCount));
    const herr_t st = H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                              H5P_DEFAULT, vec.data());
    H5Dclose(hDset);
    if (st < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO, "Interpolation: failed to read %s",
                 sPath.c_str());
        return false;
    }
    if (pDims)
        *pDims = dims;
    return true;
}

// Read a fixed- or variable-length string attribute of an HDF5 dataset.
static std::string NisarReadStringAttr(hid_t hFile, const std::string &sPath,
                                       const char *pszAttr)
{
    std::string sResult;
    hid_t hDset = H5Dopen2(hFile, sPath.c_str(), H5P_DEFAULT);
    if (hDset < 0)
        return sResult;
    if (H5Aexists(hDset, pszAttr) > 0)
    {
        hid_t hAttr = H5Aopen(hDset, pszAttr, H5P_DEFAULT);
        hid_t hType = H5Aget_type(hAttr);
        if (H5Tget_class(hType) == H5T_STRING)
        {
            if (H5Tis_variable_str(hType) > 0)
            {
                char *psz = nullptr;
                if (H5Aread(hAttr, hType, &psz) >= 0 && psz)
                {
                    sResult = psz;
                    H5free_memory(psz);
                }
            }
            else
            {
                const size_t nSize = H5Tget_size(hType);
                std::vector<char> buf(nSize + 1, '\0');
                if (H5Aread(hAttr, hType, buf.data()) >= 0)
                    sResult = buf.data();
            }
        }
        H5Tclose(hType);
        H5Aclose(hAttr);
    }
    H5Dclose(hDset);
    return sResult;
}

// Parse "seconds since YYYY-MM-DDTHH:MM:SS[.fff]" into a Unix epoch (seconds).
static bool NisarParseEpoch(const std::string &sUnits, double &dfEpoch)
{
    const size_t nPos = sUnits.find("since");
    if (nPos == std::string::npos)
        return false;
    int Y, M, D, h, m;
    double s;
    if (sscanf(sUnits.c_str() + nPos + 5, " %d-%d-%dT%d:%d:%lf", &Y, &M, &D, &h,
               &m, &s) != 6)
        return false;
    struct tm tmv = {};
    tmv.tm_year = Y - 1900;
    tmv.tm_mon = M - 1;
    tmv.tm_mday = D;
    tmv.tm_hour = h;
    tmv.tm_min = m;
    tmv.tm_sec = 0;
    dfEpoch = static_cast<double>(CPLYMDHMSToUnixTime(&tmv)) + s;
    return true;
}

// ====================================================================
// NisarInterpolatedDataset Implementation
// ====================================================================

NisarInterpolatedDataset::NisarInterpolatedDataset()
{
}

NisarInterpolatedDataset::~NisarInterpolatedDataset()
{
    // Clean up the DEM datasets when this dataset is closed
    if (m_poAlignedDEM && m_poAlignedDEM != m_poRawDEM)
        GDALClose(m_poAlignedDEM);
    if (m_poDEMValidityView)
        GDALClose(m_poDEMValidityView);
    if (m_poRawDEM)
        GDALClose(m_poRawDEM);
    if (m_pasGCPs)
    {
        GDALDeinitGCPs(m_nGCPCount, m_pasGCPs);
        CPLFree(m_pasGCPs);
    }
}

#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
CPLErr NisarInterpolatedDataset::GetGeoTransform(double *padfTransform)
{
    if (m_bRadarGrid)
        return CE_Failure;
    memcpy(padfTransform, m_adfTargetGeoTransform, 6 * sizeof(double));
    return CE_None;
}
#else
CPLErr NisarInterpolatedDataset::GetGeoTransform(GDALGeoTransform &gt) const
{
    if (m_bRadarGrid)
        return CE_Failure;
    for (int i = 0; i < 6; ++i)
    {
        gt[i] = m_adfTargetGeoTransform[i];
    }
    return CE_None;
}
#endif

const OGRSpatialReference *NisarInterpolatedDataset::GetSpatialRef() const
{
    return m_oSRS.IsEmpty() ? nullptr : &m_oSRS;
}

int NisarInterpolatedDataset::GetGCPCount()
{
    return m_nGCPCount;
}

const GDAL_GCP *NisarInterpolatedDataset::GetGCPs()
{
    return m_pasGCPs;
}

const OGRSpatialReference *NisarInterpolatedDataset::GetGCPSpatialRef() const
{
    return (m_nGCPCount > 0 && !m_oGCPSRS.IsEmpty()) ? &m_oGCPSRS : nullptr;
}

// Fractional node index of dfValue on a monotonically increasing axis (clamped).
double NisarInterpolatedDataset::AxisToNode(const std::vector<double> &axis,
                                            double dfValue)
{
    const size_t n = axis.size();
    if (n < 2)
        return 0.0;
    if (dfValue <= axis.front())
        return 0.0;
    if (dfValue >= axis.back())
        return static_cast<double>(n - 1);
    const size_t i1 =
        std::upper_bound(axis.begin(), axis.end(), dfValue) - axis.begin();
    const size_t i0 = i1 - 1;
    const double dfSpan = axis[i1] - axis[i0];
    return dfSpan > 0 ? i0 + (dfValue - axis[i0]) / dfSpan
                      : static_cast<double>(i0);
}

NisarCubeXY NisarInterpolatedDataset::CubeXY(double dfNodeX,
                                             double dfNodeY) const
{
    NisarCubeXY xy;
    dfNodeX =
        std::min(std::max(dfNodeX, 0.0), static_cast<double>(m_nCubeXSize - 1));
    dfNodeY =
        std::min(std::max(dfNodeY, 0.0), static_cast<double>(m_nCubeYSize - 1));
    xy.x0 = static_cast<int>(std::floor(dfNodeX));
    xy.y0 = static_cast<int>(std::floor(dfNodeY));
    xy.x1 = std::min(xy.x0 + 1, m_nCubeXSize - 1);
    xy.y1 = std::min(xy.y0 + 1, m_nCubeYSize - 1);
    xy.wx = dfNodeX - xy.x0;
    xy.wy = dfNodeY - xy.y0;
    return xy;
}

// Bracketing height levels for dfHeight (clamped to the cube's height range).
void NisarInterpolatedDataset::CubeZ(double dfHeight, int &z0, int &z1,
                                     double &wz) const
{
    const int nZ = m_nCubeZSize;
    z0 = 0;
    z1 = 0;
    wz = 0.0;
    if (nZ < 2 || std::isnan(dfHeight))
        return;
    if (dfHeight <= m_zVect.front())
        return;
    if (dfHeight >= m_zVect.back())
    {
        z0 = z1 = nZ - 1;
        return;
    }
    z1 = static_cast<int>(
        std::upper_bound(m_zVect.begin(), m_zVect.end(), dfHeight) -
        m_zVect.begin());
    z0 = z1 - 1;
    const double dfSpan = m_zVect[z1] - m_zVect[z0];
    wz = dfSpan > 0 ? (dfHeight - m_zVect[z0]) / dfSpan : 0.0;
}

// Raw-DEM pixel window covering the coordinateX/Y footprint (padded 5%), clamped to the DEM.
bool NisarInterpolatedDataset::DEMWindowForFootprint(
    const OGRSpatialReference *poDEMSRS, int &nX, int &nY, int &nW,
    int &nH) const
{
    double dfMinX = HUGE_VAL, dfMaxX = -HUGE_VAL, dfMinY = HUGE_VAL,
           dfMaxY = -HUGE_VAL;
    for (size_t i = 0; i < m_coordX.size(); ++i)
    {
        if (!std::isfinite(m_coordX[i]) || !std::isfinite(m_coordY[i]))
            continue;
        dfMinX = std::min(dfMinX, m_coordX[i]);
        dfMaxX = std::max(dfMaxX, m_coordX[i]);
        dfMinY = std::min(dfMinY, m_coordY[i]);
        dfMaxY = std::max(dfMaxY, m_coordY[i]);
    }
    if (dfMinX > dfMaxX || dfMinY > dfMaxY)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: coordinateX/Y cubes are all invalid.");
        return false;
    }
    const double dfPadX = 0.05 * (dfMaxX - dfMinX),
                 dfPadY = 0.05 * (dfMaxY - dfMinY);
    dfMinX -= dfPadX;
    dfMaxX += dfPadX;
    dfMinY -= dfPadY;
    dfMaxY += dfPadY;

    OGRSpatialReference oDEMSRS(*poDEMSRS);
    oDEMSRS.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
    OGRCoordinateTransformation *poCT =
        OGRCreateCoordinateTransformation(&m_oGCPSRS, &oDEMSRS);
    if (poCT == nullptr)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: cannot transform the geolocation grid CRS to "
                 "the DEM CRS.");
        return false;
    }
    // Densified footprint boundary -> bbox in DEM CRS.
    const int nSteps = 32;
    std::vector<double> adfX, adfY;
    for (int i = 0; i <= nSteps; ++i)
    {
        const double f = static_cast<double>(i) / nSteps;
        adfX.push_back(dfMinX + f * (dfMaxX - dfMinX));
        adfY.push_back(dfMinY);
        adfX.push_back(dfMinX + f * (dfMaxX - dfMinX));
        adfY.push_back(dfMaxY);
        adfX.push_back(dfMinX);
        adfY.push_back(dfMinY + f * (dfMaxY - dfMinY));
        adfX.push_back(dfMaxX);
        adfY.push_back(dfMinY + f * (dfMaxY - dfMinY));
    }
    std::vector<int> abSuccess(adfX.size(), FALSE);
    poCT->Transform(static_cast<int>(adfX.size()), adfX.data(), adfY.data(),
                    nullptr, abSuccess.data());
    OGRCoordinateTransformation::DestroyCT(poCT);

    double adfGT[6], adfInv[6];
    if (GDALGetGeoTransform(m_poRawDEM, adfGT) != CE_None ||
        !GDALInvGeoTransform(adfGT, adfInv))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: DEM has no affine geotransform (GCP/RPC-only "
                 "DEMs are not supported).");
        return false;
    }
    double dfMinP = HUGE_VAL, dfMaxP = -HUGE_VAL, dfMinL = HUGE_VAL,
           dfMaxL = -HUGE_VAL;
    for (size_t i = 0; i < adfX.size(); ++i)
    {
        if (!abSuccess[i] || !std::isfinite(adfX[i]) || !std::isfinite(adfY[i]))
            continue;
        const double p = adfInv[0] + adfInv[1] * adfX[i] + adfInv[2] * adfY[i];
        const double l = adfInv[3] + adfInv[4] * adfX[i] + adfInv[5] * adfY[i];
        dfMinP = std::min(dfMinP, p);
        dfMaxP = std::max(dfMaxP, p);
        dfMinL = std::min(dfMinL, l);
        dfMaxL = std::max(dfMaxL, l);
    }
    const int nDEMX = m_poRawDEM->GetRasterXSize(),
              nDEMY = m_poRawDEM->GetRasterYSize();
    if (dfMinP > dfMaxP || dfMinL > dfMaxL || dfMaxP < 0 || dfMaxL < 0 ||
        dfMinP > nDEMX || dfMinL > nDEMY)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: DEM does not cover the geolocation grid "
                 "footprint.");
        return false;
    }
    nX = std::max(0, static_cast<int>(std::floor(dfMinP)) - 1);
    nY = std::max(0, static_cast<int>(std::floor(dfMinL)) - 1);
    nW = std::min(nDEMX, static_cast<int>(std::ceil(dfMaxP)) + 1) - nX;
    nH = std::min(nDEMY, static_cast<int>(std::ceil(dfMaxL)) + 1) - nY;
    return nW > 0 && nH > 0;
}

// Load the Level-1 radar axes, coordinateX/Y cubes, GCPs and prepare the DEM in cube CRS.
bool NisarInterpolatedDataset::InitRadarGrid(NisarDataset *poCube,
                                             NisarDataset *poSwath,
                                             const std::string &sCubeGroup,
                                             const std::string &sRefPath)
{
    const hid_t hFile = poCube->GetHDF5Handle();
    std::vector<hsize_t> dimsX, dimsY;
    if (!NisarReadDoubles(hFile, sCubeGroup + "/slantRange", m_cubeRange) ||
        !NisarReadDoubles(hFile, sCubeGroup + "/zeroDopplerTime", m_cubeTime) ||
        !NisarReadDoubles(hFile, sCubeGroup + "/coordinateX", m_coordX,
                          &dimsX) ||
        !NisarReadDoubles(hFile, sCubeGroup + "/coordinateY", m_coordY, &dimsY))
        return false;

    if (!std::is_sorted(m_zVect.begin(), m_zVect.end()))
    {
        CPLError(
            CE_Failure, CPLE_AppDefined,
            "Interpolation: heightAboveEllipsoid levels must be ascending.");
        return false;
    }
    const size_t nExpected =
        static_cast<size_t>(m_nCubeZSize) * m_nCubeYSize * m_nCubeXSize;
    if (dimsX.size() != 3 || dimsY.size() != 3 ||
        m_coordX.size() != nExpected || m_coordY.size() != nExpected ||
        static_cast<int>(m_cubeRange.size()) != m_nCubeXSize ||
        static_cast<int>(m_cubeTime.size()) != m_nCubeYSize)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: geolocationGrid axes/coordinate cubes do not "
                 "match the "
                 "%s cube (%d x %d x %d).",
                 sCubeGroup.c_str(), m_nCubeXSize, m_nCubeYSize, m_nCubeZSize);
        return false;
    }

    // Swath axes: slantRange per column, zeroDopplerTime per line (may use another epoch),
    // from the nearest ancestor groups of the reference raster that hold them.
    const std::string sRangeGroup =
        NisarFindGroupUpward(hFile, sRefPath, {"slantRange"});
    const std::string sTimeGroup =
        NisarFindGroupUpward(hFile, sRefPath, {"zeroDopplerTime"});
    if (sRangeGroup.empty() || sTimeGroup.empty())
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: no slantRange/zeroDopplerTime axes above %s.",
                 sRefPath.c_str());
        return false;
    }
    const std::string sSwathTimePath = sTimeGroup + "/zeroDopplerTime";
    if (!NisarReadDoubles(hFile, sRangeGroup + "/slantRange", m_swathRange) ||
        !NisarReadDoubles(hFile, sSwathTimePath, m_swathTime))
        return false;
    if (static_cast<int>(m_swathRange.size()) != nRasterXSize ||
        static_cast<int>(m_swathTime.size()) != nRasterYSize)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: swath axes (%d x %d) do not match the "
                 "reference raster (%d x %d).",
                 static_cast<int>(m_swathRange.size()),
                 static_cast<int>(m_swathTime.size()), nRasterXSize,
                 nRasterYSize);
        return false;
    }
    double dfCubeEpoch = 0.0, dfSwathEpoch = 0.0;
    if (NisarParseEpoch(NisarReadStringAttr(
                            hFile, sCubeGroup + "/zeroDopplerTime", "units"),
                        dfCubeEpoch) &&
        NisarParseEpoch(NisarReadStringAttr(hFile, sSwathTimePath, "units"),
                        dfSwathEpoch) &&
        dfCubeEpoch != dfSwathEpoch)
    {
        for (auto &t : m_swathTime)
            t += dfSwathEpoch - dfCubeEpoch;
    }

    // Cube CRS (geolocationGrid/epsg) and GCPs pass through from the swath raster.
    std::vector<double> epsg;
    if (!NisarReadDoubles(hFile, sCubeGroup + "/epsg", epsg) ||
        m_oGCPSRS.importFromEPSG(static_cast<int>(epsg[0])) != OGRERR_NONE)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: invalid geolocationGrid/epsg.");
        return false;
    }
    m_oGCPSRS.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);
    SetMetadataItem("NISAR_GEOLOCATION_EPSG",
                    CPLSPrintf("%d", static_cast<int>(epsg[0])));
    m_nGCPCount = poSwath->GetGCPCount();
    if (m_nGCPCount > 0)
    {
        m_pasGCPs = GDALDuplicateGCPs(m_nGCPCount, poSwath->GetGCPs());
    }

    // DEM must be sampled in the cube CRS; warp lazily only if it differs.
    const OGRSpatialReference *poDEMSRS = m_poRawDEM->GetSpatialRef();
    if (poDEMSRS == nullptr)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: DEM has no spatial reference.");
        return false;
    }
    // Source validity: an explicit alpha band (any type), else band 1's GDAL mask band
    // (per-dataset mask, Byte alpha, or nodata-derived; all-valid when none of those).
    GDALRasterBand *poRawBand = m_poRawDEM->GetRasterBand(1);
    int nAlphaBand = 0;
    for (int i = 1; i <= m_poRawDEM->GetRasterCount(); ++i)
    {
        if (m_poRawDEM->GetRasterBand(i)->GetColorInterpretation() ==
            GCI_AlphaBand)
        {
            nAlphaBand = i;
            break;
        }
    }
    int bRawHasNoData = FALSE;
    const double dfRawNoData = poRawBand->GetNoDataValue(&bRawHasNoData);
    if (poDEMSRS->IsSame(&m_oGCPSRS))
    {
        m_poAlignedDEM = m_poRawDEM;
        const int nMaskFlags = poRawBand->GetMaskFlags();
        if (nAlphaBand > 0)
            m_poDEMMaskBand = m_poRawDEM->GetRasterBand(nAlphaBand);
        else if ((nMaskFlags & GMF_ALL_VALID) == 0 && nMaskFlags != GMF_NODATA)
            m_poDEMMaskBand = poRawBand->GetMaskBand();
    }
    else
    {
        // Restrict the warp to the DEM window covering the (padded) geolocation-grid
        // footprint: warping a global DEM into a projected CRS gives a bogus extent/resolution.
        int nWinX = 0, nWinY = 0, nWinW = 0, nWinH = 0;
        if (!DEMWindowForFootprint(poDEMSRS, nWinX, nWinY, nWinW, nWinH))
            return false;
        // Warp [height, validity] with validity as source alpha so masked cells (also those
        // combined with nodata, which the warper alone would ignore) end up as dst alpha 0.
        const std::string sValidity =
            nAlphaBand > 0 ? std::to_string(nAlphaBand) : "mask";
        const std::string sX = std::to_string(nWinX),
                          sY = std::to_string(nWinY),
                          sW = std::to_string(nWinW),
                          sH = std::to_string(nWinH);
        const char *apszTranslate[] = {
            "-of",      "VRT",      "-b",
            "1",        "-b",       sValidity.c_str(),
            "-srcwin",  sX.c_str(), sY.c_str(),
            sW.c_str(), sH.c_str(), nullptr};
        GDALTranslateOptions *psTO = GDALTranslateOptionsNew(
            const_cast<char **>(apszTranslate), nullptr);
        m_poDEMValidityView = GDALDataset::FromHandle(
            GDALTranslate("", m_poRawDEM, psTO, nullptr));
        GDALTranslateOptionsFree(psTO);
        if (m_poDEMValidityView == nullptr)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Interpolation: failed to build the DEM validity view.");
            return false;
        }
        GDALWarpOptions *psWO = GDALCreateWarpOptions();
        GDALWarpInitDefaultBandMapping(psWO, 1);
        psWO->nSrcAlphaBand = 2;
        psWO->nDstAlphaBand = 2;
        if (bRawHasNoData)
        {
            GDALWarpInitNoDataReal(psWO, dfRawNoData);
            psWO->papszWarpOptions =
                CSLSetNameValue(psWO->papszWarpOptions, "INIT_DEST", "NO_DATA");
        }
        char *pszWKT = nullptr;
        m_oGCPSRS.exportToWkt(&pszWKT);
        m_poAlignedDEM = GDALDataset::FromHandle(GDALAutoCreateWarpedVRT(
            m_poDEMValidityView, nullptr, pszWKT, GRA_Bilinear, 0.0, psWO));
        GDALDestroyWarpOptions(psWO);
        CPLFree(pszWKT);
        if (m_poAlignedDEM == nullptr)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Interpolation: failed to warp DEM to EPSG:%d.",
                     static_cast<int>(epsg[0]));
            return false;
        }
        m_poDEMMaskBand = m_poAlignedDEM->GetRasterBand(2);
    }
    if (GDALGetGeoTransform(m_poAlignedDEM, m_adfDEMGeoTransform) != CE_None)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: DEM has no affine geotransform (GCP/RPC-only "
                 "DEMs are not supported).");
        return false;
    }
    if (!GDALInvGeoTransform(m_adfDEMGeoTransform, m_adfDEMInvGeoTransform))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Interpolation: DEM geotransform is not invertible.");
        return false;
    }
    int bHasNoData = FALSE;
    m_dfDEMNoData =
        m_poAlignedDEM->GetRasterBand(1)->GetNoDataValue(&bHasNoData);
    m_bDEMHasNoData = bHasNoData != FALSE;
    m_bRadarGrid = true;
    return true;
}

GDALDataset *NisarInterpolatedDataset::Open(GDALOpenInfo *poOpenInfo)
{
    char **papszOO = poOpenInfo->papszOpenOptions;
    const char *pszDemFile = CSLFetchNameValue(papszOO, "DEM_FILE");
    const char *pszQuantity = CSLFetchNameValue(papszOO, "QUANTITY");
    const char *pszInstOpt = CSLFetchNameValue(papszOO, "INST");
    const char *pszFreqOpt = CSLFetchNameValue(papszOO, "FREQ");
    const char *pszPolOpt = CSLFetchNameValue(papszOO, "POL");

    std::string sFile, sCubePath;
    if (!NisarSplitConnectionString(poOpenInfo->pszFilename, sFile, sCubePath))
    {
        CPLError(CE_Failure, CPLE_OpenFailed, "Interpolation: empty filename.");
        return nullptr;
    }
    if (sFile.size() < 3 || !EQUAL(sFile.c_str() + sFile.size() - 3, ".h5"))
    {
        return nullptr;  // not a NISAR granule
    }
    const std::string sQuotedFile = "NISAR:\"" + sFile + "\"";

    // 1. Identify the product; without an explicit cube path, resolve it from QUANTITY.
    std::string sInst, sProduct;
    bool bIsLevel1 = false;
    NisarDataset *poCoarseCubeDS = nullptr;

    if (sCubePath.empty())
    {
        NisarDataset *poContainer = NisarOpenInternal(sQuotedFile, nullptr);
        if (poContainer == nullptr)
        {
            CPLError(CE_Failure, CPLE_OpenFailed,
                     "Interpolation: failed to open %s", sFile.c_str());
            return nullptr;
        }
        sInst = poContainer->GetInstrument();
        sProduct = poContainer->GetProductType();
        bIsLevel1 = poContainer->IsLevel1();
        GDALClose(poContainer);

        if (pszInstOpt)
            sInst = NisarUpper(pszInstOpt);
        if (sInst.empty() || sProduct.empty())
        {
            CPLError(CE_Failure, CPLE_OpenFailed,
                     "Interpolation: could not identify NISAR product in %s",
                     sFile.c_str());
            return nullptr;
        }

        // L1 quantities live in metadata/geolocationGrid, L2/L3 in metadata/radarGrid.
        const char *pszCubeGroup = bIsLevel1 ? "geolocationGrid" : "radarGrid";
        sCubePath = "/science/" + sInst + "/" + sProduct + "/metadata/" +
                    pszCubeGroup + "/" + pszQuantity;
        CPLDebug("NISAR_DRIVER", "Interpolation: QUANTITY=%s resolved to %s",
                 pszQuantity, sCubePath.c_str());
    }

    CPLDebug("NISAR_DRIVER", "Interpolation: Opening coarse cube at %s",
             sCubePath.c_str());
    poCoarseCubeDS = NisarOpenInternal(sQuotedFile + ":" + sCubePath, nullptr);
    if (poCoarseCubeDS == nullptr || poCoarseCubeDS->GetRasterCount() < 2)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to open valid 3D coarse metadata cube at %s",
                 sCubePath.c_str());
        if (poCoarseCubeDS)
            GDALClose(poCoarseCubeDS);
        return nullptr;
    }

    if (sProduct.empty())
    {
        sInst = pszInstOpt ? NisarUpper(pszInstOpt)
                           : poCoarseCubeDS->GetInstrument();
        sProduct = poCoarseCubeDS->GetProductType();
        bIsLevel1 = poCoarseCubeDS->IsLevel1();
    }

    // 2. Open the reference (imaging) grid whose shape/georeferencing the output inherits.
    const std::string sFreq =
        pszFreqOpt ? NisarUpper(pszFreqOpt) : std::string("A");
    std::string sPol = pszPolOpt ? NisarUpper(pszPolOpt) : std::string();
    std::string sRefDesc;
    NisarDataset *poTargetGridDS = nullptr;

    if (EQUAL(sProduct.c_str(), "RSLC") || EQUAL(sProduct.c_str(), "GCOV") ||
        EQUAL(sProduct.c_str(), "GSLC"))
    {
        // L1 swaths/frequency<F>/<POL> or L2 grids/frequency<F>/<POL>: NisarDataset builds the path
        char **papszRefOO = nullptr;
        papszRefOO = CSLSetNameValue(papszRefOO, "INST", sInst.c_str());
        papszRefOO = CSLSetNameValue(papszRefOO, "FREQ", sFreq.c_str());
        if (!sPol.empty())
            papszRefOO = CSLSetNameValue(papszRefOO, "POL", sPol.c_str());
        sRefDesc = CPLSPrintf("%s %s/frequency%s/%s", sProduct.c_str(),
                              bIsLevel1 ? "swaths" : "grids", sFreq.c_str(),
                              sPol.empty() ? "<default POL>" : sPol.c_str());
        poTargetGridDS = NisarOpenInternal(sQuotedFile, papszRefOO);
        CSLDestroy(papszRefOO);
    }
    else if (EQUAL(sProduct.c_str(), "GUNW"))
    {
        if (sPol.empty())
            sPol = "HH";
        sRefDesc = "/science/" + sInst + "/GUNW/grids/frequency" + sFreq +
                   "/unwrappedInterferogram/" + sPol + "/unwrappedPhase";
        poTargetGridDS =
            NisarOpenInternal(sQuotedFile + ":" + sRefDesc, nullptr);
    }
    else if (EQUAL(sProduct.c_str(), "RIFG") || EQUAL(sProduct.c_str(), "RUNW"))
    {
        // L1 interferogram grid: swaths/frequency<F>/interferogram/<POL>/<layer>
        if (sPol.empty())
            sPol = "HH";
        sRefDesc = "/science/" + sInst + "/" + sProduct + "/swaths/frequency" +
                   sFreq + "/interferogram/" + sPol + "/" +
                   (EQUAL(sProduct.c_str(), "RIFG") ? "wrappedInterferogram"
                                                    : "unwrappedPhase");
        poTargetGridDS =
            NisarOpenInternal(sQuotedFile + ":" + sRefDesc, nullptr);
    }
    else if (bIsLevel1)
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Interpolation: Level-1 product %s is not supported (only "
                 "RSLC, RIFG, RUNW).",
                 sProduct.c_str());
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }
    else
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Interpolation: unsupported product type '%s'. Cannot "
                 "determine reference grid.",
                 sProduct.c_str());
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }

    if (!poTargetGridDS)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "Interpolation: failed to open reference grid (%s).",
                 sRefDesc.c_str());
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }
    CPLDebug("NISAR_DRIVER", "Interpolation: reference grid %s (%d x %d)",
             sRefDesc.c_str(), poTargetGridDS->GetRasterXSize(),
             poTargetGridDS->GetRasterYSize());

    // Instantiate our custom dataset and copy spatial info
    NisarInterpolatedDataset *poDS = new NisarInterpolatedDataset();

    poDS->nRasterXSize = poTargetGridDS->GetRasterXSize();
    poDS->nRasterYSize = poTargetGridDS->GetRasterYSize();
    if (!bIsLevel1)
    {
        GDALGetGeoTransform(poTargetGridDS, poDS->m_adfTargetGeoTransform);
        const OGRSpatialReference *poTargetSRS =
            poTargetGridDS->GetSpatialRef();
        if (poTargetSRS)
        {
            poDS->m_oSRS = *poTargetSRS;
        }
    }

    poDS->SetDescription(poOpenInfo->pszFilename);
    poDS->SetMetadataItem("NISAR_PRODUCT_TYPE", sProduct.c_str());
    poDS->SetMetadataItem("NISAR_CUBE_PATH", sCubePath.c_str());
    const char *pszRefPath = poTargetGridDS->GetMetadataItem("HDF5_PATH");
    const std::string sRefPath =
        pszRefPath ? pszRefPath : poTargetGridDS->GetDescription();
    poDS->SetMetadataItem("NISAR_REFERENCE_GRID", sRefPath.c_str());
    if (pszQuantity)
        poDS->SetMetadataItem("NISAR_QUANTITY", pszQuantity);
    for (const char *pszKey : {"description", "units", "long_name"})
    {
        const char *pszVal = poCoarseCubeDS->GetMetadataItem(pszKey);
        if (pszVal)
            poDS->SetMetadataItem(pszKey, pszVal);
    }

    // Load Coarse Cube Data into RAM
    poDS->m_nCubeXSize = poCoarseCubeDS->GetRasterXSize();
    poDS->m_nCubeYSize = poCoarseCubeDS->GetRasterYSize();
    poDS->m_nCubeZSize = poCoarseCubeDS->GetRasterCount();

    // Read Z-axis (heightAboveEllipsoid) from metadata
    poDS->m_zVect.reserve(poDS->m_nCubeZSize);
    for (int i = 1; i <= poDS->m_nCubeZSize; ++i)
    {
        GDALRasterBand *pCBand = poCoarseCubeDS->GetRasterBand(i);
        const char *pszHeight = pCBand->GetMetadataItem("Z_VALUE");
        poDS->m_zVect.push_back(pszHeight ? CPLAtof(pszHeight)
                                          : static_cast<double>(i));
    }

    // Read the entire 3D cube into a flat 1D std::vector
    const size_t nTotalCubePixels = static_cast<size_t>(poDS->m_nCubeXSize) *
                                    poDS->m_nCubeYSize * poDS->m_nCubeZSize;
    try
    {
        poDS->m_cubeData.resize(nTotalCubePixels);
    }
    catch (const std::bad_alloc &)
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                 "Cannot allocate %llu values for the metadata cube",
                 static_cast<unsigned long long>(nTotalCubePixels));
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to read coarse cube data via RasterIO.");
        delete poDS;
        GDALClose(poCoarseCubeDS);
        GDALClose(poTargetGridDS);
        return nullptr;
    }

    for (int z = 0; z < poDS->m_nCubeZSize; ++z)
    {
        GDALRasterBand *pCBand = poCoarseCubeDS->GetRasterBand(z + 1);
        float *pDst =
            poDS->m_cubeData.data() +
            (static_cast<size_t>(z) * poDS->m_nCubeXSize * poDS->m_nCubeYSize);
        if (pCBand->RasterIO(GF_Read, 0, 0, poDS->m_nCubeXSize,
                             poDS->m_nCubeYSize, pDst, poDS->m_nCubeXSize,
                             poDS->m_nCubeYSize, GDT_Float32, 0, 0,
                             nullptr) != CE_None)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Failed to read coarse cube data via RasterIO.");
            delete poDS;
            GDALClose(poCoarseCubeDS);
            GDALClose(poTargetGridDS);
            return nullptr;
        }
    }

    // Open the DEM
    CPLDebug("NISAR_DRIVER", "Interpolation: Opening DEM %s", pszDemFile);
    poDS->m_poRawDEM = GDALDataset::FromHandle(
        GDALOpenEx(pszDemFile, GDAL_OF_RASTER | GDAL_OF_READONLY, nullptr,
                   nullptr, nullptr));
    if (!poDS->m_poRawDEM)
    {
        CPLError(CE_Failure, CPLE_OpenFailed, "Failed to open DEM file: %s",
                 pszDemFile);
        delete poDS;
        GDALClose(poCoarseCubeDS);
        GDALClose(poTargetGridDS);
        return nullptr;
    }

    if (bIsLevel1)
    {
        // Radar grid: axes, coordinateX/Y cubes, GCP passthrough, DEM in cube CRS.
        poDS->m_dfNoDataHeight =
            CPLAtof(CSLFetchNameValueDef(papszOO, "DEM_NODATA_HEIGHT", "0"));
        if (!std::isfinite(poDS->m_dfNoDataHeight))
        {
            CPLError(CE_Failure, CPLE_IllegalArg,
                     "NISAR Driver: DEM_NODATA_HEIGHT must be a finite height "
                     "in metres.");
            delete poDS;
            GDALClose(poCoarseCubeDS);
            GDALClose(poTargetGridDS);
            return nullptr;
        }
        const std::string sCubeGroup =
            sCubePath.substr(0, sCubePath.rfind('/'));
        if (!poDS->InitRadarGrid(poCoarseCubeDS, poTargetGridDS, sCubeGroup,
                                 sRefPath))
        {
            delete poDS;
            GDALClose(poCoarseCubeDS);
            GDALClose(poTargetGridDS);
            return nullptr;
        }
        poDS->SetMetadataItem("NISAR_GRID_TYPE", "RADAR");
        poDS->SetMetadataItem("DEM_NODATA_HEIGHT",
                              CPLSPrintf("%g", poDS->m_dfNoDataHeight));
        GDALClose(poTargetGridDS);
        GDALClose(poCoarseCubeDS);
        poDS->SetBand(1, new NisarInterpolatedRasterBand(poDS, 1));
        return poDS;
    }
    GDALClose(poTargetGridDS);  // Done with the target grid reference
    poDS->SetMetadataItem("NISAR_GRID_TYPE", "GEOCODED");

    char *pszTargetWKT = nullptr;
    poDS->m_oSRS.exportToWkt(&pszTargetWKT);

    //  Fetch the user's requested resampling method (Defaulting to CUBICSPLINE)
    const char *pszResampling = CSLFetchNameValueDef(
        poOpenInfo->papszOpenOptions, "DEM_RESAMPLING", "CUBICSPLINE");
    GDALResampleAlg eResampleAlg = GRA_CubicSpline;  // Default

    if (EQUAL(pszResampling, "NEAREST"))
    {
        eResampleAlg = GRA_NearestNeighbour;
    }
    else if (EQUAL(pszResampling, "BILINEAR"))
    {
        eResampleAlg = GRA_Bilinear;
    }
    else if (EQUAL(pszResampling, "CUBIC"))
    {
        eResampleAlg = GRA_Cubic;
    }
    else if (EQUAL(pszResampling, "CUBICSPLINE"))
    {
        eResampleAlg = GRA_CubicSpline;
    }
    else
    {
        CPLDebug(
            "NISAR_DRIVER",
            "Unrecognized DEM_RESAMPLING '%s', falling back to CUBICSPLINE.",
            pszResampling);
    }

    // Lazily warped DEM on the target grid: blocks are resampled on demand, so
    // memory stays bounded even for GSLC-sized grids. Uncovered pixels read as 0.
    GDALWarpOptions *psWarpOptions = GDALCreateWarpOptions();
    psWarpOptions->hSrcDS = poDS->m_poRawDEM;
    psWarpOptions->eResampleAlg = eResampleAlg;
    GDALWarpInitDefaultBandMapping(psWarpOptions, 1);
    psWarpOptions->papszWarpOptions =
        CSLSetNameValue(psWarpOptions->papszWarpOptions, "INIT_DEST", "0");

    char **papszTO = CSLSetNameValue(nullptr, "DST_SRS", pszTargetWKT);
    psWarpOptions->pfnTransformer = GDALGenImgProjTransform;
    psWarpOptions->pTransformerArg =
        GDALCreateGenImgProjTransformer2(poDS->m_poRawDEM, nullptr, papszTO);
    CSLDestroy(papszTO);
    if (psWarpOptions->pTransformerArg == nullptr)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to create DEM -> target grid transformer.");
        GDALDestroyWarpOptions(psWarpOptions);
        CPLFree(pszTargetWKT);
        delete poDS;
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }
    GDALSetGenImgProjTransformerDstGeoTransform(psWarpOptions->pTransformerArg,
                                                poDS->m_adfTargetGeoTransform);

    // The warped VRT takes ownership of the transformer and a reference to the raw DEM.
    poDS->m_poAlignedDEM = GDALDataset::FromHandle(GDALCreateWarpedVRT(
        poDS->m_poRawDEM, poDS->nRasterXSize, poDS->nRasterYSize,
        poDS->m_adfTargetGeoTransform, psWarpOptions));
    if (poDS->m_poAlignedDEM == nullptr)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to create warped DEM on the target grid.");
        GDALDestroyTransformer(psWarpOptions->pTransformerArg);
        GDALDestroyWarpOptions(psWarpOptions);
        CPLFree(pszTargetWKT);
        delete poDS;
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }
    GDALDestroyWarpOptions(psWarpOptions);
    poDS->m_poAlignedDEM->SetProjection(pszTargetWKT);
    CPLFree(pszTargetWKT);

    // Coarse cube georeferencing; the inverse maps target (X,Y) to cube pixels.
    GDALGetGeoTransform(poCoarseCubeDS, poDS->m_adfCubeGeoTransform);
    if (!GDALInvGeoTransform(poDS->m_adfCubeGeoTransform,
                             poDS->m_adfCubeInvGeoTransform))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Cannot invert coarse cube GeoTransform during dataset "
                 "initialization.");
        delete poDS;
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }

    GDALClose(poCoarseCubeDS);  // Done with the coarse cube
    CPLDebug("NISAR_DRIVER",
             "Interpolation: Successfully loaded 3D cube into RAM.");

    // Create custom raster band
    poDS->SetBand(1, new NisarInterpolatedRasterBand(poDS, 1));

    return poDS;
}
