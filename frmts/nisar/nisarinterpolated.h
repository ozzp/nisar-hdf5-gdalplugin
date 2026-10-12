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

#ifndef NISAR_INTERPOLATED_H
#define NISAR_INTERPOLATED_H

#include <vector>

#include "gdal_priv.h"
#include "gdalwarper.h"
#include "ogr_spatialref.h"
#include "gdal_version.h"

// Compatibility shim for GDAL < 3.12 GeoTransform signature
#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
#ifndef USE_LEGACY_GEOTRANSFORM
#define USE_LEGACY_GEOTRANSFORM 1
#endif
#endif

class NisarInterpolatedRasterBand;
class NisarDataset;

// Bilinear footprint of one target pixel on the coarse cube's (y, x) node grid.
struct NisarCubeXY
{
    int x0 = 0, x1 = 0, y0 = 0, y1 = 0;
    double wx = 0.0, wy = 0.0;
};

// ====================================================================
// NisarInterpolatedDataset
// Handles 3D data cube interpolation using a provided DEM.
// L2/L3: target pixel -> map (X,Y) -> DEM height -> trilinear cube lookup.
// L1:    target pixel -> (slantRange, zeroDopplerTime) -> height solved by
//        fixed-point iteration through coordinateX/Y and the DEM.
// ====================================================================
class NisarInterpolatedDataset final : public GDALDataset
{
    friend class NisarInterpolatedRasterBand;
    CPL_DISALLOW_COPY_ASSIGN(NisarInterpolatedDataset)

  private:
    GDALDataset *m_poRawDEM = nullptr;
    GDALDataset *m_poAlignedDEM =
        nullptr;  // L2: warped VRT on target grid; L1: DEM in cube CRS
    GDALDataset *m_poDEMValidityView =
        nullptr;  // L1 warp source: VRT [height, validity]

    // Coarse 3D Cube Data
    std::vector<float> m_cubeData{};
    std::vector<double> m_zVect{};  // heightAboveEllipsoid 1D array

    int m_nCubeXSize = 0;
    int m_nCubeYSize = 0;
    int m_nCubeZSize = 0;
    double m_adfCubeGeoTransform[6] = {0, 1, 0, 0, 0, 1};

    // Target High-Res Grid Info
    double m_adfTargetGeoTransform[6] = {0, 1, 0, 0, 0, 1};
    OGRSpatialReference m_oSRS{};

    // Add the cached inverse transform
    double m_adfCubeInvGeoTransform[6] = {0, 1, 0, 0, 0, 1};

    // ---- Level-1 (radar coordinates) ----
    bool m_bRadarGrid = false;
    std::vector<double> m_swathRange{};  // slantRange per output column
    std::vector<double>
        m_swathTime{};  // zeroDopplerTime per output line (cube epoch)
    std::vector<double> m_cubeRange{};  // geolocationGrid/slantRange
    std::vector<double> m_cubeTime{};   // geolocationGrid/zeroDopplerTime
    std::vector<double> m_coordX{};     // geolocationGrid/coordinateX (z, y, x)
    std::vector<double> m_coordY{};     // geolocationGrid/coordinateY (z, y, x)
    double m_adfDEMGeoTransform[6] = {0, 1, 0, 0, 0, 1};
    double m_adfDEMInvGeoTransform[6] = {0, 1, 0, 0, 0, 1};
    bool m_bDEMHasNoData = false;
    GDALRasterBand *m_poDEMMaskBand =
        nullptr;  // validity band of m_poAlignedDEM (0 = invalid), if any
    double m_dfDEMNoData = 0.0;
    double m_dfNoDataHeight = 0.0;  // DEM_NODATA_HEIGHT
    int m_nMaxIter = 10;
    double m_dfHeightTol = 0.1;  // metres

    int m_nGCPCount = 0;
    GDAL_GCP *m_pasGCPs = nullptr;
    OGRSpatialReference m_oGCPSRS{};

    bool InitRadarGrid(NisarDataset *poCube, NisarDataset *poSwath,
                       const std::string &sCubeGroup,
                       const std::string &sRefPath);
    bool DEMWindowForFootprint(const OGRSpatialReference *poDEMSRS, int &nX,
                               int &nY, int &nW, int &nH) const;

    // Interpolation helpers shared by the raster band
    NisarCubeXY CubeXY(double dfNodeX, double dfNodeY) const;
    void CubeZ(double dfHeight, int &z0, int &z1, double &wz) const;
    template <typename T>
    double Trilinear(const std::vector<T> &cube, const NisarCubeXY &xy, int z0,
                     int z1, double wz) const;
    static double AxisToNode(const std::vector<double> &axis, double dfValue);

  public:
    NisarInterpolatedDataset();
    ~NisarInterpolatedDataset() override;

    static GDALDataset *Open(GDALOpenInfo *poOpenInfo);

    const OGRSpatialReference *GetSpatialRef() const override;
    int GetGCPCount() override;
    const GDAL_GCP *GetGCPs() override;
    const OGRSpatialReference *GetGCPSpatialRef() const override;

#ifdef USE_LEGACY_GEOTRANSFORM
    CPLErr GetGeoTransform(double *padfTransform) override;
#else
    CPLErr GetGeoTransform(GDALGeoTransform &gt) const override;
#endif
};

template <typename T>
double NisarInterpolatedDataset::Trilinear(const std::vector<T> &cube,
                                           const NisarCubeXY &xy, int z0,
                                           int z1, double wz) const
{
    const size_t nPlane = static_cast<size_t>(m_nCubeXSize) * m_nCubeYSize;
    auto at = [&](int z, int y, int x) -> double
    { return cube[z * nPlane + static_cast<size_t>(y) * m_nCubeXSize + x]; };
    const double c00 =
        at(z0, xy.y0, xy.x0) * (1.0 - xy.wx) + at(z0, xy.y0, xy.x1) * xy.wx;
    const double c01 =
        at(z0, xy.y1, xy.x0) * (1.0 - xy.wx) + at(z0, xy.y1, xy.x1) * xy.wx;
    const double c10 =
        at(z1, xy.y0, xy.x0) * (1.0 - xy.wx) + at(z1, xy.y0, xy.x1) * xy.wx;
    const double c11 =
        at(z1, xy.y1, xy.x0) * (1.0 - xy.wx) + at(z1, xy.y1, xy.x1) * xy.wx;
    const double c0 = c00 * (1.0 - xy.wy) + c01 * xy.wy;
    const double c1 = c10 * (1.0 - xy.wy) + c11 * xy.wy;
    return c0 * (1.0 - wz) + c1 * wz;
}
#endif  // NISAR_INTERPOLATED_H
