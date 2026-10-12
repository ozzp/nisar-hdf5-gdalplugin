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
#include <algorithm>
#include <limits>
#include <chrono>

#include "nisarinterpolatedrasterband.h"
#include "nisarinterpolated.h"

// ====================================================================
// NisarInterpolatedRasterBand Implementation
// ====================================================================

NisarInterpolatedRasterBand::NisarInterpolatedRasterBand(
    NisarInterpolatedDataset *poDSIn, int nBandIn)
{
    this->poDS = poDSIn;
    this->nBand = nBandIn;
    this->eDataType = GDT_Float32;

    // Set the "Goldilocks" block size for optimal S3 and cache performance
    this->nBlockXSize = 512;
    this->nBlockYSize = 512;
}

CPLErr NisarInterpolatedRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff,
                                               void *pImage)
{
    // INSTRUMENTATION START
    auto t_start = std::chrono::high_resolution_clock::now();

    NisarInterpolatedDataset *poGDS =
        static_cast<NisarInterpolatedDataset *>(poDS);
    float *pafOutput = static_cast<float *>(pImage);

    if (poGDS->m_bRadarGrid)
    {
        return ReadRadarBlock(nBlockXOff, nBlockYOff, pafOutput);
    }

    int nXOff = nBlockXOff * nBlockXSize;
    int nYOff = nBlockYOff * nBlockYSize;
    int nReqXSize = std::min(nBlockXSize, nRasterXSize - nXOff);
    int nReqYSize = std::min(nBlockYSize, nRasterYSize - nYOff);

    // Initialize output buffer with NoData
    std::fill_n(pafOutput, nBlockXSize * nBlockYSize,
                std::numeric_limits<float>::quiet_NaN());

    // Read DEM heights for this block from the Warped VRT
    std::vector<float> demHeights(nReqXSize * nReqYSize,
                                  std::numeric_limits<float>::quiet_NaN());

    if (poGDS->m_poAlignedDEM)
    {
        // Automatically pushes here, and guarantees a pop when the block ends
        CPLErrorHandlerPusher oQuietError(CPLQuietErrorHandler);

        if (poGDS->m_poAlignedDEM->RasterIO(
                GF_Read, nXOff, nYOff, nReqXSize, nReqYSize, demHeights.data(),
                nReqXSize, nReqYSize, GDT_Float32, 1, nullptr, 0, 0, 0,
                nullptr) != CE_None)
        {
            // Emit a clear, context-aware error message since the default was silenced
            CPLError(CE_Failure, CPLE_AppDefined,
                     "NISAR Interpolation: Failed to read DEM data at offset "
                     "X:%d, Y:%d. "
                     "Cannot proceed with 3D interpolation for this block.",
                     nXOff, nYOff);

            // Halt execution of this block read and notify GDAL
            return CE_Failure;
        }
    }

    // Use the pre-calculated inverse GeoTransform from the Dataset class!
    double *adfCubeInvGeoTransform = poGDS->m_adfCubeInvGeoTransform;

    int nCubeX = poGDS->m_nCubeXSize;
    int nCubeY = poGDS->m_nCubeYSize;
    int nCubeZ = poGDS->m_nCubeZSize;

    // Helper lambda to fetch from 1D flat array safely
    auto get_cube_val = [&](int z, int y, int x) -> float
    { return poGDS->m_cubeData[z * (nCubeX * nCubeY) + y * nCubeX + x]; };

    // Pre-extract Target GeoTransform elements for faster inner-loop math
    double dfTargetT0 = poGDS->m_adfTargetGeoTransform[0];
    double dfTargetT1 = poGDS->m_adfTargetGeoTransform[1];  // X-pixel width
    double dfTargetT2 = poGDS->m_adfTargetGeoTransform[2];
    double dfTargetT3 = poGDS->m_adfTargetGeoTransform[3];
    double dfTargetT4 =
        poGDS->m_adfTargetGeoTransform[4];  // Y-pixel width/rotation
    double dfTargetT5 = poGDS->m_adfTargetGeoTransform[5];

    // Loop over every pixel in the requested block
    for (int y = 0; y < nReqYSize; ++y)
    {
        // HOISTED MATH: Calculate the base X and Y for the start of this row.
        // This avoids recalculating the Y-dependent part of the transform 512 times per row.
        double dfBaseGeoX = dfTargetT0 + (nXOff + 0.5) * dfTargetT1 +
                            (nYOff + y + 0.5) * dfTargetT2;
        double dfBaseGeoY = dfTargetT3 + (nXOff + 0.5) * dfTargetT4 +
                            (nYOff + y + 0.5) * dfTargetT5;

        for (int x = 0; x < nReqXSize; ++x)
        {
            float targetZ = demHeights[y * nReqXSize + x];
            if (std::isnan(targetZ))
                continue;  // Skip NoData pixels (e.g., ocean)

            // Calculate real-world X and Y for this target pixel
            double dfGeoX = dfBaseGeoX + (x * dfTargetT1);
            double dfGeoY = dfBaseGeoY + (x * dfTargetT4);

            // Convert World X,Y into the Coarse Cube's fractional pixel coordinates
            double dfCubePixelX = adfCubeInvGeoTransform[0] +
                                  dfGeoX * adfCubeInvGeoTransform[1] +
                                  dfGeoY * adfCubeInvGeoTransform[2];

            double dfCubePixelY = adfCubeInvGeoTransform[3] +
                                  dfGeoX * adfCubeInvGeoTransform[4] +
                                  dfGeoY * adfCubeInvGeoTransform[5];

            // Find X and Y indices and weights (with edge clamping)
            int x0 = static_cast<int>(std::floor(dfCubePixelX - 0.5));
            int x1 = x0 + 1;
            double wx = (dfCubePixelX - 0.5) - x0;

            if (x0 < 0)
            {
                x0 = 0;
                x1 = 0;
                wx = 0.0;
            }
            else if (x1 >= nCubeX)
            {
                x1 = nCubeX - 1;
                x0 = x1 - 1;
                wx = 1.0;
            }
            if (x0 < 0)
                x0 = 0;  // Safety for 1D edge case

            int y0 = static_cast<int>(std::floor(dfCubePixelY - 0.5));
            int y1 = y0 + 1;
            double wy = (dfCubePixelY - 0.5) - y0;

            if (y0 < 0)
            {
                y0 = 0;
                y1 = 0;
                wy = 0.0;
            }
            else if (y1 >= nCubeY)
            {
                y1 = nCubeY - 1;
                y0 = y1 - 1;
                wy = 1.0;
            }
            if (y0 < 0)
                y0 = 0;

            // Find Z indices and weights
            int z0 = 0, z1 = 0;
            double wz = 0.0;

            if (nCubeZ > 1)
            {
                bool bAscending =
                    (poGDS->m_zVect[nCubeZ - 1] > poGDS->m_zVect[0]);

                if (bAscending)
                {
                    if (targetZ <= poGDS->m_zVect[0])
                    {
                        z0 = 0;
                        z1 = 0;
                        wz = 0.0;
                    }
                    else if (targetZ >= poGDS->m_zVect[nCubeZ - 1])
                    {
                        z0 = nCubeZ - 1;
                        z1 = nCubeZ - 1;
                        wz = 1.0;
                    }
                    else
                    {
                        for (int i = 0; i < nCubeZ - 1; ++i)
                        {
                            if (targetZ >= poGDS->m_zVect[i] &&
                                targetZ <= poGDS->m_zVect[i + 1])
                            {
                                z0 = i;
                                z1 = i + 1;
                                wz =
                                    (targetZ - poGDS->m_zVect[i]) /
                                    (poGDS->m_zVect[i + 1] - poGDS->m_zVect[i]);
                                break;
                            }
                        }
                    }
                }
                else
                {  // Descending
                    if (targetZ >= poGDS->m_zVect[0])
                    {
                        z0 = 0;
                        z1 = 0;
                        wz = 0.0;
                    }
                    else if (targetZ <= poGDS->m_zVect[nCubeZ - 1])
                    {
                        z0 = nCubeZ - 1;
                        z1 = nCubeZ - 1;
                        wz = 1.0;
                    }
                    else
                    {
                        for (int i = 0; i < nCubeZ - 1; ++i)
                        {
                            if (targetZ <= poGDS->m_zVect[i] &&
                                targetZ >= poGDS->m_zVect[i + 1])
                            {
                                z0 = i;
                                z1 = i + 1;
                                wz =
                                    (poGDS->m_zVect[i] - targetZ) /
                                    (poGDS->m_zVect[i] - poGDS->m_zVect[i + 1]);
                                break;
                            }
                        }
                    }
                }
            }

            // Trilinear Interpolation (The 8 Corners)
            float c000 = get_cube_val(z0, y0, x0);
            float c001 = get_cube_val(z0, y0, x1);
            float c010 = get_cube_val(z0, y1, x0);
            float c011 = get_cube_val(z0, y1, x1);

            float c100 = get_cube_val(z1, y0, x0);
            float c101 = get_cube_val(z1, y0, x1);
            float c110 = get_cube_val(z1, y1, x0);
            float c111 = get_cube_val(z1, y1, x1);

            // Interpolate along X (bottom Z plane)
            double c00 = c000 * (1.0 - wx) + c001 * wx;
            double c01 = c010 * (1.0 - wx) + c011 * wx;
            // Interpolate along X (top Z plane)
            double c10 = c100 * (1.0 - wx) + c101 * wx;
            double c11 = c110 * (1.0 - wx) + c111 * wx;

            // Interpolate along Y
            double c0 = c00 * (1.0 - wy) + c01 * wy;
            double c1 = c10 * (1.0 - wy) + c11 * wy;

            // Interpolate along Z
            double final_val = c0 * (1.0 - wz) + c1 * wz;

            // Write the interpolated value to the output buffer
            pafOutput[y * nBlockXSize + x] = static_cast<float>(final_val);
        }
    }
    // INSTRUMENTATION END
    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> t_diff = t_end - t_start;

    CPLDebug("NISAR_INTERP_PERF",
             "Interpolation Block(X:%d, Y:%d) | Size: %dx%d | Time: %.3f ms",
             nBlockXOff, nBlockYOff, nReqXSize, nReqYSize, t_diff.count());

    return CE_None;
}

// DEM window (in cube CRS) covering a block's ground footprint, cached in RAM.
namespace
{
struct DEMWindow
{
    int nX0 = 0, nY0 = 0, nXSize = 0, nYSize = 0;
    std::vector<float> data{};
    std::vector<GByte> mask{};  // empty when the DEM has no mask band to honour
};
}  // namespace

// Level-1: each output pixel is (slantRange, zeroDopplerTime); the terrain height is
// solved by fixed-point iteration h -> DEM(coordinateX(h), coordinateY(h)).
CPLErr NisarInterpolatedRasterBand::ReadRadarBlock(int nBlockXOff,
                                                   int nBlockYOff,
                                                   float *pafOutput)
{
    auto t_start = std::chrono::high_resolution_clock::now();
    NisarInterpolatedDataset *poGDS =
        static_cast<NisarInterpolatedDataset *>(poDS);

    const int nXOff = nBlockXOff * nBlockXSize;
    const int nYOff = nBlockYOff * nBlockYSize;
    const int nReqXSize = std::min(nBlockXSize, nRasterXSize - nXOff);
    const int nReqYSize = std::min(nBlockYSize, nRasterYSize - nYOff);
    std::fill_n(pafOutput, nBlockXSize * nBlockYSize,
                std::numeric_limits<float>::quiet_NaN());

    // 1. Cube node coordinates of every column (range) and line (azimuth time) in the block.
    std::vector<double> adfNodeX(nReqXSize), adfNodeY(nReqYSize);
    for (int x = 0; x < nReqXSize; ++x)
        adfNodeX[x] = NisarInterpolatedDataset::AxisToNode(
            poGDS->m_cubeRange, poGDS->m_swathRange[nXOff + x]);
    for (int y = 0; y < nReqYSize; ++y)
        adfNodeY[y] = NisarInterpolatedDataset::AxisToNode(
            poGDS->m_cubeTime, poGDS->m_swathTime[nYOff + y]);

    const int nCubeX = poGDS->m_nCubeXSize, nCubeY = poGDS->m_nCubeYSize;
    const int nx0 = std::max(0, static_cast<int>(std::floor(*std::min_element(
                                    adfNodeX.begin(), adfNodeX.end()))));
    const int nx1 =
        std::min(nCubeX - 1, static_cast<int>(std::ceil(*std::max_element(
                                 adfNodeX.begin(), adfNodeX.end()))));
    const int ny0 = std::max(0, static_cast<int>(std::floor(*std::min_element(
                                    adfNodeY.begin(), adfNodeY.end()))));
    const int ny1 =
        std::min(nCubeY - 1, static_cast<int>(std::ceil(*std::max_element(
                                 adfNodeY.begin(), adfNodeY.end()))));

    // Ground bbox of the block's cube nodes over the height range [hlo, hhi].
    auto groundBBox = [&](double hlo, double hhi, double &minX, double &maxX,
                          double &minY, double &maxY)
    {
        int zlo, zhi, zdummy;
        double wdummy;
        poGDS->CubeZ(hlo, zlo, zdummy, wdummy);
        poGDS->CubeZ(hhi, zdummy, zhi, wdummy);
        minX = minY = std::numeric_limits<double>::max();
        maxX = maxY = -std::numeric_limits<double>::max();
        const size_t nPlane = static_cast<size_t>(nCubeX) * nCubeY;
        for (int z = zlo; z <= zhi; ++z)
            for (int y = ny0; y <= ny1; ++y)
                for (int x = nx0; x <= nx1; ++x)
                {
                    const size_t i =
                        z * nPlane + static_cast<size_t>(y) * nCubeX + x;
                    minX = std::min(minX, poGDS->m_coordX[i]);
                    maxX = std::max(maxX, poGDS->m_coordX[i]);
                    minY = std::min(minY, poGDS->m_coordY[i]);
                    maxY = std::max(maxY, poGDS->m_coordY[i]);
                }
    };

    // 2. Read the DEM window; widen the assumed height range until it contains the DEM values seen.
    const double *inv = poGDS->m_adfDEMInvGeoTransform;
    const int nDEMX = poGDS->m_poAlignedDEM->GetRasterXSize();
    const int nDEMY = poGDS->m_poAlignedDEM->GetRasterYSize();
    const double dfNoDataHeight = poGDS->m_dfNoDataHeight;
    DEMWindow win;
    auto isNoData = [&](size_t i)
    {
        const float v = win.data[i];
        return std::isnan(v) ||
               (poGDS->m_bDEMHasNoData &&
                v == static_cast<float>(poGDS->m_dfDEMNoData)) ||
               (!win.mask.empty() && win.mask[i] == 0);
    };
    double hlo = dfNoDataHeight, hhi = dfNoDataHeight;
    for (int nPass = 0; nPass < 3; ++nPass)
    {
        double minX, maxX, minY, maxY;
        groundBBox(hlo, hhi, minX, maxX, minY, maxY);
        double px0 = std::numeric_limits<double>::max(), py0 = px0, px1 = -px0,
               py1 = -px0;
        for (double X : {minX, maxX})
            for (double Y : {minY, maxY})
            {
                const double px = inv[0] + X * inv[1] + Y * inv[2];
                const double py = inv[3] + X * inv[4] + Y * inv[5];
                px0 = std::min(px0, px);
                px1 = std::max(px1, px);
                py0 = std::min(py0, py);
                py1 = std::max(py1, py);
            }
        win.nX0 = std::max(0, static_cast<int>(std::floor(px0)) - 2);
        win.nY0 = std::max(0, static_cast<int>(std::floor(py0)) - 2);
        const int nXEnd = std::min(nDEMX, static_cast<int>(std::ceil(px1)) + 2);
        const int nYEnd = std::min(nDEMY, static_cast<int>(std::ceil(py1)) + 2);
        win.nXSize = std::max(0, nXEnd - win.nX0);
        win.nYSize = std::max(0, nYEnd - win.nY0);
        if (win.nXSize == 0 || win.nYSize == 0)
            break;  // block entirely off the DEM
        if (static_cast<size_t>(win.nXSize) * win.nYSize >
            (static_cast<size_t>(1) << 26))
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "NISAR Interpolation: DEM window %dx%d for block (%d,%d) "
                     "is too large; "
                     "use a coarser DEM.",
                     win.nXSize, win.nYSize, nBlockXOff, nBlockYOff);
            return CE_Failure;
        }
        const size_t nWin = static_cast<size_t>(win.nXSize) * win.nYSize;
        win.data.assign(nWin, 0.0f);
        GDALRasterBand *poDEMBand = poGDS->m_poAlignedDEM->GetRasterBand(1);
        if (poDEMBand->RasterIO(GF_Read, win.nX0, win.nY0, win.nXSize,
                                win.nYSize, win.data.data(), win.nXSize,
                                win.nYSize, GDT_Float32, 0, 0,
                                nullptr) != CE_None)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "NISAR Interpolation: Failed to read DEM window for block "
                     "(%d,%d).",
                     nBlockXOff, nBlockYOff);
            return CE_Failure;
        }
        if (poGDS->m_poDEMMaskBand != nullptr)
        {
            win.mask.assign(nWin, 0);
            if (poGDS->m_poDEMMaskBand->RasterIO(
                    GF_Read, win.nX0, win.nY0, win.nXSize, win.nYSize,
                    win.mask.data(), win.nXSize, win.nYSize, GDT_Byte, 0, 0,
                    nullptr) != CE_None)
            {
                CPLError(CE_Failure, CPLE_AppDefined,
                         "NISAR Interpolation: Failed to read DEM mask window "
                         "for block (%d,%d).",
                         nBlockXOff, nBlockYOff);
                return CE_Failure;
            }
        }
        double dfMin = dfNoDataHeight, dfMax = dfNoDataHeight;
        for (size_t i = 0; i < nWin; ++i)
        {
            if (isNoData(i))
                continue;
            dfMin = std::min(dfMin, static_cast<double>(win.data[i]));
            dfMax = std::max(dfMax, static_cast<double>(win.data[i]));
        }
        if (dfMin >= hlo && dfMax <= hhi)
            break;
        hlo = std::min(hlo, dfMin);
        hhi = std::max(hhi, dfMax);
    }

    // Bilinear DEM sample; outside the window or on nodata returns DEM_NODATA_HEIGHT.
    auto sampleDEM = [&](double X, double Y) -> double
    {
        if (win.data.empty())
            return dfNoDataHeight;
        const double u = inv[0] + X * inv[1] + Y * inv[2] - 0.5 - win.nX0;
        const double v = inv[3] + X * inv[4] + Y * inv[5] - 0.5 - win.nY0;
        if (!(u >= 0.0 && v >= 0.0 && u <= win.nXSize - 1 &&
              v <= win.nYSize - 1))
            return dfNoDataHeight;
        const int i0 = static_cast<int>(u), j0 = static_cast<int>(v);
        const int i1 = std::min(i0 + 1, win.nXSize - 1),
                  j1 = std::min(j0 + 1, win.nYSize - 1);
        const size_t ia = static_cast<size_t>(j0) * win.nXSize + i0,
                     ib = static_cast<size_t>(j0) * win.nXSize + i1;
        const size_t ic = static_cast<size_t>(j1) * win.nXSize + i0,
                     id = static_cast<size_t>(j1) * win.nXSize + i1;
        if (isNoData(ia) || isNoData(ib) || isNoData(ic) || isNoData(id))
            return dfNoDataHeight;
        const double fu = u - i0, fv = v - j0;
        const float a = win.data[ia], b = win.data[ib], c = win.data[ic],
                    d = win.data[id];
        return (a * (1 - fu) + b * fu) * (1 - fv) +
               (c * (1 - fu) + d * fu) * fv;
    };

    // 3. Per pixel: solve h = DEM(X(h), Y(h)), then interpolate the quantity at (h, t, r).
    const int nMaxIter = poGDS->m_nMaxIter;
    const double dfTol = poGDS->m_dfHeightTol;
    double hSeed = dfNoDataHeight;
    for (int y = 0; y < nReqYSize; ++y)
    {
        for (int x = 0; x < nReqXSize; ++x)
        {
            const NisarCubeXY xy = poGDS->CubeXY(adfNodeX[x], adfNodeY[y]);
            int z0, z1;
            double wz;
            double h = hSeed;
            for (int it = 0; it < nMaxIter; ++it)
            {
                poGDS->CubeZ(h, z0, z1, wz);
                const double X =
                    poGDS->Trilinear(poGDS->m_coordX, xy, z0, z1, wz);
                const double Y =
                    poGDS->Trilinear(poGDS->m_coordY, xy, z0, z1, wz);
                const double hNew = sampleDEM(X, Y);
                const bool bDone = std::fabs(hNew - h) < dfTol;
                h = hNew;
                if (bDone)
                    break;
            }
            hSeed = h;
            poGDS->CubeZ(h, z0, z1, wz);
            pafOutput[y * nBlockXSize + x] = static_cast<float>(
                poGDS->Trilinear(poGDS->m_cubeData, xy, z0, z1, wz));
        }
    }

    std::chrono::duration<double, std::milli> t_diff =
        std::chrono::high_resolution_clock::now() - t_start;
    CPLDebug("NISAR_INTERP_PERF",
             "Radar Block(X:%d, Y:%d) | Size: %dx%d | DEM window %dx%d | Time: "
             "%.3f ms",
             nBlockXOff, nBlockYOff, nReqXSize, nReqYSize, win.nXSize,
             win.nYSize, t_diff.count());
    return CE_None;
}
