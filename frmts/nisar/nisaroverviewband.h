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

#ifndef NISAR_OVERVIEW_BAND_H
#define NISAR_OVERVIEW_BAND_H

#include "gdal_priv.h"
#include <algorithm>  // For std::min
#include <cstring>    // For memset
#include <chrono>     // For timing instrumentation

class NisarOverviewBand final : public GDALRasterBand
{
    GDALRasterBand *m_poBaseBand;
    int m_nDecimationFactor;
    CPL_DISALLOW_COPY_ASSIGN(NisarOverviewBand)

  public:
    NisarOverviewBand(GDALRasterBand *poBaseBand, int nDecimationFactor)
        : m_poBaseBand(poBaseBand), m_nDecimationFactor(nDecimationFactor)
    {
        this->poDS = poBaseBand->GetDataset();
        this->nBand = poBaseBand->GetBand();
        this->eDataType = poBaseBand->GetRasterDataType();

        // Calculate the dimensions of this specific zoom level
        this->nRasterXSize = poBaseBand->GetXSize() / m_nDecimationFactor;
        this->nRasterYSize = poBaseBand->GetYSize() / m_nDecimationFactor;

        // Keep the same chunk size as the base band for UI rendering efficiency
        int nBaseBlockXSize, nBaseBlockYSize;
        poBaseBand->GetBlockSize(&nBaseBlockXSize, &nBaseBlockYSize);
        this->nBlockXSize = nBaseBlockXSize;
        this->nBlockYSize = nBaseBlockYSize;

        CPLDebug("NISAR_OVERVIEW",
                 "Created Overview Band | Decimation: %d | Size: %dx%d | "
                 "Block: %dx%d",
                 m_nDecimationFactor, this->nRasterXSize, this->nRasterYSize,
                 this->nBlockXSize, this->nBlockYSize);
    }

    // Intercept the block read and map it down to the base resolution
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff,
                              void *pImage) override
    {
        auto start_time = std::chrono::high_resolution_clock::now();

        CPLDebug("NISAR_OVERVIEW",
                 "--- IReadBlock START | Decimation: %d | Block: [%d, %d] ---",
                 m_nDecimationFactor, nBlockXOff, nBlockYOff);

        int nPixelSize = GDALGetDataTypeSizeBytes(eDataType);

        // 0. Zero out the buffer to prevent edge-block garbage/corruption
        size_t nBytesToZero =
            static_cast<size_t>(nBlockXSize) * nBlockYSize * nPixelSize;
        memset(pImage, 0, nBytesToZero);

        // 1. Calculate the bounding box of this overview block in the BASE resolution
        int nXOff = nBlockXOff * nBlockXSize * m_nDecimationFactor;
        int nYOff = nBlockYOff * nBlockYSize * m_nDecimationFactor;

        int nXSize = std::min(nBlockXSize * m_nDecimationFactor,
                              m_poBaseBand->GetXSize() - nXOff);
        int nYSize = std::min(nBlockYSize * m_nDecimationFactor,
                              m_poBaseBand->GetYSize() - nYOff);

        int nBufXSize = nXSize / m_nDecimationFactor;
        int nBufYSize = nYSize / m_nDecimationFactor;

        CPLDebug("NISAR_OVERVIEW",
                 "Mapping to Base Band | Base Window: XOff=%d, YOff=%d, "
                 "XSize=%d, YSize=%d",
                 nXOff, nYOff, nXSize, nYSize);
        CPLDebug("NISAR_OVERVIEW",
                 "Target Buffer Window | BufXSize=%d, BufYSize=%d", nBufXSize,
                 nBufYSize);

        // do Averaging, NOT Nearest Neighbor!
        GDALRasterIOExtraArg sExtraArg;
        INIT_RASTERIO_EXTRA_ARG(sExtraArg);
        sExtraArg.eResampleAlg = GRIORA_Average;

        // 3. FORCE GDAL to respect the Block Stride for edge-blocks
        GSpacing nPixelSpace = nPixelSize;
        GSpacing nLineSpace =
            nPixelSpace *
            nBlockXSize;  // Crucial: Use Block width, not Buffer width

        CPLDebug("NISAR_OVERVIEW",
                 "Buffer Strides | PixelSpace=%lld, LineSpace=%lld",
                 static_cast<long long>(nPixelSpace),
                 static_cast<long long>(nLineSpace));

        // 4. THE BYPASS GUARD (Prevents Infinite Recursion)
        struct OverviewBypassGuard
        {
            OverviewBypassGuard()
            {
                NisarRasterBand::bDisableOverviewRouting = true;
            }

            ~OverviewBypassGuard()
            {
                NisarRasterBand::bDisableOverviewRouting = false;
            }
        };

        OverviewBypassGuard oGuard;  // Blindfold ON

        // 5. Fire the request. The Base Band is now forced to read raw chunks!
        CPLErr eErr = m_poBaseBand->RasterIO(
            GF_Read, nXOff, nYOff, nXSize, nYSize, pImage, nBufXSize, nBufYSize,
            eDataType, nPixelSpace, nLineSpace, &sExtraArg);

        // Blindfold is automatically taken OFF here as oGuard goes out of scope.

        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double, std::milli> elapsed =
            end_time - start_time;

        CPLDebug("NISAR_OVERVIEW",
                 "--- IReadBlock END | Status: %d | Time: %.2f ms ---", eErr,
                 elapsed.count());

        return eErr;
    }
};

#endif  // NISAR_OVERVIEW_BAND_H
