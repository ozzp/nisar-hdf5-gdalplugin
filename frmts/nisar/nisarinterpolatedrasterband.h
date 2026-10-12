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

#ifndef NISAR_INTERPOLATED_RASTERBAND_H
#define NISAR_INTERPOLATED_RASTERBAND_H

#include "gdal_priv.h"

// Forward declare the dataset so the band knows it exists
class NisarInterpolatedDataset;

// ====================================================================
// NisarInterpolatedRasterBand
// Calculates the trilinear interpolation block-by-block.
// ====================================================================
class NisarInterpolatedRasterBand final : public GDALRasterBand
{
    friend class NisarInterpolatedDataset;

  public:
    NisarInterpolatedRasterBand(NisarInterpolatedDataset *poDSIn, int nBandIn);
    ~NisarInterpolatedRasterBand() override = default;

    CPLErr IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage) override;

  private:
    CPLErr ReadRadarBlock(int nBlockXOff, int nBlockYOff, float *pafOutput);
};

#endif  // NISAR_INTERPOLATED_RASTERBAND_H
