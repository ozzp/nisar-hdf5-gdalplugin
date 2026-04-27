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
    NisarInterpolatedRasterBand(NisarInterpolatedDataset* poDSIn, int nBandIn);
    ~NisarInterpolatedRasterBand() override = default;

    CPLErr IReadBlock(int nBlockXOff, int nBlockYOff, void* pImage) override;
};

#endif // NISAR_INTERPOLATED_RASTERBAND_H
