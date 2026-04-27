#include "nisarinterpolatedrasterband.h"
#include "nisarinterpolated.h"

#include <cmath>
#include <cmath>
#include <algorithm>
#include <limits>

// ====================================================================
// NisarInterpolatedRasterBand Implementation
// ====================================================================

NisarInterpolatedRasterBand::NisarInterpolatedRasterBand(NisarInterpolatedDataset* poDSIn, int nBandIn)
{
    this->poDS = poDSIn;
    this->nBand = nBandIn;
    this->eDataType = GDT_Float32;
    
    // Set the "Goldilocks" block size for optimal S3 and cache performance
    this->nBlockXSize = 512;
    this->nBlockYSize = 512;
}

CPLErr NisarInterpolatedRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff, void* pImage)
{
    NisarInterpolatedDataset* poGDS = static_cast<NisarInterpolatedDataset*>(poDS);
    float* pafOutput = static_cast<float*>(pImage);

    int nXOff = nBlockXOff * nBlockXSize;
    int nYOff = nBlockYOff * nBlockYSize;
    int nReqXSize = std::min(nBlockXSize, nRasterXSize - nXOff);
    int nReqYSize = std::min(nBlockYSize, nRasterYSize - nYOff);

    // Initialize output buffer with NoData
    std::fill_n(pafOutput, nBlockXSize * nBlockYSize, std::numeric_limits<float>::quiet_NaN());

    // Read DEM heights for this block from the Warped VRT
    std::vector<float> demHeights(nReqXSize * nReqYSize, std::numeric_limits<float>::quiet_NaN());
    
    if (poGDS->m_poAlignedDEM) {
        // Automatically pushes here, and guarantees a pop when the block ends
        CPLErrorHandlerPusher oQuietError(CPLQuietErrorHandler);

        if (poGDS->m_poAlignedDEM->RasterIO(GF_Read,
                                       nXOff, nYOff, nReqXSize, nReqYSize,
                                       demHeights.data(), nReqXSize, nReqYSize,
                                       GDT_Float32, 1, nullptr, 0, 0, 0, nullptr) != CE_None)
        {
            // Emit a clear, context-aware error message since the default was silenced
            CPLError(CE_Failure, CPLE_AppDefined,
                     "NISAR Interpolation: Failed to read DEM data at offset X:%d, Y:%d. "
                     "Cannot proceed with 3D interpolation for this block.",
                     nXOff, nYOff);
                     
            // Halt execution of this block read and notify GDAL
            return CE_Failure;
        }
    }

    // TODO: switch to using precalculated transform.
    // Use the pre-calculated inverse GeoTransform from the Dataset class!
    //double* adfCubeInvGeoTransform = poGDS->m_adfCubeInvGeoTransform;

    // Pre-calculate the inverse of the coarse cube's GeoTransform
    double adfCubeInvGeoTransform[6];
    if (!GDALInvGeoTransform(poGDS->m_adfCubeGeoTransform, adfCubeInvGeoTransform)) {
        CPLError(CE_Failure, CPLE_AppDefined, "Cannot invert coarse cube GeoTransform.");
        return CE_Failure;
    }

    // Initialize output buffer with NoData
    for (int i = 0; i < nBlockXSize * nBlockYSize; ++i) {
        pafOutput[i] = std::numeric_limits<float>::quiet_NaN();
    }

    int nCubeX = poGDS->m_nCubeXSize;
    int nCubeY = poGDS->m_nCubeYSize;
    int nCubeZ = poGDS->m_nCubeZSize;

    // Helper lambda to fetch from 1D flat array safely
    auto get_cube_val = [&](int z, int y, int x) -> float {
        return poGDS->m_cubeData[z * (nCubeX * nCubeY) + y * nCubeX + x];
    };

    // Loop over every pixel in the requested block
    for (int y = 0; y < nReqYSize; ++y) 
    {
        for (int x = 0; x < nReqXSize; ++x) 
        {
            float targetZ = demHeights[y * nReqXSize + x];
            if (std::isnan(targetZ)) continue; // Skip NoData pixels (e.g., ocean)
            
            // Calculate real-world X and Y for this target pixel
            double dfGeoX = poGDS->m_adfTargetGeoTransform[0] + 
                            (nXOff + x + 0.5) * poGDS->m_adfTargetGeoTransform[1] + 
                            (nYOff + y + 0.5) * poGDS->m_adfTargetGeoTransform[2];
                            
            double dfGeoY = poGDS->m_adfTargetGeoTransform[3] + 
                            (nXOff + x + 0.5) * poGDS->m_adfTargetGeoTransform[4] + 
                            (nYOff + y + 0.5) * poGDS->m_adfTargetGeoTransform[5];

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
            
            if (x0 < 0) { x0 = 0; x1 = 0; wx = 0.0; }
            else if (x1 >= nCubeX) { x1 = nCubeX - 1; x0 = x1 - 1; wx = 1.0; }
            if (x0 < 0) x0 = 0; // Safety for 1D edge case

            int y0 = static_cast<int>(std::floor(dfCubePixelY - 0.5));
            int y1 = y0 + 1;
            double wy = (dfCubePixelY - 0.5) - y0;
            
            if (y0 < 0) { y0 = 0; y1 = 0; wy = 0.0; }
            else if (y1 >= nCubeY) { y1 = nCubeY - 1; y0 = y1 - 1; wy = 1.0; }
            if (y0 < 0) y0 = 0;

            // Find Z indices and weights
            int z0 = 0, z1 = 0;
            double wz = 0.0;
            
            if (nCubeZ > 1) {
                bool bAscending = (poGDS->m_zVect[nCubeZ - 1] > poGDS->m_zVect[0]);
                
                if (bAscending) {
                    if (targetZ <= poGDS->m_zVect[0]) { z0 = 0; z1 = 0; wz = 0.0; }
                    else if (targetZ >= poGDS->m_zVect[nCubeZ - 1]) { z0 = nCubeZ - 1; z1 = nCubeZ - 1; wz = 1.0; }
                    else {
                        for (int i = 0; i < nCubeZ - 1; ++i) {
                            if (targetZ >= poGDS->m_zVect[i] && targetZ <= poGDS->m_zVect[i + 1]) {
                                z0 = i; z1 = i + 1;
                                wz = (targetZ - poGDS->m_zVect[i]) / (poGDS->m_zVect[i + 1] - poGDS->m_zVect[i]);
                                break;
                            }
                        }
                    }
                } else { // Descending
                    if (targetZ >= poGDS->m_zVect[0]) { z0 = 0; z1 = 0; wz = 0.0; }
                    else if (targetZ <= poGDS->m_zVect[nCubeZ - 1]) { z0 = nCubeZ - 1; z1 = nCubeZ - 1; wz = 1.0; }
                    else {
                        for (int i = 0; i < nCubeZ - 1; ++i) {
                            if (targetZ <= poGDS->m_zVect[i] && targetZ >= poGDS->m_zVect[i + 1]) {
                                z0 = i; z1 = i + 1;
                                wz = (poGDS->m_zVect[i] - targetZ) / (poGDS->m_zVect[i] - poGDS->m_zVect[i + 1]);
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

    return CE_None;
}
