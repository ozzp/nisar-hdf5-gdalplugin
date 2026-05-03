#ifndef NISAR_INTERPOLATED_H
#define NISAR_INTERPOLATED_H

#include <vector>
#include "gdal_priv.h"
#include "gdalwarper.h"
#include "ogr_spatialref.h"
#include "gdal_version.h"

// Compatibility shim for GDAL < 3.12 GeoTransform signature
#if GDAL_VERSION_MAJOR < 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
    #ifndef USE_LEGACY_GEOTRANSFORM
    #define USE_LEGACY_GEOTRANSFORM 1
    #endif
#endif

class NisarInterpolatedRasterBand;

// ====================================================================
// NisarInterpolatedDataset
// Handles 3D data cube interpolation using a provided DEM.
// ====================================================================
class NisarInterpolatedDataset final : public GDALDataset
{
    friend class NisarInterpolatedRasterBand;

private:
    GDALDataset* m_poRawDEM = nullptr;
    GDALDataset* m_poAlignedDEM = nullptr; // The Warped VRT

    // Coarse 3D Cube Data
    std::vector<float> m_cubeData;
    std::vector<double> m_zVect; // heightAboveEllipsoid 1D array

    int m_nCubeXSize = 0;
    int m_nCubeYSize = 0;
    int m_nCubeZSize = 0;
    double m_adfCubeGeoTransform[6] = {0, 1, 0, 0, 0, 1};

    // Target High-Res Grid Info
    double m_adfTargetGeoTransform[6] = {0, 1, 0, 0, 0, 1};
    OGRSpatialReference m_oSRS;

    // Add the cached inverse transform
    double m_adfCubeInvGeoTransform[6] = {0, 1, 0, 0, 0, 1};

public:
    NisarInterpolatedDataset();
    ~NisarInterpolatedDataset() override;

    static GDALDataset* Open(GDALOpenInfo* poOpenInfo);

    const OGRSpatialReference* GetSpatialRef() const override;

#ifdef USE_LEGACY_GEOTRANSFORM
    CPLErr GetGeoTransform( double * padfTransform ) override;
#else
    CPLErr GetGeoTransform(GDALGeoTransform &gt) const override;
#endif
};
#endif // NISAR_INTERPOLATED_H
