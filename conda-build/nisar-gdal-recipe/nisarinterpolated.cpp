#include "nisarinterpolated.h"
#include "nisarinterpolatedrasterband.h"
#include "gdalwarper.h"
#include "vrtdataset.h"

#include <cmath>
#include <algorithm>
#include <limits>

// ====================================================================
// NisarInterpolatedDataset Implementation
// ====================================================================

NisarInterpolatedDataset::NisarInterpolatedDataset()
{
}

NisarInterpolatedDataset::~NisarInterpolatedDataset()
{
    // Clean up the DEM datasets when this dataset is closed
    if (m_poAlignedDEM) GDALClose(m_poAlignedDEM);
    if (m_poRawDEM) GDALClose(m_poRawDEM);
}

#if GDAL_VERSION_MAJOR < 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
CPLErr NisarInterpolatedDataset::GetGeoTransform(double* padfTransform)
{
    memcpy(padfTransform, m_adfTargetGeoTransform, 6 * sizeof(double));
    return CE_None;
}
#else
CPLErr NisarInterpolatedDataset::GetGeoTransform(GDALGeoTransform& gt) const
{
    for (int i = 0; i < 6; ++i) {
        gt[i] = m_adfTargetGeoTransform[i];
    }
    return CE_None;
}
#endif

const OGRSpatialReference* NisarInterpolatedDataset::GetSpatialRef() const
{
    return m_oSRS.IsEmpty() ? nullptr : &m_oSRS;
}
GDALDataset* NisarInterpolatedDataset::Open(GDALOpenInfo* poOpenInfo)
{
    const char* pszDemFile = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "DEM_FILE");

    // Open the Coarse 3D Data Cube using the exact string provided by the user/routing hook
    CPLDebug("NISAR_DRIVER", "Interpolation: Opening coarse cube at %s", poOpenInfo->pszFilename);
    
    const char* const apszAllowedDrivers[] = { "NISAR", nullptr };
    GDALDataset* poCoarseCubeDS = static_cast<GDALDataset*>(
        GDALOpenEx(poOpenInfo->pszFilename, GDAL_OF_RASTER | GDAL_OF_INTERNAL, apszAllowedDrivers, nullptr, nullptr));

    if (poCoarseCubeDS == nullptr || poCoarseCubeDS->GetRasterCount() < 2) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to open valid 3D coarse metadata cube.");
        return nullptr;
    }

    // Determine the Target High-Res Grid to steal dimensions
    std::string sFullInput(poOpenInfo->pszFilename);
    std::string sHighResGridPath;
    
    // Isolate the base filename by finding the end of the .h5 extension
    size_t h5_pos = sFullInput.find(".h5");
    if (h5_pos == std::string::npos) {
        CPLError(CE_Failure, CPLE_AppDefined, "Could not locate .h5 extension in input string.");
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }
    std::string sBaseFilename = sFullInput.substr(0, h5_pos + 3);

    // Dynamically choose the reference grid based on product type
    if (sFullInput.find("GCOV") != std::string::npos) {
        sHighResGridPath = sBaseFilename + ":/science/LSAR/GCOV/grids/frequencyA/HHHH";
    } 
    else if (sFullInput.find("GUNW") != std::string::npos) {
        sHighResGridPath = sBaseFilename + ":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase";
    } 
    else {
        CPLError(CE_Failure, CPLE_AppDefined, "Unsupported product type. Cannot determine reference grid.");
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }

    CPLDebug("NISAR_DRIVER", "Interpolation: Stealing dimensions from %s", sHighResGridPath.c_str());

    // Open the target high-res grid
    GDALDataset* poTargetGridDS = (GDALDataset*)GDALOpenEx(sHighResGridPath.c_str(), GDAL_OF_RASTER | GDAL_OF_READONLY, nullptr, nullptr, nullptr);

    if (!poTargetGridDS) {
        CPLError(CE_Failure, CPLE_OpenFailed, "Failed to open high-res target grid to determine dimensions.");
        GDALClose(poCoarseCubeDS);
        return nullptr;
    }

    // Instantiate our custom dataset and copy spatial info
    NisarInterpolatedDataset* poDS = new NisarInterpolatedDataset();

    poDS->nRasterXSize = poTargetGridDS->GetRasterXSize();
    poDS->nRasterYSize = poTargetGridDS->GetRasterYSize();
    poTargetGridDS->GetGeoTransform(poDS->m_adfTargetGeoTransform);

    const OGRSpatialReference* poTargetSRS = poTargetGridDS->GetSpatialRef();
    if (poTargetSRS) {
        poDS->m_oSRS = *poTargetSRS;
    }
    GDALClose(poTargetGridDS); // Done with the target grid reference

    // Open and Warp the DEM
    CPLDebug("NISAR_DRIVER", "Interpolation: Warping DEM from %s", pszDemFile);
    poDS->m_poRawDEM = (GDALDataset*)GDALOpenEx(pszDemFile, GDAL_OF_RASTER | GDAL_OF_READONLY, nullptr, nullptr, nullptr);
    if (!poDS->m_poRawDEM) {
        CPLError(CE_Failure, CPLE_OpenFailed, "Failed to open DEM file: %s", pszDemFile);
        delete poDS; GDALClose(poCoarseCubeDS); return nullptr;
    }

    char* pszTargetWKT = nullptr;
    poDS->m_oSRS.exportToWkt(&pszTargetWKT);

    // Create in-memory DEM locked to the exact NISAR grid dimensions
    GDALDriver* poMemDriver = GetGDALDriverManager()->GetDriverByName("MEM");
    poDS->m_poAlignedDEM = poMemDriver->Create("", poDS->nRasterXSize, poDS->nRasterYSize, 1, GDT_Float32, nullptr);
    
    // MOVE THE SAFETY CHECK HERE!
    if (!poDS->m_poAlignedDEM) {
        CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate memory for aligned DEM.");
        CPLFree(pszTargetWKT); delete poDS; GDALClose(poCoarseCubeDS); return nullptr;
    }

    poDS->m_poAlignedDEM->SetProjection(pszTargetWKT);
    poDS->m_poAlignedDEM->SetGeoTransform(poDS->m_adfTargetGeoTransform);
    poDS->m_poAlignedDEM->GetRasterBand(1)->Fill(0.0);

    //  Fetch the user's requested resampling method (Defaulting to CUBICSPLINE)
    const char* pszResampling = CSLFetchNameValueDef(poOpenInfo->papszOpenOptions, "DEM_RESAMPLING", "CUBICSPLINE");
    GDALResampleAlg eResampleAlg = GRA_CubicSpline; // Default

    if (EQUAL(pszResampling, "NEAREST")) {
        eResampleAlg = GRA_NearestNeighbour;
    } else if (EQUAL(pszResampling, "BILINEAR")) {
        eResampleAlg = GRA_Bilinear;
    } else if (EQUAL(pszResampling, "CUBIC")) {
        eResampleAlg = GRA_Cubic;
    } else if (EQUAL(pszResampling, "CUBICSPLINE")) {
        eResampleAlg = GRA_CubicSpline;
    } else {
        CPLDebug("NISAR_DRIVER", "Unrecognized DEM_RESAMPLING '%s', falling back to CUBICSPLINE.", pszResampling);
    }

    // Configure Warp Options
    GDALWarpOptions* psWarpOptions = GDALCreateWarpOptions();
    psWarpOptions->hSrcDS = poDS->m_poRawDEM;
    psWarpOptions->hDstDS = poDS->m_poAlignedDEM;
    psWarpOptions->eResampleAlg = eResampleAlg;

    psWarpOptions->pfnTransformer = GDALGenImgProjTransform;
    psWarpOptions->pTransformerArg = GDALCreateGenImgProjTransformer(
        poDS->m_poRawDEM, nullptr,
        poDS->m_poAlignedDEM, nullptr,
        FALSE, 0.0, 1);

    // Execute Warp silently
    CPLPushErrorHandler(CPLQuietErrorHandler);
    GDALWarpOperation oWarpOp;
    if (oWarpOp.Initialize(psWarpOptions) == CE_None) {
        oWarpOp.ChunkAndWarpImage(0, 0, poDS->nRasterXSize, poDS->nRasterYSize);
    }
    CPLPopErrorHandler();

    // Cleanup
    CPLFree(pszTargetWKT);
    if (psWarpOptions->pTransformerArg) {
        GDALDestroyGenImgProjTransformer(psWarpOptions->pTransformerArg);
    }
    GDALDestroyWarpOptions(psWarpOptions);

    // Load Coarse Cube Data into RAM
    poDS->m_nCubeXSize = poCoarseCubeDS->GetRasterXSize();
    poDS->m_nCubeYSize = poCoarseCubeDS->GetRasterYSize();
    poDS->m_nCubeZSize = poCoarseCubeDS->GetRasterCount();
    poCoarseCubeDS->GetGeoTransform(poDS->m_adfCubeGeoTransform);

    // Read Z-axis (heightAboveEllipsoid) from metadata
    poDS->m_zVect.reserve(poDS->m_nCubeZSize);
    for (int i = 1; i <= poDS->m_nCubeZSize; ++i) {
        GDALRasterBand* pCBand = poCoarseCubeDS->GetRasterBand(i);
        const char* pszHeight = pCBand->GetMetadataItem("Z_VALUE");
        poDS->m_zVect.push_back(pszHeight ? CPLAtof(pszHeight) : static_cast<double>(i));
    }

    // Read the entire 3D cube into a flat 1D std::vector
    int nTotalCubePixels = poDS->m_nCubeXSize * poDS->m_nCubeYSize * poDS->m_nCubeZSize;
    poDS->m_cubeData.resize(nTotalCubePixels);

    for (int z = 0; z < poDS->m_nCubeZSize; ++z) {
        GDALRasterBand* pCBand = poCoarseCubeDS->GetRasterBand(z + 1);
        float* pDst = poDS->m_cubeData.data() + (z * poDS->m_nCubeXSize * poDS->m_nCubeYSize);
        pCBand->RasterIO(GF_Read, 0, 0, poDS->m_nCubeXSize, poDS->m_nCubeYSize,
                         pDst, poDS->m_nCubeXSize, poDS->m_nCubeYSize,
                         GDT_Float32, 0, 0, nullptr);
    }

    GDALClose(poCoarseCubeDS); // Done with the coarse cube
    CPLDebug("NISAR_DRIVER", "Interpolation: Successfully loaded 3D cube into RAM.");

    // Create custom raster band
    poDS->SetBand(1, new NisarInterpolatedRasterBand(poDS, 1));

    return poDS;
}
