Implemention Plan for 3D data cube interpolation.  Borrowing from get_product_geometry_from_cubes.py
EPSG4326.vrt: Copernicus 30-m EPSG:4326(GLO-30)DEM with extent -180 to 180, -90 to 90
Optimize for DEM provided as AWS s3 object in VRT format with leaf nodes being COG files internally tiled with Block=512x512
to read DEM using either GDAL's /vsis3/ or /vsicurl/


### Declare the Open Options
Reimplement get_product_geometry_from_cubes.py use of argparse section (get_parser()) handling --dem, --out-inc-angle, etc.
Inform GDAL that the driver accepts special CLI arguments.

In driver registration function (`GDALRegister_NISAR()`), update the `GDAL_DMD_OPENOPTIONLIST` metadata XML:

```cpp
poDriver->SetMetadataItem(
    GDAL_DMD_OPENOPTIONLIST,
    "<OpenOptionList>"
    // ... existing options ...
    "  <Option name='DEM_FILE' type='string' description='Path to DEM (VRT or raster) for 3D cube interpolation'/>"
    "  <Option name='QUANTITY' type='string' description='Quantity to interpolate (e.g., LookAngle, IncidenceAngle)'/>"
    "</OpenOptionList>");
```

### New Logic in `NisarDataset::Open()`
When a user provides the `-oo QUANTITY=` flag, intercept the standard opening process. Skip normal subdataset discovery and instead return a special **Virtual Interpolation Dataset**.

```cpp
const char* pszQuantity = poOpenInfo->papszOpenOptions 
    ? CSLFetchNameValue(poOpenInfo->papszOpenOptions, "QUANTITY") 
    : nullptr;

if (pszQuantity != nullptr) 
{
    const char* pszDemFile = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "DEM_FILE");
    if (pszDemFile == nullptr) {
        CPLError(CE_Failure, CPLE_OpenFailed, "DEM_FILE open option is required when QUANTITY is specified.");
        return nullptr;
    }

    // Identify which 3D cube matches pszQuantity (e.g. /metadata/radarGrid/incidenceAngle)
    // Read 1D coordinate arrays: X-coords, Y-coords, and Z-coords (heightAboveEllipsoid)
    // Read the 3D data cube into memory (e.g., 20x261x194)
    // Instantiate and return a NisarInterpolatedDataset (passing it the DEM, the cube, and the coords)
}
```

### GDAL Warped VRTs
Reimplement get_product_geometry_from_cubes.py: isce3.geogrid.get_radar_grid(..., dem_raster, geogrid_obj, orbit, ..., interpolated_dem_raster)

DEM is a provided as VRT: DEM projection, resolution, and pixel alignment may or may not match NISAR product grid.

Use GDAL's Warp API in `NisarInterpolatedDataset` constructor: Dynamically create internal "Warped VRT" of the DEM that is a match your NISAR grid.

```cpp
// Inside NisarInterpolatedDataset constructor
GDALDataset* poRawDEM = (GDALDataset*) GDALOpen(pszDemFile, GA_ReadOnly);

// Create a WarpOptions structure
GDALWarpOptions* psWarpOptions = GDALCreateWarpOptions();
psWarpOptions->hSrcDS = poRawDEM;
// ... set up resampling (e.g., GRA_Bilinear) ...

// Use AutoCreateWarpedVRT to create an in-memory DEM that matches NISAR grid!
m_poAlignedDEM = GDALAutoCreateWarpedVRT(
    poRawDEM, 
    poRawDEM->GetProjectionRef(), 
    this->GetProjectionRef(), // The NISAR Projection
    GRA_Bilinear, 
    5.0, // Error threshold
    psWarpOptions
);
// Now, m_poAlignedDEM is a raster with the same X/Y dimensions and GeoTransform as NISAR target grid!
```

### The Custom Raster Band (`IReadBlock`)
Replace get_product_geometry_from_cubes.py: SciPy's RegularGridInterpolator with trilinear interpolation loop.
The SciPy RegularGridInterpolator handles NaN values natively (or based on the bounds_error=False setting).
In trilinear interpolation loop need to handle:
   - DEM height is NoData value
   - one or more of 8 bounding points is NaN
Explicitly define dfNoDataValue and use it for pixels for which interpolated values can't be computed. 

Create a `NisarInterpolatedRasterBand` class and implement the `IReadBlock()` method. Translate `isce3` logic into GDAL.

```cpp
CPLErr NisarInterpolatedRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff, void* pImage)
{
    float* pafOutput = static_cast<float*>(pImage);
    
    // Calculate the pixel coordinates for this specific block
    int nXOff = nBlockXOff * nBlockXSize;
    int nYOff = nBlockYOff * nBlockYSize;
    int nReqXSize = std::min(nBlockXSize, nRasterXSize - nXOff);
    int nReqYSize = std::min(nBlockYSize, nRasterYSize - nYOff);

    // Read the DEM heights ONLY for this block from Aligned VRT
    std::vector<float> demHeights(nReqXSize * nReqYSize);
    m_poAlignedDEM->RasterIO(GF_Read, nXOff, nYOff, nReqXSize, nReqYSize,
                             demHeights.data(), nReqXSize, nReqYSize, 
                             GDT_Float32, 1, nullptr, 0, 0, 0, nullptr);

    // Perform Interpolation
    for (int y = 0; y < nReqYSize; ++y) 
    {
        for (int x = 0; x < nReqXSize; ++x) 
        {
            float targetZ = demHeights[y * nReqXSize + x];
            
            // Calculate World X and Y using the GeoTransform
            double dfGeoX = ...;
            double dfGeoY = ...;

            // ISCE3 TRILINEAR INTERPOLATION LOGIC
            // Find bounding indices in the coarse 1D X/Y arrays
            // Find bounding indices in the coarse 1D Z array (heightAboveEllipsoid)
            // Perform Bilinear interpolation on the lower Z plane
            // Perform Bilinear interpolation on the upper Z plane
            // Perform Linear interpolation between the two Z results using targetZ
            
            pafOutput[y * nBlockXSize + x] = interpolatedValue;
        }
    }

    return CE_None;
}
```

### User CLI
`gdal_translate -oo DEM_FILE=dem.vrt -oo QUANTITY=LookAngle ...` NISAR:nisar_l2_product.h5
    - Driver reads the coarse `LookAngle` cube and X/Y/Z axes into memory.
    - Driver opens `dem.vrt` and wraps it in a GDAL Warped VRT so its pixels perfectly align with the target SAR grid.
    - GDAL begins looping through the file, reading in blocks (say 512x512).
    - For each block, `IReadBlock()` fetches 512x512 heights from the Aligned DEM, loops over the 262,144 pixels, runs the interpolation math against the coarse cube, and hands the calculated block back to GDAL to be written to the GeoTIFF.

### 
