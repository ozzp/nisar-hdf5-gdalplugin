# GDAL Plugin for NISAR HDF5

A read-only GDAL driver for NISAR L-band and S-band(soon) HDF5 products, with a focus on supporting efficient, cloud-optimized data access.

## Features

  * **Dynamic Loading:** Implemented as a loadable shared library that GDAL can discover at runtime.
  * **NISAR Product Identification:** Automatically identifies NISAR HDF5 files based on their internal structure and metadata.
  * **Subdataset Discovery:** Parses HDF5 structure to find and expose relevant raster datasets under `/science/LSAR/`.
  * **Cloud-Optimized Access:** Leverages the HDF5 ROS3 VFD (Read-Only S3 Virtual File Driver) to efficiently read data directly from cloud object stores using HTTP Range Requests.
  * **Metadata Handling:** Extracts and exposes georeferencing (EPSG/WKT), projections, and other metadata through the GDAL API.
  * **Overview Support:** (TBD) Support for reading resampled datasets (overviews).
  * **GDAL Integration:** Fully registered with the GDAL framework, enabling use with standard utilities like `gdalinfo`, `gdal_translate`, and `gdalwarp`.

-----

## Usage

### AWS Authentication

For accessing files in S3, the driver uses the HDF5 ROS3 VFD, which requires AWS credentials. Please export the following environment variables:

```shell
export AWS_REGION="<your-region>"
export AWS_ACCESS_KEY_ID="<your-key-id>"
export AWS_SECRET_ACCESS_KEY="<your-secret-key>"
export AWS_SESSION_TOKEN="<your-secret-key>"
```

### Sample Commands

Replace `<NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5>` with the path to your local or S3 object URL.

  * **Get info for all subdatasets:**

    ```shell
    gdalinfo NISAR:NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5
    ```

  * **Get info for a specific subdataset:**

    ```shell
    # Opens the HH polarization dataset for frequency A
    gdalinfo 'NISAR:NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5:/science/LSAR/GSLC/swaths/frequencyA/HH'
    ```

  * **Convert a specific subdataset to GeoTIFF:**

    ```shell
    gdal_translate -of GTiff 'NISAR:NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5:/science/LSAR/GSLC/swaths/frequencyA/HH' output_HH.tif
    ```

  * **Reproject a single subdataset:**

    ```shell
    gdalwarp -t_srs EPSG:4326 'NISAR:NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5:/science/LSAR/GSLC/swaths/frequencyA/HV' output_HV_wgs84.tif
    ```

  * **Clip a subdataset to a geographic extent:**

    ```shell
    gdalwarp -te <xmin> <ymin> <xmax> <ymax> 'NISAR:NISAR_L2_XX_XXXX_XXX_XXX_X_XXX_XXXX_XXXX_X_XXXXXXXXXXXXXXX_XXXXXXXXXXXXXXX_XXXXXX_X_X_X_XXX.h5:/science/LSAR/GSLC/swaths/frequencyA/HH' output_clipped.tif
    ```

-----

## Building from Source

To build the plugin, you must first have a compatible HDF5 library compiled with ROS3 VFD support.

**1. Build HDF5 Library:**

  * **Prerequisites:**

    ```shell
    # On Red Hat / AlmaLinux
    sudo dnf install openssl-devel libcurl-devel
    ```

  * **Configure and Compile:**

    ```shell
    # Download and extract HDF5 source
    ./configure --prefix=/usr/local/hdf5 \
                --enable-cxx \
                --enable-ros3-vfd

    make && sudo make install
    ```

    *Verify the ROS3 VFD is enabled by running `h5cc -showconfig` and checking for `(Read-Only) S3 VFD: yes`.*

**2. Build the Plugin:**

Please follow the instructions in **[BUILDING.md]** to build the conda package for your target platform.

-----

## C++ Implementation Details

\<details\>
\<summary\>Click to expand developer notes on the driver's C++ class structure.\</summary\>

  * **`NisarDataset` (subclass of `GDALDataset`):** Represents the NISAR HDF5 dataset. It supports opening files from both local storage and AWS S3 (using the HDF5 ROS3 VFD). The driver can parse connection strings to open specific HDF5 datasets within the file; if no specific path is given, it performs subdataset discovery by iterating through the HDF5 structure to find and list relevant raster datasets (under `/science/LSAR/`). When a dataset is opened, the driver determines its raster properties (dimensions, data type, band count), creates corresponding raster bands, and attempts to set an optimized HDF5 chunk cache. It provides georeferencing information by reading the `epsg_code` or WKT from a projection dataset and calculating the GeoTransform from coordinate datasets. Metadata is handled for a default domain (attributes from the opened HDF5 dataset) and a custom `"NISAR_GLOBAL"` domain (attributes from the root group).

  * **`NisarRasterBand` (subclass of `GDALRasterBand`):** Represents a single raster band within the dataset. Its constructor initializes properties like the GDAL data type and block size (matched to the HDF5 chunk size for efficiency). The core functionality lies in the overridden `IReadBlock` method, which calculates the appropriate HDF5 hyperslab corresponding to GDAL's block request, reads the pixel data using `H5Dread`, and correctly handles partial blocks at raster edges by padding the buffer.

  * **Driver Registration:** The driver is registered via `GDALRegister_NISAR()`, which is invoked when GDAL loads the plugin. This function creates and configures a `GDALDriver` object, assigning the static `NisarDataset::Open` method to the `pfnOpen` function pointer. This makes the driver available for use within the GDAL ecosystem.

\</details\>

-----

## Visualization & Validation

The compiled plugin allows NISAR HDF5 files to be opened directly in any GDAL-compatible software, such as **QGIS**, for visualization and analysis.

## Related Projects & References

  * **VICAR GDAL Plugin:** [https://github.com/Cartography-jpl/vicar-gdalplugin](https://github.com/Cartography-jpl/vicar-gdalplugin)
  * **HDF-EOS Information:** [https://www.hdfeos.org/software/gdal.php](https://www.hdfeos.org/software/gdal.php)

-----
