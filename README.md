# GDAL Plugin for NISAR HDF5

A read-only GDAL driver for NISAR L-band and S-band(future) HDF5 products, with a focus on supporting efficient, cloud-optimized data access.

## Features

  * **Dynamic Loading:** Implemented as a loadable shared library that GDAL can discover at runtime.
  * **NISAR Product Identification:** Automatically identifies NISAR HDF5 files based on their internal structure and metadata.
  * **Subdataset Discovery:** Parses the HDF5 structure to find and expose relevant raster datasets under `/science/LSAR/`.
  * **Level 1 (RSLC) Support:** Automatically generates Ground Control Points (GCPs) from the `geolocationGrid` for accurate reprojection.
  * **Level 2 (GCOV/GSLC) Support:** Correctly reads georeferencing (GeoTransform and Spatial Reference) for all datasets, including auxiliary metadata layers.
  * **Cloud-Optimized Access:** Leverages the HDF5 ROS3 VFD (Read-Only S3 Virtual File Driver) to efficiently read data directly from cloud object stores using `s3://` or `/vsis3/` paths.
  * **AWS Authentication:** Supports AWS credentials via explicit environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`).
  * **Full GDAL Integration:** Enables use with standard utilities like `gdalinfo`, `gdal_translate`, and `gdalwarp`.

## Installation

The recommended way to install this plugin is via the conda package manager from the `nisar-forge` channel on Anaconda.org.

```shell
conda install -c nisar-forge -c conda-forge gdal-driver-nisar
```

## AWS Authentication

Accessing files from S3 requires AWS credentials. This driver is built on the high-performance HDF5 ROS3 (Read-Only S3) Virtual File Driver, which provides optimized, chunked reads directly from cloud storage.

The HDF5 ROS3 driver is a low-level tool and **does not support AWS profiles** (from `~/.aws/config`) or IAM roles directly. It requires explicit, temporary credentials to be set as environment variables in your shell.

The following four environment variables **must** be set in your session *before* running GDAL commands:

  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`
  * `AWS_SESSION_TOKEN`
  * `AWS_REGION`

### Recommended Workflow

If you use AWS profiles (e.g., `saml-pub`) for authentication, you must first use the AWS CLI to fetch temporary credentials from your profile and export them to your environment. This is the standard and most secure method.

#### 1\. Set Your AWS Region

You must have your region set. You only need to do this once per session.

```shell
export AWS_REGION=us-west-2
```

#### 2\. Export Credentials from Your Profile

Use the `aws configure export-credentials` command to fetch and set your temporary keys and token. This command uses your profile (e.g., `saml-pub`) to authenticate and then prints the `export` commands for your temporary credentials. The `eval` command executes them.

```shell
eval $(aws configure export-credentials --format env --profile saml-pub)
```

This single command will set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` in your terminal.

-----

## Sample Commands

  * **Get info for all subdatasets (local file):**

    ```shell
    gdalinfo NISAR:/path/to/local/NISAR_L2_file.h5
    ```

  * **Get info for a specific subdataset (from S3):**

    ```shell
    # Assumes AWS env vars are set per the "AWS Authentication" section above
    gdalinfo 'NISAR:s3://my-bucket/path/to/file.h5:/science/LSAR/GSLC/swaths/frequencyA/HH'
    ```

  * **Convert a specific L2 subdataset to GeoTIFF:**

    ```shell
    gdal_translate -of GTiff \
        'NISAR:/path/to/local/L2_GCOV_file.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' \
        output_HHHH.tif
    ```

  * **Warp an L1 subdataset to a GeoTIFF using GCPs:**

    ```shell
    # Use -tps for high accuracy, or -order 2 for a fast preview
    gdalwarp -t_srs EPSG:4326 -tps -r cubic \
        'NISAR:/path/to/local/L1_RSLC_file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH' \
        output_warped.tif
    ```


## Building from Source

If you need to build the plugin from the latest source code, the recommended method is to build the conda package yourself.

### Building with Conda (Recommended)

Platform-specific instructions for creating the conda packages for both `osx-arm64` and `linux-64` are available in **[BUILDING.md]**. This is the preferred method as it handles all dependencies automatically.

### Manual Build (Advanced)

Building the plugin manually requires first compiling a compatible HDF5 library with ROS3 VFD support.

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

Once HDF5 is installed, you can compile the plugin using CMake.

```shell
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/path/to/install
make && make install
```

## C++ Implementation Details

  * **`NisarDataset` (subclass of `GDALDataset`):** This class serves as the main entry point for interacting with a NISAR HDF5 file.

      * **Core Functionality:** As a subclass of `GDALDataset`, it represents the entire file. It is responsible for parsing the user-provided connection string, which can be a path to a local file or an S3 URL.
      * **Data Access:** It can open a specific data path (e.g., `/science/LSAR/GSLC/swaths/frequencyA/HH`) within the HDF5 file. If no specific path is given, it will automatically search the file for compatible raster datasets and present them as GDAL subdatasets.
      * **Georeferencing:** It extracts the spatial reference system (SRS) and calculates the geotransform, enabling GDAL to correctly position the raster in geographic space.
      * **Metadata:** It reads and exposes metadata both from the global level of the HDF5 file (in a custom `NISAR_GLOBAL` domain) and from the specific raster dataset being accessed.

  * **`NisarRasterBand` (subclass of `GDALRasterBand`):** This class represents a single band of raster data (e.g., the HH polarization).

      * **Data Reading:** Its primary role is to read pixel data from the HDF5 file in response to requests from GDAL. The core logic resides in the `IReadBlock` method, which is heavily optimized. It reads data in chunks that align with the HDF5 file's internal layout ("hyperslabs") to maximize I/O performance.
      * **Efficiency:** Block sizes for the GDAL band are matched to the HDF5 chunk sizes, ensuring efficient data transfers.
      * **NoData Handling:** It correctly manages NoData values, both for reporting the value to GDAL and for padding the edges of the raster when a requested block extends beyond the data's boundaries.

  * **Driver Registration:** The driver is registered via `GDALRegister_NISAR()`, which is invoked when GDAL loads the plugin. This function creates and configures a `GDALDriver` object, assigning the static `NisarDataset::Open` method to the `pfnOpen` function pointer. This makes the driver available for use within the GDAL ecosystem.

## Visualization & Validation

The compiled plugin allows NISAR HDF5 files to be opened directly in any GDAL-compatible software, such as **QGIS**, for visualization and analysis.

## Data & Specifications

  * **NISAR Sample Data & Product Specs:** [https://science.nasa.gov/mission/nisar/sample-data/](https://science.nasa.gov/mission/nisar/sample-data/)

## Related Projects & References

  * **VICAR GDAL Plugin:** [https://github.com/Cartography-jpl/vicar-gdalplugin](https://github.com/Cartography-jpl/vicar-gdalplugin)
  * **HDF-EOS Information:** [https://www.hdfeos.org/software/gdal.php](https://www.hdfeos.org/software/gdal.php)
  * **NISAR Data Reader Examples (by Michael Aivazis):**
      * [https://github.com/aivazis/qed/tree/main/pkg/readers/nisar](https://github.com/aivazis/qed/tree/main/pkg/readers/nisar)
      * [https://github.com/aivazis/qed](https://github.com/aivazis/qed)
      * [https://github.com/pyre/pyre](https://github.com/pyre/pyre)

```
```
