# GDAL Plugin for NISAR HDF5

A read-only GDAL driver for NISAR L-band and S-band(soon) HDF5 products, with a focus on supporting efficient, cloud-optimized data access.

## Features

  * **Dynamic Loading:** Implemented as a loadable shared library that GDAL can discover at runtime.
  * **NISAR Product Identification:** Automatically identifies NISAR HDF5 files based on their internal structure and metadata.
  * **Subdataset Discovery:** Parses HDF5 structure to find and expose relevant raster datasets under `/science/LSAR/`.
  * **Cloud-Optimized Access:** Leverages the HDF5 ROS3 VFD (Read-Only S3 Virtual File Driver) to efficiently read data directly from cloud object stores.
  * **Metadata Handling:** Extracts and exposes georeferencing (EPSG/WKT), projections, and other metadata through the GDAL API.
  * **Overview Support:** (TBD) Support for reading resampled datasets.
  * **GDAL Integration:** Fully registered with the GDAL framework, enabling use with standard utilities like `gdalinfo`, `gdal_translate`, and `gdalwarp`.

-----

## Installation

The recommended way to install this plugin is via the conda package manager from the `nisar-forge` channel on Anaconda.org.

```shell
conda install -c nisar-forge gdal-driver-nisar
```

-----

## Usage

### AWS Authentication

For accessing files in S3, the driver requires AWS credentials. Please export the following environment variables:

```shell
export AWS_REGION="<your-region>"
export AWS_ACCESS_KEY_ID="<your-key-id>"
export AWS_SECRET_ACCESS_KEY="<your-secret-key>"
# If using temporary credentials, also set:
export AWS_SESSION_TOKEN="<your-session-token>"
```

### Sample Commands

Replace `<NISAR-XXXXXX-file.h5>` with the path to your local file or an S3 URL (`s3://...`).

  * **Get info for all subdatasets:**

    ```shell
    gdalinfo NISAR:<NISAR-XXXXXX-file.h5>
    ```

  * **Get info for a specific subdataset:**

    ```shell
    # Opens the HH polarization dataset for frequency A
    gdalinfo 'NISAR:<NISAR-XXXXXX-file.h5>:/science/LSAR/GSLC/swaths/frequencyA/HH'
    ```

  * **Convert a specific subdataset to GeoTIFF:**

    ```shell
    gdal_translate -of GTiff 'NISAR:<NISAR-XXXXXX-file.h5>:/science/LSAR/GSLC/swaths/frequencyA/HH' output_HH.tif
    ```

-----

## Building from Source

If you need to build the plugin from the latest source code, the recommended method is to build the conda package yourself.

### Building with Conda (Recommended)

Platform-specific instructions for creating the conda packages for both `osx-arm64` and `linux-64` are available in **[BUILDING.md])**. This is the preferred method as it handles all dependencies automatically.

### Manual Build (Advanced)

\<details\>
\<summary\>Click for advanced instructions on building dependencies manually.\</summary\>

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

\</details\>

-----

## C++ Implementation Details

\<details\>
\<summary\>Click to expand developer notes on the driver's C++ class structure.\</summary\>

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

\</details\>

-----

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
