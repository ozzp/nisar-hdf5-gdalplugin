# GDAL Plugin for NISAR HDF5

A read-only GDAL driver for NISAR L-band and S-band(future) HDF5 products, with a focus on supporting efficient, cloud-optimized data access.

## Features

  * **Dynamic Loading:** Implemented as a loadable shared library that GDAL can discover at runtime.
  * **NISAR Product Identification:** Automatically identifies NISAR HDF5 files based on their internal structure and metadata.
  * **Subdataset Discovery:** Parses the HDF5 structure to find and expose relevant raster datasets under `/science/LSAR/`.
  * **Level 1 (RSLC) Support:** Automatically generates Ground Control Points (GCPs) from the `geolocationGrid` for accurate reprojection.
  * **Level 2 (GCOV/GSLC) Support:** Correctly reads georeferencing (GeoTransform and Spatial Reference) for all datasets, including auxiliary metadata layers.
  * **Cloud-Optimized Access:** Leverages the HDF5 ROS3 VFD (Read-Only S3 Virtual File Driver) to efficiently read data directly from cloud object stores using `s3://` or `/vsis3/` paths.
  * **AWS Authentication:** Supports AWS credentials via environment variables (`AWS_SESSION_TOKEN, etc.`)
  * **Full GDAL Integration:** Enables use with standard utilities like `gdalinfo`, `gdal_translate`, and `gdalwarp`.

## Installation

The recommended way to install this plugin is via the conda package manager from the `nisar-forge` channel on Anaconda.org.

```shell
conda install -c nisar-forge -c conda-forge gdal-driver-nisar
```

## AWS Authentication (Command-Line / Shell)

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

### Sample Commands

#### Get info for all subdatasets (local file)

```shell
gdalinfo NISAR:/path/to/local/NISAR_L2_file.h5
```

#### Get info for a specific subdataset (from S3)

```shell
# Assumes AWS env vars are set per the "AWS Authentication" section above
gdalinfo 'NISAR:s3://my-bucket/path/to/file.h5:/science/LSAR/GSLC/swaths/frequencyA/HH'
```

#### Convert a specific L2 subdataset to GeoTIFF

```shell
gdal_translate -of GTiff \
    'NISAR:/path/to/local/L2_GCOV_file.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' \
    output_HHHH.tif
```

#### Warp an L1 subdataset to a GeoTIFF (high accuracy)

```shell
# Use -tps for the most accurate reprojection
gdalwarp -t_srs EPSG:4326 -tps -r cubic \
    'NISAR:/path/to/local/L1_RSLC_file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH' \
    output_warped_high_accuracy.tif
```

#### Warp an L1 subdataset to a GeoTIFF (fast preview)

```shell
# Use -order 2 for a fast preview
gdalwarp -t_srs EPSG:4326 -order 2 -r cubic \
    'NISAR:/path/to/local/L1_RSLC_file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH' \
    output_warped_preview.tif
```

## AWS Authentication (Jupyter Notebook / Python)

Jupyter Notebook kernels are separate processes and **do not** inherit environment variables from user's terminal. User must set the credentials *inside the notebook* using Python.

The recommended solution is to use the **Boto3** library (the AWS SDK for Python) to fetch the credentials and set them for your notebook's environment.

### Recommended Workflow

#### 1\. Install Boto3

If you haven't already, install `boto3` into your Conda environment:

```shell
mamba install boto3
```

#### 2\. Run This Code in the First Cell

Place the following code in the **first cell** of your notebook and run it. It will use your AWS profile to get temporary credentials and set the four required environment variables for your driver.

```python
import os
import boto3
from osgeo import gdal

# Boto3 Authentication
# Set this to the name of your AWS profile
PROFILE_NAME = 'saml-pub' 

print(f"Attempting to get credentials for AWS profile: '{PROFILE_NAME}'...")

try:
    # This creates a session using your ~/.aws/config
    session = boto3.Session(profile_name=PROFILE_NAME)
    
    # This call may trigger a browser window to pop up for SAML/SSO login
    creds = session.get_credentials()
    
    # Set the explicit environment variables that the HDF5 ROS3 driver needs
    os.environ['AWS_ACCESS_KEY_ID'] = creds.access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = creds.secret_key
    os.environ['AWS_SESSION_TOKEN'] = creds.token
    
    # Also grab the region from the session
    if session.region_name:
        os.environ['AWS_REGION'] = session.region_name
    else:
        # Fallback in case the profile doesn't have a default region
        print("Warning: No region set in profile. Set AWS_REGION manually.")
        # os.environ['AWS_REGION'] = 'us-west-2' # Set manually if needed

    print("Successfully set AWS credentials in the notebook environment.")

except Exception as e:
    print(f"Error during AWS authentication: {e}")
    print("Please ensure your AWS profile is correct and you are logged in.")
    # Stop execution if auth fails
    raise
```

#### 3\. Run GDAL Code in a Second Cell

Now that the environment is set, you can run your GDAL code in any subsequent cell.

```python
# Run Your GDAL Code
from osgeo import gdal

gdal.UseExceptions()

file_path = 'NISAR:s3://my-bucket/path/to/file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH'
print(f"\nAttempting to open dataset: {file_path}")

try:
    ds = gdal.Open(file_path)
    if ds:
        print("Successfully opened the dataset with GDAL!")
        print(ds.GetMetadata())
    else:
        print("gdal.Open() failed.")

except Exception as e:
    print(f"An error occurred during gdal.Open: {e}")
```

## C++ Implementation Details

      * **`NisarDataset` (subclass of `GDALDataset`):** This class serves as the main entry point for interacting with a NISAR HDF5 file.
      * **Core Functionality:** As a subclass of `GDALDataset`, it represents the entire file. It is responsible for parsing the user-provided connection string, which can be a path to a local file or an S3 URL.
      * **Data Access:** It can open a specific data path (e.g., `/science/LSAR/GSLC/swaths/frequencyA/HH`) within the HDF5 file. If no specific path is given, it will automatically search the file for compatible raster datasets and present them as GDAL subdatasets.
      * **Georeferencing:** It extracts the spatial reference system (SRS) and calculates the geotransform, enabling GDAL to correctly position the raster in geographic space.
      * **Metadata:** It reads and exposes metadata from the HDF5 file. For container datasets, it reads file-level metadata. For raster datasets, it reads both dataset-specific attributes and relevant auxiliary metadata (e.g., `numberOfSubSwaths`).

  * **`NisarRasterBand` (subclass of `GDALRasterBand`):** This class represents a single band of raster data (e.g., the HH polarization).

      * **Data Reading:** Its primary role is to read pixel data from the HDF5 file in response to requests from GDAL. The core logic resides in the `IReadBlock` method, which is heavily optimized. It reads data in chunks that align with the HDF5 file's internal layout ("hyperslabs") to maximize I/O performance.
      * **Efficiency:** Block sizes for the GDAL band are matched to the HDF5 chunk sizes, ensuring efficient data transfers.
      * **NoData Handling:** It correctly manages NoData values, both for reporting the value to GDAL and for padding the edges of the raster when a requested block extends beyond the data's boundaries.

  * **Driver Registration:** The driver is registered via `GDALRegister_NISAR()`, which is invoked when GDAL loads the plugin. This function creates and configures a `GDALDriver` object, assigning the static `NisarDataset::Open` method to the `pfnOpen` function pointer. This makes the driver available for use within the GDAL ecosystem.


## Data & Specifications

  * **NISAR Sample Data & Product Specs:** [https://science.nasa.gov/mission/nisar/sample-data/](https://science.nasa.gov/mission/nisar/sample-data/)

## Related Projects & References

  * **VICAR GDAL Plugin:** [https://github.com/Cartography-jpl/vicar-gdalplugin](https://github.com/Cartography-jpl/vicar-gdalplugin)
  * **HDF-EOS Information:** [https://www.hdfeos.org/software/gdal.php](https://www.hdfeos.org/software/gdal.php)
  * **NISAR Data Reader Examples (by Michael Aivazis):**
      * [https://github.com/aivazis/qed/tree/main/pkg/readers/nisar](https://github.com/aivazis/qed/tree/main/pkg/readers/nisar)
      * [https://github.com/aivazis/qed](https://github.com/aivazis/qed)
      * [https://github.com/pyre/pyre](https://github.com/pyre/pyre)
