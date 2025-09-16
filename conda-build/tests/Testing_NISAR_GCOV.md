## NISAR GDAL Driver Test Suite using GCOV Level 2 sample product

This document outlines the functionality of the test script used to validate the `gdal-driver-nisar` plugin and details the observed performance and behavioral differences when compared to standard GDAL drivers for accessing cloud-hosted data.

### Test Script Overview

The test script is a suite designed to validate the driver's functionality, correctness, and performance in a clean, reproducible environment.

#### 1\. Configuration and Setup

  - **Variables**: The script begins by defining key variables, including the conda environment name, paths to the S3 test file, and names for various output files.
  - **AWS Credentials**: It securely fetches temporary AWS credentials from a specified user profile, exporting them as environment variables (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, etc.) for the session.
  - **Performance Tuning**: Environment variables such as `GDAL_CACHEMAX` and `NISAR_CHUNK_CACHE_SIZE_MB` are set to allocate memory for caching, optimizing data access from the cloud.
  - **Conda Environment**: To ensure a clean test, the script systematically removes any old test environment, creates a new one, and installs the `gdal-driver-nisar` package along with its dependencies (`gdal`, `hdf5`).

#### 2\. Functional Tests

The script executes a series of tests to validate the core features of the driver:

  - **Driver Registration**: Confirms that GDAL can successfully find and register the `gdal_NISAR.dylib` plugin.
  - **Subdataset Discovery**: Runs `gdalinfo` on the S3 file URI in "container" mode to ensure the driver can correctly identify and list all available raster subdatasets.
  - **Direct Raster Access**: Verifies that a specific raster subdataset can be opened directly and that its dimensions are read correctly.
  - **Reprojection**: Uses `gdalwarp` to reproject a subdataset to a different coordinate system (EPSG:4326), confirming that the driver correctly exposes the file's georeferencing information.
  - **Data Integrity**: Converts a subdataset to a Cloud-Optimized GeoTIFF (COG) using `gdal_translate` to ensure pixel data is read correctly.
  - **Multiband Creation**: Employs `gdal_merge.py` to combine multiple subdatasets into a single, multi-band GeoTIFF, testing the driver's ability to handle concurrent read operations from the same source file.

#### 3\. Performance and Behavior Comparison

Compares the custom NISAR driver against GDAL's standard drivers:

  - **NISAR Driver**: The `gdal_translate` command is timed using the `NISAR:` prefix to benchmark the custom driver's performance.
  - **netCDF/HDF5 Drivers**: The same operation is attempted using the standard `NETCDF:` and `HDF5:` prefixes, with the custom NISAR driver explicitly disabled via `GDAL_SKIP=NISAR`. This provides a direct comparison of S3 access patterns and reliability.

#### 4\. Cleanup

Upon completion, the script removes all temporary files and deactivates the conda environment, leaving the system clean.

-----

### How to Use the Test Script

Follow these steps to configure your environment and run the validation suite.

#### Prerequisites

1.  **Software**: Ensure you have a working installation of **Conda** or **Miniconda** and the **AWS CLI**.

2.  **AWS Configuration**: Your local AWS configuration must be set up correctly. The script requires two files in your home directory's `.aws` folder.

    **`~/.aws/credentials`**
    This file stores your secret keys under a specific profile name.
    *Example:*

    ```ini
    [your-profile-name]
    aws_access_key_id = YOUR_ACCESS_KEY_HERE
    aws_secret_access_key = YOUR_SECRET_KEY_HERE
    aws_session_token = YOUR_SESSION_TOKEN_HERE
    ```

    **`~/.aws/config`**
    This file stores non-secret settings like the region. Note the required `profile` prefix in the section header.
    *Example:*

    ```ini
    [profile your-profile-name]
    region = us-west-2
    output = json
    ```

#### Execution

Run the script from your terminal, providing the **AWS profile name** and the **full S3 URL** to the target NISAR HDF5 file as two separate command-line arguments.

**Command Syntax:**

```bash
bash run_tests_nisar_gdal.sh <aws-profile-name> "<s3-file-path>"
```

**Example:**

```bash
bash run_tests_nisar_gdal.sh your-profile-name "s3://your-bucket/path-to-object/NISAR_L2_PR_GCOV_001_030_A_019_2800_SHNA_A_20081012T060911_20081012T060925_D00404_N_F_J_001.h5"
```

The script will then proceed to set up the environment, run all functional and performance tests, and print the results to your console.

-----

### Driver Performance and Behavior

Differences in how the custom NISAR driver and the standard GDAL drivers (netCDF and HDF5) handle cloud-based HDF5 files.

| Feature | `gdal-driver-nisar` | Standard GDAL `netCDF` / `HDF5` Drivers |
| :--- | :--- | :--- |
| **S3 Access Method** | Uses the HDF5 library's internal **ROS3 (Read-Only S3) VFD**. This method is specifically designed for efficient, direct access to HDF5 files in object storage. | Rely on GDAL's generic `/vsis3/` virtual file system, which treats the remote file like a local one. |
| **HTTP Requests** | Issues a few, highly targeted HTTP GET requests to read the necessary HDF5 metadata and data chunks. | Issues a large number of small, scattered GET requests, which is inefficient for object storage and leads to high latency. |
| **Reliability** | **Successfully** opens and processes all tested datasets on macOS. | **Fails** to open the dataset. The `netCDF` driver requires a Linux-specific kernel feature (`userfaultfd`), and the `HDF5` driver gets confused by the file's internal layout over a high-latency connection. |
| **Performance** | **Fast and efficient**. The optimized access pattern results in quick processing times for cloud-based files. | **Slow and unreliable**. The inefficient access pattern leads to poor performance and ultimately results in a failure to read the data. |
| **Error Handling** | May show non-fatal HDF5 diagnostic messages during complex operations but proceeds to a successful completion. | Fails with a fatal error, preventing any data from being read. |
