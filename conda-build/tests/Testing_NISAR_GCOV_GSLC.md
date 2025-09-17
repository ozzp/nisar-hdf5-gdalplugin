# NISAR GDAL Driver Test Suite

This document outlines the functionality and usage of a comprehensive test script designed to validate the `gdal-driver-nisar` plugin. It details the functional tests for both **GCOV** and **GSLC** Level 2 products and analyzes the driver's performance against standard GDAL drivers for both cloud and local data access.

### Test Script Overview

The script is a validation suite that confirms the driver's functionality, correctness, and performance in a clean, reproducible environment.

#### 1\. Configuration and Setup

  - **Parameters**: The script is controlled via command-line arguments for the AWS profile and the S3 path to the test file.
  - **AWS Credentials**: It reads credentials from a specified AWS profile to authenticate with S3.
  - **File Download**: For performance comparisons, the script downloads a temporary local copy of the S3 test file.
  - **Performance Tuning**: Key environment variables like `GDAL_CACHEMAX` are set to optimize data caching.
  - **Conda Environment**: To ensure a clean and isolated test, the script systematically creates a new Conda environment and installs the `gdal-driver-nisar` package along with all necessary dependencies and GDAL plugins (`gdal`, `libgdal-netcdf`, `libgdal-hdf5`).

#### 2\. Functional Tests

The script executes a series of tests to validate the core features of the driver against both GCOV and GSLC products.

**Common Tests (GCOV & GSLC):**

  - **Driver Registration**: Confirms that GDAL successfully registers the NISAR driver plugin.
  - **Subdataset Discovery**: Verifies the driver can list all available raster subdatasets from the remote S3 file.
  - **Direct Raster Access**: Ensures a specific raster subdataset can be opened directly.
  - **Reprojection (`gdalwarp`)**: Validates that the driver correctly exposes georeferencing information, allowing reprojection to different coordinate systems.
  - **Data Conversion (`gdal_translate`)**: Converts subdatasets to GeoTIFF and Cloud-Optimized GeoTIFF (COG) to ensure pixel data is read correctly.

**GSLC-Specific Tests:**

  - **Pixel Value Lookup (`gdallocationinfo`)**: Verifies that pixel values can be queried from a specific coordinate within a raster.
  - **Derived Subdatasets**: Confirms the driver can correctly generate and read derived subdatasets, such as **AMPLITUDE** and **PHASE**, from complex-valued data.

#### 3\. Performance and Behavior Comparison

The script benchmarks the custom NISAR driver against GDAL's standard `netCDF` and `HDF5` drivers in two scenarios:

1.  **Cloud Access**: Tests are run directly against the S3 object to measure performance over the network.
2.  **Local Access**: The same tests are run against the downloaded local file to measure raw parsing and processing speed without network latency.

#### 4\. Cleanup

Upon completion, the script removes all temporary files (including the downloaded HDF5 file) and provides instructions for removing the temporary Conda environment.

-----

### How to Use the Test Script

#### Prerequisites

1.  **Software**: Ensure you have a working installation of **Conda** or **Miniconda** and the **AWS CLI**.

2.  **AWS Configuration**: Your local AWS configuration must be set up correctly in `~/.aws/credentials` and `~/.aws/config`.

    **`~/.aws/credentials`** (stores secret keys)
    *Example:*

    ```ini
    [your-profile-name]
    aws_access_key_id = YOUR_ACCESS_KEY_HERE
    aws_secret_access_key = YOUR_SECRET_KEY_HERE
    aws_session_token = YOUR_SESSION_TOKEN_HERE
    ```

    **`~/.aws/config`** (stores non-secret settings)
    *Example:*

    ```ini
    [profile your-profile-name]
    region = us-west-2
    ```

#### Execution

Run the script from your terminal, providing the **AWS profile name** and the **full S3 URL** to the target NISAR file as two separate command-line arguments.

**Command Syntax:**

```bash
bash run_tests_nisar.sh <aws-profile-name> "<s3-file-path>"
```

**Example (GCOV Product):**

```bash
bash run_tests_nisar.sh your-profile "s3://nisar-st-data-ondemand/DATA_NISAR_SAMPLE/R4.0.4/GCOV/NISAR_L2_PR_GCOV_001_030_A_019_2800_SHNA_A_20081012T060911_20081012T060925_D00404_N_F_J_001.h5"
```

**Example (GSLC Product):**

```bash
bash run_tests_nisar.sh your-profile "s3://nisar-st-data-ondemand/DATA_NISAR_SAMPLE/R4.0.4/GSLC/NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5"
```

-----

### Key Findings: Driver Performance and Behavior

The test suite reveals critical differences in how the drivers handle NISAR data.

| Access | Driver | Result & Analysis |
| :--- | :--- | :--- |
| **Cloud (S3)** | `gdal-driver-nisar` | **SUCCESS**. The only driver that correctly handles AWS session credentials to access the remote file. Its use of the HDF5 ROS3 VFD is efficient for cloud object storage. |
| **Cloud (S3)** | Standard `netCDF` / `HDF5` | **FAILURE**. Both drivers fail with `HTTP 403 Forbidden` errors. Their underlying `/vsicurl/` access method does not properly handle the temporary AWS credentials required for authentication. |
| **Local File** | `gdal-driver-nisar` | **SUCCESS (Fastest)**. Consistently outperforms the standard drivers, proving its parsing and processing logic is highly optimized for the NISAR data structure. |
| **Local File** | Standard `netCDF` | **SUCCESS (Slower)**. Able to read the local file correctly but is measurably slower than the specialized NISAR driver. |
| **Local File** | Standard `HDF5` | **SUCCESS (Slowest)**. While functional with the correct subdataset syntax, it is the least performant of the three, likely due to its generic parsing approach. |

**Overall Conclusion**: The `gdal-driver-nisar` is essential for reliable and performant access to cloud-hosted NISAR data and remains the most efficient option for local files.
