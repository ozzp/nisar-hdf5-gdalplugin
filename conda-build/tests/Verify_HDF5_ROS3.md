# HDF5 ROS3 Driver Test Script

## What is this script for?

This Python script is a diagnostic tool designed to verify that your Python environment can successfully read HDF5 (`.h5`) files directly from Amazon S3. It specifically tests the functionality of the **HDF5 ROS3 (Read-Only S3) driver**.

The script performs the following actions:

1.  Loads AWS credentials from a specified profile in your local AWS configuration files.
2.  Accepts an `s3://` URL as a command-line argument.
3.  Uses the `h5py` library to open the HDF5 file from the S3 URL.
4.  Attempts to read a specific dataset within the file to confirm that the connection and data access are working correctly.

Its primary purpose is to troubleshoot and validate your environment for ability to process HDF5 data stored in the cloud.

-----

## Prerequisites

Before running this script, you must have the following set up on your system.

### 1\. Python and Libraries

You need a Python environment with the following libraries installed. You can install them using `conda` or `pip`.

  * **`h5py`**: The library that provides the Python interface to the HDF5 library. It must have been compiled with S3 VFD support.
  * **`configparser`**: Used to read the AWS configuration files. (Included in standard Python 3).

### 2\. AWS Configuration Files

The script is designed to read credentials from the standard AWS configuration files located in your home directory (`~/.aws/`). These files must be structured correctly.

**`~/.aws/credentials`**
This file should contain only your secret credentials under the correct profile name.

*Example for the `saml-pub` profile:*

```ini
[saml-pub]
aws_access_key_id = YOUR_ACCESS_KEY_HERE
aws_secret_access_key = YOUR_SECRET_KEY_HERE
aws_session_token = YOUR_SESSION_TOKEN_HERE
```

**`~/.aws/config`**
This file contains non-secret configuration, such as the region. Note the required `profile` prefix in the section header.

*Example for the `saml-pub` profile:*

```ini
[profile saml-pub]
region = us-west-2
output = json
```

-----

## How to Use

To execute the script, run it from your terminal using the `python` interpreter. You must provide the full S3 URL to the target HDF5 file as a single command-line argument.

### Command Syntax

```bash
python your_script_name.py "s3://<bucket-name>/<path-to-your-file>.h5"
```

### Example

```bash
python testing_hdf5_ros3.py "s3://your-bucket/NISAR_L2_PR_GCOV_001_030_A_019_2800_SHNA_A_20081012T060911_20081012T060925_D00404_N_F_J_001.h5"
```

### Expected Output

If the environment is configured correctly and the credentials are valid, the script will print a success message confirming it was able to load the credentials and read a dataset from the file.

```
Successfully loaded credentials and region for profile 'saml-pub'.
Attempting to open HDF5 file from S3: s3://...
Using h5py version: 3.14.0
--------------------
Success! Your HDF5 installation supports the ROS3 driver.
Successfully read dataset '/science/LSAR/identification/diagnosticModeFlag' with shape ()
```
