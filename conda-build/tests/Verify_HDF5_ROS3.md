# `verify_hdf5_ros3.py`

## Script Purpose

This Python script is a diagnostic tool designed to verify that a Python environment can successfully read HDF5 (`.h5`) files directly from Amazon S3. It specifically tests the functionality of the **HDF5 ROS3 (Read-Only S3) driver** using the `h5py` library.

The script performs the following actions:

1.  Loads AWS credentials from a specified profile in local AWS configuration files.
2.  Accepts an `s3://` URL and other optional parameters from the command line.
3.  Authenticates with AWS and opens the HDF5 file directly from the S3 URL.
4.  Attempts to read a dataset within the file to confirm that cloud data access is working correctly.

Its primary purpose is to troubleshoot and validate environment that needs to read HDF5 data stored in the cloud.

-----

## Prerequisites

Before running this script, you must have the following set up on your system.

### 1\. Python Libraries

You need a Python environment with the following libraries installed:

  * **`h5py`**: Must be a version compiled with S3 VFD (Virtual File Driver) support.
  * **`configparser`**: Included in the standard Python 3 library.

### 2\. AWS Configuration Files

The script reads credentials from the standard AWS configuration files located in your home directory (e.g., `~/.aws/`). These files must be structured correctly.

**`~/.aws/credentials`**
This file should contain your secret keys and tokens.
*Example:*

```ini
[saml-pub]
aws_access_key_id = YOUR_ACCESS_KEY_HERE
aws_secret_access_key = YOUR_SECRET_KEY_HERE
aws_session_token = YOUR_SESSION_TOKEN_HERE
```

**`~/.aws/config`**
This file contains non-secret settings like the region. Note the required `profile` prefix in the section header.
*Example:*

```ini
[profile saml-pub]
region = us-west-2
output = json
```

-----

## Parameters

The script is controlled via the following command-line arguments:

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `s3_url` | **Required** | The full S3 URL of the HDF5 file to open (e.g., `"s3://bucket/file.h5"`). |
| `--profile` | Optional | The AWS profile name from your credentials file to use for authentication. **Defaults to `saml-pub`**. |
| `--print-creds` | Optional | A flag that prints the loaded AWS credentials to standard output before proceeding to open the file. |

-----

## How to Run

Execute the script from your terminal, providing the required S3 URL and any optional arguments.

### Example 1: Basic Execution

This command uses the default `saml-pub` profile to open the specified S3 file.

```bash
python verify_hdf5_ros3.py "s3://nisar-st-data-ondemand/path/to/your/file.h5"
```

### Example 2: Using a Different Profile

This command specifies a different AWS profile named `my-dev-profile`.

```bash
python verify_hdf5_ros3.py "s3://bucket/file.h5" --profile my-dev-profile
```

### Example 3: Printing Credentials

This command first prints the loaded credentials for the `saml-pub` profile to the screen and then proceeds to open the S3 file.

```bash
python verify_hdf5_ros3.py "s3://bucket/file.h5" --print-creds
```
