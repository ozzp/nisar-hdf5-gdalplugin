import h5py
import sys
import os
import configparser
import argparse # Import the argparse library

# Configuration
# The AWS profile to use from your credentials file.
aws_profile_name = 'saml-pub'
#

def load_aws_credentials(profile_name):
    """Loads AWS credentials and region from standard config files."""
    creds = {}

    # Read Credentials (Key, Secret, Token)
    creds_file = os.path.expanduser('~/.aws/credentials')
    if not os.path.exists(creds_file):
        print(f"Error: Credentials file not found at {creds_file}", file=sys.stderr)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(creds_file)

    if profile_name not in config:
        print(f"Error: Profile '{profile_name}' not found in {creds_file}", file=sys.stderr)
        sys.exit(1)

    try:
        creds['aws_key'] = config[profile_name]['aws_access_key_id'].encode('utf-8')
        creds['aws_secret'] = config[profile_name]['aws_secret_access_key'].encode('utf-8')
        creds['aws_token'] = config[profile_name]['aws_session_token'].encode('utf-8')
    except KeyError as e:
        print(f"Error: Missing {e} in profile '{profile_name}' in {creds_file}", file=sys.stderr)
        sys.exit(1)

    # Read Region from Config File
    config_file = os.path.expanduser('~/.aws/config')
    if not os.path.exists(config_file):
        print(f"Error: Config file not found at {config_file}", file=sys.stderr)
        sys.exit(1)

    config.read(config_file)
    config_profile_name = f'profile {profile_name}'

    if config_profile_name not in config:
        print(f"Error: Profile '{config_profile_name}' not found in {config_file}", file=sys.stderr)
        sys.exit(1)

    try:
        creds['aws_region'] = config[config_profile_name]['region'].encode('utf-8')
    except KeyError as e:
        print(f"Error: Missing {e} in profile '{config_profile_name}' in {config_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Successfully loaded credentials and region for profile '{profile_name}'.")
    return creds

# Main script execution

# Set up the argument parser
parser = argparse.ArgumentParser(description="Open an HDF5 file from an S3 URL using the ROS3 driver.")
parser.add_argument("s3_url", help="The full S3 URL of the HDF5 file to open (e.g., s3://bucket/file.h5)")
args = parser.parse_args()

# Use the s3_url from the command-line arguments
s3_url = args.s3_url

credentials = load_aws_credentials(aws_profile_name)

print(f"Attempting to open HDF5 file from S3: {s3_url}")
print(f"Using h5py version: {h5py.__version__}")
print("-" * 20)

try:
    # Open the file, passing all credentials loaded from the config files.
    with h5py.File(
        s3_url,
        'r',
        driver='ros3',
        aws_region=credentials['aws_region'],
        secret_id=credentials['aws_key'],
        secret_key=credentials['aws_secret'],
        session_token=credentials['aws_token']
    ) as f:
        print("Success! Your HDF5 installation supports the ROS3 driver.")

        # Optional: Print a dataset to confirm we can read data
        if '/science/LSAR/identification/diagnosticModeFlag' in f:
            dataset = f['/science/LSAR/identification/diagnosticModeFlag']
            print(f"Successfully read dataset '{dataset.name}' with shape {dataset.shape}")
        else:
            print("Could not find the example dataset, but the file opened successfully.")

except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)
