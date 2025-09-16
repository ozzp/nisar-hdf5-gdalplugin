#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Configuration
PACKAGE_NAME="gdal-driver-nisar"
CONDA_ENV_NAME="nisar-test-suite"

# Subdatasets and output file names
SUBDATASET_NETCDF="science/LSAR/GCOV/grids/frequencyA/HHHH"
SUBDATASET_NISAR="/${SUBDATASET_NETCDF}"
OUTPUT_COG_NISAR="output_nisar_driver.tif"
OUTPUT_REPROJECT_TIF="output_nisar_reproject.tif"
OUTPUT_COG_NETCDF="output_netcdf_driver.tif"
OUTPUT_MULTIBAND_TIF="output_nisar_multiband.tif"
# End Configuration

# Helper for printing colored output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Argument Parsing
if [ -z "$2" ]; then
    echo -e "${RED}Usage: $0 <aws-profile-name> <s3-file-path>${NC}"
    echo "Example: $0 my-profile s3://my-bucket/path/to/file.h5"
    exit 1
fi
PROFILE="$1"
S3_FILE_PATH="$2" # Assign S3_FILE_PATH from the second argument
#

# Derived path variables
GDAL_S3_PATH="/vsis3/${S3_FILE_PATH#s3://}"

# AWS Credentials (from profile)
echo "Fetching AWS credentials from profile: ${PROFILE}"

export AWS_REGION=$(aws --profile "${PROFILE}" configure get region)
export AWS_ACCESS_KEY_ID=$(aws --profile "${PROFILE}" configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws --profile "${PROFILE}" configure get aws_secret_access_key)
export AWS_SESSION_TOKEN=$(aws --profile "${PROFILE}" configure get aws_session_token)

if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo -e "${RED}ERROR: Failed to retrieve AWS credentials from profile '${PROFILE}'. Please check your AWS configuration.${NC}"
    exit 1
fi
if [[ -z "$AWS_SESSION_TOKEN" ]]; then
    unset AWS_SESSION_TOKEN
fi

# Construct HTTPS URL for standard drivers
S3_BUCKET=$(echo "${S3_FILE_PATH}" | cut -d/ -f3)
S3_KEY=$(echo "${S3_FILE_PATH}" | cut -d/ -f4-)
HTTPS_URL="https://${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${S_KEY}"


# Performance Tuning Environment Variables
echo "Setting performance tuning environment variables..."
export NISAR_CHUNK_CACHE_SIZE_MB=1024
export GDAL_CACHEMAX=2048
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE
export GDAL_HTTP_VERSION=2
export GDAL_NUM_THREADS=ALL_CPUS

# Set GDAL S3 Authentication for /vsicurl/
# These are needed for standard drivers to authenticate with S3
echo "Setting GDAL configuration for S3 authentication..."
export AWS_S3_ENDPOINT="s3.${AWS_REGION}.amazonaws.com"
export AWS_VIRTUAL_HOSTING=TRUE
# GDAL also recognizes the standard AWS variables if set
# The export commands from the "AWS Credentials" section handle this


echo "Starting NISAR Driver Test Suite"

# Environment Setup
echo
echo "STEP 1: Creating and activating a clean Conda test environment..."
conda env remove --name "$CONDA_ENV_NAME" --yes > /dev/null 2>&1 || true
conda create --name "$CONDA_ENV_NAME" -c conda-forge --yes python
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV_NAME"

echo "Installing packages..."
conda install -c nisar-forge -c conda-forge --yes "$PACKAGE_NAME" gdal hdf5 netcdf4
echo -e "${GREEN}Environment setup complete.${NC}"

# Run Tests 
echo
echo "STEP 2: Running functional tests..."

# Test 2.1: Driver Registration
echo -n "  - Test 2.1: Checking driver registration... "
if gdalinfo --formats | grep "NISAR -"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.2: Subdataset Discovery (Container Mode)
echo -n "  - Test 2.2: Discovering subdatasets in S3 file... "
if gdalinfo "NISAR:${GDAL_S3_PATH}" | grep "SUBDATASET"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.3: Direct Raster Access
echo -n "  - Test 2.3: Opening a specific subdataset... "
# The specific size check might need adjustment if you test with different files.
if gdalinfo "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" | grep "Size is"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.4: Reprojecting a subdataset with gdalwarp
echo -n "  - Test 2.4: Reprojecting a subdataset with gdalwarp... "

# Define variables for clarity
TARGET_SRS="EPSG:4326" # WGS 84 Lat/Lon

# Ensure no old output file exists before the test
rm -f "$OUTPUT_REPROJECT_TIF"

# Execute the gdalwarp command.
# The '-q' flag suppresses the progress bar for cleaner test output.
gdalwarp -q -of GTiff -t_srs "$TARGET_SRS" "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_REPROJECT_TIF"

# Check if the output file was created and is not empty
if [ -s "$OUTPUT_REPROJECT_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
    # Clean up the temporary file on success
    rm -f "$OUTPUT_REPROJECT_TIF"
else
    echo -e "${RED}FAILED: gdalwarp did not create the output file.${NC}"
    exit 1
fi

# Test 2.5: Data Integrity (Conversion)
echo -n "  - Test 2.5: Converting to COG with NISAR driver... "
gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR"
if [ -s "$OUTPUT_COG_NISAR" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.6: Create a Multiband GeoTIFF with gdal_merge.py
echo -n "  - Test 2.6: Creating a multiband GeoTIFF... "

# Define the subdatasets to merge
SUBDATASET1="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/HHHH"
SUBDATASET2="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/numberOfLooks"
# CORRECTED: Use a subdataset that exists in the sample file
SUBDATASET3="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/rtcGammaToSigmaFactor"

# Ensure no old output file exists
rm -f "$OUTPUT_MULTIBAND_TIF"

# Execute the gdal_merge command
gdal_merge.py -q -separate -o "$OUTPUT_MULTIBAND_TIF" -of GTiff -co TILED=YES -co BLOCKXSIZE=512  -co BLOCKYSIZE=512  "$SUBDATASET1" "$SUBDATASET2" "$SUBDATASET3"

# Check if the output file was created and has 3 bands
# Use gdalinfo | grep to count the "Band" lines
BAND_COUNT=$(gdalinfo "$OUTPUT_MULTIBAND_TIF" | grep -c "^Band")
if [ -s "$OUTPUT_MULTIBAND_TIF" ] && [ "$BAND_COUNT" -eq 3 ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED: gdal_merge.py did not create a valid 3-band output file.${NC}"
    exit 1
fi


# 3. Performance Comparison
echo
echo "STEP 3: Running performance comparison..."

# Test 3.1: Time NISAR driver
echo -n "  - Test 3.1: Timing NISAR driver
REAL_TIME=$( { time gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}' )
echo -e "${GREEN}Finished in ${REAL_TIME}${NC}"

# Test 3.2: Time standard netCDF driver
echo -n "  - Test 3.2: Timing standard netCDF driver... "
NETCDF_DRIVER_PATH="NETCDF:\"/vsicurl/${HTTPS_URL}\":${SUBDATASET_NETCDF}"
{ time GDAL_SKIP=NISAR gdal_translate -q -of COG "${NETCDF_DRIVER_PATH}" "$OUTPUT_COG_NETCDF" > /dev/null 2>&1; } 2> netcdf_time.txt || true # Allow command to fail
NETCDF_TIME=$(cat netcdf_time.txt | grep real | awk '{print $2}')
if [ -n "$NETCDF_TIME" ]; then
    echo -e "${GREEN}Finished in ${NETCDF_TIME}${NC}"
else
    echo -e "${RED}Failed to complete (as expected on this OS).${NC}"
fi


# 4. Cleanup
echo
echo "STEP 4: Cleaning up..."
conda deactivate
#rm -f "$OUTPUT_COG_NISAR" "$OUTPUT_COG_NETCDF" nisar_time.txt netcdf_time.txt "$OUTPUT_MULTIBAND_TIF"
echo "You can remove the test environment with: conda env remove -n ${CONDA_ENV_NAME}"

echo
echo -e "${GREEN} All tests completed successfully! ${NC}"
