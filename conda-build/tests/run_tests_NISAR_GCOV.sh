#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Configuration
PACKAGE_NAME="gdal-driver-nisar"
CONDA_ENV_NAME="nisar-test-suite"

# Subdatasets and output file names
SUBDATASET_NETCDF="science/LSAR/GCOV/grids/frequencyA/HHHH"
SUBDATASET_NISAR="//${SUBDATASET_NETCDF}"
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
S3_FILE_PATH="$2"
#

# Derived path variables
GDAL_S3_PATH="/vsis3/${S3_FILE_PATH#s3://}"

# AWS Credentials (from profile)
# Your saml-pub profile already contains temporary session credentials.
# This block reads them directly from your AWS config files.
echo "Reading credentials from AWS profile: ${PROFILE}"

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile "${PROFILE}")
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile "${PROFILE}")
export AWS_SESSION_TOKEN=$(aws configure get aws_session_token --profile "${PROFILE}")
export AWS_REGION=$(aws configure get region --profile "${PROFILE}")

if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_REGION" ]]; then
    echo -e "${RED}ERROR: Failed to read credentials or region from profile '${PROFILE}'. Please ensure you have logged in via SAML and the profile is correctly configured.${NC}"
    exit 1
fi

# The session token might be optional in some credential setups, but if it's
# missing from a SAML profile, it's usually an error. We check for it.
if [[ -z "$AWS_SESSION_TOKEN" ]]; then
     echo -e "${RED}ERROR: AWS session token not found in profile '${PROFILE}'. Please log in again.${NC}"
    exit 1
fi
#

# Download S3 File for Local Tests
LOCAL_HDF5_FILE="local_test_file.h5"
echo "Downloading S3 file to local path for performance comparison..."
echo "  Source: ${S3_FILE_PATH}"
echo "  Destination: ${LOCAL_HDF5_FILE}"
aws s3 cp "${S3_FILE_PATH}" "${LOCAL_HDF5_FILE}"
#

# Construct HTTPS URL for standard drivers
S3_BUCKET=$(echo "${S3_FILE_PATH}" | cut -d/ -f3)
S3_KEY=$(echo "${S3_FILE_PATH}" | cut -d/ -f4-)
# CORRECTED: Was ${S_KEY}, now ${S3_KEY}
HTTPS_URL="https://${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${S3_KEY}"


# Performance Tuning Environment Variables
echo "Setting performance tuning environment variables..."
export NISAR_CHUNK_CACHE_SIZE_MB=1024
export GDAL_CACHEMAX=2048
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE
export GDAL_HTTP_VERSION=2
export GDAL_NUM_THREADS=ALL_CPUS

# Set GDAL S3 Authentication for /vsicurl/
echo "Setting GDAL configuration for S3 authentication..."
export AWS_S3_ENDPOINT="s3.${AWS_REGION}.amazonaws.com"
export AWS_VIRTUAL_HOSTING=TRUE
# GDAL will automatically use the credentials from the exported AWS_PROFILE


echo "Starting NISAR Driver Test Suite"

# Environment Setup
echo
echo "STEP 1: Creating and activating a clean Conda test environment..."
conda env remove --name "$CONDA_ENV_NAME" --yes > /dev/null 2>&1 || true
# Use --override-channels for a more reproducible environment build
conda create --name "$CONDA_ENV_NAME" --channel conda-forge --override-channels --yes python
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV_NAME"

echo "Installing packages..."
conda install --channel nisar-forge --channel conda-forge --override-channels --yes "$PACKAGE_NAME" gdal hdf5 libgdal-hdf5 netcdf4 libgdal-netcdf
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
# Generic check for "Size is" is more flexible for different test files
if gdalinfo "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" | grep "Size is"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.4: Reprojecting a subdataset with gdalwarp
echo -n "  - Test 2.4: Reprojecting a subdataset with gdalwarp... "
TARGET_SRS="EPSG:4326" # WGS 84 Lat/Lon
rm -f "$OUTPUT_REPROJECT_TIF"
gdalwarp -q -of GTiff -t_srs "$TARGET_SRS" "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_REPROJECT_TIF"
if [ -s "$OUTPUT_REPROJECT_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
    rm -f "$OUTPUT_REPROJECT_TIF"
else
    echo -e "${RED}FAILED: gdalwarp did not create the output file.${NC}"
    exit 1
fi

# Test 2.5: Data Integrity (Conversion)
echo -n "  - Test 2.5: Converting to COG with NISAR driver... "
rm -f "$OUTPUT_COG_NISAR"
gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR"
if [ -s "$OUTPUT_COG_NISAR" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.6: Create a Multiband GeoTIFF with gdal_merge.py
echo -n "  - Test 2.6: Creating a multiband GeoTIFF... "
SUBDATASET1="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/HHHH"
SUBDATASET2="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/numberOfLooks"
SUBDATASET3="NISAR:${GDAL_S3_PATH}:/science/LSAR/GCOV/grids/frequencyA/rtcGammaToSigmaFactor"
rm -f "$OUTPUT_MULTIBAND_TIF"
gdal_merge.py -q -separate -o "$OUTPUT_MULTIBAND_TIF" -of GTiff -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 "$SUBDATASET1" "$SUBDATASET2" "$SUBDATASET3"
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

# S3 Access Performance
echo
echo "Testing Performance from S3"

# Test 3.1: Time NISAR driver from S3
echo "  - Test 3.1: Timing NISAR driver (from S3)... "
if TIME_OUTPUT=$( { time gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR" 2>/dev/null; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
fi

# Test 3.2: Time standard netCDF driver from S3
echo "  - Test 3.2: Timing standard netCDF driver (from S3)... "
NETCDF_DRIVER_PATH="NETCDF:\"/vsicurl/${HTTPS_URL}\":${SUBDATASET_NETCDF}"
NETCDF_ERROR_LOG="netcdf_error.log"
if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${NETCDF_DRIVER_PATH}" "$OUTPUT_COG_NETCDF" 2> "$NETCDF_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED (as expected).${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$NETCDF_ERROR_LOG"
fi

# Test 3.3: Time standard HDF5 driver from S3
echo "  - Test 3.3: Timing standard HDF5 driver (from S3)... "
HDF5_DRIVER_PATH="HDF5:\"/vsicurl/${HTTPS_URL}\":${SUBDATASET_NISAR}"
HDF5_ERROR_LOG="hdf5_error.log"
if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${HDF5_DRIVER_PATH}" "output_hdf5_driver.tif" 2> "$HDF5_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED (as expected).${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$HDF5_ERROR_LOG"
fi

# Local File Access Performance
echo
echo "Testing Performance from Local File"

# Test 3.1.1: Time NISAR driver from Local File
echo "  - Test 3.1.1: Timing NISAR driver (from Local File)... "
NISAR_LOCAL_PATH="NISAR:${LOCAL_HDF5_FILE}:${SUBDATASET_NISAR}"
LOCAL_NISAR_ERROR_LOG="local_nisar_error.log"
echo "    Executing: gdal_translate -q -of COG \"${NISAR_LOCAL_PATH}\" \"${OUTPUT_COG_NISAR}\""
if TIME_OUTPUT=$( { time gdal_translate -q -of COG "${NISAR_LOCAL_PATH}" "$OUTPUT_COG_NISAR" 2> "$LOCAL_NISAR_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$LOCAL_NISAR_ERROR_LOG"
fi

# Test 3.2.1: Time standard netCDF driver from Local File
echo "  - Test 3.2.1: Timing standard netCDF driver (from Local File)... "
NETCDF_LOCAL_PATH="NETCDF:\"${LOCAL_HDF5_FILE}\":${SUBDATASET_NETCDF}"
LOCAL_NETCDF_ERROR_LOG="local_netcdf_error.log"
echo "    Executing: gdal_translate -q -of COG \"${NETCDF_LOCAL_PATH}\" \"${OUTPUT_COG_NETCDF}\""
if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${NETCDF_LOCAL_PATH}" "$OUTPUT_COG_NETCDF" 2> "$LOCAL_NETCDF_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$LOCAL_NETCDF_ERROR_LOG"
fi

# Test 3.3.1: Time standard HDF5 driver from Local File
echo "  - Test 3.3.1: Timing standard HDF5 driver (from Local File)... "
# Use the HDF5 convention with "//" for the subdataset path
HDF5_LOCAL_PATH="HDF5:\"${LOCAL_HDF5_FILE}\"://${SUBDATASET_NETCDF}"
HDF5_ERROR_LOG="hdf5_error.log"
echo "    Executing: gdal_translate -q -of COG \"${HDF5_LOCAL_PATH}\" \"output_hdf5_driver.tif\""

if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${HDF5_LOCAL_PATH}" "output_hdf5_driver.tif" 2> "$HDF5_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$HDF5_ERROR_LOG"
fi

# 4. Cleanup
echo
echo "STEP 4: Cleaning up..."
conda deactivate
rm -f "$OUTPUT_COG_NISAR" "$OUTPUT_COG_NETCDF" "$OUTPUT_MULTIBAND_TIF" netcdf_time.txt "$LOCAL_HDF5_FILE" netcdf_error.log hdf5_error.log "output_hdf5_driver.tif"
echo "You can remove the test environment with: conda env remove -n ${CONDA_ENV_NAME}"

echo
echo -e "${GREEN} All tests completed successfully! ${NC}"
