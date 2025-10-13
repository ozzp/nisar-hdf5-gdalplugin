#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Configuration ---
# Package and environment names
PACKAGE_NAME="gdal-driver-nisar"
CONDA_ENV_NAME="nisar-gslc-test-suite"

# Subdatasets and output file names for GSLC product
SUBDATASET_NETCDF="science/LSAR/GSLC/grids/frequencyA/HH"
SUBDATASET_NISAR="//${SUBDATASET_NETCDF}" # Use HDF5 driver convention
OUTPUT_COG_NISAR="output_nisar_driver.tif"
OUTPUT_REPROJECT_TIF="output_gslc_reproject.tif"
OUTPUT_AMPLITUDE_TIF="output_amplitude.tif"
OUTPUT_PHASE_TIF="output_phase.tif"
OUTPUT_COG_GSLC="output_gslc_cog.tif"
OUTPUT_TILED_TIF="output_tiled.tif"
OUTPUT_COG_NETCDF="output_netcdf_driver.tif"
# --- End Configuration ---

# Helper for printing colored output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Argument Parsing
if [ -z "$2" ]; then
    echo -e "${RED}Usage: $0 <aws-profile-name> <s3-file-path>${NC}"
    echo "Example: $0 my-profile s3://my-bucket/path/to/GSLC_file.h5"
    exit 1
fi
PROFILE="$1"
S3_FILE_PATH="$2"
#

# Derived path variables
GDAL_S3_PATH="/vsis3/${S3_FILE_PATH#s3://}"

# AWS Credentials
echo "Reading credentials from AWS profile: ${PROFILE}"
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile "${PROFILE}")
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile "${PROFILE}")
export AWS_SESSION_TOKEN=$(aws configure get aws_session_token --profile "${PROFILE}")
export AWS_REGION=$(aws configure get region --profile "${PROFILE}")

if [[ -z "$AWS_ACCESS_KEY_ID" || -z "$AWS_REGION" ]]; then
    echo -e "${RED}ERROR: Failed to read credentials from profile '${PROFILE}'. Please log in again.${NC}"
    exit 1
fi
if [[ -z "$AWS_SESSION_TOKEN" ]]; then
     echo -e "${RED}ERROR: AWS session token not found in profile '${PROFILE}'. Please log in again.${NC}"
    exit 1
fi
#

# Download S3 File for Local Tests
LOCAL_HDF5_FILE="local_gscl_test_file.h5"
echo "Downloading S3 file to local path for performance comparison: ${LOCAL_HDF5_FILE}"
aws s3 cp "${S3_FILE_PATH}" "${LOCAL_HDF5_FILE}"
#

# Construct HTTPS URL for standard drivers
S3_BUCKET=$(echo "${S3_FILE_PATH}" | cut -d/ -f3)
S3_KEY=$(echo "${S3_FILE_PATH}" | cut -d/ -f4-)
HTTPS_URL="https://${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com/${S3_KEY}"

# Performance Tuning Environment Variables
echo "Setting performance tuning environment variables..."
export NISAR_CHUNK_CACHE_SIZE_MB=1024
export GDAL_CACHEMAX=2048
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE
export GDAL_HTTP_VERSION=2
export GDAL_NUM_THREADS=ALL_CPUS
export GDAL_PAM_ENABLED=NO

# Set GDAL S3 Authentication
echo "Setting GDAL configuration for S3 authentication..."
export AWS_S3_ENDPOINT="s3.${AWS_REGION}.amazonaws.com"
export AWS_VIRTUAL_HOSTING=TRUE


echo "Starting NISAR GSLC Driver Test Suite"

# Environment Setup
echo
echo "STEP 1: Creating and activating a clean Conda test environment..."
conda env remove --name "$CONDA_ENV_NAME" --yes > /dev/null 2>&1 || true
conda create --name "$CONDA_ENV_NAME" --channel conda-forge --override-channels --yes python

source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV_NAME"

echo "Installing packages..."
conda install --channel nisar-forge --channel conda-forge --override-channels --yes "$PACKAGE_NAME" gdal hdf5 netcdf4 libgdal-netcdf libgdal-hdf5
echo -e "${GREEN}Environment setup complete.${NC}"

# Run Functional Tests
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

# Test 2.2: Subdataset Discovery
echo -n "  - Test 2.2: Discovering subdatasets in S3 file... "
if gdalinfo "NISAR:${GDAL_S3_PATH}" | grep "SUBDATASET"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.3: Full Read vs. Windowed Read Performance
echo "  - Test 2.3: Comparing full vs. windowed data reads from S3..."
SOURCE_SUBDATASET="NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"

# Get the full dimensions (maxx, maxy) of the source subdataset
echo "    - Getting full dataset dimensions..."
SIZE_INFO=$(gdalinfo "$SOURCE_SUBDATASET" | grep "Size is")
MAXX=$(echo "$SIZE_INFO" | awk '{print $3}' | sed 's/,//')
MAXY=$(echo "$SIZE_INFO" | awk '{print $4}')
echo "      -> Full size is ${MAXX}x${MAXY}"

# 1. Time the operation to read the FULL dataset
echo -n "    - Reading full dataset (${MAXX}x${MAXY}) with gdal_translate... "
FULL_READ_OUTPUT="full_read_output.tif"
rm -f "$FULL_READ_OUTPUT"
TIME_FULL_READ=$( { time gdal_translate -q -srcwin 0 0 "$MAXX" "$MAXY" "$SOURCE_SUBDATASET" "$FULL_READ_OUTPUT" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
if [ ! -s "$FULL_READ_OUTPUT" ]; then
    echo -e "${RED}FAILED: Full read did not create an output file.${NC}"
    exit 1
fi
echo -e "${GREEN}Finished in ${TIME_FULL_READ}${NC}"


# 2. Time the operation to read a specific WINDOW of pixels
echo -n "    - Reading partial dataset (2000x4000) with gdal_translate... "
WINDOW_OUTPUT="window_read_output.tif"
rm -f "$WINDOW_OUTPUT"
TIME_WINDOW_READ=$( { time gdal_translate -q -srcwin 100 400 2000 4000 "$SOURCE_SUBDATASET" "$WINDOW_OUTPUT" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
if [ ! -s "$WINDOW_OUTPUT" ]; then
    echo -e "${RED}FAILED: Windowed read did not create an output file.${NC}"
    exit 1
fi
echo -e "${GREEN}Finished in ${TIME_WINDOW_READ}${NC}"

# 3. Calculate and report the time difference
# Convert times from "0mX.YYYs" format to seconds for calculation
SECONDS_FULL=$(echo "$TIME_FULL_READ" | sed 's/m/ /' | sed 's/s//' | awk '{print $1 * 60 + $2}')
SECONDS_WINDOW=$(echo "$TIME_WINDOW_READ" | sed 's/m/ /' | sed 's/s//' | awk '{print $1 * 60 + $2}')

TIME_DELTA=$(echo "$SECONDS_FULL - $SECONDS_WINDOW" | bc -l)
echo -e " - ${GREEN}Performance Result${NC}: The partial windowed read was ${TIME_DELTA} seconds faster than the full data read."

# Test 2.4: Reprojecting a subdataset with gdalwarp
echo "  - Test 2.4: Reprojecting a subdataset with gdalwarp... "
TARGET_SRS="EPSG:4326" # WGS 84 Lat/Lon
SOURCE_DATASET="NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
rm -f "$OUTPUT_REPROJECT_TIF"

# Print the arguments for clarity
echo "    - Source: ${SOURCE_DATASET}"
echo "    - Target SRS: ${TARGET_SRS}"
echo "    - Output File: ${OUTPUT_REPROJECT_TIF}"

# Execute the gdalwarp command and capture its execution time
TIME_WARP=$( { time gdalwarp -q -of GTiff -t_srs "$TARGET_SRS" "$SOURCE_DATASET" "$OUTPUT_REPROJECT_TIF" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )

# Check if the output file was created and is not empty
if [ -s "$OUTPUT_REPROJECT_TIF" ]; then
    echo -e "    ${GREEN}PASSED: Finished in ${TIME_WARP}${NC}"
    rm -f "$OUTPUT_REPROJECT_TIF"
else
    echo -e "    ${RED}FAILED: gdalwarp did not create the output file.${NC}"
    exit 1
fi

# Test 2.5: Pixel Value Lookup
echo -n "  - Test 2.5: Looking up pixel value with gdallocationinfo... "
SOURCE_INPUT="NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
if gdallocationinfo "$SOURCE_INPUT" 2000 6000 | grep "Value:"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Note: The following derived subdataset tests are run on a local copy of the
# S3 file. This is a workaround for a known limitation in GDAL where the complex
# DERIVED_SUBDATASET syntax fails with the authenticated S3 virtual file path.
echo "  - NOTE: Running derived subdataset tests on local file due to GDAL S3 syntax problems that need to resolve/work around."

# Test 2.6: Derived Amplitude Subdataset (from Local File)
echo -n "  - Test 2.6: Creating derived AMPLITUDE subdataset... "
rm -f "$OUTPUT_AMPLITUDE_TIF"
# Use the confirmed working HDF5 syntax with quotes around the filename
LOCAL_AMPLITUDE_INPUT="DERIVED_SUBDATASET:AMPLITUDE:HDF5:\"${LOCAL_HDF5_FILE}\":${SUBDATASET_NISAR}"
gdal_translate -q -of GTiff "$LOCAL_AMPLITUDE_INPUT" "$OUTPUT_AMPLITUDE_TIF"
if [ -s "$OUTPUT_AMPLITUDE_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.7: Derived Phase Subdataset (from Local File)
echo -n "  - Test 2.7: Creating derived PHASE subdataset... "
rm -f "$OUTPUT_PHASE_TIF"
# Use the confirmed working HDF5 syntax with quotes around the filename
LOCAL_PHASE_INPUT="DERIVED_SUBDATASET:PHASE:HDF5:\"${LOCAL_HDF5_FILE}\":${SUBDATASET_NISAR}"
gdal_translate -q -of GTiff "$LOCAL_PHASE_INPUT" "$OUTPUT_PHASE_TIF"
if [ -s "$OUTPUT_PHASE_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.8: Advanced COG Creation
echo "  - Test 2.9: Converting to COG with specific options... "
rm -f "$OUTPUT_COG_GSLC"
COG_OPTIONS="-of COG -co OVERVIEWS=NONE -co COMPRESS=DEFLATE -co LEVEL=9"

# Print the arguments for clarity
echo "    - Source: NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
echo "    - Options: ${COG_OPTIONS}"
echo "    - Output File: ${OUTPUT_COG_GSLC}"

gdal_translate -q $COG_OPTIONS "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_GSLC"

if [ -s "$OUTPUT_COG_GSLC" ]; then
    echo -e "    ${GREEN}PASSED${NC}"
else
    echo -e "    ${RED}FAILED${NC}"
    exit 1
fi

# Test 2.9: Tiled GeoTIFF Creation
echo "  - Test 2.10: Creating a tiled GeoTIFF... "
rm -f "$OUTPUT_TILED_TIF"
TILED_OPTIONS="-of GTIFF -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512"

# Print the arguments for clarity
echo "    - Source: NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
echo "    - Tiling Parameters: ${TILED_OPTIONS}"
echo "    - Output File: ${OUTPUT_TILED_TIF}"

gdal_translate -q $TILED_OPTIONS "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_TILED_TIF"

if [ -s "$OUTPUT_TILED_TIF" ]; then
    echo -e "    ${GREEN}PASSED${NC}"
else
    echo -e "    ${RED}FAILED${NC}"
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
S3_NISAR_ERROR_LOG="s3_nisar_error.log"
if TIME_OUTPUT=$( { time gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR" 2> "$S3_NISAR_ERROR_LOG"; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
    echo "    Error message from GDAL:"
    sed 's/^/      /' "$S3_NISAR_ERROR_LOG"
fi

# Local File Access Performance
echo
echo "Testing Performance from Local File"

# Test 3.1.1: Time NISAR driver from Local File
echo "  - Test 3.1.1: Timing NISAR driver (from Local File)... "
NISAR_LOCAL_PATH="NISAR:${LOCAL_HDF5_FILE}:${SUBDATASET_NISAR}"
if TIME_OUTPUT=$( { time gdal_translate -q -of COG "${NISAR_LOCAL_PATH}" "$OUTPUT_COG_NISAR" 2>/dev/null; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
fi

# Test 3.2.1: Time standard netCDF driver from Local File
echo "  - Test 3.2.1: Timing standard netCDF driver (from Local File)... "
NETCDF_LOCAL_PATH="NETCDF:\"${LOCAL_HDF5_FILE}\":${SUBDATASET_NETCDF}"
if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${NETCDF_LOCAL_PATH}" "$OUTPUT_COG_NETCDF" 2> /dev/null; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
fi

# Test 3.3.1: Time standard HDF5 driver from Local File
echo "  - Test 3.3.1: Timing standard HDF5 driver (from Local File)... "
HDF5_LOCAL_PATH="HDF5:\"${LOCAL_HDF5_FILE}\"://${SUBDATASET_NETCDF}"
if TIME_OUTPUT=$( { time GDAL_SKIP=NISAR gdal_translate -q -of COG "${HDF5_LOCAL_PATH}" "output_hdf5_driver.tif" 2> /dev/null; } 2>&1 ); then
    REAL_TIME=$(echo "$TIME_OUTPUT" | grep real | awk '{print $2}')
    echo -e "    ${GREEN}PASSED: Finished in ${REAL_TIME}${NC}"
else
    echo -e "    ${RED}FAILED to complete.${NC}"
fi
# Cleanup
echo
echo "STEP 4: Cleaning up..."
conda deactivate
#rm -f "$OUTPUT_COG_NISAR" "$OUTPUT_REPROJECT_TIF" "$OUTPUT_AMPLITUDE_TIF" "$OUTPUT_PHASE_TIF" "$OUTPUT_COG_GSLC" "$OUTPUT_TILED_TIF" "$OUTPUT_COG_NETCDF" "$LOCAL_HDF5_FILE"
echo "You can remove the test environment with: conda env remove -n ${CONDA_ENV_NAME}"

echo
echo -e "${GREEN} All GSLC tests completed successfully! ${NC}"
