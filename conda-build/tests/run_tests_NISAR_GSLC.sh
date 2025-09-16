#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Configuration
# Package and environment names
PACKAGE_NAME="gdal-driver-nisar"
CONDA_ENV_NAME="nisar-gslc-test-suite"

# Subdatasets and output file names for GSLC product
SUBDATASET_NETCDF="science/LSAR/GSLC/grids/frequencyA/HH"
SUBDATASET_NISAR="/${SUBDATASET_NETCDF}"
OUTPUT_COG_NISAR="output_nisar_driver.tif"
OUTPUT_REPROJECT_TIF="output_gslc_reproject.tif"
OUTPUT_AMPLITUDE_TIF="output_amplitude.tif"
OUTPUT_PHASE_TIF="output_phase.tif"
OUTPUT_COG_GSLC="output_gslc_cog.tif"
OUTPUT_TILED_TIF="output_tiled.tif"
#End Configuration

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
S3_FILE_PATH="$2" # Set S3_FILE_PATH from the second argument
#

# Derived path variables
GDAL_S3_PATH="/vsis3/${S3_FILE_PATH#s3://}"

# AWS Credentials
echo "Setting AWS_PROFILE to: ${PROFILE}"
export AWS_PROFILE="${PROFILE}"
export AWS_REGION=$(aws configure get region --profile "${PROFILE}")

if [ -z "$AWS_REGION" ]; then
    echo -e "${RED}ERROR: Failed to retrieve AWS region from profile '${PROFILE}'. Please check your AWS configuration.${NC}"
    exit 1
fi
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

# Set GDAL S3 Authentication for /vsicurl/
echo "Setting GDAL configuration for S3 authentication..."
export AWS_S3_ENDPOINT="s3.${AWS_REGION}.amazonaws.com"
export AWS_VIRTUAL_HOSTING=TRUE


echo "Starting NISAR GSLC Driver Test Suite"

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
else
    echo -e "${RED}FAILED: gdalwarp did not create the output file.${NC}"
    exit 1
fi

# Test 2.5: Data Integrity (Conversion)
echo -n "  - Test 2.5: Basic conversion with gdal_translate... "
rm -f "$OUTPUT_COG_NISAR"
gdal_translate -q -of GTiff "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR"
if [ -s "$OUTPUT_COG_NISAR" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.6: Pixel Value Lookup
echo -n "  - Test 2.6: Looking up pixel value with gdallocationinfo... "
SOURCE_INPUT="NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
if gdallocationinfo "$SOURCE_INPUT" 2000 6000 | grep "Value:"; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.7: Derived Amplitude Subdataset
echo -n "  - Test 2.7: Creating derived AMPLITUDE subdataset... "
rm -f "$OUTPUT_AMPLITUDE_TIF"
DERIVED_AMPLITUDE_INPUT="DERIVED_SUBDATASET:AMPLITUDE:NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
gdal_translate -q -of GTiff "$DERIVED_AMPLITUDE_INPUT" "$OUTPUT_AMPLITUDE_TIF"
if [ -s "$OUTPUT_AMPLITUDE_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.8: Derived Phase Subdataset
echo -n "  - Test 2.8: Creating derived PHASE subdataset... "
rm -f "$OUTPUT_PHASE_TIF"
DERIVED_PHASE_INPUT="DERIVED_SUBDATASET:PHASE:NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}"
gdal_translate -q -of GTiff "$DERIVED_PHASE_INPUT" "$OUTPUT_PHASE_TIF"
if [ -s "$OUTPUT_PHASE_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.9: Advanced COG Creation
echo -n "  - Test 2.9: Converting to COG with specific options... "
rm -f "$OUTPUT_COG_GSLC"
gdal_translate -q -of COG -co OVERVIEWS=NONE -co COMPRESS=DEFLATE -co LEVEL=9 "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_GSLC"
if [ -s "$OUTPUT_COG_GSLC" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# Test 2.10: Tiled GeoTIFF Creation
echo -n "  - Test 2.10: Creating a tiled GeoTIFF... "
rm -f "$OUTPUT_TILED_TIF"
gdal_translate -q -of GTIFF -co "TILED=YES" -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_TILED_TIF"
if [ -s "$OUTPUT_TILED_TIF" ]; then
    echo -e "${GREEN}PASSED${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi

# 3. Performance Comparison
echo
echo "STEP 3: Running performance comparison..."

# Test 3.1: Time NISAR driver
echo -n "  - Test 3.1: Timing your NISAR driver... "
REAL_TIME=$( { time gdal_translate -q -of COG "NISAR:${GDAL_S3_PATH}:${SUBDATASET_NISAR}" "$OUTPUT_COG_NISAR" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}' )
echo -e "${GREEN}Finished in ${REAL_TIME}${NC}"

# Test 3.2: Time standard netCDF driver
echo -n "  - Test 3.2: Timing standard netCDF driver... "
NETCDF_DRIVER_PATH="NETCDF:\"/vsicurl/${HTTPS_URL}\":${SUBDATASET_NETCDF}"
{ time GDAL_SKIP=NISAR gdal_translate -q -of COG "${NETCDF_DRIVER_PATH}" "$OUTPUT_COG_NETCDF" > /dev/null 2>&1; } 2> netcdf_time.txt || true
NETCDF_TIME=$(cat netcdf_time.txt | grep real | awk '{print $2}')
if [ -n "$NETCDF_TIME" ]; then
    echo -e "${GREEN}Finished in ${NETCDF_TIME}${NC}"
else
    echo -e "${RED}Failed to complete.${NC}"
fi


# 4. Cleanup
echo
echo "STEP 4: Cleaning up..."
conda deactivate
rm -f "$OUTPUT_COG_NISAR" "$OUTPUT_REPROJECT_TIF" "$OUTPUT_AMPLITUDE_TIF" "$OUTPUT_PHASE_TIF" "$OUTPUT_COG_GSLC" "$OUTPUT_TILED_TIF" netcdf_time.txt
echo "You can remove the test environment with: conda env remove -n ${CONDA_ENV_NAME}"

echo
echo -e "${GREEN} All GSLC tests completed successfully! ${NC}"
