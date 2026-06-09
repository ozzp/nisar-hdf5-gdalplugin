#!/bin/bash

set -ex # Exit on error and print commands

rm -rf build/*
mkdir build
cd build

# Configure the build for the Conda Sandbox
# Use $PREFIX for all paths, and ${SHLIB_EXT} for dynamic extension handling
cmake .. ${CMAKE_ARGS} \
    -DGDAL_INCLUDE_DIR="$PREFIX/include" \
    -DGDAL_LIBRARY="$PREFIX/lib/libgdal${SHLIB_EXT}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" \
    -DCMAKE_PREFIX_PATH="$PREFIX" \
    -DCMAKE_VERBOSE_MAKEFILE=ON

# Compile the plugin
make -j${CPU_COUNT}

# Install the plugin into the correct gdalplugins directory
make install

# Verify the plugin actually ended up in the right place
if [ ! -f "$PREFIX/lib/gdalplugins/gdal_NISAR${SHLIB_EXT}" ]; then
    echo "ERROR: Plugin was not installed to $PREFIX/lib/gdalplugins"
    exit 1
fi

echo "SUCCESS: Plugin Built and Installed!"
