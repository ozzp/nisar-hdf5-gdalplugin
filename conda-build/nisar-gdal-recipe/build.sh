#!/bin/bash

set -ex # Exit on error and print commands

mkdir build
cd build

# Configure the build. CMake will find libgdal-core and HDF5 in the
# conda environment automatically.
cmake .. ${CMAKE_ARGS} \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_PREFIX_PATH=$PREFIX \
    -DCMAKE_VERBOSE_MAKEFILE=ON

# Compile the plugin
make -j${CPU_COUNT}

# Install the plugin into the correct gdalplugins directory
make install

# Verify the plugin actually ended up in the right place.
# GDAL expects plugins in $PREFIX/lib/gdalplugins
if [ ! -f "$PREFIX/lib/gdalplugins/gdal_NISAR.so" ]; then
    echo "ERROR: Plugin was not installed to $PREFIX/lib/gdalplugins"
    echo "Check your CMakeLists.txt install destination."
    exit 1
fi
