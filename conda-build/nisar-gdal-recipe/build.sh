#!/bin/bash

set -ex # Exit on error and print commands

mkdir build
cd build

# Configure the build. CMake will find libgdal-core and HDF5 in the
# conda environment automatically.
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_PREFIX_PATH=$PREFIX

# Compile the plugin
make -j${CPU_COUNT}

# Install the plugin into the correct gdalplugins directory
make install
