````markdown
# Building the GDAL NISAR Plugin

This document provides instructions for building the `gdal-driver-nisar` conda package for two target platforms:
* **macOS arm64** (e.g., Apple Silicon M1/M2/M3)
* **Linux x86_64** (the most common Linux architecture)

## Prerequisites

1.  **Conda/Mamba**: A working installation of Conda or Mamba is required for the native macOS build.
2.  **Docker Desktop**: Required for cross-compiling the `linux-64` package on a macOS machine.

---

## Recipe Files

The build process relies on the following key files in the project directory.

### `meta.yaml`

This file defines the package metadata and dependencies.

```yaml
{% set name = "gdal-driver-nisar" %}
{% set version = "0.1.0" %}

package:
  name: {{ name|lower }}
  version: {{ version }}

source:
  path: .

build:
  number: 0

requirements:
  build:
    - {{ compiler('cxx') }}
    - cmake
    - make

  host:
    - libgdal
    - hdf5

  run:
    - libgdal
    - hdf5

test:
  requirements:
    - gdal
    - libgdal
  commands:
    # Test that the driver is registered with GDAL
    - gdalinfo --formats | grep NISAR

about:
  home: # https://github.com/ozzp/nisar-hdf5-gdalplugin/
  license: # Apache-2.0
  summary: 'A GDAL plugin to read NISAR HDF5 files.'
````

### `build.sh`

This script compiles the C++ plugin. It is cross-platform and does not need to be changed.

```bash
#!/bin/bash

set -ex # Exit on error and print commands

mkdir build
cd build

# Configure the build.
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_PREFIX_PATH=$PREFIX

# Compile the plugin
make -j${CPU_COUNT}

# Install the plugin
make install
```

-----

## Building for macOS (Native)

Building for native architecture (`osx-arm64`) is straightforward.

1.  Open terminal in the project directory.
2.  Run the `conda build` command:
    ```bash
    conda build . --output-folder ./conda-bld
    ```
3.  The final package will be located in `./conda-bld/osx-arm64/`.

-----

## Building for Linux x86\_64 (Cross-Platform)

Building for `linux-64` on a Mac requires using Docker to create an emulated Linux environment.

### Use the Dockerfile from this project's root.

### Build the Docker Image using the Dockerfile in this project's root.

```bash
docker build --platform linux/amd64 -t conda-builder .
```

### Build the Conda Package

Run the container and execute the build command inside it.

1.  **Start the container**:

    ```bash
    docker run --rm -it -v "$(pwd)":/build_space conda-builder
    ```

2.  **Run `conda build` inside the container**:

    ```bash
    conda build . --output-folder /build_space/conda-bld
    ```

3.  **Exit the container** by typing `exit`. The final package will be located in `./conda-bld/linux-64/`.

<!-- end list -->

```
```
