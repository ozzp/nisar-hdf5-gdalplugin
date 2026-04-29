# Building the GDAL NISAR Plugin

This document provides instructions for building the `gdal-driver-nisar` conda package for two target platforms:

  * **macOS arm64** (e.g., Apple Silicon M1/M2/M3)
  * **Linux x86\_64** (the most common Linux architecture)

## Prerequisites

1.  **Conda/Mamba**: A working installation of Conda or Mamba is required for the native macOS build.
2.  **Docker Desktop**: Required for cross-compiling the `linux-64` package on a macOS machine.
3.  **Conda Channel Configuration**: To avoid build failures from Anaconda's channel rate limits, your Conda installation should be configured to use the **`conda-forge`** channel exclusively. This is a one-time setup.
    Run the following commands in your terminal:
    ```bash
    conda config --remove channels defaults
    conda config --add channels conda-forge
    conda config --set channel_priority strict
    ```
4.  **HDF5 (Read-Only) S3 VFD**: Make sure HDF5 library is installed.  For access of source objects stored on AWS S3, make sure that HDF5 Version is 1.14.4 or higher and run "h5cc -showconfig" to confirm that (Read-Only) S3 VFD: yes" 

-----

## Required Files

The build process relies on the following key files.

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
  home: https://github.com/ozzp/nisar-hdf5-gdalplugin/
  license: Apache-2.0
  summary: 'A GDAL plugin to read NISAR HDF5 files.'
```

### `build.sh`

This script compiles the C++ plugin and is cross-platform.

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

### `Dockerfile`

This file is used to create the arm64 and x86_64 build environment.

# Use AlmaLinux as the base image (Alma natively supports multi-arch)
FROM almalinux:latest

# Set metadata
LABEL maintainer="Your Name <you@example.com>"
LABEL description="Multi-arch Docker image for NISAR GDAL driver building (x86_64/arm64)"

# ARG TARGETARCH is provided by Docker Buildx automatically
ARG TARGETARCH

# Set environment variables
ENV LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    PATH="/opt/conda/bin:$PATH" \
    GDAL_DRIVER_PATH="/opt/conda/lib/gdalplugins" \
    GDAL_PAM_ENABLED=NO \
    PROJ_LIB="/opt/conda/share/proj"

# 1. Install system dependencies
RUN dnf update -y && \
    dnf install -y \
      glibc \
      wget \
      unzip \
      bzip2 \
      make \
      gcc-c++ \
      git \
      tar && \
    dnf clean all && \
    rm -rf /var/cache/dnf/*

# 2. Download and install the AWS CLI v2 (Architecture-aware)
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH_NAME="aarch64"; \
    else \
        ARCH_NAME="x86_64"; \
    fi && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH_NAME}.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# 3. Install Miniconda (Architecture-aware)
# We use the aarch64 installer for arm64 and x86_64 for Intel
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        CONDA_ARCH="aarch64"; \
    else \
        CONDA_ARCH="x86_64"; \
    fi && \
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py311_24.5.0-0-Linux-${CONDA_ARCH}.sh" -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

# 4. Configure conda
# Conda-forge packages are cross-platform; conda will find the correct arch automatically
RUN conda config --system --remove channels defaults && \
    conda config --system --add channels conda-forge && \
    conda config --system --set channel_priority strict && \
    conda config --system --set conda_build.pkg_format 2

# 5. Install conda build tools
# Note: libstdcxx-ng is crucial for the C++ driver's compatibility
RUN conda install --name base --yes --override-channels -c conda-forge \
    conda-build \
    boa \
    libstdcxx-ng \
    gdal \
    proj \
    proj-data && \
    conda clean --all --force --yes

WORKDIR /build_space
COPY . .

CMD ["/bin/bash"]

-----

## Building for macOS (Native)

1.  Open terminal in the project directory.
2.  **Important Note for Homebrew Users:** The C++ compiler can sometimes get confused and use libraries from a Homebrew installation (`/opt/homebrew`) instead of the isolated Conda environment. This can cause the build to fail. To prevent this, you should temporarily hide your Homebrew directory during the build.

      * **Hide Homebrew:** Before building, run the following command. It will ask for your password.
        ```bash
        sudo mv /opt/homebrew /opt/homebrew.bak
        ```
      * **Run the Build:** Execute the `conda build` command (see step 3).
      * **Restore Homebrew:** After the build is finished, restore your Homebrew directory with this command:
        ```bash
        sudo mv /opt/homebrew.bak /opt/homebrew
        ```
3.  Run the `conda build` command:
    ```bash
    (unset CFLAGS; unset LDFLAGS; unset CXXFLAGS; unset CPPFLAGS; conda build . --no-test --override-channels -c conda-forge --output-folder ./conda-bld)
    ```
This command will:
1. Start a temporary subshell (.
2. Unset the common build-related environment variables for that subshell only.
3. Run the conda build command in that clean environment.
4. Exit the subshell ), restoring original environment variables.

This will force the compiler to use only the headers and libraries within the conda environment

-----

## Building for Linux x86_64/arm64 (Cross-Platform)

Building for `linux-64`/arm64 on a Mac requires using Docker.

1.  **Build the Docker Image**
    From terminal in the project directory, run the following command.

    ```bash
    # Create and use the builder
    docker buildx create --name mybuilder --use || docker buildx use mybuilder

    # Build and load images individually (to circumvent local Docker daemon limitations)
    docker buildx build --platform linux/amd64 -t conda-builder-x86 --load .
    docker buildx build --platform linux/arm64 -t conda-builder-arm --load .
    ```

2.  **Build the Conda Package**
    Start the container and execute the build command inside it.

    ```bash
    # Build for AWS Graviton (Native arm64 - Fast)
    docker run --platform linux/arm64 --rm -v "$(pwd)":/build_space \
    conda-builder-arm \
    conda build nisar-gdal-recipe -m nisar-gdal-recipe/conda_build_config.yaml --output-folder /build_space/conda-bld/

    # Build for Intel/AMD (Emulated x86_64 - Slower)
    docker run --platform linux/amd64 --rm -v "$(pwd)":/build_space \
    conda-builder-x86 \
    conda build nisar-gdal-recipe -m nisar-gdal-recipe/conda_build_config.yaml --output-folder /build_space/conda-bld/
    ```
-----

## Build Output

After a successful build, the final .conda packages will be located in the `conda-bld` directory, sorted by platform:

  * ./conda-bld/linux-64/ — For Intel/AMD EC2 instances (m5, c5, r5).
  * ./conda-bld/linux-aarch64/— For AWS Graviton instances (m7g, c7g, r7g).
