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

This file is used to create the Linux build environment.

```dockerfile
# Use AlmaLinux as the base image
FROM almalinux:latest

# Set metadata for the image
LABEL maintainer="Your Name <you@example.com>"
LABEL description="Docker image for building linux-64 conda packages with Miniconda."

# Set environment variables
ENV LANG="C.UTF-8"
ENV LC_ALL="C.UTF-8"
ENV PATH="/opt/conda/bin:$PATH"

# Install system dependencies, including glibc for QEMU emulation
RUN dnf update -y && \
    dnf install -y \
      glibc \
      wget \
      unzip \
      bzip2 \
      make \
      gcc-c++ \
      git && \
    dnf clean all && \
    rm -rf /var/cache/dnf/*

# Download and install the AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Verify the installation
RUN aws --version

# Install a specific version of Miniconda with Python 3.11
RUN wget "https://repo.anaconda.com/miniconda/Miniconda3-py311_24.5.0-0-Linux-x86_64.sh" -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

# Configure conda to use conda-forge exclusively and set package format
RUN conda config --system --remove channels defaults && \
    conda config --system --add channels conda-forge && \
    conda config --system --set channel_priority strict && \
    conda config --system --set conda_build.pkg_format 2

# Install conda build tools
RUN conda install --name base --yes conda-build boa libstdcxx-ng && \
    conda clean --all --force --yes

# Set up the build space
WORKDIR /build_space
COPY . .

# Set the default command
CMD ["/bin/bash"]
```

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

## Building for Linux x86\_64 (Cross-Platform)

Building for `linux-64` on a Mac requires using Docker.

1.  **Build the Docker Image**
    From terminal in the project directory, run the following command.

    ```bash
    docker build --platform linux/amd64 -t conda-builder .
    ```

2.  **Build the Conda Package**
    Start the container and execute the build command inside it.

    ```bash
    # Start the container with this project's directory mounted
    docker run --platform linux/amd64 --rm -it -v "$(pwd)":/build_space conda-builder

    # Once inside the container's shell, run the build
    conda build . --output-folder /build_space/conda-bld
    ```

3.  **Exit the container** by typing `exit`.

-----

## Build Output

After a successful build, the final `.conda` packages will be located in the `conda-bld` directory, sorted by platform:

  * `./conda-bld/osx-arm64/`
  * `./conda-bld/linux-64/`
