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

# Install system dependencies
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

# Download and install the AWS CLI v2 (Architecture-aware)
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        ARCH_NAME="aarch64"; \
    else \
        ARCH_NAME="x86_64"; \
    fi && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH_NAME}.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Install Miniconda (Architecture-aware)
# We use the aarch64 installer for arm64 and x86_64 for Intel
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        CONDA_ARCH="aarch64"; \
    else \
        CONDA_ARCH="x86_64"; \
    fi && \
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py311_24.5.0-0-Linux-${CONDA_ARCH}.sh" -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh

# Configure conda
# Conda-forge packages are cross-platform; conda will find the correct arch automatically
RUN conda config --system --remove channels defaults && \
    conda config --system --add channels conda-forge && \
    conda config --system --set channel_priority strict && \
    conda config --system --set conda_build.pkg_format 2

# Install conda build tools
# Note: libstdcxx-ng is crucial for the C++ driver's compatibility
RUN conda install --name base --yes --override-channels -c conda-forge \
    conda-build \
    boa \
    libstdcxx-ng \
    gdal \
    hdf5 \
    proj \
    proj-data && \
    conda clean --all --force --yes

WORKDIR /build_space
COPY . .

CMD ["/bin/bash"]
