# Building the GDAL NISAR Plugin

This document describes how to build the `gdal-driver-nisar` conda package from source for:

- **macOS arm64** (Apple Silicon), built natively with `conda build`
- **Linux x86_64** (`linux-64`) and **Linux aarch64** (`linux-aarch64`, e.g. AWS Graviton), built inside Docker

If you only want to *use* the driver, install the published package instead
(see [README.md](README.md#installation)).

## Recipe Layout

The C++ sources and their standalone `CMakeLists.txt` live in `frmts/nisar/`; the conda recipe in
`conda-build/nisar-gdal-recipe/` builds from there (`source: path: ../../frmts/nisar`).
`frmts/nisar/CMakeLists.gdal.txt` is the equivalent build file for an in-tree GDAL build.
The files that control the build are:

| File | Role |
| ---- | ---- |
| `meta.yaml` | Package name, version, build number, build/host/run dependencies and the package test (`gdalinfo --formats \| grep NISAR`). The `run` dependencies are pinned to the `gdal`, `libgdal-core` and `hdf5` minor versions that were present at build time (`pin_compatible(..., max_pin='x.x')`). |
| `conda_build_config.yaml` | Variant pins: GDAL version (`3.12`), Python version and compiler versions (clang 16 on macOS, GCC 12 on Linux). |
| `build.sh` | Runs CMake with `$PREFIX` as prefix, builds with `make`, installs to `$PREFIX/lib/gdalplugins/` and verifies that `gdal_NISAR${SHLIB_EXT}` exists. Also copies `README.md`, `docs/HOWTO.md`, `docs/AGENT_GUIDE.md` and `.agents/skills/nisar-gdal/SKILL.md` (reached through `$RECIPE_DIR/../..`, since the recipe source is `frmts/nisar` only) to `$PREFIX/share/doc/gdal-driver-nisar/`; repository-relative links in the README are rewritten to the installed `HOWTO.md` or to GitHub so the offline copy has no dead links. |
| `CMakeLists.txt` | Defines the `gdal_NISAR` MODULE target (C++17), links `GDAL::GDAL`, `HDF5::HDF5` and `zlib-ng` (falls back to `zlib` if `zlib-ng` is not found), and sets the `.dylib` suffix / no `lib` prefix required by GDAL plugins on macOS. |
| `../../Dockerfile` (repository root) | Multi-arch AlmaLinux image with Miniconda, `conda-build`, `boa` and `conda-libmamba-solver`, used for the Linux builds. |

Always read these files rather than copies in documentation: they are the source of truth.

## Prerequisites

1. **Conda or Mamba** — required for the native macOS build and to run `conda build`.
2. **Docker Desktop** with `buildx` — required for the Linux builds (multi-arch images).
3. **conda-forge only channel configuration** — avoids build failures caused by Anaconda's
   `defaults` channel rate limits and mixed-channel solves. One-time setup:

   ```bash
   conda config --remove channels defaults
   conda config --add channels conda-forge
   conda config --set channel_priority strict
   ```

4. **`conda-build`** in your base environment:

   ```bash
   conda install -n base -c conda-forge conda-build
   ```

All compile-time dependencies (`gdal`, `libgdal-core`, `hdf5`, `zlib-ng`, `cmake`, `make`,
compilers) are resolved by `conda build` from `meta.yaml`; nothing needs to be installed
system-wide. In particular, no special HDF5 build (e.g. with the ROS3 VFD) is required: the
driver performs remote I/O through GDAL's virtual file system, not through HDF5's S3 driver.

## Versioning

Before releasing, bump all of the following so that they match the package you are about to
publish:

- `version` (and reset `build: number`) in `conda-build/nisar-gdal-recipe/meta.yaml`
- `project(VERSION ...)` in `frmts/nisar/CMakeLists.txt`, which becomes the `DRIVER_VERSION`
  metadata item (shown by `gdalinfo --format NISAR`)
- the `test_driver_version` assertion in `conda-build/tests/test_interpolation_earthaccess.py`
  and the current-version references in `README.md`, `docs/HOWTO.md` (including the version
  history table) and `.agents/skills/nisar-gdal/SKILL.md` — these files are shipped inside the
  package, so they must describe the version being built

## Building for macOS (Native)

1. Open a terminal in `conda-build/nisar-gdal-recipe/` (the C++ sources are in `../../frmts/nisar/`).
2. **Homebrew users:** the compiler can pick up headers/libraries from `/opt/homebrew`
   instead of the isolated conda environment, which breaks the build. Temporarily hide
   Homebrew for the duration of the build:

   ```bash
   sudo mv /opt/homebrew /opt/homebrew.bak
   # ... build ...
   sudo mv /opt/homebrew.bak /opt/homebrew
   ```

3. Run `conda build` in a sub-shell with the usual compiler environment variables unset, so
   that only the conda toolchain flags are used:

   ```bash
   (unset CFLAGS CXXFLAGS CPPFLAGS LDFLAGS; \
    conda build . -m conda_build_config.yaml --override-channels -c conda-forge \
        --output-folder ../../conda-bld)
   ```

   Add `--no-test` to skip the `gdalinfo --formats | grep NISAR` package test.

The resulting package is written to `conda-bld/osx-arm64/`.

## Building for Linux x86_64 / aarch64 (Docker)

Run these commands from the **repository root** (where the `Dockerfile` is).

### 1. Build the builder images

```bash
docker buildx create --name mybuilder --use || docker buildx use mybuilder

docker buildx build --platform linux/amd64 -t conda-builder-x86 --load .
docker buildx build --platform linux/arm64 -t conda-builder-arm --load .
```

The images are built one architecture at a time with `--load` because a local Docker daemon
cannot load a multi-platform manifest.

### 2. Build the conda package

The repository is bind-mounted into the container at `/build_space`; `--rm` removes the
container afterwards.

**linux-aarch64 (AWS Graviton).** Native on Apple Silicon, so this is the fast path:

```bash
docker run --platform linux/arm64 --rm -v "$(pwd)":/build_space \
  conda-builder-arm \
  conda build conda-build/nisar-gdal-recipe \
  -m conda-build/nisar-gdal-recipe/conda_build_config.yaml \
  --output-folder /build_space/conda-bld/
```

**linux-64 (Intel/AMD).** Emulated on Apple Silicon, therefore noticeably slower:

```bash
docker run --platform linux/amd64 --rm -v "$(pwd)":/build_space \
  conda-builder-x86 \
  conda build conda-build/nisar-gdal-recipe \
  -m conda-build/nisar-gdal-recipe/conda_build_config.yaml \
  --output-folder /build_space/conda-bld/
```

## Build Output

Packages are written to `conda-bld/` in the repository root, organised by platform:

| Directory | Target |
| --------- | ------ |
| `conda-bld/osx-arm64/` | Apple Silicon Macs |
| `conda-bld/linux-64/` | Intel/AMD Linux (e.g. EC2 m5 / c5 / r5) |
| `conda-bld/linux-aarch64/` | ARM Linux (e.g. AWS Graviton m7g / c7g / r7g) |

## Testing a Local Build

Install the freshly built package into a clean environment and verify that GDAL registers the
driver:

```bash
conda create -n nisar-test -c ./conda-bld -c conda-forge gdal-driver-nisar gdal
conda activate nisar-test
gdalinfo --formats | grep NISAR
gdalinfo --format NISAR        # shows DRIVER_VERSION and the open-option list
ls $CONDA_PREFIX/share/doc/gdal-driver-nisar/   # README.md HOWTO.md SKILL.md AGENT_GUIDE.md
```

An end-to-end functional and performance test script against a GSLC product on S3 is provided
in `conda-build/tests/run_tests_NISAR_GSLC.sh` (see
`conda-build/tests/Testing_NISAR_GCOV_GSLC.md`).

## Publishing

Upload the packages to the `nisar-forge` channel on Anaconda.org:

```bash
anaconda upload --user nisar-forge conda-bld/<platform>/gdal-driver-nisar-*.conda
```

Anaconda.org only hosts package files; the project documentation is not uploaded separately.
The package page shows the `summary`/`description` from `meta.yaml` and links to `home`,
`doc_url` and `dev_url`, and the same documents are installed with the package under
`share/doc/gdal-driver-nisar/`.
