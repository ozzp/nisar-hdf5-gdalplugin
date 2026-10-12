# GDAL Plugin for NISAR HDF5

A read-only [GDAL](https://gdal.org) raster driver for [NASA-ISRO SAR (NISAR)](https://nisar.jpl.nasa.gov/) HDF5 products.
It exposes NISAR Level 1, Level 2 and Level 3 datasets as regular GDAL rasters so they can be
used with `gdalinfo`, `gdal_translate`, `gdalwarp`, QGIS, `rasterio`, TiTiler and any other
GDAL-based tool, and it is built for efficient, cloud-optimized access to products stored on
AWS S3 or behind HTTP.

The driver is distributed as the conda package `gdal-driver-nisar` and registers itself in
GDAL under the short name **`NISAR`**.

## Table of Contents

- [Features](#features)
- [Supported Products](#supported-products)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Connection String Syntax](#connection-string-syntax)
- [Open Options](#open-options)
- [Configuration Options (Environment Variables)](#configuration-options-environment-variables)
- [Cloud Access and AWS Authentication](#cloud-access-and-aws-authentication)
- [Usage Examples](#usage-examples)
- [Performance Notes](#performance-notes)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Repository Layout](#repository-layout)
- [Architecture Overview](#architecture-overview)
- [Data and Specifications](#data-and-specifications)
- [Related Projects and References](#related-projects-and-references)
- [License](#license)

## Features

- **Loadable GDAL plugin.** Built as a shared library (`gdal_NISAR.so` / `gdal_NISAR.dylib`) that GDAL discovers at runtime from `GDAL_DRIVER_PATH`.
- **Automatic product identification.** Recognises NISAR HDF5 files from their internal structure (`/science/<INST>/identification`) and determines the instrument (`LSAR`/`SSAR`), product type and product level.
- **Subdataset discovery.** Walks the HDF5 hierarchy and exposes every 2-D raster under `/science/<INST>/<PRODUCT>/` as a GDAL subdataset.
- **Level 1 georeferencing.** Generates Ground Control Points (GCPs) from the product's `geolocationGrid` so radar-coordinate (slant range / azimuth) images can be warped to map projections. See [Level_1_Product_Processing.md](Level_1_Product_Processing.md).
- **Level 2 / Level 3 georeferencing.** Reads the projection (EPSG) and computes the GeoTransform from the `xCoordinates` / `yCoordinates` grids for geocoded products.
- **Cloud-native I/O.** Remote files (`s3://`, `/vsis3/`, `/vsicurl/`, `http(s)://`) are read through GDAL's Virtual File System, so GDAL's HTTP range-request caching, retries and AWS credential handling all apply.
- **Chunk-aligned "mega-fetch" reads.** GDAL block size is matched to the HDF5 chunk size; neighbouring chunks are coalesced into a single large range request and decompressed in parallel.
- **Virtual overviews.** Power-of-two overview levels are synthesised on the fly so that viewers and tile servers can zoom out without pre-built pyramids.
- **Complex data support.** Complex (SLC) bands expose GDAL `DERIVED_SUBDATASET`s for `AMPLITUDE`, `PHASE`, `REAL`, `IMAG`, `INTENSITY`, `CONJ` and `LOGAMPLITUDE`.
- **Validity masks.** Optional per-band mask derived from the product's `mask` layers (GCOV and GUNW layouts are understood).
- **3-D metadata cube interpolation.** Interpolates coarse `radarGrid` metadata cubes (incidence angle, look angle, ...) onto the full-resolution grid using an external DEM.
- **Optional Kerchunk / virtual Zarr sidecar.** Can emit a JSON reference file describing the HDF5 chunk layout for use with Zarr/xarray tooling.
- **SIMD-accelerated decompression.** Links against `zlib-ng` when available (falls back to standard `zlib`).

## Supported Products

| Level | Product types | Raster location inside the HDF5 file |
| ----- | ------------- | ------------------------------------ |
| L1 (radar coordinates) | `RSLC`, `RIFG`, `RUNW` | `/science/<INST>/<PRODUCT>/swaths/frequency{A,B}/...` |
| L2 (geocoded) | `GSLC`, `GCOV`, `GUNW`, `GOFF` | `/science/<INST>/<PRODUCT>/grids/frequency{A,B}/...` |
| L3 (geocoded) | `SME2` | `/science/<INST>/<PRODUCT>/grids/radarData/frequency{A,B}/...` |

`<INST>` is `LSAR` for the L-band instrument (current products) or `SSAR` for the future S-band instrument.

## Requirements

| Component | Version | Notes |
| --------- | ------- | ----- |
| GDAL / `libgdal-core` | 3.12.x | The conda package is pinned to the GDAL minor version it was built against (see `conda-build/nisar-gdal-recipe/conda_build_config.yaml`). |
| HDF5 | as pinned by the package (currently 2.x from conda-forge) | Pulled in automatically by conda; the recipe pins the run dependency to the HDF5 minor version it was built against. |
| `zlib-ng` | any | Optional at build time; used for SIMD decompression when present. |
| OS / arch | Linux x86_64, Linux aarch64, macOS arm64 | Windows is not supported by the recipe. |

All of the above are installed automatically when you install the conda package.

## Installation

### Conda (recommended)

The plugin is published on the `nisar-forge` channel on Anaconda.org. Create (or activate) a
conda environment and run:

```shell
conda install -c nisar-forge -c conda-forge gdal-driver-nisar
```

This installs the driver together with a compatible `gdal`, `libgdal-core` and `hdf5`. A copy
of this README, [docs/HOWTO.md](docs/HOWTO.md), the `nisar-gdal` skill document and the short
[docs/AGENT_GUIDE.md](docs/AGENT_GUIDE.md) (where the driver and its documents live, aimed at
AI agents) is installed to `$CONDA_PREFIX/share/doc/gdal-driver-nisar/` for offline reference.

> **Tip:** if you already have GDAL in the environment and conda refuses to solve, the
> installed GDAL is probably a different minor version than the plugin was built against.
> Install the plugin into a fresh environment instead:
> `conda create -n nisar -c nisar-forge -c conda-forge gdal-driver-nisar gdal`.

### Verify the installation

```shell
gdalinfo --formats | grep NISAR
```

Expected output (version string may differ):

```
  NISAR -raster- (rovs): NISAR HDF5 (*.h5)
```

If nothing is printed, see [Troubleshooting](#troubleshooting).

### Building from source

Building the plugin and the conda package (natively on macOS, or for `linux-64` /
`linux-aarch64` through Docker) is documented in [BUILDING.md](BUILDING.md).

## Quick Start

A longer, task-oriented walkthrough (installation, credentials, per-product
recipes, warping, masking, performance tuning) is in [docs/HOWTO.md](docs/HOWTO.md).

```shell
# 1. List the subdatasets (rasters) in a local product
gdalinfo NISAR:/path/to/NISAR_L2_GCOV_file.h5

# 2. Inspect one subdataset
gdalinfo 'NISAR:/path/to/NISAR_L2_GCOV_file.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH'

# 3. Export it as a GeoTIFF
gdal_translate -of GTiff \
    'NISAR:/path/to/NISAR_L2_GCOV_file.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' \
    HHHH.tif
```

The same commands work on a file in S3 once credentials are configured (see
[Cloud Access and AWS Authentication](#cloud-access-and-aws-authentication)):

```shell
gdalinfo 'NISAR:s3://my-bucket/path/to/NISAR_L2_GCOV_file.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH'
```

## Connection String Syntax

```
NISAR:<file>[:<hdf5-dataset-path>]
```

| Part | Description |
| ---- | ----------- |
| `NISAR:` | Driver prefix. Optional for local `.h5` files, but recommended: it guarantees this driver (and not GDAL's generic `HDF5` driver) opens the file. |
| `<file>` | A local path, `s3://bucket/key`, `/vsis3/bucket/key`, `/vsicurl/https://...` or a plain `https://...` URL. `s3://` is rewritten to `/vsis3/` and `http(s)://` to `/vsicurl/` internally. |
| `<hdf5-dataset-path>` | Optional absolute HDF5 path of the raster to open, e.g. `/science/LSAR/GSLC/grids/frequencyA/HH`. When omitted the driver opens the file as a *container* and lists the available rasters in the `SUBDATASETS` metadata domain. |

Quote the whole string in the shell: the `:` separators and the `//` in URLs are otherwise
easy to mangle.

Instead of spelling out the HDF5 path you can also select a raster with the `INST`, `FREQ`
and `POL` [open options](#open-options).

## Open Options

Open options are passed with `-oo NAME=VALUE` on the command line or the `open_options`
argument of `gdal.OpenEx()` in Python.

| Option | Type / values | Default | Description |
| ------ | ------------- | ------- | ----------- |
| `INST` | `LSAR`, `SSAR` | `LSAR` | Instrument group to open when no HDF5 path is given in the connection string. |
| `FREQ` | `A`, `B` | `A` | Frequency sub-band to open. |
| `POL` | polarization, e.g. `HH`, `VV`, `HHHH` | `HH` (`HHHH` for GCOV) | Polarization / covariance term to open. Validated against the product's `listOfPolarizations` / `listOfCovarianceTerms`. Setting any of `INST`, `FREQ`, `POL` makes the driver open that single raster instead of the container. |
| `METADATA` | `ALL` or a comma-separated list of `ATTITUDE`, `CALIBRATIONINFORMATION`, `CEOSANALYSISREADYDATA`, `ORBIT`, `PROCESSINGINFORMATION`, `RADARGRID`, `SOURCEDATA` | *(none)* | Loads the selected `/metadata/...` HDF5 groups and exposes them as GDAL metadata domains named `NISAR_<GROUP>` (e.g. `NISAR_ORBIT`). Off by default to keep `gdalinfo` fast. |
| `MASK` | `YES` / `NO` | `NO` | Attach a per-band validity mask built from the product's mask layer (`GDAL_MASK_FLAGS`, `GetMaskBand()`). |
| `DEM_FILE` | path or `/vsis3/...` URL | *(none)* | DEM used for [3-D metadata cube interpolation](#3-d-metadata-cube-interpolation). Required when `QUANTITY` is set. |
| `DEM_RESAMPLING` | `NEAREST`, `BILINEAR`, `CUBIC`, `CUBICSPLINE` | `CUBICSPLINE` | Resampling method used when warping the DEM onto a geocoded (L2/L3) grid. |
| `DEM_NODATA_HEIGHT` | metres | `0` | Level 1 interpolation: height assumed where the DEM is nodata, masked or absent. Must be finite. |
| `QUANTITY` | cube name, e.g. `incidenceAngle` | *(none)* | Switches the driver into interpolation mode. The cube is the dataset named in the connection string, or `/science/<INST>/<PRODUCT>/metadata/radarGrid/<QUANTITY>` (L2/L3) / `.../metadata/geolocationGrid/<QUANTITY>` (L1) when only the file is given. Must be combined with `DEM_FILE`. |
| `DUMP` | `YES` / `NO` | `NO` | Optional: advertise the `NISAR_DUMP` domain in the domain list (`gdalinfo -mdd all`). Not needed for `gdalinfo -mdd NISAR_DUMP`; never changes `SUBDATASETS`. See [Inspecting the full HDF5 hierarchy](#inspecting-the-full-hdf5-hierarchy--mdd-nisar_dump). |
| `DUMP_ROOT` | HDF5 group path | `/science/<INST>` | Group where the `NISAR_DUMP` traversal starts; `/` dumps the whole file. Must exist. `<INST>` is the instrument of an explicit raster path, else `INST`, else the detected one. |
| `DUMP_MODE` | `HEADER` / `FULL` | `HEADER` | `HEADER` prints objects, datatypes, dataspaces and attributes only; `FULL` also prints values of scalars and 1-D datasets up to `NISAR_DUMP_MAX_ELEMENTS` elements. |
| `ENABLE_PAGE_BUFFERING` | `YES` / `NO` | `NO` | Reserved for a discovery pass that aligns the HDF5 page buffer. Currently the driver always uses a 4 MiB page buffer regardless of this setting. |

Example:

```shell
gdalinfo -oo FREQ=A -oo POL=VV -oo METADATA=ORBIT,RADARGRID NISAR:/path/to/NISAR_L2_GSLC_file.h5
```

## Configuration Options (Environment Variables)

These are GDAL *configuration options*: set them as environment variables, with
`--config NAME VALUE` on the command line, or with `gdal.SetConfigOption()` in Python.

| Option | Default | Description |
| ------ | ------- | ----------- |
| `NISAR_PREFETCH_GRID` | `1` | Size (in blocks) of the square block grid that is fetched in a single request when a block is missing from the cache. `1` = fetch only the requested block (best for tile servers and small windows). Larger values (e.g. `24`) coalesce many chunks into one large range read, which is much faster for full-frame batch processing. |
| `NISAR_MAX_MEGAFETCH_BYTES` | `16777216` (16 MiB) | Upper bound on the size of one coalesced ("mega-fetch") read. Prevents `NISAR_PREFETCH_GRID` from producing requests that are too large for the network or memory. |
| `NISAR_MAX_VIRTUAL_OVR` | `16` | Largest decimation factor for which a virtual overview is synthesised (powers of two up to this value). Set to `1` to disable virtual overviews. |
| `NISAR_DUMP_MAX_ELEMENTS` | `64` | Largest value (in elements) printed in the `NISAR_DUMP` domain: scalar / 1-D datasets in `DUMP_MODE=FULL`, and attributes in either mode. Anything bigger is reported as `DATA { (N elements, not printed) }`. |
| `NISAR_DUMP_MAX_STRING_CHARS` | `2048` | Longest string value (in UTF-8 code points; never split mid-character) printed in the `NISAR_DUMP` domain before truncation with `...`. Also bounds how much of a string *dataset* is read: fixed-length strings are read through a memory type of this size (×4 bytes for UTF-8), and a variable-length string array whose payload exceeds `elements × (4 × cap + 1)` bytes is not read at all (`(variable-length string, N bytes, not printed)`). Variable-length string *attributes* and *scalar* variable-length string datasets are read whole. |
| `NISAR_EXPORT_ZARR` | `NO` | When `YES`, writes a Kerchunk-style JSON sidecar (`/tmp/nisar_kerchunk_<dataset>.json`) describing the HDF5 chunk map of the opened raster, for use with Zarr / xarray tooling. |
| `GDAL_NUM_THREADS` | number of CPUs | Number of threads used to decompress chunks in parallel. `ALL_CPUS` or unset uses every hardware thread. |
| `GDAL_HTTP_MAX_RETRY` | GDAL default | Number of retries GDAL performs on failed HTTP range requests. Setting it (e.g. `5`) makes long remote reads robust to transient S3 errors. |
| `GDAL_CACHEMAX` | GDAL default | Size of GDAL's block cache. Increasing it (e.g. `2048` MB) helps when repeatedly reading large remote rasters. |
| `GDAL_DRIVER_PATH` | conda sets `$CONDA_PREFIX/lib/gdalplugins` | Directory GDAL scans for plugins. Only needs to be set if the driver is installed in a non-standard location. |

Standard GDAL `/vsis3/` and `/vsicurl/` options (`AWS_*`, `CPL_VSIL_CURL_*`, `GDAL_HTTP_*`,
`VSI_CACHE`, ...) also apply; see the
[GDAL virtual file systems documentation](https://gdal.org/en/stable/user/virtual_file_systems.html).

## Cloud Access and AWS Authentication

Remote files are read through GDAL's `/vsis3/` (S3) and `/vsicurl/` (HTTP) virtual file
systems. This means **authentication works exactly as for any other GDAL driver** and no
NISAR-specific configuration is needed. GDAL resolves S3 credentials in this order:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`) environment variables
2. The `~/.aws/credentials` and `~/.aws/config` files, using `AWS_PROFILE` / `AWS_DEFAULT_PROFILE` (or `default`)
3. EC2 instance / ECS task / IAM role credentials

Set `AWS_REGION` (or `AWS_DEFAULT_REGION`) to the bucket's region, e.g. `us-west-2`.
For public buckets set `AWS_NO_SIGN_REQUEST=YES`.

### Shell / command line

The simplest option is to point GDAL at an existing profile:

```shell
export AWS_PROFILE=saml-pub
export AWS_REGION=us-west-2
gdalinfo 'NISAR:s3://my-bucket/path/to/file.h5'
```

If your profile is backed by SSO/SAML and GDAL cannot refresh it, export temporary credentials
into the environment instead. Either use the AWS CLI:

```shell
export AWS_REGION=us-west-2
eval $(aws configure export-credentials --format env --profile saml-pub)
```

or the helper script shipped in this repository, which prints the same four `export`
statements from a profile in `~/.aws/credentials`:

```shell
eval $(./aws_env.sh saml-pub)
```

(`aws_creds.sh` prints the values in both shell `export` and Python `os.environ[...]` form for
copy-and-paste into a notebook.)

### Jupyter / Python

Notebook kernels do not inherit variables exported in a terminal. Either set `AWS_PROFILE`
before starting the kernel, or set the credentials from Python in the first cell. Using
`boto3` (`conda install boto3`) to resolve the profile works for SSO/SAML profiles as well:

```python
import os
import boto3

session = boto3.Session(profile_name="saml-pub")   # may open a browser for SSO login
creds = session.get_credentials().get_frozen_credentials()

os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
if creds.token:
    os.environ["AWS_SESSION_TOKEN"] = creds.token
os.environ["AWS_REGION"] = session.region_name or "us-west-2"
```

Then in any later cell:

```python
from osgeo import gdal

gdal.UseExceptions()
ds = gdal.Open("NISAR:s3://my-bucket/path/to/file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH")
print(ds.RasterXSize, ds.RasterYSize, gdal.GetDataTypeName(ds.GetRasterBand(1).DataType))
print(ds.GetMetadata())
```

## Usage Examples

All examples below use local paths; replace them with `s3://...` URLs for cloud data.

### Level 2 products (GCOV, GSLC, GUNW, GOFF)

```shell
# List subdatasets
gdalinfo NISAR:L2_GCOV.h5

# Convert a covariance term to GeoTIFF
gdal_translate -of GTiff \
    'NISAR:L2_GCOV.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' HHHH.tif

# Convert to a Cloud-Optimized GeoTIFF
gdal_translate -of COG -co COMPRESS=DEFLATE \
    'NISAR:L2_GCOV.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' HHHH_cog.tif

# Reproject to WGS 84
gdalwarp -t_srs EPSG:4326 \
    'NISAR:L2_GCOV.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' HHHH_4326.tif

# Query a pixel value at a geographic coordinate
gdallocationinfo -wgs84 \
    'NISAR:L2_GSLC.h5:/science/LSAR/GSLC/grids/frequencyA/HH' -118.25 34.05
```

### Level 1 products (RSLC, RIFG, RUNW)

L1 rasters are in radar coordinates and carry GCPs instead of a GeoTransform. Use `gdalwarp`
to geocode them:

```shell
# Highest accuracy: thin-plate-spline transform from the GCPs
gdalwarp -t_srs EPSG:4326 -tps -r cubic \
    'NISAR:L1_RSLC.h5:/science/LSAR/RSLC/swaths/frequencyA/HH' RSLC_HH_tps.tif

# Fast preview: 2nd-order polynomial transform
gdalwarp -t_srs EPSG:4326 -order 2 -r cubic \
    'NISAR:L1_RSLC.h5:/science/LSAR/RSLC/swaths/frequencyA/HH' RSLC_HH_preview.tif
```

### Complex (SLC) data: derived subdatasets

Complex bands list derived subdatasets in the `DERIVED_SUBDATASETS` metadata domain. Open
them with GDAL's `DERIVED_SUBDATASET:` prefix:

```shell
gdal_translate \
    'DERIVED_SUBDATASET:AMPLITUDE:NISAR:L2_GSLC.h5:/science/LSAR/GSLC/grids/frequencyA/HH' \
    amplitude.tif

gdal_translate \
    'DERIVED_SUBDATASET:PHASE:NISAR:L2_GSLC.h5:/science/LSAR/GSLC/grids/frequencyA/HH' \
    phase.tif
```

Available algorithms: `AMPLITUDE`, `PHASE`, `REAL`, `IMAG`, `INTENSITY`, `CONJ`, `LOGAMPLITUDE`.

### Validity masks

```shell
gdal_translate -oo MASK=YES \
    'NISAR:L2_GCOV.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH' HHHH_masked.tif
```

With `MASK=YES` the band reports a mask band (`GetMaskFlags()` / `GetMaskBand()`); GDAL
utilities honour it when warping or writing alpha/NoData.

### 3-D metadata cube interpolation

NISAR products store geometry quantities (incidence angle, look angle, ...) as coarse 3-D
cubes under `/metadata/radarGrid/` (L2/L3) or `/metadata/geolocationGrid/` (L1). The driver
can interpolate one of these cubes onto the product's full-resolution grid, using a DEM to
pick the correct height slice:

```shell
# cube resolved from QUANTITY; output on the product's frequency-A default grid
gdal_translate \
    -oo QUANTITY=incidenceAngle \
    -oo DEM_FILE=/vsis3/my-dem-bucket/copernicus_glo30_epsg4326.vrt \
    -oo DEM_RESAMPLING=BILINEAR \
    'NISAR:"L2_GCOV.h5"' incidence_angle.tif

# explicit cube path; output on the GSLC frequency-B HV grid
gdal_translate \
    -oo QUANTITY=incidenceAngle -oo FREQ=B -oo POL=HV \
    -oo DEM_FILE=/vsis3/my-dem-bucket/copernicus_glo30_epsg4326.vrt \
    'NISAR:"L2_GSLC.h5":/science/LSAR/GSLC/metadata/radarGrid/incidenceAngle' \
    incidence_angle_gslc_B.tif

# RSLC: output in radar coordinates (swath pixel/line grid, GCPs attached)
gdal_translate \
    -oo QUANTITY=incidenceAngle -oo DEM_NODATA_HEIGHT=0 \
    -oo DEM_FILE=/vsis3/my-dem-bucket/copernicus_glo30_epsg4326.vrt \
    'NISAR:"L1_RSLC.h5"' incidence_angle_rslc.tif

# RUNW: output on the multilooked interferogram/HH/unwrappedPhase grid
gdal_translate \
    -oo QUANTITY=incidenceAngle \
    -oo DEM_FILE=/vsis3/my-dem-bucket/copernicus_glo30_epsg4326.vrt \
    'NISAR:"L1_RUNW.h5"' incidence_angle_runw.tif
```

`DEM_FILE` is mandatory. The output grid is the product's imaging grid (GCOV, GSLC, GUNW,
RSLC, RIFG or RUNW, identified from the granule's metadata) selected by `INST`/`FREQ`/`POL`.
On Level-1 products the output stays in radar coordinates and carries the grid's GCPs; the
terrain height of each (slant range, zero-Doppler time) pixel is solved by fixed-point
iteration through the geolocation grid's `coordinateX`/`coordinateY` cubes and the DEM, with
`DEM_NODATA_HEIGHT` used where the DEM has no value. RSLC uses the `frequency<F>/<POL>` swath;
RIFG and RUNW use the multilooked `frequency<F>/interferogram/<POL>/wrappedInterferogram` /
`.../unwrappedPhase` grid (`POL` defaults to `HH`) with the radar axes stored next to it. When
the DEM CRS differs from the geolocation grid's (e.g. an EPSG:4326 DEM with a UTM RIFG/RUNW
grid), only the DEM window covering the grid footprint is reprojected. The resolved cube and
reference grid are reported as `NISAR_CUBE_PATH` / `NISAR_REFERENCE_GRID` metadata items.
The design is described in
[L2 3D Data Cube Interpolation Implementation Plan.md](<L2 3D Data Cube Interpolation Implementation Plan.md>).

### Inspecting the full HDF5 hierarchy (`-mdd NISAR_DUMP`)

The default container listing shows only what the driver can open as a raster. To see the
complete HDF5 contents of an L1/L2/L3 granule (groups, string and 1-D datasets, datatypes,
dataspaces and attributes) in h5dump-like form, request the `NISAR_DUMP` metadata domain.
No open option is required; the listing is built lazily when the domain is first read:

```shell
# headers only, from /science/LSAR (default root)
gdalinfo -mdd NISAR_DUMP 'NISAR:"L2_GCOV.h5"'

# one group, with values of scalars and small 1-D arrays
gdalinfo -mdd NISAR_DUMP -oo DUMP_MODE=FULL \
    -oo DUMP_ROOT=/science/LSAR/identification 'NISAR:"L2_GCOV.h5"'

# the whole file, including root attributes
gdalinfo -mdd NISAR_DUMP -oo DUMP_ROOT=/ 'NISAR:"L1_RSLC.h5"'

# DUMP=YES advertises the domain so that -mdd all includes it
gdalinfo -mdd all -oo DUMP=YES 'NISAR:"L2_GCOV.h5"'
```

```
Metadata (NISAR_DUMP):
  HDF5 "L2_GCOV.h5" FULL {
  GROUP "/science/LSAR/identification" {
  DATASET "/science/LSAR/identification/productType" {
     DATATYPE  H5T_STRING { STRSIZE 4; STRPAD H5T_STR_NULLTERM; CSET H5T_CSET_ASCII; CTYPE H5T_C_S1; }
     DATASPACE  SCALAR
     DATA { "GCOV" }
     ATTRIBUTE "description" {
        DATATYPE  H5T_STRING { STRSIZE 12; STRPAD H5T_STR_NULLTERM; CSET H5T_CSET_ASCII; CTYPE H5T_C_S1; }
        DATASPACE  SCALAR
        DATA { "Product type" }
     }
  }
  DATASET "/science/LSAR/identification/listOfFrequencies" {
     DATATYPE  H5T_STRING { STRSIZE 1; STRPAD H5T_STR_NULLTERM; CSET H5T_CSET_ASCII; CTYPE H5T_C_S1; }
     DATASPACE  SIMPLE { ( 2 ) / ( 2 ) }
     DATA { "A", "B" }
  }
  }
  }
```

Each metadata entry is one line of text. Groups enclose their children as in h5dump
(a group's `}` comes after its last descendant); object names are always absolute HDF5
paths. `DUMP_MODE=HEADER` (default) never reads dataset values; `FULL` prints scalars and
1-D datasets of at most `NISAR_DUMP_MAX_ELEMENTS` (64) elements and reports larger arrays
as `DATA { (N elements, not printed) }`, so rasters are never pulled. Attribute values are
printed in both modes under the same element cap, with the same marker when exceeded.
Normal `gdalinfo` output is unchanged: the domain is only traversed when requested, and only
appears in the domain list (`-mdd all`) with `-oo DUMP=YES`.

`DUMP_ROOT` scopes the `NISAR_DUMP` listing only; `SUBDATASETS` stays the openable-raster
inventory of the detected instrument group and is not affected by any `DUMP*` option. String,
compound, scalar and 1-D datasets therefore appear in the dump but never in `SUBDATASETS`.

Soft and external links are listed h5dump-style (`SOFTLINK … { LINKTARGET … }`,
`EXTERNAL_LINK … { TARGETFILE … TARGETPATH … }`) without being followed, and a second hard link to
an already-dumped object prints `HARDLINK "first path"` instead of repeating its contents
(current NISAR products have none of these).

**Remote files:** the listing is built once, lazily, by a single `H5Lvisit` traversal that in
HEADER mode touches object headers and attributes only (a few page-sized range requests; a
full `/science/LSAR` walk of an RSLC or GUNW granule over HTTPS took 2-4 s in testing).

### Python (`osgeo.gdal`)

```python
from osgeo import gdal

gdal.UseExceptions()

# Container: list the rasters in the product
container = gdal.Open("NISAR:L2_GCOV.h5")
for name, desc in container.GetSubDatasets():
    print(name, "-", desc)

# Open one raster with open options and read a window
ds = gdal.OpenEx(
    "NISAR:L2_GCOV.h5",
    open_options=["FREQ=A", "POL=HHHH", "MASK=YES", "METADATA=ORBIT"],
)
band = ds.GetRasterBand(1)
window = band.ReadAsArray(xoff=0, yoff=0, win_xsize=1024, win_ysize=1024)
print(window.shape, band.GetNoDataValue(), ds.GetGeoTransform())
print(ds.GetMetadata("NISAR_ORBIT"))

# h5dump-style listing of one group
dump = gdal.OpenEx("NISAR:L2_GCOV.h5",
                   open_options=["DUMP_ROOT=/science/LSAR/identification"])
print("\n".join(dump.GetMetadata_List("NISAR_DUMP")))
```

## Performance Notes

- **Block size = HDF5 chunk size.** Every `IReadBlock` maps exactly onto one HDF5 chunk, so no chunk is read twice. Read windows aligned to the chunk grid are fastest.
- **Mega-fetch.** When a block is missing from the cache, the driver reads a rectangular grid of `NISAR_PREFETCH_GRID × NISAR_PREFETCH_GRID` chunks in one contiguous range request (capped by `NISAR_MAX_MEGAFETCH_BYTES`), decompresses them in parallel (`GDAL_NUM_THREADS`) and pushes all of them into the GDAL block cache. Keep the default `1` for interactive / tiled access; raise it (e.g. `24`) for full-scene batch jobs.
- **HDF5 page buffer.** Files are opened with a 4 MiB HDF5 page buffer so metadata reads on remote files are served from a few large requests.
- **HTTP retries.** Set `GDAL_HTTP_MAX_RETRY` (e.g. `5`) so transient S3 errors do not fail the read; the driver does not change it.
- **Virtual overviews.** Overview levels are computed by decimating the full-resolution chunks; the cost is proportional to the number of chunks touched, so limit `NISAR_MAX_VIRTUAL_OVR` on very large products if zoomed-out views are slow.
- **zlib-ng.** The conda package is built against `zlib-ng`, which decompresses DEFLATE chunks significantly faster than stock `zlib`.

## Testing

Test assets live in `conda-build/tests/`:

| File | Purpose |
| ---- | ------- |
| `run_tests_NISAR_GSLC.sh <aws-profile> <s3://.../GSLC.h5>` | End-to-end validation: creates a clean conda environment, installs `gdal-driver-nisar`, and runs driver registration, subdataset discovery, `gdal_translate`/COG, `gdalwarp`, `gdallocationinfo` and derived-subdataset tests, then benchmarks the driver against GDAL's `HDF5` and `netCDF` drivers for both S3 and local access. |
| `Testing_NISAR_GCOV_GSLC.md` | Description of the test suite above and how to interpret its output. |
| `verify_hdf5_ros3.py <aws-profile> <s3://.../file.h5>` | Stand-alone diagnostic that checks whether an `h5py` build can read HDF5 files from S3 (useful when comparing against the driver, or debugging credentials). |
| `Verify_HDF5_ROS3.md` | Usage notes for the script above. |
| `test_interpolation_earthaccess.py` | `pytest` suite for metadata-cube interpolation (GCOV regression, GSLC, auto-resolved vs explicit cube, `FREQ`/`POL`, L1 rejection). Locates the granules with `earthaccess` (Earthdata `~/.netrc`) and reads them over HTTPS, or over S3 when run in `us-west-2`; set `NISAR_TEST_DATA_DIR` to use local copies. |

The conda recipe also runs `gdalinfo --formats | grep NISAR` as its package test.

## Troubleshooting

| Symptom | Cause / fix |
| ------- | ----------- |
| `gdalinfo --formats` does not list `NISAR` | The plugin is not on GDAL's plugin path. Check that `gdal-driver-nisar` is installed in the *active* environment (`conda list gdal-driver-nisar`) and that `$CONDA_PREFIX/lib/gdalplugins/gdal_NISAR.*` exists. If GDAL comes from outside conda, set `GDAL_DRIVER_PATH` to that directory. |
| `gdalinfo file.h5` opens with the `HDF5` driver instead of `NISAR` | Prefix the path with `NISAR:` (or pass `-if NISAR`). |
| `H5Fopen failed for '...'` on a remote file | Usually a credentials or region problem. Test the URL directly with `gdalinfo /vsis3/bucket/key.h5` and check `AWS_REGION`, `AWS_PROFILE`, or run `aws s3 ls s3://bucket/key.h5 --profile <profile>`. Add `--debug on` to see the HTTP requests GDAL makes. |
| `DEM_FILE open option is REQUIRED when QUANTITY is specified` | Interpolation mode needs both `QUANTITY` and `DEM_FILE`. |
| `Invalid INST open option`, `Invalid FREQ open option` or `Invalid POL open option` | Check `INST` (`LSAR`/`SSAR`), `FREQ` (`A`/`B`) and `POL` against the product's `listOfPolarizations` / `listOfCovarianceTerms`. |
| Slow full-scene reads from S3 | Increase `NISAR_PREFETCH_GRID` (e.g. `24`) and `GDAL_CACHEMAX`; make sure `GDAL_NUM_THREADS` is not limited to `1`. |
| conda cannot solve the environment | GDAL minor version mismatch. Install into a fresh environment (see [Installation](#installation)). |

Enable driver debug output with `CPL_DEBUG=NISAR_DRIVER` (or `CPL_DEBUG=ON` for everything).

## Repository Layout

```
.
├── README.md                      This file
├── BUILDING.md                    Building the plugin / conda package (macOS native, Linux via Docker)
├── docs/HOWTO.md                  Reading NISAR Products with GDAL: task-oriented user guide (also as .docx)
├── Level_1_Product_Processing.md  How GCPs are derived for L1 products
├── L2 3D Data Cube Interpolation Implementation Plan.md
├── Dockerfile                     Multi-arch (x86_64 / arm64) AlmaLinux conda-build image
├── aws_env.sh, aws_creds.sh       Helpers that print AWS credentials from a profile as export statements
├── LICENSE                        Apache-2.0
├── UPSTREAM_CHECKLIST.md          Remaining human items for upstreaming the driver into GDAL
├── frmts/nisar/                   Driver sources (GDAL in-tree layout; single source of truth)
│   ├── CMakeLists.txt             Standalone plugin build: gdal_NISAR module (GDAL, HDF5, zlib-ng)
│   ├── CMakeLists.gdal.txt        In-tree GDAL build (add_gdal_driver); copy to <gdal>/frmts/nisar/CMakeLists.txt
│   ├── nisar.cpp                  GDALRegister_NISAR(): driver metadata and open options
│   ├── nisardataset.{h,cpp}       NisarDataset: identification, path parsing, subdatasets, georeferencing, metadata
│   ├── nisarrasterband.{h,cpp}    NisarRasterBand: chunk-aligned reads, mega-fetch, overviews, masks, Zarr sidecar
│   ├── nisaroverviewband.h        Virtual overview band
│   ├── nisarinterpolated*.{h,cpp} NisarInterpolatedDataset: 3-D cube interpolation with a DEM
│   ├── hdf5vfl.{h,cpp}            HDF5 Virtual File Layer driver that routes HDF5 I/O through GDAL VSI
│   └── nisar_priv.h               Private helpers, mask types, HDF5 callbacks
└── conda-build/
    ├── nisar-gdal-recipe/         Conda recipe (builds from ../../frmts/nisar)
    │   ├── meta.yaml              Package metadata, dependencies, version
    │   ├── conda_build_config.yaml  GDAL / compiler pins
    │   └── build.sh               CMake configure + build + install into $PREFIX/lib/gdalplugins
    └── tests/                     Test script, diagnostics and their documentation
```

## Architecture Overview

- **`GDALRegister_NISAR()`** (`nisar.cpp`) creates the `GDALDriver`, declares the open-option list and wires `NisarDataset::Identify` / `NisarDataset::Open`.
- **`NisarDataset`** (`GDALDataset` subclass) parses the connection string, rewrites `s3://` / `http(s)://` to `/vsis3/` / `/vsicurl/`, opens the file with `H5Fopen`, reads `/science/<INST>/identification` to classify the product, and either enumerates subdatasets (container mode) or opens one raster. For L2/L3 rasters it derives the SRS and GeoTransform from the coordinate vectors; for L1 rasters it builds GCPs from the `geolocationGrid`. Metadata groups are loaded lazily into `NISAR_*` domains on request.
- **`NisarVFL`** (`hdf5vfl.cpp`) is a custom HDF5 Virtual File Layer driver. For remote files it is installed on the file-access property list so that every HDF5 read becomes a `VSIFReadL()` call, letting GDAL's VSI layer perform the HTTP range requests, caching, retries and authentication.
- **`NisarRasterBand`** (`GDALRasterBand` subclass) reports the HDF5 chunk size as the GDAL block size, maps chunk offsets with `H5Dchunk_iter`, and implements `IReadBlock` as a coalesced "mega-fetch" of neighbouring chunks that are decompressed in parallel. It also creates virtual overview bands, exposes derived subdatasets for complex data, attaches `NisarHDF5MaskBand` when `MASK=YES`, and can emit a Kerchunk sidecar.
- **`NisarInterpolatedDataset`** wraps a coarse 3-D `radarGrid` cube and a DEM and produces a full-resolution interpolated raster when `QUANTITY` + `DEM_FILE` are given.

The C++ sources are located in `frmts/nisar/` (the conda recipe uses
`source: path: ../../frmts/nisar`), laid out like a GDAL in-tree driver.

## Data and Specifications

- **NISAR sample data and product specifications:** https://science.nasa.gov/mission/nisar/sample-data/
- **NISAR mission:** https://nisar.jpl.nasa.gov/

## Related Projects and References

- **GDAL raster driver documentation:** https://gdal.org/en/stable/drivers/raster/
- **GDAL virtual file systems (`/vsis3/`, `/vsicurl/`):** https://gdal.org/en/stable/user/virtual_file_systems.html
- **VICAR GDAL plugin:** https://github.com/Cartography-jpl/vicar-gdalplugin
- **HDF-EOS and GDAL:** https://www.hdfeos.org/software/gdal.php
- **NISAR data reader examples (Michael Aivazis):**
  - https://github.com/aivazis/qed/tree/main/pkg/readers/nisar
  - https://github.com/aivazis/qed
  - https://github.com/pyre/pyre

## License

Copyright 2025, by the California Institute of Technology. ALL RIGHTS RESERVED. United States
Government Sponsorship acknowledged. Released under the [Apache License 2.0](LICENSE).

This software may be subject to U.S. export control laws. By accepting this software, the user
agrees to comply with all applicable U.S. export laws and regulations.
