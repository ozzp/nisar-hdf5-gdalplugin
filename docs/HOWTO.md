# Reading NISAR Products with GDAL

## A Practical How-To Guide for the NISAR HDF5 GDAL Driver

All product types: RSLC · RIFG · RUNW · GSLC · GCOV · GUNW · GOFF · SME2 · radar-grid metadata cubes

Prepared for NISAR science data users

21 September 2026

Reflects `gdal-driver-nisar` version 0.7.2 (built against GDAL 3.12). Behaviour statements have been checked against the driver source, the repository README and the `nisar-gdal` skill document; anything that could not be re-verified is marked *observed* or *historical*.

# Contents

1. About this guide
2. Installation and verification
3. Data access paths and credentials
4. The addressing model: containers, subdatasets, open options
5. Exploring a granule
6. Open option reference
7. Per-product recipes
   7.1 RSLC, RIFG, RUNW · 7.2 GSLC · 7.3 GCOV · 7.4 GUNW and GOFF · 7.5 SME2 · 7.6 Radar-grid metadata cubes · 7.7 Static layers
8. Reprojection and warping to latitude-longitude
9. Masking
10. Derived subdatasets and virtual overviews
11. Performance
12. Python (`osgeo.gdal`)
13. Troubleshooting
14. Version history and behaviour changes
15. Verification checklist
16. Quick reference

# 1. About this guide

The NISAR GDAL driver (conda package `gdal-driver-nisar`, distributed through the `nisar-forge` channel, source in the `nisar-hdf5-gdalplugin` repository) lets you read NISAR HDF5 granules directly with the standard GDAL command-line tools, and with anything built on GDAL: QGIS, `rasterio`, TiTiler and the `osgeo.gdal` Python bindings. You do not need to download whole granules, convert them to GeoTIFF first, or write HDF5 code. A single `gdalinfo`, `gdal_translate` or `gdalwarp` invocation can reach into a granule sitting in an S3 bucket, pull only the byte ranges it needs, and hand you a georeferenced raster.

The driver is **read-only**, registers in GDAL under the short name **`NISAR`**, and installs one shared library (`gdal_NISAR.so` on Linux, `gdal_NISAR.dylib` on macOS) into GDAL's plugin directory. It is not a Python package and there is nothing to import; once installed, every GDAL tool in that environment gains NISAR support.

This guide is a task-oriented reference. It assumes you are comfortable with SAR and InSAR concepts and with GDAL itself; it does not explain what a covariance matrix or an unwrapped interferogram is. What it does explain is how the driver maps NISAR HDF5 structure onto GDAL's dataset model (container → subdatasets, 3-D cube → bands, mask dataset → mask band, GCPs versus GeoTransform), which open options apply to which product, and where the sharp edges are.

## 1.1 How to read the command examples

Every command in this guide is a complete, runnable line. Where a command is too long for the page it is broken with a trailing backslash, which is a shell line continuation, so you can paste the whole block into bash or zsh as it stands. Where a value is a placeholder it is written in angle brackets.

Two conventions matter throughout. First, the driver is addressed through a `NISAR:` connection-string prefix, described in section 4. Second, most behaviour is controlled by GDAL open options passed as `-oo KEY=VALUE`, and these are the main thing to learn.

NISAR granule names are long enough to make a command unreadable, so the examples refer to granules through shell variables. Set them once at the start of a session and every command in this guide will run as written. The paths below are real paths from an operational bucket at the time of writing, but they will age out; replace them with your own granules, keeping the variable names.

In the operational buckets each granule sits in a directory named after the granule itself, so the paths can be built from a bucket prefix and a granule ID:

```
B=/vsis3/nisar-ops-rs-fwd/products

G=NISAR_L2_PR_GCOV_006_068_D_093_4005_DHDH_A_20251009T222629_20251009T222704_P00410_N_F_J_001
export GCOV=$B/L2_L_GCOV/2025/10/09/$G/$G.h5

G=NISAR_L1_PR_RSLC_006_068_D_093_4005_DHDH_A_20251009T222629_20251009T222704_P00410_N_F_J_001
export RSLC=$B/L1_L_RSLC/2025/10/09/$G/$G.h5

G=NISAR_L2_PR_GSLC_006_019_A_018_4005_DHDH_A_20251006T120932_20251006T121008_P00410_N_F_J_001
export GSLC=$B/L2_L_GSLC/2025/10/06/$G/$G.h5

G=NISAR_L3_PR_SME2_006_019_A_018_4005_DHDH_A_20251006T120932_20251006T121008_P00410_N_F_J_001
export SME2=$B/L3_L_SME2/2025/10/06/$G/$G.h5

# GUNW granule IDs carry two acquisition date-time pairs, so they are longer still
G=NISAR_L2_PR_GUNW_006_019_A_018_007_4000_SH_20251006T120932_20251006T121008_\
20251018T120933_20251018T121008_P05000_N_F_J_001
export GUNW=$B/L2_L_GUNW/2025/10/06/$G/$G.h5

# global public DEM, used for metadata cube interpolation in section 7.6
export DEM=/vsis3/sds-n-cumulus-prod-nisar-products/DEM/v1.2/EPSG4326/EPSG4326.vrt
```

The variables hold bare paths, without the `NISAR:` prefix, so the same variable works both on its own and with an HDF5 dataset path appended. Note that the GUNW assignment above is split across two lines with a backslash purely to fit the page; the granule ID is a single unbroken string.

## 1.2 What is verified and what is not

This edition was redrafted against the driver source for version 0.7.0 and updated for 0.7.1 and 0.7.2, the repository `README.md` and the `nisar-gdal` skill document. Three kinds of statement appear, and the guide tries to keep them apart:

- **Verified in source.** How the connection string is parsed, which open options exist and what they do, how georeferencing, masks, statistics and derived subdatasets are produced. These are stated plainly.
- ***Observed.*** Behaviour seen on a specific granule with a specific driver build (timings, memory figures, a warning printed on one product). Marked as such; not re-run for this edition.
- ***Historical.*** Behaviour of earlier releases, kept only where it explains why an old command or an inherited pipeline behaves differently. See section 14.

HDF5 layer names inside a product (for example the exact spelling of a GUNW layer) come from the NISAR product specifications, and vary with product type, instrument and specification version.

> **Rule of thumb.** Never trust a layer path from any document, including this one. Run `gdalinfo` on the granule first and copy the `SUBDATASET_n_NAME` string verbatim. Section 5 shows how.

# 2. Installation and verification

## 2.1 Installing the driver

The driver ships as a conda package that places the plugin shared library into `$CONDA_PREFIX/lib/gdalplugins`, the directory GDAL scans for plugins. The package is pinned to the GDAL minor version it was built against (3.12 for 0.7.2) and pulls in compatible `gdal`, `libgdal-core` and `hdf5` packages. Supported platforms are Linux x86_64, Linux aarch64 and macOS arm64; Windows is not supported.

To create a dedicated environment, which is the recommended approach:

```
mamba create -n nisar-env -c nisar-forge -c conda-forge gdal-driver-nisar
conda activate nisar-env
```

To add or update the driver inside an environment you already have:

```
mamba install -c nisar-forge -c conda-forge gdal-driver-nisar
```

If conda refuses to solve, the GDAL already in that environment is almost certainly a different minor version from the one the plugin was built against. Do not fight the solver; install into a fresh environment instead.

Pin an exact version in any processing pipeline whose outputs you intend to compare over time:

```
mamba install -c nisar-forge -c conda-forge gdal-driver-nisar=0.7.2
```

The package also installs this guide, the repository `README.md`, the `nisar-gdal` skill document and `AGENT_GUIDE.md` (a one-page map of the driver and its documents for AI agents) to `$CONDA_PREFIX/share/doc/gdal-driver-nisar/` for offline reference.

Building from source (natively on macOS, or for Linux through Docker) is documented in the repository's `BUILDING.md`. The plugin is a CMake project in `frmts/nisar/`; if you install it somewhere GDAL does not scan, point `GDAL_DRIVER_PATH` at that directory.

## 2.2 The stale-version trap

This deserves its own subsection because it has caught users repeatedly, and because it fails silently rather than loudly. An install or update can report success while `conda list` continues to show the old version and the old plugin binary stays in place (*observed*). The symptom is not an error; it is wrong output. In one documented case a user spent time debugging an 80 m pixel size that looked like frequency B data, when in fact the driver was simply an old build with a broken GeoTransform.

Always confirm the version from the loaded binary after installing (section 2.3), and if it has not moved, remove the package outright and install it again rather than reaching for `--force-reinstall` a second time:

```
gdalinfo --format NISAR | grep DRIVER_VERSION
# if the version is not what you expect:
conda remove gdal-driver-nisar
mamba install -c nisar-forge -c conda-forge gdal-driver-nisar
gdalinfo --format NISAR | grep DRIVER_VERSION
```

## 2.3 Verifying the driver loaded

Three checks, in increasing specificity. The first confirms GDAL can see the plugin at all:

```
gdalinfo --formats | grep NISAR
```

Expected output (the capability flags may differ slightly between GDAL builds):

```
  NISAR -raster- (rovs): NISAR HDF5 (*.h5)
```

The second confirms the plugin does not have a broken dynamic link. This matters because a plugin that fails to load prints its error on every GDAL invocation, including ones that have nothing to do with NISAR:

```
gdalinfo --version
```

If that command emits a GLIBC error rather than a version string, the plugin binary is newer than your system C library. Older hosts, including some JupyterHub base images, produce an error naming the exact missing symbol version. The fix is a driver rebuild for your platform, not a configuration change on your side.

```
# symptom of a GLIBC mismatch:
ERROR 1: /lib64/libc.so.6: version `GLIBC_2.38' not found
        (required by .../lib/gdalplugins/gdal_NISAR.so)
```

The third check asks the driver to report its own version and build date, and also prints the list of open options it registers. This is the authoritative answer to the question of which driver you are running, and it is more trustworthy than `conda list` because it comes from the binary that is actually loaded:

```
gdalinfo --format NISAR
```

Look for a `DRIVER_VERSION` entry of the form `v0.7.2 (Build Date: ...)`.

# 3. Data access paths and credentials

## 3.1 The four ways to point at a granule

The driver reads local files and remote objects through the same interface. All remote I/O goes through GDAL's Virtual File System (VSI): the driver installs a custom HDF5 Virtual File Layer so that every HDF5 read becomes a `VSIFReadL()` call, and GDAL performs the HTTP range requests, caching, retries and authentication. There is no separate HDF5 ROS3 code path any more, so the same `AWS_*` and `GDAL_HTTP_*` settings you use for any other GDAL driver apply here.

| Form | Example | Notes |
| --- | --- | --- |
| Local file | `/scratch/data/granule.h5` | Granule already on disk or on a mounted volume. Fastest, and no credentials needed. |
| GDAL virtual S3 | `/vsis3/bucket/key.h5` | Native GDAL form. Honours all `AWS_*` and `CPL_VSIL_CURL_*` settings. |
| S3 URL | `s3://bucket/key.h5` | Accepted; rewritten internally to `/vsis3/bucket/key.h5`. |
| HTTPS / CloudFront | `https://nisar.asf.earthdatacloud.nasa.gov/...` or `/vsicurl/https://...` | Rewritten internally to `/vsicurl/`. Out-of-region access; higher latency, and benefits most from the tuning in section 11. |

The file part of the connection string **must end in `.h5`**; otherwise the driver silently declines to open it and GDAL tries other drivers.

In-region access, meaning an EC2 instance in the same AWS region as the bucket, is dramatically faster than out-of-region access over CloudFront (*observed* throughout driver development). If you are doing anything at scale, put the compute next to the data.

> **Path quoting.** *Historical:* early driver versions were sensitive to single versus double slashes and to redundant quoting in S3 paths. The current parser strips one pair of double quotes around the file part and treats `://` correctly (section 4.1). If a path still fails for no apparent reason, run `gdalinfo /vsis3/bucket/key.h5` without the `NISAR:` prefix to separate a driver problem from a credentials problem.

## 3.2 Credential resolution

Because the driver relies on GDAL's `/vsis3/` layer, S3 credentials are resolved the way GDAL resolves them for every driver, in this order:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (plus `AWS_SESSION_TOKEN` for temporary credentials) in the environment.
2. The `~/.aws/credentials` and `~/.aws/config` files, using `AWS_PROFILE` / `AWS_DEFAULT_PROFILE` (or `default`).
3. EC2 instance / ECS task / IAM role credentials.

The region is not part of the credentials and must be set separately. The NISAR operational buckets are in Oregon:

```
export AWS_REGION=us-west-2
```

For a public bucket, disable request signing:

```
export AWS_NO_SIGN_REQUEST=YES
```

Partial credential sets break signing: a stale `AWS_SESSION_TOKEN` left over from a previous profile is a classic cause of `H5Fopen failed` on a remote file. `unset` the three variables before switching profiles.

## 3.3 Using an existing AWS profile

The simplest option is to point GDAL at a profile that already exists:

```
export AWS_PROFILE=saml-pub
export AWS_REGION=us-west-2
gdalinfo NISAR:"$GCOV"
```

If the profile is backed by SSO or SAML and GDAL cannot refresh it, export temporary credentials into the environment instead. Either use the AWS CLI:

```
export AWS_REGION=us-west-2
eval $(aws configure export-credentials --format env --profile saml-pub)
```

or the helper scripts shipped in the repository, which print the same `export` statements from a profile in `~/.aws/credentials`:

```
eval $(./aws_env.sh saml-pub)
./aws_creds.sh saml-pub      # same, plus os.environ[...] lines for pasting into a notebook
```

`aws configure export-credentials` can fail if you already have partial AWS environment variables set, because those take precedence over the profile. Clear them first:

```
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
eval $(aws configure export-credentials --format env --profile saml-pub)
```

*JPL-specific (from the original draft, not re-verified):* badged JPL users obtain the `saml-pub` profile through the standard login helpers (`aws-login`, then `bash aws-creds.sh saml-pub`), and the ASF DAAC buckets behind `nisar.asf.earthdatacloud.nasa.gov` need a separate profile created through `ondemand-help aws-login-daac`.

## 3.4 Jupyter and Python kernels

Notebook kernels do not inherit variables exported in a terminal. Either set `AWS_PROFILE` and `AWS_REGION` before starting the kernel, or set the credentials from Python in the first cell. Using `boto3` to resolve the profile works for SSO/SAML profiles as well:

```
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

Section 12 shows how to read data from Python once the environment is set.

# 4. The addressing model: containers, subdatasets, open options

This section is the conceptual core of the guide. Almost every difficulty users hit with the driver comes from confusion about how to name the thing they want to read.

## 4.1 The connection string

A NISAR granule is an HDF5 file holding many rasters, so it does not map onto a single GDAL dataset. The driver therefore behaves like other GDAL subdataset drivers: opening the granule gives you a **container** that lists what is inside, and you then open one specific raster.

```
NISAR:<file>[:<hdf5-dataset-path>]
```

The container form is the file path alone; the subdataset form appends the absolute HDF5 dataset path after a colon:

```
NISAR:"<path-to-granule.h5>"
NISAR:"<path-to-granule.h5>":<hdf5-dataset-path>
```

How the driver parses this, from the source:

- The `NISAR:` prefix is removed if present.
- The remainder is split at the **last colon**, unless that colon is immediately followed by `//`. So `NISAR:s3://bucket/x.h5` is a container (the only colon belongs to the URL scheme), and `NISAR:s3://bucket/x.h5:/science/LSAR/GCOV/grids/frequencyA/HHHH` is a file plus a dataset path.
- One pair of double quotes around the file part is stripped, so `NISAR:"s3://bucket/x.h5":/science/...` is accepted. This is also the form the driver itself emits in `SUBDATASET_n_NAME`.
- The file part must end in `.h5`.
- `s3://` is rewritten to `/vsis3/`; `http://` and `https://` are rewritten to `/vsicurl/`.
- Opening with update access fails with "The NISAR driver only supports read access."

Quote the whole connection string in the shell, or quote the file part as shown: the `:` separators and the `//` in URLs are otherwise easy to mangle.

> **Use the prefix.** For a *local* `.h5` file the prefix is optional: the driver recognises the HDF5 signature and probes for `/science/LSAR/identification` or `/science/SSAR/identification`. For a *remote* file without the prefix the driver only claims the file if the URL contains the string `NISAR` (it deliberately avoids opening remote files during identification). In either case which driver wins also depends on GDAL's driver order, and GDAL's generic `HDF5` driver will open the file without constructing NISAR georeferencing. Always use `NISAR:` or pass `-if NISAR` in anything scripted.

## 4.2 Addressing by open option instead of by path

Typing a full HDF5 dataset path is error-prone, so the driver accepts a semantic alternative for the principal rasters: open the container and describe the layer you want with the `INST`, `FREQ` and `POL` open options, and the driver builds the path for you.

These two commands are equivalent, and the second is easier to get right:

```
gdalinfo NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH

gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV"
```

Setting *any* of `INST`, `FREQ` or `POL` switches the driver from container mode to single-raster mode; the others take their defaults (`LSAR`, `A`, and `HHHH` for GCOV or `HH` for everything else). The constructed path is

```
/science/<INST>/<PRODUCT>/swaths/frequency<F>/<POL>              # L1: RSLC, RIFG, RUNW
/science/<INST>/<PRODUCT>/grids/frequency<F>/<POL>               # L2: GSLC, GCOV, GUNW, GOFF
/science/<INST>/<PRODUCT>/grids/radarData/frequency<F>/<POL>     # L3: SME2
```

where the product type and level are read from the granule's `identification` group. `POL` is validated against the frequency group's `listOfCovarianceTerms` (GCOV) or `listOfPolarizations` (everything else); an invalid value fails with `Invalid POL open option`.

The open-option route therefore works where a raster sits directly under the frequency group: RSLC, GSLC and GCOV, and SME2 where the layer name matches an entry of that list. It does **not** reach the nested GUNW, GOFF, RIFG and RUNW layers (`.../frequencyA/<layerGroup>/<pol>/<layer>`); for those, address by full HDF5 path copied from the subdataset list. On GUNW the open-option form fails with `The HDF5 dataset '...' does not exist`.

How the driver decides what to open, in priority order: an explicit `:<hdf5-path>` in the connection string → `INST`/`FREQ`/`POL` open options → container mode (no raster bands, `SUBDATASETS` metadata domain populated).

## 4.3 Instrument selection

NISAR carries two radars, and the HDF5 path begins with the instrument: `/science/LSAR/...` for the NASA L-band instrument and `/science/SSAR/...` for the ISRO S-band instrument. The driver detects the instrument automatically by looking for `/science/LSAR/identification` first and `/science/SSAR/identification` second, and reports it in the debug log as `INST=LSAR` or `INST=SSAR`.

The `INST` open option only matters when the driver is constructing a path from open options (section 4.2); it must be `LSAR` or `SSAR`. When you pass an explicit HDF5 path, the instrument is simply whatever the path says.

```
gdalinfo -oo INST=SSAR -oo FREQ=A -oo POL=HHHH NISAR:"<s-band-granule.h5>"
```

S-band granules from ISRO have been read successfully (*observed*), though some HDF5 link-existence diagnostics were printed on them. Product specification versions also differ between the two instruments, so do not assume identical internal layout.

# 5. Exploring a granule

## 5.1 Listing what is inside

Always start here. Open the container with no other arguments and read the `Subdatasets` block:

```
gdalinfo NISAR:"$GCOV"
```

The driver walks everything under `/science/<INST>/` and lists every numeric (non-string) HDF5 dataset. Each entry is a pair. `SUBDATASET_n_NAME` is a complete, copy-pasteable connection string in the form `NISAR:"<file>":<hdf5-path>`; `SUBDATASET_n_DESC` gives `[<dims>] <hdf5-path> (<type>)`, for example `[4320x4392] /science/LSAR/GCOV/grids/frequencyA/HHHH (Float32)` or `[21x720x748] .../radarGrid/incidenceAngle (Float32)` for a 3-D cube. Complex rasters are described `(complex, Float32)`. The dimensions are immediately useful, because they tell you whether you are looking at a frequency A grid, a frequency B grid, or a coarse metadata cube, without needing to know the posting conventions.

Granules contain a lot of subdatasets (an S-band GCOV granule has been *observed* with more than 40), so filter:

```
gdalinfo NISAR:"$GCOV" | grep HHHH
gdalinfo NISAR:"$GUNW" | grep unwrappedInterferogram
```

The container's default metadata domain also carries every scalar under `/science/<INST>/identification/`, keyed by its full path (`/science/LSAR/identification/productType`, `.../boundingPolygon`, `.../zeroDopplerStartTime`, ...):

```
gdalinfo NISAR:"$GCOV" | grep identification/
```

## 5.2 Inspecting one raster

Once you have a subdataset name, inspect it directly. This is where you confirm the georeferencing before committing to a warp:

```
gdalinfo NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH
```

Four things in that output deserve attention. The coordinate system block should name a real projected CRS, typically a UTM zone such as EPSG:32612 for L2 geocoded products (polar stereographic at high latitudes). `Origin` and `Pixel Size` should be plausible ground values in metres, not ones and zeros. `Corner Coordinates` are printed both in projected units and in degrees, and the degrees should place the granule where you expect it. Finally, the `Metadata` block carries an `HDF5_PATH` entry echoing which HDF5 dataset you actually opened, which is a useful sanity check when you used open options rather than an explicit path. The same block shows the dataset's own HDF5 attributes (`description`, `units`, `_FillValue`, `min_value`, ...) and the frequency-group scalars (`listOfPolarizations`, spacing values, `numberOfSubSwaths`, ...).

For an L1 raster add `-nogcp`, otherwise thousands of GCPs bury the rest of the output:

```
gdalinfo -nogcp NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH
```

## 5.3 Metadata

The bulk metadata groups under `/science/<INST>/<PRODUCT>/metadata/` are not loaded by default, to keep `gdalinfo` fast. Retrieving them requires two arguments together, which is a common stumbling block: `-oo METADATA=` tells the driver which groups to load, and `-mdd` tells `gdalinfo` which metadata domains to print. Neither alone gives you the full picture.

```
gdalinfo -mdd all -oo METADATA=ALL NISAR:"$GCOV"                 # everything
gdalinfo -mdd NISAR_ORBIT -oo METADATA=ORBIT NISAR:"$GCOV"       # one group
gdalinfo -mdd all -oo METADATA=ORBIT,RADARGRID NISAR:"$GSLC"     # a comma list
```

Each requested group appears as a domain named `NISAR_<GROUP>`. The metadata domains you will see:

| Domain | Content |
| --- | --- |
| default | Container: root attributes plus the `identification` scalars keyed by full path. Raster: the dataset's HDF5 attributes plus frequency-group scalars. |
| `NISAR_GLOBAL` | Root-level HDF5 attributes (CF-style discovery attributes such as `title`, `institution`, `reference_document`). |
| `SUBDATASETS` | Container only. |
| `NISAR_DUMP` | On request (`-mdd NISAR_DUMP`): h5dump-style listing of the HDF5 hierarchy under `DUMP_ROOT` (section 5.3.1). Advertised in the domain list only with `-oo DUMP=YES`. |
| `DERIVED_SUBDATASETS` | Numeric rasters only; see section 10. |
| `NISAR_ATTITUDE`, `NISAR_CALIBRATIONINFORMATION`, `NISAR_CEOSANALYSISREADYDATA`, `NISAR_ORBIT`, `NISAR_PROCESSINGINFORMATION`, `NISAR_RADARGRID`, `NISAR_SOURCEDATA` | Only when requested with `METADATA=`. |

Useful items surfaced this way include the identification block (`granuleId`, `productType`, `productVersion`, `productSpecificationVersion`), the processing information including the ISCE3 software version that generated the product, and the pointer to the companion static-layers granule (section 7.7).

## 5.3.1 Dumping the whole HDF5 hierarchy (`-mdd NISAR_DUMP`)

When you need to see *everything* in a granule, including string datasets, 1-D arrays, named datatypes and every HDF5 attribute, rather than only what the driver turns into rasters and metadata items, ask for an h5dump-style listing. It lives in its own metadata domain and is built lazily the first time that domain is requested, so it changes nothing unless you ask for it — no open option is needed:

```
gdalinfo -mdd NISAR_DUMP NISAR:"$GCOV"                                                 # /science/LSAR, headers only
gdalinfo -mdd NISAR_DUMP -oo DUMP_ROOT=/science/LSAR/identification NISAR:"$GCOV"
gdalinfo -mdd NISAR_DUMP -oo DUMP_MODE=FULL -oo DUMP_ROOT=/ NISAR:"$RSLC"                # whole file, with small values
gdalinfo -mdd all -oo DUMP=YES NISAR:"$GCOV"                                           # DUMP=YES makes -mdd all include it
```

The traversal starts at `DUMP_ROOT` (default `/science/<INST>`, where `<INST>` comes from an explicit raster path in the connection string, else `INST`, else the detected instrument; `/` is the file root) and walks every link below it in one `H5Lvisit` pass. Each line of the domain is one line of h5dump-like text:

```
Metadata (NISAR_DUMP):
  HDF5 "…/NISAR_L2_PR_GCOV_….h5" HEADER {
  GROUP "/science/LSAR" {
  GROUP "/science/LSAR/GCOV" {
  …
  DATASET "/science/LSAR/GCOV/grids/frequencyA/HHHH" {
     DATATYPE  H5T_IEEE_F32LE  (GDAL Float32)
     DATASPACE  SIMPLE { ( 4320, 4392 ) / ( 4320, 4392 ) }
     …
  }
  …
  }
  GROUP "/science/LSAR/identification" {
  DATASET "/science/LSAR/identification/absoluteOrbitNumber" {
     DATATYPE  H5T_STD_U32LE  (GDAL UInt32)
     DATASPACE  SCALAR
     ATTRIBUTE "description" {
        DATATYPE  H5T_STRING { STRSIZE 21; STRPAD H5T_STR_NULLPAD; CSET H5T_CSET_ASCII; CTYPE H5T_C_S1; }
        DATASPACE  SCALAR
        DATA { "Absolute orbit number" }
     }
  }
  …
  }
  …
  }
  }
```

Groups enclose their children as in h5dump (a group's `}` follows its last descendant, so the closing braces pile up at the end of each subtree); every object name is an absolute HDF5 path, so lines are meaningful on their own when grepping.

Links are reported the way h5dump does, and are not followed: a soft link prints as `SOFTLINK "path" { LINKTARGET "target" }`, an external link as `EXTERNAL_LINK "path" { TARGETFILE "file" TARGETPATH "object" }`, and a second hard link to an object that was already dumped prints as `GROUP|DATASET "path" { HARDLINK "first path" }` (the object's contents appear once, under the first name encountered). Current NISAR products contain hard links only, one per object, so these records only show up in non-standard files.

`DUMP_MODE=HEADER` (default) prints object headers, datatypes, dataspaces and attributes, including attribute values, but never reads dataset values. `DUMP_MODE=FULL` adds a `DATA { … }` line for scalars and 1-D datasets of at most `NISAR_DUMP_MAX_ELEMENTS` elements (config option, default 64); anything larger is reported as `DATA { (N elements, not printed) }`, so a FULL dump never pulls raster pixels. The same element cap applies to attribute values in both modes (an attribute with more elements gets the same `(N elements, not printed)` marker instead of its values). Strings longer than `NISAR_DUMP_MAX_STRING_CHARS` (default 2048) are truncated with `...`; the cap counts UTF-8 code points and never splits a multi-byte character. It also bounds the *read* of string datasets in FULL mode: fixed-length strings are read through a memory type of that many characters (4 bytes per character for `H5T_CSET_UTF8`), and a variable-length string array whose total payload exceeds `elements × (4 × cap + 1)` bytes is reported as `(variable-length string, N bytes, not printed)` rather than read. Variable-length string attributes and scalar variable-length string datasets are read whole (their values are small in NISAR products).

`-oo DUMP=YES` is optional and does one thing: it adds `NISAR_DUMP` to the dataset's metadata domain list, so `gdalinfo -mdd all` and `GetMetadataDomainList()` include it. Without the option a plain `gdalinfo` never traverses the hierarchy. It does not change `SUBDATASETS`, which remains the inventory of openable rasters under the detected instrument group (`/science/LSAR` or `/science/SSAR`) — string, compound, scalar and 1-D datasets are visible in the dump only, and `DUMP_ROOT` scopes the dump only.

An explicit `DUMP_ROOT` must name an existing HDF5 *group* (not a dataset, not empty) or the open fails; an invalid `DUMP_MODE` also fails the open.

**Remote files.** The listing is built lazily, the first time the `NISAR_DUMP` domain is read, and cached. In HEADER mode it reads object headers and attributes only, which on a remote granule means a handful of page-sized range requests (*observed*: 2-4 s over HTTPS for a full `/science/LSAR` walk of an RSLC or GUNW granule, on top of the normal open). FULL mode additionally reads the small datasets it prints; large arrays are still skipped.

## 5.4 Statistics: know what `-stats` actually returns

The driver overrides `GetStatistics()`, `GetMinimum()` and `GetMaximum()` to avoid scanning a remote raster when the producer already wrote the answer. If the HDF5 dataset carries `min_value`/`max_value` (or `valid_min`/`valid_max`) attributes, **or** the caller allows approximation (`gdalinfo -approx_stats`), the driver returns those attribute values without touching pixels. Mean and standard deviation come from `mean_value`/`sample_stddev` attributes if present; otherwise they are **synthesised** as `(min+max)/2` and `(max-min)/6`. With no attributes at all, `-approx_stats` falls back to `0` and `5`.

Only `gdalinfo -stats` on a band *without* min/max attributes triggers a real pixel scan (which then does honour the mask band if `MASK=YES`):

```
gdalinfo -stats -oo FREQ=A -oo POL=VVVV NISAR:"$GCOV"
```

If you need measured statistics, translate to GeoTIFF first and run `gdalinfo -stats` on that, or compute them in NumPy. Do not quote driver-reported mean or standard deviation in a report unless you have confirmed the dataset carries `mean_value`/`sample_stddev`.

## 5.5 Point queries

To read a single pixel value without extracting anything, use `gdallocationinfo` with pixel and line coordinates, or with a geographic coordinate on a geocoded product:

```
gdallocationinfo NISAR:"$SME2":/science/LSAR/SME2/grids/radarData/frequencyA/sigma0HH 800 800

gdallocationinfo -wgs84 NISAR:"$GSLC":/science/LSAR/GSLC/grids/frequencyA/HH -118.25 34.05
```

Because granules use HDF5 paged aggregation with a 4 MiB page, a single-pixel read on a remote file still costs at least one page (*observed*); it is fast, but not free.

## 5.6 Debugging with `CPL_DEBUG`

When the driver does something you do not expect, turn on GDAL's debug output. `CPL_DEBUG=NISAR_DRIVER` shows only the driver's messages; `CPL_DEBUG=ON` shows everything, including the HTTP requests GDAL makes:

```
CPL_DEBUG=NISAR_DRIVER gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV"
```

The log tells you which product it identified and at what level, whether it routed I/O through the VSI layer, where it found coordinate arrays for the GeoTransform, which EPSG code it read, and how many GCPs it set. Lines look like this:

```
NISAR_DRIVER: Identified Product: INST=LSAR, Type=GCOV, Level=L2 (L1=0, L2=1, L3=0)
NISAR_DRIVER: Remote cloud file detected. Routing HDF5 through GDAL VSIL VFL...
NISAR_DRIVER: Set HDF5 FAPL page buffer to 4 MiB.
NISAR_DRIVER: GetGeoTransform: Found coordinate arrays at: /science/LSAR/GCOV/grids/frequencyA
NISAR_DRIVER: GetGeoTransform: Derived from coordinates. Origin=(...) Res=(...)
NISAR_DRIVER: Successfully imported EPSG:32612.
```

## 5.7 The granule footprint

The bounding polygon lives at `/science/LSAR/identification/boundingPolygon` as an HDF5 string scalar, not a raster. The driver builds GDAL raster bands from numeric arrays only, so string datasets are skipped by the subdataset listing (unless `DUMP=YES`, section 5.3.1) and cannot be opened as a dataset.

The driver does read the value into the container's default metadata domain, keyed by its full path, so retrieve it as a metadata item:

```
gdalinfo NISAR:"$GCOV" | grep boundingPolygon
```

```
from osgeo import gdal
ds = gdal.Open('NISAR:granule.h5')
wkt = ds.GetMetadataItem('/science/LSAR/identification/boundingPolygon')
```

The driver is a raster driver only; there is no OGR vector layer for footprints, so `ogrinfo` and `ogr2ogr` do not apply.

# 6. Open option reference

Open options are passed as `-oo KEY=VALUE` and may be repeated; every GDAL utility accepts them, and `gdal.OpenEx(..., open_options=[...])` takes them in Python. They are registered in the driver's `DMD_OPENOPTIONLIST`, so `gdalinfo --format NISAR` prints the authoritative list for the version you are running. GDAL matches option names case-insensitively; the canonical spelling is upper case and that is what this guide uses. (*Historical:* earlier release notes wrote `freq` and `pol` in lower case. Both spellings work.)

## 6.1 All options

| Option | Values | Default | Meaning |
| --- | --- | --- | --- |
| `INST` | `LSAR`, `SSAR` | `LSAR` | Instrument group, used only when constructing a path from open options. |
| `FREQ` | `A`, `B` | `A` | Frequency sub-band. Frequency B is absent from many granules and has a coarser posting. |
| `POL` | product-dependent, see 6.2 | `HHHH` for GCOV, `HH` otherwise | Polarization or covariance term. Validated against the granule's `listOfCovarianceTerms` / `listOfPolarizations`. Setting any of `INST`, `FREQ`, `POL` opens that single raster instead of the container. |
| `METADATA` | `ALL` or a comma list of `ATTITUDE`, `CALIBRATIONINFORMATION`, `CEOSANALYSISREADYDATA`, `ORBIT`, `PROCESSINGINFORMATION`, `RADARGRID`, `SOURCEDATA` | none | Loads the named `/metadata/<group>` trees into `NISAR_<GROUP>` metadata domains. Pair with `gdalinfo -mdd`. |
| `MASK` | `YES`, `NO` | `NO` | Expose the product validity mask as the GDAL mask band. See section 9. |
| `DEM_FILE` | path, `/vsis3/...` or `/vsicurl/...` URL | none | DEM for 3-D metadata cube interpolation. Required when `QUANTITY` is set; has no effect otherwise. |
| `DEM_RESAMPLING` | `NEAREST`, `BILINEAR`, `CUBIC`, `CUBICSPLINE` | `CUBICSPLINE` | Resampling used when warping the DEM onto a geocoded (L2/L3) target grid. Not used on Level 1, where the DEM is sampled bilinearly at each solved ground point. |
| `DEM_NODATA_HEIGHT` | metres | `0` | Level 1 only: height assumed where the DEM is nodata, masked or has no coverage (e.g. ocean). Must be finite (`nan`/`inf` are rejected). |
| `QUANTITY` | cube name, e.g. `incidenceAngle` | none | Its presence routes the open to the cube-interpolation dataset (section 7.6). With a bare `NISAR:"file.h5"` the cube is resolved to `/science/<INST>/<PRODUCT>/metadata/radarGrid/<QUANTITY>` (L2/L3) or `.../metadata/geolocationGrid/<QUANTITY>` (L1); an explicit HDF5 path in the connection string overrides it. The reference grid follows `INST`/`FREQ`/`POL`. |
| `DUMP` | `YES`, `NO` | `NO` | Optional: advertise the `NISAR_DUMP` domain in the domain list (`gdalinfo -mdd all`). Not needed for `gdalinfo -mdd NISAR_DUMP`, which always works; does not change `SUBDATASETS`. Section 5.3.1. |
| `DUMP_ROOT` | HDF5 group path | `/science/<INST>` | Where the dump traversal starts; `/` dumps the whole file. Must exist. |
| `DUMP_MODE` | `HEADER`, `FULL` | `HEADER` | `HEADER` prints objects, datatypes, dataspaces and attributes; `FULL` also prints values of scalars and 1-D datasets up to `NISAR_DUMP_MAX_ELEMENTS` elements. |
| `ENABLE_PAGE_BUFFERING` | `YES`, `NO` | `NO` | Reserved. The driver always uses a 4 MiB HDF5 page buffer; this option has no other effect today. |

There are **no `LAYER` or `MEASURE` options**. Earlier drafts of this guide described them; they do not exist in the driver. GUNW, GOFF, RIFG and RUNW layers are addressed by full HDF5 path (section 7.4).

## 6.2 Polarization values by product

GCOV holds covariance-matrix terms, so its `POL` values are four characters: the diagonal terms `HHHH`, `HVHV`, `VHVH` and `VVVV` are real-valued, while off-diagonal terms such as `HHHV` are complex. Products that hold a single-polarization signal, including RSLC, GSLC and GUNW, use two-character values such as `HH`, `HV`, `VH` and `VV`. Compact-polarimetric and S-band granules introduce further values such as `RHRH`, so list the granule rather than guessing; the driver will tell you when a value is not in the granule's list.

# 7. Per-product recipes

Paths below follow the driver's own construction rules and the NISAR product specifications; the concrete layer names inside them must be confirmed against the subdataset list of the granule in front of you.

## 7.1 RSLC, RIFG and RUNW (Level 1)

Level 1 products are in radar geometry: range and azimuth, not a map projection. Their rasters live under `/science/<INST>/<PRODUCT>/swaths/frequency{A,B}/...` (note `swaths`, where the geocoded products use `grids`). The driver attaches **Ground Control Points** built from `metadata/geolocationGrid` rather than an affine GeoTransform, and everything about handling L1 follows from that. `GetGeoTransform()` deliberately fails for L1, so tools that insist on an affine transform will complain.

List the swath rasters, then open one. Adding `-nogcp` suppresses the GCP listing, which is otherwise long enough to bury the rest of the output:

```
gdalinfo NISAR:"$RSLC"

gdalinfo -nogcp NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH
gdalinfo -nogcp -oo FREQ=A -oo POL=HH NISAR:"$RSLC"      # same raster via open options
```

Drop `-nogcp` when you actually want to examine the GCPs, for instance to check their spacing before a warp. RSLC data are complex, so the derived subdatasets of section 10 apply.

GCP line coordinates follow the product spec, `(t - swaths/zeroDopplerTime[0]) / zeroDopplerTimeSpacing`; pixel coordinates use `slantRange[0]` and `slantRangeSpacing` from `swaths/frequencyA` regardless of which raster you opened (inference from source, not observed), so treat frequency-B L1 georeferencing with suspicion and verify it independently.

There is no `mask` dataset alongside L1 rasters, so `MASK=YES` yields GDAL's all-valid mask. (Earlier drafts stated that RSLC validity is derived on the fly from acquisition start/stop vectors; the current driver does not do this.)

## 7.2 GSLC (Level 2)

GSLC is geocoded complex imagery. It behaves like GCOV for addressing purposes, with two-character polarization values, and like GCOV it carries a real projected CRS and a proper GeoTransform:

```
gdalinfo NISAR:"$GSLC"

gdalinfo -oo FREQ=A -oo POL=HH NISAR:"$GSLC"
```

Because the pixels are complex, the derived subdatasets described in section 10 are particularly useful here: they let you get amplitude, phase or intensity without writing any code.

Metadata cube interpolation (section 7.6) works on all three in radar coordinates: the output has the reference raster's pixel/line grid, carries its GCPs, and the terrain height at each pixel is solved through the geolocation grid's `coordinateX`/`coordinateY` cubes and the DEM. RSLC uses the `frequency<F>/<POL>` swath; RIFG and RUNW use the multilooked `frequency<F>/interferogram/<POL>/wrappedInterferogram` (RIFG) or `.../unwrappedPhase` (RUNW) grid, whose `slantRange`/`zeroDopplerTime` axes live in the `interferogram` group. ROFF is not routed.

## 7.3 GCOV (Level 2)

GCOV is the best-exercised product and a good place to build intuition. The principal rasters are the covariance terms under `grids`:

```
gdal_translate -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV" gcov_hhhh.tif
```

There is also a mask raster you can read as a layer in its own right, which is worth doing when you want to understand why pixels are being dropped. Opened directly, the driver gives it NoData 255, a colour table and category names (`Invalid or partially focused`, `Valid (Sub-swath 1)` ... `Valid (Sub-swath 5)`):

```
gdalinfo NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/mask
```

### Noise equivalent backscatter and other calibration grids

Noise equivalent backscatter is not one of the standard grid rasters. It sits on a coarser calibration grid under `metadata/calibrationInformation`, with its own `xCoordinates`/`yCoordinates` vectors and `projection` dataset. The driver's GeoTransform lookup walks *up* from the opened dataset until it finds a sibling `xCoordinates`, and it does this for any path containing `/grids/`, `/calibrationInformation/` or `/radarGrid/`, so these rasters come back georeferenced.

Two things to internalise. First, the calibration grid and the principal grid have deliberately different corner coordinates and extents; seeing different corners between the two is expected and is not evidence of a bug. Second, a pixel size near 1 unit means the GeoTransform was *not* found (*historical:* this was the failure mode before calibration grids were handled), and warping such a raster produces a tiny square in the wrong place on the map. Inspect before warping:

```
gdalinfo NISAR:"$GCOV":\
  /science/LSAR/GCOV/metadata/calibrationInformation/frequencyA/noiseEquivalentBackscatter/HH \
  | grep -A1 "Pixel Size"
```

## 7.4 GUNW and GOFF (Level 2)

GUNW and GOFF are geocoded, so they already have a projected CRS and need no GCP handling. Their complication is depth: layers nest under a layer group and then a polarization, `grids/frequencyA/<layerGroup>/<pol>/<layer>`. Georeferencing is resolved by walking up to the frequency group, so it works at any depth, but `FREQ`/`POL` cannot express these paths. Address them by full HDF5 path, copied from the subdataset list:

```
gdalinfo NISAR:"$GUNW" | grep -E "unwrappedInterferogram|pixelOffsets"
```

Layer names below follow the GUNW product specification; confirm the spelling against your granule (an early announcement mis-spelled `coherenceMagnitude`, which is exactly why copying from `gdalinfo` is safer):

```
# unwrapped phase
gdalinfo NISAR:"$GUNW":\
  /science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase

# coherence and connected components from the same granule
gdal_translate \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/coherenceMagnitude \
  gunw_coherence.tif

gdal_translate \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/connectedComponents \
  gunw_concomp.tif

# pixel offsets live in their own layer group
gdal_translate \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/pixelOffsets/HH/alongTrackOffset \
  gunw_along_track_offset.tif
```

Other GUNW layers you will typically find under `unwrappedInterferogram/<pol>/` include `ionospherePhaseScreen`, `ionospherePhaseScreenUncertainty` and `mask`; under `pixelOffsets/<pol>/`, `slantRangeOffset` and `correlationSurfacePeak`; and a `wrappedInterferogram` group holding the complex wrapped interferogram. GOFF holds the `pixelOffsets` layers only. Connected components are integer-valued; the wrapped interferogram is complex.

## 7.5 SME2 (Level 3)

SME2 soil-moisture rasters are read the same way, under a `radarData` group:

```
gdalinfo NISAR:"$SME2"

gdallocationinfo NISAR:"$SME2":/science/LSAR/SME2/grids/radarData/frequencyA/sigma0HH 800 800
```

The driver applies the same L2 georeferencing logic (coordinate vectors and `projection` dataset found by walking up the path) to L3 products. SME2 is the least exercised product: a warning reading "Unknown NISAR product structure. Georeferencing may be absent." was *observed* on an earlier driver version while the returned pixel values were correct, and that message is no longer present in the current source. Treat the lesson as still valid: verify the CRS and GeoTransform before you warp or overlay SME2 output.

## 7.6 Radar-grid metadata cubes

Quantities such as incidence angle, look angle and the line-of-sight or along-track unit vector components are stored under `metadata/radarGrid/` as coarse three-dimensional cubes: height × y × x. The driver exposes a 3-D dataset as a multi-band raster, one band per height level, with a GeoTransform from the cube's own coordinate vectors. When a sibling `heightAboveEllipsoid` dataset exists and has one entry per band, each band is described `Height: <z> m` and carries `HEIGHT_METERS` and `Z_VALUE` band metadata.

```
gdalinfo NISAR:"$GCOV":/science/LSAR/GCOV/metadata/radarGrid/incidenceAngle
```

Expect a coarse pixel size (500 m in one *observed* case) and a band count matching the number of height levels (21 in that same case). Select a single height level with the standard GDAL band argument:

```
gdal_translate -b 5 -of GTiff \
  NISAR:"$GCOV":/science/LSAR/GCOV/metadata/radarGrid/incidenceAngle \
  cube_band5.tif
```

One cosmetic issue: the cube's dataset metadata is reported for every band, so the same values repeat once per band in `gdalinfo` output.

### Interpolating a cube onto the imaging grid

Selecting a single height level is rarely what you actually want. The physically correct operation is to interpolate the cube in three dimensions, horizontally onto the imaging grid and vertically using the terrain height at each pixel, so that the result lands on the same grid as the imagery. The driver implements this when `QUANTITY` and `DEM_FILE` are both given:

```
# Cube resolved from QUANTITY; reference grid is the product's frequency-A default layer
gdal_translate -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 -co COMPRESS=ZSTD \
  -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" -oo DEM_RESAMPLING=CUBICSPLINE \
  NISAR:"$GCOV" incidence_angle.tif

# Same, on the GSLC frequency-B HV grid, with the cube path spelled out
gdal_translate -oo QUANTITY=incidenceAngle -oo FREQ=B -oo POL=HV -oo DEM_FILE="$DEM" \
  NISAR:"$GSLC":/science/LSAR/GSLC/metadata/radarGrid/incidenceAngle \
  incidence_angle_gslc_B.tif
```

What happens, from the source (v0.7.0):

1. The presence of `QUANTITY` routes the open to the interpolation dataset. If `DEM_FILE` is missing the open fails with "DEM_FILE open option is REQUIRED when QUANTITY is specified."
2. The **cube** is the dataset the connection string points at. If the connection string is a bare `NISAR:"file.h5"`, the driver reads the product identification and resolves the cube to `/science/<INST>/<PRODUCT>/metadata/radarGrid/<QUANTITY>`; a missing quantity fails with "Failed to open valid 3D coarse metadata cube at ...".
3. The **target grid** is chosen from the product type recorded in the granule (not from the file name), honouring `INST`, `FREQ` and `POL`: GCOV and GSLC use `/science/<INST>/<PRODUCT>/grids/frequency<F>/<POL>` (default `POL` is `HHHH` for GCOV and `HH` for GSLC); GUNW uses `.../grids/frequency<F>/unwrappedInterferogram/<POL>/unwrappedPhase` (default `HH`); RSLC uses `.../swaths/frequency<F>/<POL>` (default `HH`), RIFG `.../swaths/frequency<F>/interferogram/<POL>/wrappedInterferogram` and RUNW `.../swaths/frequency<F>/interferogram/<POL>/unwrappedPhase` (default `HH`), and on Level 1 the cube is resolved under `metadata/geolocationGrid` instead of `metadata/radarGrid`.
4. On a geocoded grid the DEM is exposed as a lazily-warped view on the target grid using `DEM_RESAMPLING` (blocks are resampled on demand, so a full-resolution GSLC grid does not pin a grid-sized DEM in memory); the whole cube is loaded into memory, and each output pixel is interpolated in height. The output is a single-band Float32 raster with the target grid's georeferencing, 512×512 blocks, and NaN where the cube has no value. Pixels outside the DEM's coverage take height 0.
5. On Level 1 (RSLC, RIFG, RUNW) the output is in **radar coordinates**: every pixel is a (slant range, zero-Doppler time) pair taken from the `slantRange` and `zeroDopplerTime` vectors stored nearest above the reference raster (`swaths/frequency<F>` + `swaths` for RSLC, `swaths/frequency<F>/interferogram` for RIFG/RUNW), and the cube is indexed by the geolocation grid's own `slantRange`/`zeroDopplerTime` axes. The terrain height is not known up front (the DEM is a map, the pixel is not), so the driver solves it per pixel by fixed-point iteration: start at `DEM_NODATA_HEIGHT`, interpolate `coordinateX`/`coordinateY` at that height to get the ground point, read the DEM there (bilinear, in the geolocation grid's CRS; if the DEM CRS differs, the DEM window covering the geolocation-grid footprint is warped lazily at its native resolution — RIFG/RUNW grids are typically UTM while the public DEM is EPSG:4326), repeat until the height changes by less than 0.1 m (at most 10 passes), then interpolate the requested cube at the converged height. DEM nodata (band nodata value, NaN, a per-dataset mask band or an alpha band — in any combination; when the DEM CRS differs the validity is warped as source alpha alongside the heights) or missing coverage takes `DEM_NODATA_HEIGHT` (default 0 m); the DEM must have an affine geotransform. The result carries the reference raster's GCPs and GCP CRS (`NISAR_GRID_TYPE=RADAR`, `NISAR_GEOLOCATION_EPSG`), so it warps to a map with `gdalwarp` exactly like the swath itself. Requesting `QUANTITY=coordinateX`/`coordinateY` gives the solved ground coordinates of every pixel (Float32, so ~1e-6° at mid-latitudes).

The output carries `NISAR_PRODUCT_TYPE`, `NISAR_CUBE_PATH`, `NISAR_REFERENCE_GRID` and `NISAR_QUANTITY` metadata items recording what was resolved, so `gdalinfo` on the result (or on the interpolated open itself) shows which grid and cube were used.

```
# RSLC: incidence angle on the HH swath grid, terrain-corrected through the DEM
gdal_translate -co TILED=YES -co COMPRESS=ZSTD \
  -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" \
  NISAR:"$RSLC" rslc_incidence_angle.tif

# Ocean / DEM gaps assumed at 20 m instead of 0 m
gdal_translate -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" -oo DEM_NODATA_HEIGHT=20 \
  NISAR:"$RSLC" rslc_incidence_angle.tif

# RUNW: incidence angle on the multilooked interferogram/HH/unwrappedPhase grid (UTM GCPs)
gdal_translate -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" \
  NISAR:"$RUNW" runw_incidence_angle.tif
```

On a 26126 × 7600 RSLC swath the full-scene incidence-angle translate took 37 s with a ~1.2 GB peak footprint against the public HTTPS DEM (*observed*, this machine); a 512 × 512 block costs about 10 ms once the DEM tiles are cached, so the run is dominated by DEM fetches and GeoTIFF writing.

A global public DEM VRT is available in both S3 and HTTPS form (*observed*); its resolution need not match NISAR posting, which is why `DEM_RESAMPLING` exists:

```
# S3:    /vsis3/sds-n-cumulus-prod-nisar-products/DEM/v1.2/EPSG4326/EPSG4326.vrt
# HTTPS: https://nisar.asf.earthdatacloud.nasa.gov/NISAR/DEM/v1.2/EPSG4326/EPSG4326.vrt
```

Passing `DEM_FILE` to `gdalinfo` *without* `QUANTITY` does nothing: the option is only read on the interpolation path. (Earlier drafts stated otherwise.)

## 7.7 Static layers

Static layers ship as a separate companion granule rather than inside the product. Its URL is carried in the granule metadata (look for a `staticLayersDataAccess`-style item in the identification or processing-information domains, section 5.3); retrieve it and then open that granule as a NISAR product in its own right.

# 8. Reprojection and warping to latitude-longitude

This is the most requested operation, and the answer depends entirely on whether your product is geocoded.

## 8.1 Level 2 and Level 3, the straightforward case

GCOV, GSLC, GUNW, GOFF and SME2 are already geocoded, typically into a UTM zone. Reprojecting to geographic coordinates is therefore an ordinary CRS transformation, and `gdalwarp` handles it with no NISAR-specific arguments beyond the open options or path that select your layer. The GeoTransform is derived from the product's `xCoordinates`/`yCoordinates` (pixel centres, shifted half a pixel to GDAL's corner convention) and the CRS from the sibling `projection` dataset's `epsg_code` attribute (or its WKT).

A complete, production-quality command for GUNW unwrapped phase:

```
gdalwarp -t_srs EPSG:4326 -r near -of GTiff \
  -srcnodata nan -dstnodata nan \
  -co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES -co BIGTIFF=IF_SAFER \
  -multi -wo NUM_THREADS=ALL_CPUS \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase \
  gunw_unwrapped_phase_ll.tif
```

The same pattern for a GCOV backscatter term, using open options:

```
gdalwarp -t_srs EPSG:4326 -r bilinear -of GTiff \
  -oo FREQ=A -oo POL=HHHH \
  -co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES \
  -multi -wo NUM_THREADS=ALL_CPUS \
  NISAR:"$GCOV" gcov_hhhh_ll.tif
```

## 8.2 Level 1, and why you should be careful

RSLC carries GCPs rather than a GeoTransform, so warping it means fitting a transformation to those GCPs. GDAL will do this, and the result looks convincing, but it is an approximation to a geometry that is properly handled by geocoding with a DEM. Use it for quick looks and for demonstrating that the GCPs exist, not for accurate geolocation.

```
# highest accuracy from the GCPs: thin-plate spline
gdalwarp -t_srs EPSG:4326 -tps -r cubic \
  NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH RSLC_HH_tps.tif

# fast preview: second-order polynomial, cropped to an extent in degrees
gdalwarp -t_srs EPSG:4326 -order 2 -r near -co COMPRESS=LZW \
  -te -62.424 -8.862 -62.299 -8.761 \
  NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH warped-cropped-rslc.tif
```

If you need geolocated Level 1 imagery for analysis, use the geocoded L2 products, which were produced with the full geometry and a DEM.

## 8.3 Choosing a resampling method

The default resampling in `gdalwarp` is nearest neighbour, and for most NISAR layers that is also the right choice. The table below gives the reasoning, which is worth understanding rather than memorising.

| Layer type | Method | Why |
| --- | --- | --- |
| Unwrapped phase | `-r near` | Piecewise continuous across connected-component boundaries. Interpolation invents values straddling those discontinuities and across decorrelated edges. |
| Connected components | `-r near` | Integer labels. Any averaging produces meaningless intermediate labels. |
| Wrapped phase, complex data | `-r near` | Interpolating a wrapped quantity across the branch cut is wrong. Resample real and imaginary parts separately if you must interpolate. |
| Backscatter (GCOV) | `-r bilinear` | Continuous positive quantity. Bilinear or cubic is defensible; consider `-r average` when downsampling. |
| Coherence | `-r bilinear` | Continuous and bounded. Interpolation is acceptable. |
| Masks, flags | `-r near` | Categorical by definition. |
| Incidence angle, look angle | `-r bilinear` | Smooth geometric field. |

If you want a visually smooth phase product, do not interpolate the phase. Warp connected components alongside it with nearest neighbour and mask afterwards.

## 8.4 Extent, resolution and alignment

To crop while warping, give a target extent in the units of the target CRS, which means degrees when warping to EPSG:4326:

```
-te <xmin> <ymin> <xmax> <ymax>
```

Output resolution is set with `-tr`, again in target CRS units. It is usually better to omit it and let `gdalwarp` derive a resolution from the source, because hand-specifying degrees to match a metre posting is easy to get wrong. When you do need a specific grid, `-tap` aligns the output to whole multiples of the pixel size, which makes tiles from different granules line up:

```
-tr 0.0008 0.0008 -tap
```

Nodata handling matters for phase and offset layers, which use NaN as fill. Set both source and destination nodata so that fill is not warped into your valid data:

```
-srcnodata nan -dstnodata nan
```

# 9. Masking

## 9.1 What the mask means, by product

With `-oo MASK=YES` the driver implements GDAL's mask-band concept: it looks for a sibling dataset named `mask` in the opened raster's parent group (`grids/frequencyA/mask` for GCOV and GSLC, `grids/frequencyA/unwrappedInterferogram/HH/mask` for GUNW unwrapped layers) and exposes it as a per-dataset mask band (`GMF_PER_DATASET`). If there is no such dataset you get GDAL's all-valid mask. The mapping from stored values to valid or invalid, from the source:

| Product | Mask source and interpretation |
| --- | --- |
| GUNW | Stored byte `v` in 1–254 is decoded as reference sub-swath `(v/10)%10` and secondary sub-swath `v%10`; the pixel is valid only if **both** are non-zero. 0 and 255 are invalid. |
| Everything else (GCOV, GSLC, ...) | Stored mask raster. Values 1 through 5 (sub-swath number) are valid; all other values, including 0 and 255, are invalid. |
| L1 (RSLC, RIFG, RUNW) | No `mask` dataset next to the raster; `MASK=YES` yields an all-valid mask. |

## 9.2 The default is `NO`; pin it

Masking is **off by default**. `gdal_translate` and `gdalwarp` therefore copy stored fill values unless you set `MASK=YES`. Any pipeline you inherited, and any comparison across driver versions, needs the mask state pinned explicitly rather than left to the default:

```
-oo MASK=YES    # apply the validity mask
-oo MASK=NO     # keep all stored values, including masked-out pixels
```

*Historical:* masking was applied by default in versions 0.1.4 through 0.2.x and made opt-in in 0.3.0, because applying it introduced visible artifacts in some products and not applying it is noticeably faster for `gdal_translate`. The same command run on driver versions either side of 0.3.0 returns different pixel values.

Keeping masked values is often what you actually want in analysis, for example when computing histograms where excluded pixels would bias the distribution. Quantifying the effect with `gdalinfo -stats` only works on a band that has no `min_value`/`max_value` attributes (section 5.4); otherwise the driver returns the producer's attributes regardless of `MASK`. (*Observed on an earlier version:* one GCOV case showed the mean moving from 0.075 to 0.083 and the standard deviation from 1.438 to 1.539 between `MASK=NO` and `MASK=YES`, with the maximum unchanged.)

## 9.3 Carrying the mask into your output

A GeoTIFF has no equivalent of a GDAL mask band unless you create one. With `gdalwarp`, add `-dstalpha` to write the mask as an alpha band, which preserves it for downstream tools and for display:

```
gdalwarp -of GTiff -dstalpha \
  -oo FREQ=A -oo POL=HVHV -oo MASK=YES \
  NISAR:"$GCOV" masked_output_HVHV.tif
```

`gdal_translate` copies the mask as an internal TIFF mask by default (`-mask auto`). Derived subdatasets (section 10) do **not** inherit the mask.

# 10. Derived subdatasets and virtual overviews

## 10.1 Derived subdatasets

For every numeric raster the driver advertises GDAL derived subdatasets in the `DERIVED_SUBDATASETS` metadata domain: virtual datasets that apply a pixel function on read. Nothing is precomputed and nothing is stored, so this costs you only the read you were doing anyway.

- Complex rasters (GSLC, RSLC, off-diagonal GCOV terms, wrapped GUNW interferograms) advertise `AMPLITUDE`, `PHASE`, `REAL`, `IMAG`, `INTENSITY` and `CONJ`.
- Real-valued rasters advertise `LOGAMPLITUDE` (log10 of amplitude), useful for backscatter spanning many orders of magnitude.

The advertised names are complete connection strings you can pass straight to another GDAL command:

```
DERIVED_SUBDATASET:AMPLITUDE:"NISAR:<granule.h5>:<hdf5-path>"
```

Quote the whole thing as a single shell argument:

```
gdal_translate \
  "DERIVED_SUBDATASET:AMPLITUDE:NISAR:$GSLC:/science/LSAR/GSLC/grids/frequencyA/HH" \
  amplitude.tif

gdal_translate \
  "DERIVED_SUBDATASET:PHASE:NISAR:$GSLC:/science/LSAR/GSLC/grids/frequencyA/HH" \
  phase.tif

gdal_translate \
  "DERIVED_SUBDATASET:LOGAMPLITUDE:NISAR:$GCOV:/science/LSAR/GCOV/grids/frequencyA/HHHH" \
  hhhh_db.tif
```

A derived subdataset is an ordinary GDAL dataset, but the driver's mask band is attached to the underlying raster, not to the derived view; do not assume a derived subdataset inherits masking.

## 10.2 Virtual overviews

Every band exposes virtual power-of-two overviews (2×, 4×, ... up to `NISAR_MAX_VIRTUAL_OVR`, default 16), computed by decimated reads of the full-resolution chunks; nothing is stored. This is enough for QGIS and tile servers to zoom out without pre-built pyramids. The cost is proportional to the number of chunks touched, so if zoomed-out views of a very large product are slow, lower `NISAR_MAX_VIRTUAL_OVR` (or set it to `1` to disable), and for repeated analysis convert to a Cloud-Optimized GeoTIFF instead:

```
gdal_translate -of COG -co COMPRESS=DEFLATE \
  NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH HHHH_cog.tif
```

# 11. Performance

## 11.1 How the driver reads

Four facts explain most of the performance behaviour you will observe:

- **GDAL block size = HDF5 chunk size.** Every `IReadBlock` maps onto exactly one HDF5 chunk, so no chunk is read twice. Read windows aligned to the chunk grid are fastest. Chunk offsets are mapped lazily on first read.
- **Mega-fetch.** When a block is missing from the cache, the driver reads a square grid of `NISAR_PREFETCH_GRID × NISAR_PREFETCH_GRID` neighbouring chunks in one contiguous range request (capped by `NISAR_MAX_MEGAFETCH_BYTES`), decompresses them in parallel (`GDAL_NUM_THREADS`) and pushes all of them into the GDAL block cache.
- **Fixed HDF5 tuning.** Files are opened with a 4 MiB HDF5 page buffer, matching the 4 MiB paged-aggregation page NISAR granules are written with, and each dataset gets an 8 MiB / 521-slot chunk cache. Neither is configurable today.
- **Retries.** Set `GDAL_HTTP_MAX_RETRY` (e.g. `5`) so transient S3 errors do not fail the read; the driver does not change it.

## 11.2 Environment variables

These are GDAL configuration options: set them as environment variables, with `--config NAME VALUE` on the command line, or with `gdal.SetConfigOption()` in Python.

| Variable | Default | Effect |
| --- | --- | --- |
| `NISAR_PREFETCH_GRID` | `1` | Side of the N×N grid of HDF5 chunks coalesced into one range request on a cache miss. `1` fetches only the requested chunk (best for tile servers, QGIS and small windows). Larger values such as `24` coalesce many chunks into one large read, much faster for full-scene batch processing. |
| `NISAR_MAX_MEGAFETCH_BYTES` | `16777216` (16 MiB) | Upper bound on one coalesced read, so a large `NISAR_PREFETCH_GRID` cannot produce requests too big for the network or memory. |
| `NISAR_MAX_VIRTUAL_OVR` | `16` | Largest decimation factor for which a virtual overview is synthesised. `1` disables virtual overviews. |
| `NISAR_EXPORT_ZARR` | `NO` | When `YES`, writes a Kerchunk-style JSON sidecar under `/tmp/` describing the HDF5 chunk map of the opened raster, for Zarr / xarray tooling. Debug and interoperability only. |
| `GDAL_NUM_THREADS` | GDAL default | Threads used to decompress chunks in parallel. `ALL_CPUS` is reasonable. |
| `GDAL_CACHEMAX` | GDAL default | GDAL block cache size in MB. Raise it (e.g. `2048`) when repeatedly reading large remote rasters. |
| `GDAL_HTTP_MAX_RETRY` | GDAL default | Retries on failed HTTP range requests; `5` is a good value for S3. |
| `GDAL_DISABLE_READDIR_ON_OPEN` | — | `EMPTY_DIR` stops GDAL listing the containing S3 prefix on open, which is pure latency when you already know the exact key. |
| `GDAL_PAM_ENABLED` | — | `NO` stops GDAL writing `.aux.xml` sidecars next to local granules, which is harmless but clutters directories and fails on read-only media. |

A reasonable starting point for full-scene batch work from S3:

```
export NISAR_PREFETCH_GRID=24
export GDAL_CACHEMAX=2048
export GDAL_NUM_THREADS=ALL_CPUS
export GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
export GDAL_PAM_ENABLED=NO
```

For interactive or tiled access (QGIS, TiTiler) leave `NISAR_PREFETCH_GRID` at its default of `1`. Standard GDAL `/vsis3/` and `/vsicurl/` options (`AWS_*`, `CPL_VSIL_CURL_*`, `GDAL_HTTP_*`, `VSI_CACHE`, ...) also apply.

Variables from older notes such as `NISAR_CHUNK_CACHE_SIZE_MB` do not exist in the current driver and are silently ignored.

## 11.3 What to expect (observed, not benchmarks)

These figures come from development tuning runs on specific granules and instances; they show the shape of the behaviour, not guaranteed numbers.

- NISAR granules use HDF5 paged aggregation with a 4 MiB page, so a single-pixel read still transfers at least one page; there is a floor on the cost of any remote read. Reading a full 1.1 GB GCOV raster took on the order of 150 range requests.
- Peak memory for a full-raster read was about 1.1 GiB for the driver, against roughly 24 GiB for an h5py script and 28 GiB for xarray that used a page buffer large enough to hold entire layers. For time-series work, where you read many granules concurrently or accumulate statistics block by block, this bounded footprint is decisive; build workflows around block-wise reads.
- On full-raster reads the driver was roughly twice as fast as Cloud-Optimized GeoTIFF of the same data (about 12 range requests against 40–50, at the cost of higher transfer volume), and around five times faster than h5py. On very small windows (a tenth of a GUNW raster, involving very few chunks) fixed overheads dominate and h5py compared favourably.
- Optimal settings depend on product type, window size, data type, and whether you are in-region or out-of-region. A single tuning configuration is not optimal everywhere.

# 12. Python (`osgeo.gdal`)

Everything above is available from Python through the GDAL bindings; the connection strings and open options are identical.

```
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

# Open by full path on S3
ds = gdal.Open("NISAR:s3://my-bucket/path/to/file.h5:/science/LSAR/RSLC/swaths/frequencyA/HH")
print(ds.RasterXSize, ds.RasterYSize, gdal.GetDataTypeName(ds.GetRasterBand(1).DataType))
```

Read in chunk-aligned windows rather than calling `ReadAsArray()` on a whole remote raster, and set `GDAL_CACHEMAX` / `NISAR_PREFETCH_GRID` with `gdal.SetConfigOption()` before the first open.

# 13. Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `gdalinfo --formats` does not list `NISAR` | The plugin is not on GDAL's plugin path. Check that `gdal-driver-nisar` is installed in the *active* environment and that `$CONDA_PREFIX/lib/gdalplugins/gdal_NISAR.*` exists. If GDAL comes from outside conda, set `GDAL_DRIVER_PATH`. |
| `gdalinfo file.h5` opens with the `HDF5` driver instead of `NISAR` | Prefix the path with `NISAR:` (or pass `-if NISAR`). |
| `H5Fopen failed for '...'` on a remote file | Usually credentials or region. Test the URL directly with `gdalinfo /vsis3/bucket/key.h5`, check `AWS_REGION` and `AWS_PROFILE`, unset stale `AWS_*` variables, or run `aws s3 ls s3://bucket/key.h5 --profile <profile>`. Add `--debug on` to see the HTTP requests GDAL makes. |
| The driver silently declines and another driver (or "not recognized") appears | A remote URL without the `NISAR:` prefix does not contain `NISAR` (prefix the path with `NISAR:`), or the file is not HDF5. |
| `The HDF5 dataset '...' does not exist` | The path (typed or constructed from `FREQ`/`POL`) is not in this granule. List the container and copy the path; for GUNW/GOFF/RIFG/RUNW use the full path, not open options. |
| `Invalid INST open option`, `Invalid FREQ open option`, `Invalid POL open option: '...'` | Check `INST` (`LSAR`/`SSAR`), `FREQ` (`A`/`B`) and `POL` against the granule's `listOfPolarizations` / `listOfCovarianceTerms`. |
| `DEM_FILE open option is REQUIRED when QUANTITY is specified` | Interpolation mode needs both `QUANTITY` and `DEM_FILE`. |
| `Interpolation: Level-1 product ROFF is not supported (only RSLC, RIFG, RUNW).` | Cube interpolation supports GCOV, GSLC, GUNW, RSLC, RIFG and RUNW; ROFF reference grids are not routed. |
| `Interpolation: DEM does not cover the geolocation grid footprint.` | Level 1 with a DEM in another CRS: the DEM's extent does not intersect the geolocation grid's `coordinateX`/`coordinateY` bounding box. Use a DEM that covers the scene. |
| `Interpolation: geolocationGrid axes/coordinate cubes do not match the ... cube` | The `coordinateX`/`coordinateY` cubes or `slantRange`/`zeroDopplerTime` vectors under `metadata/geolocationGrid` have a different shape than the requested cube; check the granule, or spell out the cube path. |
| `NISAR Interpolation: DEM window ... is too large; use a coarser DEM.` | The ground footprint of one 512 × 512 radar block covers more than 64 Mpixel of DEM; the DEM is far finer than needed for a metadata cube. |
| `Failed to open valid 3D coarse metadata cube at ...` | The `QUANTITY` (or explicit cube path) does not exist in the granule; list `.../metadata/radarGrid` for the available cubes. |
| Pixel size near 1, origin near 0 | No GeoTransform was found for this dataset. For L1 that is expected (use the GCPs); for L2/L3 check the driver version and the path. |
| Warped output is a tiny square in the wrong place | Same cause as above; you warped a raster with an identity GeoTransform. |
| Statistics look implausible or identical with and without `MASK` | `-stats`/`-approx_stats` returned producer attributes or synthesised values (section 5.4). Translate to GeoTIFF and compute there. |
| Slow full-scene reads from S3 | Increase `NISAR_PREFETCH_GRID` (e.g. `24`) and `GDAL_CACHEMAX`; make sure `GDAL_NUM_THREADS` is not `1`; compute in-region. |
| `GLIBC_2.xx not found` on every GDAL command | The Linux plugin binary is newer than the host libc; needs a rebuild for your platform. |
| conda cannot solve the environment | GDAL minor-version mismatch with the plugin's pin. Install into a fresh environment. |

Enable driver debug output with `CPL_DEBUG=NISAR_DRIVER` (or `CPL_DEBUG=ON` for everything).

# 14. Version history and behaviour changes

Because several releases changed output rather than only fixing crashes, knowing your version is part of knowing your data. The entries for versions before 0.6.6 are taken from release announcements and field notes and have not been re-verified; the rows for 0.6.6 and later summarise what was verified in source.

| Version | Change | Affects results |
| --- | --- | --- |
| 0.1.1 | Rebuild addressing the GLIBC dependency error on older Linux hosts. | no |
| 0.1.3 | Changes to metadata output. | no |
| 0.1.4 | `MASK` open option introduced, applied by default. | yes |
| 0.1.7 | GeoTransform fixed for GUNW principal rasters. Calibration-grid projection metadata handled, addressing the NEB georeferencing problem. | yes |
| 0.1.8 | Radar-grid metadata cubes interpreted as multi-band rasters with a correct GeoTransform; band selection with `-b`. | yes |
| 0.1.9 | `DRIVER_VERSION` with build date reported via `gdalinfo --format NISAR`. | no |
| 0.3.0 | Path quoting and slash handling reworked. Mask no longer applied by default. Remote reads through HDF5's ROS3 driver with AWS-style credential sourcing. | yes |
| unreleased | Sources moved to `frmts/nisar/` with Caltech / Apache-2.0 license headers. The driver no longer sets `GDAL_HTTP_MAX_RETRY=5` when unset; set it yourself for remote reads (see [section 11.1](#111-how-the-driver-reads)). `GetMetadata` no longer leaks the `NISAR_GLOBAL`/`SUBDATASETS` lists. Code brought in line with GDAL's development practices (warning-free under GDAL's `-Weffc++`/`-Wold-style-cast` flags, overflow-safe `VSI_MALLOC2` allocations, `std::bad_alloc` handling for chunk-map, cube and decompression buffers). Fixed the ARM NEON 8-byte de-shuffle tail loop, which restarted at element 0 and overran the block buffer on aarch64. `NISAR:` connection strings no longer need the file part to contain `.h5` (in `Open()` as well as `Identify()`). A band whose chunk map cannot be allocated now makes `Open()` fail instead of returning a dataset that silently reads zeros. `GetMetadataDomainList()` no longer lists `SUBDATASETS` twice. | no |
| 0.7.2 (current) | Packaging only: `README.md`, this guide and the `nisar-gdal` skill are installed to `$CONDA_PREFIX/share/doc/gdal-driver-nisar/`; package metadata (`doc_url`, description) updated on Anaconda.org. Driver code unchanged from 0.7.1. | no |
| 0.7.1 | `NISAR_DUMP` metadata domain: h5dump-style listing of the HDF5 hierarchy via `gdalinfo -mdd NISAR_DUMP`, scoped by `DUMP_ROOT`, with `DUMP_MODE=HEADER\|FULL` and bounded values (`NISAR_DUMP_MAX_ELEMENTS`, `NISAR_DUMP_MAX_STRING_CHARS`); `DUMP=YES` only advertises the domain for `-mdd all` (see [section 5](#5-exploring-a-granule)). Default output and `SUBDATASETS` unchanged. | no |
| 0.7.0 | Cube interpolation generalised: reference grid chosen from the granule's product type (GCOV, GSLC, GUNW, RSLC) honouring `INST`/`FREQ`/`POL`; cube auto-resolved from `QUANTITY` under `metadata/radarGrid` (L2/L3) or `metadata/geolocationGrid` (L1) when no HDF5 path is given; quoted file names accepted in interpolation connection strings; DEM aligned through a lazily-warped VRT instead of a grid-sized in-memory raster; resolved grid/cube reported as `NISAR_*` metadata. RSLC interpolation in radar coordinates with per-pixel terrain height solved through `coordinateX`/`coordinateY` and the DEM (`DEM_NODATA_HEIGHT`), GCPs passed through. | yes (GSLC and RSLC interpolation) |
| 0.6.6 | Built against GDAL 3.12. All remote I/O routed through GDAL VSI via a custom HDF5 Virtual File Layer (ROS3 no longer used; standard GDAL `AWS_*` configuration applies). Open options registered in `DMD_OPENOPTIONLIST`; `DEM_RESAMPLING` added; no `LAYER`/`MEASURE` options. Chunk-aligned mega-fetch reads with `NISAR_PREFETCH_GRID` / `NISAR_MAX_MEGAFETCH_BYTES`, virtual overviews, attribute-based statistics, GUNW-specific mask decoding, optional Kerchunk sidecar. | yes (statistics, masks) |

# 15. Verification checklist

Before you use driver output for anything quantitative, work through this list. Each item corresponds to a failure that has actually occurred.

- Confirm the driver version from the binary, `gdalinfo --format NISAR`, not only from `conda list`. Reinstalls fail silently.
- Pin the mask state explicitly with `-oo MASK=YES` or `-oo MASK=NO`. The default is `NO` and it was once `YES`.
- Copy layer paths from `SUBDATASET_n_NAME` output rather than from documentation, this document included; use `FREQ`/`POL` only where the product allows.
- Check that the reported CRS is a real projected system and that pixel size is a plausible ground distance. A pixel size near one unit means the GeoTransform is missing.
- Check corner coordinates in degrees against where you expect the granule to be.
- For NEB and other calibration-grid rasters, verify the GeoTransform specifically. Different corners from the principal grid are expected; a one-unit pixel size is not.
- For phase, offsets and connected components, confirm your resampling is nearest neighbour and that NaN nodata is set on both sides of the warp.
- Do not quote `gdalinfo -stats` / `-approx_stats` mean and standard deviation unless the dataset carries `mean_value` / `sample_stddev` attributes; they may be synthesised from min and max.
- For metadata cubes, decide whether you want a single height band or a DEM-interpolated raster. They are not interchangeable; interpolation covers GCOV, GSLC, GUNW and RSLC (RSLC output stays in radar coordinates with GCPs).
- For L1 frequency-B rasters, verify GCP geolocation independently.
- For SME2, verify CRS and GeoTransform before overlaying; it is the least exercised product.

# 16. Quick reference

`$GRANULE` stands for any granule path; `$GCOV`, `$GSLC`, `$GUNW` and `$DEM` are the variables set in section 1.1.

```
# what is in this granule
gdalinfo NISAR:"$GRANULE"
gdalinfo NISAR:"$GRANULE" | grep HHHH

# which driver am I running
gdalinfo --format NISAR | grep DRIVER_VERSION

# full metadata (both flags required)
gdalinfo -mdd all -oo METADATA=ALL NISAR:"$GRANULE"

# open a principal layer semantically (RSLC, GSLC, GCOV, SME2)
gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV"

# open a nested layer by full path (GUNW, GOFF)
gdalinfo NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase

# extract to GeoTIFF / COG
gdal_translate -oo FREQ=A -oo POL=HHHH -oo MASK=YES NISAR:"$GCOV" out.tif
gdal_translate -of COG -co COMPRESS=DEFLATE NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH out_cog.tif

# warp a geocoded product to lat-lon
gdalwarp -t_srs EPSG:4326 -r near -srcnodata nan -dstnodata nan \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase out_ll.tif

# geocode an L1 raster from its GCPs (quick look only)
gdalwarp -t_srs EPSG:4326 -tps -r cubic NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH rslc_ll.tif

# amplitude / dB of a layer, no code required
gdal_translate "DERIVED_SUBDATASET:AMPLITUDE:NISAR:$GSLC:/science/LSAR/GSLC/grids/frequencyA/HH" amp.tif
gdal_translate "DERIVED_SUBDATASET:LOGAMPLITUDE:NISAR:$GCOV:/science/LSAR/GCOV/grids/frequencyA/HHHH" db.tif

# interpolate a metadata cube onto the imaging grid
gdal_translate -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" \
  NISAR:"$GCOV":/science/LSAR/GCOV/metadata/radarGrid/incidenceAngle inc.tif

# single pixel
gdallocationinfo NISAR:"$GRANULE":<path> <pixel> <line>
gdallocationinfo -wgs84 NISAR:"$GRANULE":<path> <lon> <lat>

# batch tuning from S3
export NISAR_PREFETCH_GRID=24 GDAL_CACHEMAX=2048 GDAL_NUM_THREADS=ALL_CPUS

# what is the driver doing
CPL_DEBUG=NISAR_DRIVER gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GRANULE"
```
