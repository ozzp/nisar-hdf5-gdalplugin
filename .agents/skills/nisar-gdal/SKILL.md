---
name: nisar-gdal
description: Read NISAR HDF5 products (RSLC, RIFG, RUNW, GSLC, GCOV, GUNW, GOFF, SME2, radar-grid metadata cubes) with GDAL through the gdal-driver-nisar plugin. Use when writing or debugging gdalinfo / gdal_translate / gdalwarp / gdallocationinfo commands against NISAR granules, when reprojecting a NISAR layer to lat-lon, when choosing open options (INST, FREQ, POL, METADATA, MASK, QUANTITY, DEM_FILE, DEM_RESAMPLING), when a NISAR raster comes back with wrong georeferencing, unexpected statistics or pixel values, or when tuning remote S3 / HTTPS reads of NISAR data.
---

# Reading NISAR products with GDAL

`gdal-driver-nisar` (conda channel `nisar-forge`, source in the `nisar-hdf5-gdalplugin` repo) is a
**read-only GDAL raster plugin**, short name `NISAR`. It installs one shared library into GDAL's
plugin directory (`$CONDA_PREFIX/lib/gdalplugins/gdal_NISAR.so` / `.dylib`). It is not a Python
package — there is nothing to import; use the GDAL CLI or `osgeo.gdal`.

This document describes driver **v0.7.2 built against GDAL 3.12**. Anything marked *historical*
or *observed* comes from earlier release notes or field notes and has not been re-verified against
the current source. When in doubt, the driver's own output (`gdalinfo`, `CPL_DEBUG`) wins.

## First move: inventory the granule, never type a layer path from memory

```bash
gdalinfo NISAR:granule.h5                 # container: lists SUBDATASET_n_NAME / _DESC
gdalinfo NISAR:granule.h5 | grep HHHH     # granules have dozens of subdatasets — filter
```

Copy the `SUBDATASET_n_NAME` string verbatim (it already carries the `NISAR:"…":` form), or let
the driver resolve the path with open options (below). Layer paths differ by product, level,
instrument and product-spec version.

`SUBDATASET_n_DESC` gives `[<dims>] <hdf5-path> <type>` (e.g. `[21x720x748] …/incidenceAngle
(Float32)`), which tells a frequency-A grid
from a frequency-B grid or a coarse 3D metadata cube without knowing posting conventions.

## Addressing model

```text
NISAR:<file>[:<hdf5-dataset-path>]
```

- `<file>` may be local, `s3://bucket/key.h5`, `/vsis3/bucket/key.h5`, `/vsicurl/https://…`, or a
  bare `http(s)://` URL. The driver rewrites `s3://` → `/vsis3/` and `http(s)://` → `/vsicurl/`;
  all remote I/O goes through GDAL's VSI layer (there is no HDF5 ROS3 path any more).
- `<file>` **must end in `.h5`**; anything else is rejected silently (the driver returns "not mine").
- The parser splits on the **last colon**, unless that colon is followed by `//` (so
  `NISAR:s3://bucket/x.h5` is a container, not a file + path). Double quotes around the whole
  `<file>` part are stripped: `NISAR:"s3://bucket/x.h5":/science/LSAR/GCOV/grids/frequencyA/HHHH`.
- Read-only: opening with update access fails with "The NISAR driver only supports read access."
- Without the `NISAR:` prefix a local file is still recognised (the driver probes for
  `/science/LSAR|SSAR/identification`), but a **remote** file is only claimed if the URL contains
  the string `NISAR`. Which driver wins also depends on GDAL's driver order, so always use the
  prefix or `-if NISAR` in anything scripted.

Put bare paths in shell variables (no `NISAR:` prefix) so the same variable works alone and with a
dataset path appended:

```bash
B=/vsis3/nisar-ops-rs-fwd/products
G=NISAR_L2_PR_GCOV_006_068_D_093_4005_DHDH_A_20251009T222629_20251009T222704_P00410_N_F_J_001
export GCOV=$B/L2_L_GCOV/2025/10/09/$G/$G.h5

gdalinfo NISAR:"$GCOV"                                          # container
gdalinfo NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH  # one raster
gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV"                    # same raster via open options
```

## Open options (`-oo KEY=VALUE`, repeatable)

Registered in `DMD_OPENOPTIONLIST`, so `gdalinfo --format NISAR` lists them. Option names are
matched case-insensitively by GDAL; the canonical spelling is upper case.

| Option | Values / default | Notes |
|---|---|---|
| `INST` | `LSAR` (default), `SSAR` | Instrument group under `/science`. |
| `FREQ` | `A` (default), `B` | Frequency B is absent from many granules and coarser. |
| `POL` | product-dependent; default `HHHH` for GCOV, `HH` otherwise | Validated against `listOfCovarianceTerms` / `listOfPolarizations` in the file — an invalid value errors with the valid list. |
| `METADATA` | `ALL` or comma list of `ATTITUDE`, `CALIBRATIONINFORMATION`, `CEOSANALYSISREADYDATA`, `ORBIT`, `PROCESSINGINFORMATION`, `RADARGRID`, `SOURCEDATA` | Loads the named `/metadata/<group>` trees into `NISAR_<NAME>` metadata domains. Pair with `gdalinfo -mdd all` (or `-mdd NISAR_ORBIT`) or you will not see them. |
| `MASK` | `YES` / `NO` (default `NO`) | Expose the product validity mask as the GDAL mask band. |
| `DEM_FILE` | path or `/vsi…` URL | DEM for 3D metadata-cube interpolation. Required with `QUANTITY`. |
| `DEM_RESAMPLING` | `NEAREST`, `BILINEAR`, `CUBIC`, `CUBICSPLINE` (default) | How the DEM is warped onto a geocoded (L2/L3) target grid; unused on L1. |
| `DEM_NODATA_HEIGHT` | metres, default `0` | L1 interpolation only: height assumed where the DEM is nodata / masked / absent (ocean). Must be finite. |
| `QUANTITY` | cube name, e.g. `incidenceAngle` | Routes the open to the cube-interpolation dataset (see below); with a bare `NISAR:"file.h5"` the cube is resolved under `metadata/radarGrid/<QUANTITY>` (L2/L3) or `metadata/geolocationGrid/<QUANTITY>` (L1). |
| `DUMP` | `YES` / `NO` (default `NO`) | Optional. Only advertises the `NISAR_DUMP` domain (`-mdd all`); never changes `SUBDATASETS`. `gdalinfo -mdd NISAR_DUMP` works without it. |
| `DUMP_ROOT` | HDF5 group, default `/science/<INST>` (instrument of the explicit raster path, else `INST`, else detected) | Start of the dump traversal; `/` = whole file. Must exist or the open fails. |
| `DUMP_MODE` | `HEADER` (default) / `FULL` | `HEADER`: objects, datatypes, dataspaces, attributes (with values), no dataset values. `FULL`: also `DATA { … }` for scalars / 1-D datasets ≤ `NISAR_DUMP_MAX_ELEMENTS` (64) elements; larger ones print `(N elements, not printed)`. |
| `ENABLE_PAGE_BUFFERING` | boolean, default `NO` | Reserved. The driver always sets a 4 MiB HDF5 page buffer; this option has no other effect today. |

**There are no `LAYER` or `MEASURE` options.** GUNW/GOFF/RUNW/RIFG layers must be addressed by
full HDF5 path (copy it from the subdataset list). `FREQ`/`POL` build the path
`…/<swaths|grids>/frequency<F>/<POL>`, which only exists for RSLC, GSLC and GCOV (and, via
`grids/radarData/frequency<F>/<POL>`, SME2); on GUNW it fails with "The HDF5 dataset … does not
exist".

How the driver picks what to open, in priority order: explicit `:<hdf5-path>` → `INST`/`FREQ`/`POL`
open options → container mode (no raster bands, `SUBDATASETS` domain populated).

Polarization values: GCOV holds covariance terms, four characters (`HHHH`, `HVHV`, `VVVV` real;
off-diagonal such as `HHHV` complex). RSLC/GSLC/GUNW use two (`HH`, `HV`, `VH`, `VV`). Compact-pol
and S-band granules add values such as `RHRH` — list the granule rather than guessing.

## Exploring a granule

```bash
gdalinfo NISAR:"$GCOV"                                           # inventory
gdalinfo NISAR:"$GCOV" | grep identification/                    # productType, boundingPolygon, times…
gdalinfo -mdd all -oo METADATA=ALL NISAR:"$GCOV"                 # every metadata domain
gdalinfo -mdd NISAR_ORBIT -oo METADATA=ORBIT NISAR:"$GCOV"       # one group
gdalinfo -mdd NISAR_DUMP NISAR:"$GCOV"                            # h5dump-like walk of /science/LSAR (no option needed)
gdalinfo -mdd NISAR_DUMP -oo DUMP_MODE=FULL -oo DUMP_ROOT=/science/LSAR/identification NISAR:"$GCOV"
gdalinfo -mdd all -oo DUMP=YES NISAR:"$GCOV"                      # DUMP=YES: advertise the domain to -mdd all
gdalinfo -nogcp NISAR:"$RSLC":/science/LSAR/RSLC/swaths/frequencyA/HH   # L1: suppress GCP dump
gdallocationinfo NISAR:"$GCOV":/science/LSAR/GCOV/grids/frequencyA/HHHH 100 200
CPL_DEBUG=NISAR_DRIVER gdalinfo -oo FREQ=A -oo POL=HHHH NISAR:"$GCOV"
```

Metadata domains you will see:

- default domain — container: root attributes plus every scalar under
  `/science/<INST>/identification/` keyed by full path (e.g.
  `/science/LSAR/identification/boundingPolygon`). Raster: the dataset's own HDF5 attributes
  (`description`, `units`, `_FillValue`, `min_value`, …) plus frequency-group scalars
  (`numberOfSubSwaths`, `listOfPolarizations`, spacing values, …).
- `NISAR_GLOBAL` — root-level HDF5 attributes.
- `SUBDATASETS` — container only.
- `NISAR_DUMP` — on request (`-mdd NISAR_DUMP`, no open option needed): one line per h5dump-style text line (`GROUP "..." {`,
  `DATASET "..." {`, `DATATYPE`, `DATASPACE`, `ATTRIBUTE`, `DATA`, `}`, plus `SOFTLINK` /
  `EXTERNAL_LINK` / `HARDLINK` records for links, not followed) under `DUMP_ROOT`. Built
  lazily in one `H5Lvisit` pass; HEADER mode reads object headers/attributes only (2-4 s for a
  full `/science/LSAR` walk of a remote RSLC/GUNW). Listed by `GetMetadataDomainList()` /
  `-mdd all` only with `-oo DUMP=YES`. In Python: `ds.GetMetadata_List("NISAR_DUMP")`.
- `DERIVED_SUBDATASETS` — numeric rasters only (see below).
- `NISAR_<GROUP>` — only when requested with `METADATA=`.

`boundingPolygon` is an HDF5 string scalar, not a raster: it never appears as a subdataset. Read
it from the container's default metadata domain:

```python
from osgeo import gdal
ds = gdal.Open('NISAR:granule.h5')
wkt = ds.GetMetadataItem('/science/LSAR/identification/boundingPolygon')
```

The driver is raster-only; there is no OGR layer for footprints.

The debug log (`CPL_DEBUG=NISAR_DRIVER`, or `CPL_DEBUG=ON` for everything) is informative — read
it before speculating:

```text
NISAR_DRIVER: Identified Product: INST=LSAR, Type=GCOV, Level=L2 (L1=0, L2=1)
NISAR_DRIVER: Priority 2: Using OpenOptions: INST=LSAR, FREQ=A, POL=HHHH
NISAR_DRIVER: Successfully Opened Subdataset: /science/LSAR/GCOV/grids/frequencyA/HHHH
NISAR_DRIVER: Detected 3D Dataset: 21 Bands x 720 Y x 748
```

## Statistics: know what `-stats` actually returns

`GetStatistics()`/`GetMinimum()`/`GetMaximum()` are overridden. If the HDF5 dataset carries
`min_value`/`max_value` (or `valid_min`/`valid_max`) attributes, **or** the caller allows
approximation (`gdalinfo -approx_stats`), the driver returns those attribute values without
touching pixels. Mean and stddev come from `mean_value`/`sample_stddev` attributes if present,
otherwise they are **synthesised** as `(min+max)/2` and `(max-min)/6`. With no attributes at all,
`-approx_stats` / `Min=…Max=…` fall back to `0` and `5`.

Only `gdalinfo -stats` on a band **without** min/max attributes triggers a real pixel scan. If you
need measured statistics, translate to GeoTIFF first (or compute in NumPy).

## Per-product notes

Paths below are the driver's own construction rules; the concrete layer names inside them come
from the product spec and must be confirmed against the subdataset list.

- **L1 — RSLC, RIFG, RUNW**: radar geometry under `/science/<INST>/<PROD>/swaths/frequency{A,B}/…`.
  No GeoTransform; the driver attaches **GCPs** built from `metadata/geolocationGrid` (thousands
  of them — `-nogcp`). `GetGeoTransform()` deliberately fails when GCPs are present, so tools that
  need an affine transform will complain. GCP lines follow the spec (`(t - swaths/zeroDopplerTime[0])
  / zeroDopplerTimeSpacing`); GCP pixels read `slantRange[0]`/`slantRangeSpacing` from
  **`swaths/frequencyA`** regardless of the raster opened — treat frequency-B L1 georeferencing
  with suspicion (inference from source, not observed).
  No mask dataset exists, so `MASK=YES` yields an all-valid mask.
- **L2 — GSLC, GCOV, GUNW, GOFF**: geocoded under `/science/<INST>/<PROD>/grids/frequency{A,B}/…`,
  usually UTM (or polar stereographic). GeoTransform comes from a `GeoTransform` attribute if the
  producer wrote one, else from the sibling `xCoordinates`/`yCoordinates` vectors (interpreted as
  pixel centres, shifted half a pixel to GDAL's corner convention). CRS comes from the sibling
  `projection` dataset (`epsg_code` attribute or WKT/`spatial_ref`). The same walk-up lookup covers
  the coarser calibration grids (`metadata/calibrationInformation/…`, e.g.
  `noiseEquivalentBackscatter`) and radar-grid cubes — different corners from the principal grid
  are expected; a pixel size near 1 unit is not.
- **GCOV**: best-exercised product. Its mask is also a raster in its own right
  (`grids/frequencyA/mask`); opened directly it gets NoData=255, category names and a colour table.
- **GUNW / GOFF**: layers nest deeper (`grids/frequencyA/<layerGroup>/<pol>/<layer>`, e.g.
  `unwrappedInterferogram/HH/unwrappedPhase`, `pixelOffsets/HH/alongTrackOffset`). Address by full
  path only. Georeferencing is resolved by walking up to the frequency group, so it works at any
  depth.
- **L3 — SME2**: `/science/<INST>/SME2/grids/radarData/frequency{A,B}/…`, same L2 georeferencing
  logic. Least exercised — verify CRS and GeoTransform before warping or overlaying.
- **Radar-grid metadata cubes** (`metadata/radarGrid/incidenceAngle`, `losUnitVectorX`, …): coarse
  3D datasets (height × y × x) exposed as multi-band rasters, one band per height level, with a
  GeoTransform from their own coordinate vectors. When a sibling `heightAboveEllipsoid` exists each
  band is described `Height: <z> m` and carries `HEIGHT_METERS`/`Z_VALUE` band metadata. Select one
  level with `-b N`; for values on the imaging grid use cube interpolation (next section).
- **Static layers** ship as a separate companion granule; find its URL in the identification /
  processing metadata and open it as a NISAR product in its own right.

## 3D cube interpolation (`QUANTITY` + `DEM_FILE`)

```bash
# cube resolved from QUANTITY, reference grid = product default layer (frequency A)
gdal_translate -co TILED=YES -co BLOCKXSIZE=512 -co BLOCKYSIZE=512 -co COMPRESS=ZSTD \
  -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" -oo DEM_RESAMPLING=CUBICSPLINE \
  NISAR:"$GCOV" inc.tif
# explicit cube path + GSLC frequency-B HV grid
gdal_translate -oo QUANTITY=incidenceAngle -oo FREQ=B -oo POL=HV -oo DEM_FILE="$DEM" \
  NISAR:"$GSLC":/science/LSAR/GSLC/metadata/radarGrid/incidenceAngle inc_gslc_B.tif
# RSLC: radar-coordinate output on the HH swath grid, GCPs attached; ocean/DEM gaps at 0 m
gdal_translate -co TILED=YES -co COMPRESS=ZSTD -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" \
  -oo DEM_NODATA_HEIGHT=0 NISAR:"$RSLC" inc_rslc.tif
# RUNW/RIFG: same, on the multilooked interferogram/HH/<layer> grid (UTM GCPs)
gdal_translate -oo QUANTITY=incidenceAngle -oo DEM_FILE="$DEM" NISAR:"$RUNW" inc_runw.tif
```

What the driver does (v0.7.0):

1. `QUANTITY` present → open is routed to the interpolation dataset. `DEM_FILE` missing → hard
   error "DEM_FILE open option is REQUIRED when QUANTITY is specified."
2. The **cube** is the dataset the connection string points at; with a bare `NISAR:"file.h5"` it is
   resolved to `/science/<INST>/<PRODUCT>/metadata/radarGrid/<QUANTITY>` from the granule's
   identification metadata ("Failed to open valid 3D coarse metadata cube at …" if absent).
3. The **target grid** is chosen from the product type in the granule, honouring `INST`/`FREQ`/`POL`:
   GCOV, GSLC → `/science/<INST>/<PRODUCT>/grids/frequency<F>/<POL>` (default `HHHH` / `HH`);
   GUNW → `.../grids/frequency<F>/unwrappedInterferogram/<POL>/unwrappedPhase` (default `HH`);
   RSLC → `.../swaths/frequency<F>/<POL>` (default `HH`); RIFG → `.../swaths/frequency<F>/
   interferogram/<POL>/wrappedInterferogram`, RUNW → `.../interferogram/<POL>/unwrappedPhase`
   (default `HH`); on L1 the cube is under `metadata/geolocationGrid`. ROFF is not routed
   ("Level-1 product ROFF is not supported (only RSLC, RIFG, RUNW)").
4. L2/L3: the DEM is a lazily-warped VRT on the target grid (`DEM_RESAMPLING`; blocks resampled on
   demand, so memory is bounded on GSLC-sized grids); the whole cube is loaded into RAM and each
   output pixel is interpolated in height. Output is a single-band **Float32** raster with the target
   grid's georeferencing, 512×512 blocks, NaN where the cube is missing, height 0 outside DEM coverage.
5. L1 (RSLC, RIFG, RUNW): output pixels are (slant range, zero-Doppler time) from the `slantRange`/
   `zeroDopplerTime` vectors nearest above the reference raster (RSLC: `frequency<F>` + `swaths`;
   RIFG/RUNW: `frequency<F>/interferogram`), indexed into the cube by the geolocation grid's own
   axes. Per pixel the terrain height is solved by fixed-point iteration h → DEM(coordinateX(h),
   coordinateY(h)) (bilinear DEM sample in the geolocation CRS — a DEM in another CRS is warped
   lazily over the footprint window only; RIFG/RUNW grids are UTM — 0.1 m tolerance, ≤10 passes),
   DEM nodata/absent → `DEM_NODATA_HEIGHT`. No GeoTransform; the GCPs + GCP CRS are passed through
   (`NISAR_GRID_TYPE=RADAR`, `NISAR_GEOLOCATION_EPSG`), so `gdalwarp` the result like the swath.
   `QUANTITY=coordinateX|coordinateY` yields the solved ground coordinates (Float32 precision).
   Observed: full 26126×7600 swath in 37 s / ~1.2 GB RSS over HTTPS; ~10 ms per 512×512 block.
6. The result carries `NISAR_PRODUCT_TYPE`, `NISAR_CUBE_PATH`, `NISAR_REFERENCE_GRID` and
   `NISAR_QUANTITY` metadata items recording what was resolved.

Public DEM (observed): `/vsis3/sds-n-cumulus-prod-nisar-products/DEM/v1.2/EPSG4326/EPSG4326.vrt`
(also over HTTPS from the ASF DAAC). Its resolution need not match NISAR posting.

## Warping to lat-lon

L2/L3 are already geocoded, so this is an ordinary CRS transform:

```bash
gdalwarp -t_srs EPSG:4326 -r near -of GTiff \
  -srcnodata nan -dstnodata nan \
  -co COMPRESS=DEFLATE -co PREDICTOR=3 -co TILED=YES -co BIGTIFF=IF_SAFER \
  -multi -wo NUM_THREADS=ALL_CPUS \
  NISAR:"$GUNW":/science/LSAR/GUNW/grids/frequencyA/unwrappedInterferogram/HH/unwrappedPhase \
  gunw_phase_ll.tif
```

L1 has GCPs only. `gdalwarp -tps` (README's recommendation for accuracy) or `-order 2` will
produce a convincing image, but it is a quick look — the geocoded L2 products were made with the
full geometry and a DEM; use them for anything quantitative.

Resampling: nearest for unwrapped phase, connected components, wrapped/complex data and any
mask/flag layer; bilinear is fine for backscatter, coherence and smooth geometric fields. Use
`-srcnodata nan -dstnodata nan` on phase/offset layers (NaN fill). Prefer letting `gdalwarp` derive
output resolution; when tiles from several granules must align, `-tr <x> <y> -tap`.

## Masking (`MASK=YES`)

Off by default. With `-oo MASK=YES` the driver looks for a sibling dataset named `mask` in the
raster's parent group (`grids/frequencyA/mask` for GCOV/GSLC,
`grids/frequencyA/unwrappedInterferogram/HH/mask` for GUNW) and exposes it as a per-dataset GDAL
mask band (`GMF_PER_DATASET`). If there is no such dataset you get GDAL's all-valid mask.

Value semantics (from source):

- **GUNW**: byte `v` in 1–254 is decoded as reference sub-swath `(v/10)%10` and secondary
  sub-swath `v%10`; valid (255) only if **both** are non-zero. 0 and 255 are invalid.
- **Everything else** (GCOV, GSLC, …): values **1–5 valid**, all other values (0, 255, …)
  invalid.

Because the default is `NO`, `gdal_translate`/`gdalwarp` copy stored fill values unless you set
`MASK=YES`. To carry the mask into a GeoTIFF use `gdalwarp -dstalpha`, or let `gdal_translate`
copy it as an internal TIFF mask (its default `-mask auto`). Derived subdatasets do **not** inherit
the mask.

*Historical:* masking was applied by default in 0.1.4–0.2.x and made opt-in in 0.3.0. Pin `MASK`
explicitly in anything reproducible.

## Derived subdatasets

GDAL's `DERIVED_SUBDATASET:` driver is advertised in the `DERIVED_SUBDATASETS` domain:
`AMPLITUDE`, `PHASE`, `REAL`, `IMAG`, `INTENSITY`, `CONJ` for complex rasters,
`LOGAMPLITUDE` for real-valued ones. Quote the whole connection string as one argument:

```bash
gdal_translate "DERIVED_SUBDATASET:AMPLITUDE:NISAR:$GSLC:/science/LSAR/GSLC/grids/frequencyA/HH" amp.tif
gdal_translate "DERIVED_SUBDATASET:LOGAMPLITUDE:NISAR:$GCOV:/science/LSAR/GCOV/grids/frequencyA/HHHH" db.tif
```

Computed on read, nothing stored. Most valuable on complex data (GSLC, wrapped GUNW layers,
off-diagonal GCOV terms).

## Overviews

Every band exposes virtual power-of-two overviews (2× … up to `NISAR_MAX_VIRTUAL_OVR`, default
16), computed by decimated reads — nothing is stored. Enough for QGIS/zoom-out; for repeated
analysis convert to COG (`gdal_translate -of COG`).

## Access paths and credentials

Local files need no credentials. Remote forms (`s3://`, `/vsis3/`, `https://`, `/vsicurl/`) all go
through GDAL VSI via a custom HDF5 virtual file layer, so **standard GDAL configuration applies**:
`AWS_*` env vars, `AWS_PROFILE`, `~/.aws/credentials|config`, instance/role credentials,
`AWS_NO_SIGN_REQUEST=YES` for public buckets, `GDAL_HTTP_*`, proxies, etc. In-region compute is
dramatically faster than out-of-region.

```bash
# any AWS setup
export AWS_PROFILE=<profile>            # or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
export AWS_REGION=us-west-2             # not included in exported creds

# repo helpers (print export lines from an existing profile)
eval $(./aws_env.sh <profile>)          # e.g. saml-pub after a JPL SAML login
./aws_creds.sh <profile>                # same, plus os.environ[...] lines for notebooks
```

Jupyter kernels do not inherit a terminal's exports — set `AWS_PROFILE` before starting the
kernel, or `gdal.SetConfigOption(...)`/`os.environ[...]` in the first cell. Partial credential
sets (e.g. a stale `AWS_SESSION_TOKEN`) break signing; `unset` them before switching profiles.

Set `GDAL_HTTP_MAX_RETRY=5` for long remote reads; the driver leaves it at the GDAL default. `conda-build/tests/verify_hdf5_ros3.py`
is an h5py/ROS3 diagnostic for the *HDF5 library's* own S3 path and is unrelated to how this driver
reads remote files.

## Performance knobs

| Variable | Default | Effect |
|---|---|---|
| `NISAR_PREFETCH_GRID` | `1` | Side of the N×N grid of HDF5 chunks coalesced into one range request when a block misses the cache (1 = one chunk). Keep `1` for tiled/interactive access; raise (e.g. `24`) for full-scene batch reads. |
| `NISAR_MAX_MEGAFETCH_BYTES` | `16777216` (16 MiB) | Upper bound on one coalesced "mega-fetch" range request. |
| `NISAR_MAX_VIRTUAL_OVR` | `16` | Largest virtual overview decimation factor. |
| `NISAR_EXPORT_ZARR` | `NO` | Write a kerchunk-style virtual Zarr sidecar JSON to `/tmp/nisar_kerchunk<dataset>.json` on first chunk map. Debug/interop only. |
| `GDAL_NUM_THREADS` | GDAL default | Parallel chunk decompression. `ALL_CPUS` is reasonable. |
| `GDAL_HTTP_MAX_RETRY` | GDAL default | Retries on transient HTTP errors; `5` is a good value for S3. |
| `GDAL_CACHEMAX` | GDAL default | GDAL block cache; raise for large translations. |
| `GDAL_DISABLE_READDIR_ON_OPEN` | — | `EMPTY_DIR` avoids listing the S3 prefix on open. |
| `GDAL_PAM_ENABLED` | — | `NO` stops `.aux.xml` sidecars next to local granules. |

Fixed inside the driver (not configurable today): 4 MiB HDF5 page buffer, 8 MiB / 521-slot HDF5
chunk cache per dataset, GDAL block size = HDF5 chunk size, chunk offsets mapped lazily on first
read (`H5Dchunk_iter`), mega-fetched chunks decompressed in parallel. Variables from older notes
such as `NISAR_CHUNK_CACHE_SIZE_MB` do not exist.

*Observed, not benchmarks:* NISAR granules use HDF5 paged aggregation with a 4 MiB page, so a
single-pixel read still costs at least one page; a full 1.1 GB GCOV raster took on the order of
150 range requests; peak memory for a full-raster read was ~1.1 GiB versus tens of GiB for
h5py/xarray with a whole-layer page buffer. Build workflows around block-wise reads.

## Install and verification

```bash
mamba create -n nisar-env -c nisar-forge -c conda-forge gdal-driver-nisar && conda activate nisar-env
mamba install -c nisar-forge -c conda-forge gdal-driver-nisar=0.7.2     # pin in pipelines
```

The package installs this document, the repository README, `docs/HOWTO.md` and
`docs/AGENT_GUIDE.md` (one-page map of the driver and its documents) to
`$CONDA_PREFIX/share/doc/gdal-driver-nisar/` (offline copy of the user documentation).

Verify from the binary, not just `conda list`:

```bash
gdalinfo --formats | grep NISAR    # "NISAR -raster- (ro…): NISAR HDF5"
gdalinfo --format NISAR            # DRIVER_VERSION "v0.7.2 (Build Date: …)" + open option list
gdalinfo --version                 # a broken plugin errors on every GDAL invocation
```

Gotchas:

- A conda update can report success while leaving the old `gdal_NISAR.so` in place (*observed*).
  If `DRIVER_VERSION` has not moved, `conda remove gdal-driver-nisar` then install again.
- The plugin is built against a pinned GDAL (`3.12` for 0.7.2). Mixing it with another GDAL
  produces load errors or a missing `NISAR` entry in `--formats`.
- `GLIBC_2.38 not found` means the Linux binary is newer than the host libc (older JupyterHub
  images) — needs a rebuild, not configuration.
- Building from source: see `BUILDING.md`; the plugin is a CMake project in
  `frmts/nisar/` installed into `$PREFIX/lib/gdalplugins`. Set
  `GDAL_DRIVER_PATH` if GDAL cannot find it.

## Checklist before trusting output quantitatively

- Confirm the driver version from `gdalinfo --format NISAR`.
- Pin `MASK` explicitly (default `NO`).
- Copy layer paths from `SUBDATASET_n_NAME`, or use `FREQ`/`POL` where the product allows.
- Confirm the CRS is a real projected system and pixel size is a plausible ground distance. A pixel
  size near 1 means the GeoTransform was not found.
- Check corner coordinates in degrees against where the granule should be.
- Calibration-grid rasters (NEB etc.): different corners from the principal grid are expected; a
  one-unit pixel size is not.
- Phase, offsets, connected components: nearest-neighbour resampling, NaN nodata on both sides.
- Do not quote `gdalinfo -stats`/`-approx_stats` mean/stddev unless the dataset has
  `mean_value`/`sample_stddev` attributes — they may be synthesised from min/max.
- Metadata cubes: decide between a single height band and a DEM-interpolated raster; not
  interchangeable. Interpolation is GCOV, GSLC and GUNW only (no L1 yet).
- L1 frequency-B rasters: verify GCP geolocation independently.

## How to talk about this

The audience is normally SAR/InSAR-fluent: skip mission background and explain the driver's mapping
onto GDAL's dataset model (container → subdatasets, 3D cube → bands, mask dataset → mask band,
GCPs vs GeoTransform). Distinguish clearly between behaviour verified in the current source,
behaviour observed on a specific granule/version, and layouts reconstructed from the product spec.
