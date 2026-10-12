# AGENTS.md

Guidance for coding agents working in this repository. Humans: see [README.md](README.md)
(usage) and [BUILDING.md](BUILDING.md) (release packaging).

## What this repo is

A **read-only GDAL raster driver** (`gdal_NISAR.so` / `.dylib`, short name `NISAR`) for
NASA-ISRO SAR (NISAR) HDF5 products, written in C++17 and shipped as the conda package
`gdal-driver-nisar` on the `nisar-forge` channel. There is no Python package: users drive
it through the GDAL CLI or `osgeo.gdal`.

The C++ sources live in `frmts/nisar/` (same layout as a GDAL in-tree driver); the conda
recipe builds from there:

```
frmts/nisar/
  nisar.cpp                         GDALRegister_NISAR(): driver metadata, DRIVER_VERSION (standalone only), open-option list
  nisardataset.{h,cpp}              NisarDataset: connection-string parsing, product identification,
                                    subdatasets, L1 GCPs / L2-L3 GeoTransform+SRS, metadata domains
  nisarrasterband.{h,cpp}           NisarRasterBand: chunk-aligned IReadBlock, "mega-fetch", overviews,
                                    derived subdatasets, NisarHDF5MaskBand, kerchunk sidecar
  nisaroverviewband.h               Virtual power-of-two overview band
  nisarinterpolated*.{h,cpp}        NisarInterpolatedDataset/RasterBand: 3-D metadata-cube + DEM interpolation
  hdf5vfl.{h,cpp}                   HDF5 Virtual File Layer driver routing all HDF5 I/O through GDAL VSI
  nisar_priv.h                      Private helpers, mask decoding, HDF5 iteration callbacks
  CMakeLists.txt                    Standalone plugin build (gdal_NISAR MODULE; defines NISAR_DRIVER_VERSION)
  CMakeLists.gdal.txt               In-tree GDAL build (add_gdal_driver); copy to <gdal>/frmts/nisar/CMakeLists.txt
conda-build/nisar-gdal-recipe/      build.sh, meta.yaml (source: path ../../frmts/nisar), conda_build_config.yaml
conda-build/tests/                  pytest suite + shell/diagnostic scripts (see Testing)
.agents/skills/nisar-gdal/SKILL.md  How to *use* the driver (connection strings, open options, gotchas)
docs/AGENT_GUIDE.md                 One-page map: where the driver installs, which docs ship with it
docs/HOWTO.md, Level_1_Product_Processing.md, L2 3D Data Cube Interpolation Implementation Plan.md
```

Read `.agents/skills/nisar-gdal/SKILL.md` before writing or debugging any `gdalinfo` /
`gdal_translate` / `gdalwarp` command against a NISAR granule.

## Development environment

The dev environment is a conda/micromamba prefix at `~/nisar-env` (GDAL 3.12, HDF5, zlib-ng,
cmake, numpy, earthaccess, pytest). `GDAL_DRIVER_PATH` is set to
`~/build:~/nisar-env/lib/gdalplugins`, so an out-of-tree build in `~/build` is picked up
automatically. Nothing needs to be installed system-wide.

If `~/nisar-env` is missing, recreate it:

```bash
micromamba create -y -p ~/nisar-env -c conda-forge python=3.12 gdal=3.12 libgdal-core \
    libgdal-hdf5 hdf5 zlib-ng cmake make cxx-compiler numpy earthaccess pytest
export PATH="$HOME/nisar-env/bin:$PATH" GDAL_DRIVER_PATH="$HOME/build:$HOME/nisar-env/lib/gdalplugins" \
       PROJ_DATA="$HOME/nisar-env/share/proj"
```

## Build

Development build (fast, incremental; produces `~/build/gdal_NISAR.so`):

```bash
cmake -S frmts/nisar -B "$HOME/build" -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_PREFIX_PATH="$HOME/nisar-env" -DCMAKE_INSTALL_PREFIX="$HOME/nisar-env"
cmake --build "$HOME/build" -j$(nproc)
gdalinfo --formats | grep NISAR      # must print:  NISAR -raster- (rovs): NISAR HDF5 (*.h5)
gdalinfo --format NISAR              # DRIVER_VERSION + open-option list
```

Use `$HOME` rather than `~` inside `-D...=` arguments: the shell does not tilde-expand there,
and a literal `~/nisar-env` in `CMAKE_PREFIX_PATH` is silently ignored.
`~/build` is outside the repo on purpose; never commit build output. Release packages are
produced with `conda build` (natively on macOS, via the root `Dockerfile` for Linux) — see
BUILDING.md; do not change `meta.yaml` / `conda_build_config.yaml` pins casually.

A broken plugin makes **every** GDAL invocation print an error, so `gdalinfo --version`
is a quick smoke test after a build.

## Test

There is no unit-test harness for the C++; the tests exercise the built plugin through
`osgeo.gdal`.

```bash
# Interpolation / L1 georeferencing suite (48 tests). Granules are located via
# earthaccess (Earthdata ~/.netrc) and read over HTTPS; the DEM is the public NISAR DEM VRT.
pytest -v -p no:cacheprovider conda-build/tests/test_interpolation_earthaccess.py

# Iterate on a subset
pytest -q -p no:cacheprovider conda-build/tests/test_interpolation_earthaccess.py -k "rslc and not real_dem"
```

Notes:

- The suite needs an Earthdata login in `~/.netrc` (`machine urs.earthdata.nasa.gov login … password …`).
  Without it, `earthaccess.login(strategy="netrc")` raises `LoginStrategyUnavailable` and most
  tests fail — that is a credentials problem, not a driver regression.
- Remote reads are slow (granules are 40 MB – 1 GB); a full run takes many minutes. Set
  `NISAR_TEST_DATA_DIR=<dir with local .h5 copies>` to read local files instead. Other
  overrides: `NISAR_TEST_GRANULE`, `NISAR_TEST_IFG_GRANULE`, `NISAR_TEST_DEM`,
  `NISAR_TEST_ACCESS=external|direct` (see the module docstring).
- `-p no:cacheprovider` keeps `.pytest_cache` out of the tree; also delete
  `conda-build/tests/__pycache__` before committing.
- `test_driver_version` asserts the exact `DRIVER_VERSION` prefix — update it together with
  `project(VERSION ...)` in `frmts/nisar/CMakeLists.txt` and `meta.yaml` when bumping the version.
- `conda-build/tests/run_tests_NISAR_GSLC.sh` (end-to-end + benchmark against an S3 GSLC) and
  `verify_hdf5_ros3.py` (h5py/ROS3 diagnostic, unrelated to how this driver reads remote files)
  need AWS credentials and are not part of the routine loop.

Manual checks against real data:

```bash
CPL_DEBUG=NISAR_DRIVER gdalinfo -oo FREQ=A -oo POL=HHHH 'NISAR:"/vsicurl/https://…/NISAR_L2_PR_GCOV_….h5"'
gdalinfo -nogcp 'NISAR:"…RSLC….h5":/science/LSAR/RSLC/swaths/frequencyA/HH'
```

Always copy layer paths from the container's `SUBDATASET_n_NAME` list; never type them from
memory (they differ by product, level and spec version).

## Lint / formatting

There is no linter, formatter config or pre-commit hook in the repo. The compiler is the
gate: build with the command above and treat **new warnings in files you touched** as errors.
Match the surrounding style instead of reformatting:

- 4-space indentation, GDAL-style naming (`poDataset`, `hFile`, `nXSize`, `psInfo`, `osPath`,
  `bFlag`, `dfValue`), `CPL*`/`VSI*` allocation and string helpers, `CPLError(CE_Failure,
  CPLE_*, …)` for user-facing errors, `CPLDebug("NISAR_DRIVER", …)` for diagnostics
  (other categories in use: `NISAR_NET_PERF`, `NISAR_MASK_PERF`, `NISAR_INTERP_PERF`,
  `NISAR_OVERVIEW`, `NISAR_VISITOR`).
- Every C++ source file starts with the Caltech license block used in `frmts/nisar/nisar.cpp`
  (`Copyright 2025, California Institute of Technology. All rights reserved. U.S. Government
  sponsorship acknowledged.`, the full Apache-2.0 notice and `SPDX-License-Identifier:
  Apache-2.0`, in the style of GDAL's `frmts/mrf` Caltech header); copy it into new files
  verbatim. `hdf5vfl.{h,cpp}` additionally keep the MIT notice of the GDAL code they derive
  from. The former export-control / commercial-negotiation header must not be reintroduced
  (see `UPSTREAM_CHECKLIST.md` for the pending legal sign-off).
- Python test code follows plain PEP 8 (numpy + `osgeo.gdal`, `gdal.UseExceptions()`) and starts
  with the short Caltech copyright + `SPDX-License-Identifier: Apache-2.0` comment block used in
  `conda-build/tests/test_dump.py`.

## Conventions and gotchas

- **Read-only driver.** Never add write/update paths; `Open` rejects `GDAL_OF_UPDATE`.
- **HDF5 hygiene.** Every `H5*open`/`H5*create` handle must be closed on every return path
  (`H5Dclose`, `H5Gclose`, `H5Sclose`, `H5Tclose`, `H5Pclose`, `H5Aclose`). Leaked handles
  keep remote files open and surface as confusing errors at `H5Fclose` time. Follow the existing
  RAII/early-close patterns in `nisardataset.cpp`.
- **Remote I/O goes through GDAL VSI**, not HDF5's ROS3 VFD. `hdf5vfl.cpp` turns each HDF5
  read into `VSIFReadL`; `s3://` → `/vsis3/`, `http(s)://` → `/vsicurl/`. Do not add
  AWS/HTTP-specific code paths — set GDAL config options instead (`GDAL_HTTP_*`, `AWS_*`).
- **Performance is the point.** GDAL block size == HDF5 chunk size; neighbouring chunks are
  coalesced into one range request (`NISAR_PREFETCH_GRID`, `NISAR_MAX_MEGAFETCH_BYTES`) and
  decompressed in parallel. Changes to `IReadBlock`, the chunk map (`H5Dchunk_iter`) or the
  page-buffer/FAPL setup need a before/after timing on a remote granule, not just a green test.
- **Georeferencing model.** L2/L3 (`GSLC`, `GCOV`, `GUNW`, `GOFF`, `SME2`): GeoTransform from
  `xCoordinates`/`yCoordinates` (pixel-centre → corner shift) + SRS from the sibling `projection`
  dataset, resolved by walking *up* the group hierarchy. L1 (`RSLC`, `RIFG`, `RUNW`): no
  GeoTransform, GCPs from `metadata/geolocationGrid`; line index derives from
  `swaths/zeroDopplerTime[0]` + `zeroDopplerTimeSpacing` (never `nominalAcquisitionPRF`). See
  `Level_1_Product_Processing.md`.
- **Open options** are declared once in `nisar.cpp` (`GDAL_DMD_OPENOPTIONLIST`) and parsed
  in `NisarDataset::Open`. Adding one means: XML entry in `nisar.cpp`, parsing, README table,
  `docs/HOWTO.md`, and the `nisar-gdal` skill table.
- **Interpolation mode** (`QUANTITY` + mandatory `DEM_FILE`) routes to
  `NisarInterpolatedDataset`. The target grid is resolved from the product type honouring
  `INST`/`FREQ`/`POL`; L1 output stays in radar coordinates with GCPs passed through. Keep the
  numpy reference kernels in the pytest suite in sync when the C++ interpolation changes.
- **Mask semantics** live in `nisar_priv.h` / `nisarrasterband.cpp`: GUNW byte encodes
  reference/secondary sub-swath digits; all other products treat values 1–5 as valid.
  `MASK` defaults to `NO`.
- **Version bump** = three places: `version`/`build number` in `meta.yaml`, `project(VERSION ...)`
  in `frmts/nisar/CMakeLists.txt` (becomes `DRIVER_VERSION`), and the assertion in `test_interpolation_earthaccess.py::test_driver_version`
  (plus the current-version lines in the skill file and `docs/HOWTO.md`, including its version
  history table, and the release line in `docs/AGENT_GUIDE.md`). README, HOWTO, AGENT_GUIDE and
  the skill file are installed by `build.sh` into the package under `share/doc/gdal-driver-nisar/`;
  AGENTS.md is not shipped.
- **Docs are part of the change.** User-visible behaviour is documented in README.md
  (open options, config options, troubleshooting), `docs/HOWTO.md` and
  `.agents/skills/nisar-gdal/SKILL.md`; update them in the same PR.
- `aws_env.sh` / `aws_creds.sh` print credentials from an AWS profile; never commit real
  credentials, `.netrc`, or `.urs_cookies`.

## Pull requests

- Branch from `main`; one focused change per PR, descriptive commit subjects (see `git log`
  for the house style, e.g. `L1 interpolation: route RIFG/RUNW through the radar-coordinate path`).
- Before opening a PR: clean build with no new warnings, `gdalinfo --formats | grep NISAR`,
  and the relevant subset of the pytest suite (the full suite if georeferencing, interpolation
  or block reading changed).
- Describe *why* and any observed performance/behaviour deltas on a real granule in the PR body.
