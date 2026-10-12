# NISAR GDAL Driver: where it is and what documentation ships with it

Short orientation note for AI agents and users working with NISAR HDF5 products through GDAL.

## What it is

`gdal-driver-nisar` is a **read-only GDAL raster plugin** (short name `NISAR`) for NASA-ISRO
NISAR HDF5 products (RSLC, RIFG, RUNW, GSLC, GCOV, GUNW, GOFF, SME2, radar-grid metadata
cubes). It is not a Python package: there is nothing to import. Use the GDAL command-line tools
(`gdalinfo`, `gdal_translate`, `gdalwarp`, `gdallocationinfo`) or `osgeo.gdal`.

Current release: **0.7.2**, built against GDAL 3.12. Packages on `nisar-forge`: linux-64 and
linux-aarch64 (osx-arm64 is available for 0.7.0; newer macOS builds are made natively, see
`BUILDING.md`).

## Where to get it

| What | Where |
| ---- | ----- |
| Conda package | channel `nisar-forge` on Anaconda.org: https://anaconda.org/nisar-forge/gdal-driver-nisar |
| Source code and issues | https://github.com/JPL-Devin/nisar-hdf5-gdalplugin |
| Online HOWTO | https://github.com/JPL-Devin/nisar-hdf5-gdalplugin/blob/main/docs/HOWTO.md |

Install into a fresh environment (recommended, avoids GDAL version conflicts):

```bash
conda create -n nisar -c nisar-forge -c conda-forge gdal-driver-nisar gdal
conda activate nisar
```

or into an existing one:

```bash
conda install -c nisar-forge -c conda-forge gdal-driver-nisar
```

## Where it lands after installation

| Item | Path |
| ---- | ---- |
| Plugin library | `$CONDA_PREFIX/lib/gdalplugins/gdal_NISAR.so` (`.dylib` on macOS) |
| Documentation | `$CONDA_PREFIX/share/doc/gdal-driver-nisar/` |

GDAL loads the plugin automatically from `lib/gdalplugins`. Verify:

```bash
gdalinfo --formats | grep NISAR   # NISAR -raster- (rovs): NISAR HDF5 (*.h5)
gdalinfo --format NISAR           # DRIVER_VERSION and the open-option list
ls $CONDA_PREFIX/share/doc/gdal-driver-nisar/
```

## Documentation installed with the package

All files are Markdown and readable offline in `$CONDA_PREFIX/share/doc/gdal-driver-nisar/`.
Links between them resolve locally; links to files that are not packaged point to GitHub.

| File | Read it when you need |
| ---- | --------------------- |
| `AGENT_GUIDE.md` | This file (`docs/AGENT_GUIDE.md` in the repository). |
| `SKILL.md` | **Start here if you are an agent.** Compact operating guide: connection-string syntax, open options (`INST`, `FREQ`, `POL`, `METADATA`, `MASK`, `QUANTITY`, `DEM_FILE`, `DEM_RESAMPLING`), per-product gotchas, remote S3/HTTPS tuning, and the rule *inventory the granule with `gdalinfo` first, never type a layer path from memory*. |
| `HOWTO.md` | Task-oriented user guide: installation and verification, Earthdata/S3 credentials, the container/subdataset addressing model, open-option reference, per-product recipes (RSLC/RIFG/RUNW, GSLC, GCOV, GUNW/GOFF, SME2, metadata cubes, static layers), warping to lat-lon, masking, derived subdatasets and overviews, performance tuning, Python examples, troubleshooting, version history. |
| `README.md` | Reference and architecture: feature list, full open-option and configuration-option tables, georeferencing model (L1 GCPs vs. L2/L3 GeoTransform), interpolation mode, mask semantics, remote-I/O design (HDF5 VFL over GDAL VSI, mega-fetch), troubleshooting, license. |

Repository-only documents (linked from the installed README, on GitHub):

| File | Content |
| ---- | ------- |
| `BUILDING.md` | Building the plugin and the conda package (native macOS, Docker for Linux), publishing to `nisar-forge`. |
| `Level_1_Product_Processing.md` | How L1 radar-coordinate products are georeferenced with GCPs from `geolocationGrid`. |
| `L2 3D Data Cube Interpolation Implementation Plan.md` | Design of the DEM-driven interpolation of 3-D metadata cubes (`QUANTITY` + `DEM_FILE`). |
| `AGENTS.md` | Contributor guidance for agents editing the driver source (not shipped). |

## Minimal first command

```bash
gdalinfo 'NISAR:"/path/or/vsicurl/URL/to/granule.h5"'      # lists SUBDATASET_n_NAME
gdalinfo -oo FREQ=A -oo POL=HHHH 'NISAR:"granule.h5"'       # resolve a GCOV layer by option
```

Copyright 2025, by the California Institute of Technology. Apache License 2.0.
