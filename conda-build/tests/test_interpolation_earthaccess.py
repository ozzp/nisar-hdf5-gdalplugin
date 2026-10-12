# Copyright 2025, by the California Institute of Technology.
# ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
# SPDX-License-Identifier: Apache-2.0

"""
Metadata-cube interpolation tests for the NISAR GDAL driver, driven by earthaccess.

Granules are located through NASA Earthdata (CMR) and read through the driver over
HTTPS (CloudFront) with `.netrc` credentials, or over S3 when running inside AWS
us-west-2. The DEM is the public NISAR DEM VRT.

Run with pytest (or directly with `python test_interpolation_earthaccess.py`):

    earthaccess login --strategy netrc        # or place ~/.netrc first
    pytest -v conda-build/tests/test_interpolation_earthaccess.py

Environment overrides:
    NISAR_TEST_GRANULE   granule ID stem shared by the RSLC/GCOV/GSLC triple
                         (default: a 2025-11-09 DHDH acquisition, ~1 GB each)
    NISAR_TEST_IFG_GRANULE  granule ID stem shared by the RIFG/RUNW pair
                         (default: a 2025-10-17/29 SH pair, 190 MB + 40 MB)
    NISAR_TEST_DATA_DIR  directory with local copies of the granules; when the
                         files exist there they are used instead of remote reads
    NISAR_TEST_DEM       DEM path/URL (default: public EPSG4326.vrt over HTTPS)
    NISAR_TEST_ACCESS    "external" (HTTPS, default) or "direct" (S3, in-region)
"""

import os
import sys
import tempfile
import urllib.request

import numpy as np
import pytest

try:
    import earthaccess
except ImportError:  # pragma: no cover
    earthaccess = None

from osgeo import gdal, osr

gdal.UseExceptions()

DEFAULT_STEM = (
    "004_159_A_024_2005_DHDH_A_20251109T051646_20251109T051650_X05010_N_P_J_001"
)
DEFAULT_IFG_STEM = (
    "003_005_D_074_004_4000_SH_20251017T132342_20251017T132345_"
    "20251029T132342_20251029T132346_X05010_N_P_J_001"
)
DEFAULT_DEM = (
    "/vsicurl/https://nisar.asf.earthdatacloud.nasa.gov/NISAR/DEM/v1.2/"
    "EPSG4326/EPSG4326.vrt"
)
SHORT_NAMES = {
    "RSLC": "NISAR_L1_RSLC_BETA_V1",
    "GCOV": "NISAR_L2_GCOV_BETA_V1",
    "GSLC": "NISAR_L2_GSLC_BETA_V1",
    "RIFG": "NISAR_L1_RIFG_BETA_V1",
    "RUNW": "NISAR_L1_RUNW_BETA_V1",
}
IFG_PRODUCTS = ("RIFG", "RUNW")
IFG_LAYER = {"RIFG": "wrappedInterferogram", "RUNW": "unwrappedPhase"}
QUANTITY = "incidenceAngle"
WINDOW_M = 10000.0  # side of the test window, metres
RSLC_GRID = "/science/LSAR/RSLC/metadata/geolocationGrid"
RSLC_WINDOW = (20000, 3000, 600, 500)  # xoff, yoff, xsize, ysize in the HH swath


def _in_us_west_2():
    """True only on an EC2 instance whose IMDS reports us-west-2."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        return region == "us-west-2"
    try:
        with open("/sys/devices/virtual/dmi/id/sys_vendor") as f:
            if "amazon" not in f.read().lower():
                return False
        req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(req, timeout=2).read().decode()
        req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/placement/region",
            headers={"X-aws-ec2-metadata-token": token})
        return urllib.request.urlopen(req, timeout=2).read().decode() == "us-west-2"
    except Exception:
        return False


def _configure_gdal(access):
    gdal.SetConfigOption("GDAL_PAM_ENABLED", "NO")
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    gdal.SetConfigOption("GDAL_CACHEMAX", "536870912")
    if access == "direct":
        gdal.SetConfigOption("AWS_REGION", "us-west-2")
    else:
        gdal.SetConfigOption("GDAL_ENABLE_HDF5_ROS3", "NO")
        gdal.SetConfigOption("GDAL_HTTP_NETRC", "YES")
        gdal.SetConfigOption("GDAL_HTTP_UNRESTRICTED_AUTH", "YES")
        gdal.SetConfigOption("GDAL_HTTP_TIMEOUT", "60")
        gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
        gdal.SetConfigOption("GDAL_HTTP_VERSION", "2")
        gdal.SetConfigOption("GDAL_HTTP_MULTIPLEX", "YES")
        gdal.SetConfigOption("CPL_VSIL_CURL_CHUNK_SIZE", "8388608")
        gdal.SetConfigOption("CPL_VSIL_CURL_CACHE_SIZE", "536870912")
        gdal.SetConfigOption("GDAL_INGESTED_BYTES_AT_OPEN", "32768")
        gdal.SetConfigOption("GDAL_HTTP_MERGE_CONSECUTIVE_RANGES", "YES")
        cookies = os.path.join(os.path.expanduser("~"), ".urs_cookies")
        gdal.SetConfigOption("GDAL_HTTP_COOKIEFILE", cookies)
        gdal.SetConfigOption("GDAL_HTTP_COOKIEJAR", cookies)


class Granules:
    """Resolves product -> URI for one RSLC/GCOV/GSLC acquisition and one RIFG/RUNW pair."""

    def __init__(self):
        self.stem = os.environ.get("NISAR_TEST_GRANULE", DEFAULT_STEM)
        self.ifg_stem = os.environ.get("NISAR_TEST_IFG_GRANULE", DEFAULT_IFG_STEM)
        self.local_dir = os.environ.get("NISAR_TEST_DATA_DIR")
        self.dem = os.environ.get("NISAR_TEST_DEM", DEFAULT_DEM)
        self.access = os.environ.get(
            "NISAR_TEST_ACCESS", "direct" if _in_us_west_2() else "external"
        )
        _configure_gdal(self.access)
        self._uris = {}
        self._window = None

    def _filename(self, product):
        level = "L1" if product in ("RSLC",) + IFG_PRODUCTS else "L2"
        stem = self.ifg_stem if product in IFG_PRODUCTS else self.stem
        return f"NISAR_{level}_PR_{product}_{stem}.h5"

    def uri(self, product):
        if product in self._uris:
            return self._uris[product]
        fn = self._filename(product)
        local = os.path.join(self.local_dir, fn) if self.local_dir else None
        if local and os.path.exists(local):
            self._uris[product] = local
            return local
        if earthaccess is None:
            pytest.skip("earthaccess not installed and no local granule copy")
        earthaccess.login(strategy="netrc")
        results = earthaccess.search_data(
            short_name=SHORT_NAMES[product], granule_name=fn[:-3], count=1
        )
        if not results:
            pytest.skip(f"{fn} not found in CMR")
        links = results[0].data_links(access=self.access)
        if not links:
            pytest.skip(f"no {self.access} data link for {fn}")
        self._uris[product] = links[0]
        return links[0]

    def conn(self, product, path=None):
        s = f'NISAR:"{self.uri(product)}"'
        return s + ":" + path if path else s

    def hdf5_arrays(self, product, *paths):
        """Raw HDF5 arrays (via GDAL's HDF5 multidim driver) for reference computations."""
        if gdal.GetDriverByName("HDF5") is None:
            pytest.skip("GDAL HDF5 driver not available")
        uri = self.uri(product)
        if uri.startswith("http"):
            uri = "/vsicurl/" + uri
        elif uri.startswith("s3://"):
            uri = "/vsis3/" + uri[5:]
        root = gdal.OpenEx(uri, gdal.OF_MULTIDIM_RASTER,
                           allowed_drivers=["HDF5"]).GetRootGroup()
        return [root.OpenMDArrayFromFullname(p).ReadAsArray() for p in paths]

    def window(self):
        """WINDOW_M square at the GCOV grid centre, snapped to 40 m so it tiles
        both the 20 m GCOV and the 10 x 5 m GSLC postings."""
        if self._window is None:
            ref = gdal.OpenEx(self.conn("GCOV"), gdal.OF_RASTER,
                              open_options=["FREQ=A", "POL=HHHH"])
            gt = ref.GetGeoTransform()
            cx = gt[0] + gt[1] * ref.RasterXSize / 2.0
            cy = gt[3] + gt[5] * ref.RasterYSize / 2.0
            ulx = gt[0] + 40.0 * round((cx - WINDOW_M / 2 - gt[0]) / 40.0)
            uly = gt[3] - 40.0 * round((gt[3] - cy - WINDOW_M / 2) / 40.0)
            self._window = [ulx, uly, ulx + WINDOW_M, uly - WINDOW_M]
        return self._window


@pytest.fixture(scope="module")
def granules():
    if gdal.GetDriverByName("NISAR") is None:
        pytest.skip("NISAR driver not registered (set GDAL_DRIVER_PATH)")
    return Granules()


def _open_interp(granules, product, path=None, **oo):
    opts = [f"QUANTITY={QUANTITY}", f"DEM_FILE={granules.dem}"]
    opts += [f"{k}={v}" for k, v in oo.items()]
    return gdal.OpenEx(granules.conn(product, path), gdal.OF_RASTER, open_options=opts)


def _read_window(granules, ds):
    return gdal.Translate("", ds, format="MEM", projWin=granules.window())


def test_driver_version():
    drv = gdal.GetDriverByName("NISAR")
    assert drv is not None
    assert drv.GetMetadataItem("DRIVER_VERSION").startswith("v0.7.2")


def test_quantity_requires_dem(granules):
    with pytest.raises(RuntimeError, match="DEM_FILE open option is REQUIRED"):
        gdal.OpenEx(granules.conn("GCOV"), gdal.OF_RASTER,
                    open_options=[f"QUANTITY={QUANTITY}"])


def test_gcov_explicit_cube_path_matches_reference_grid(granules):
    cube = f"/science/LSAR/GCOV/metadata/radarGrid/{QUANTITY}"
    ds = _open_interp(granules, "GCOV", cube)
    ref = gdal.OpenEx(granules.conn("GCOV"), gdal.OF_RASTER,
                      open_options=["FREQ=A", "POL=HHHH"])
    assert (ds.RasterXSize, ds.RasterYSize) == (ref.RasterXSize, ref.RasterYSize)
    assert ds.GetGeoTransform() == ref.GetGeoTransform()
    assert ds.GetSpatialRef().IsSame(ref.GetSpatialRef())
    assert ds.RasterCount == 1
    assert ds.GetRasterBand(1).DataType == gdal.GDT_Float32
    md = ds.GetMetadata()
    assert md["NISAR_PRODUCT_TYPE"] == "GCOV"
    assert md["NISAR_CUBE_PATH"] == cube
    assert md["NISAR_REFERENCE_GRID"] == "/science/LSAR/GCOV/grids/frequencyA/HHHH"
    assert md["NISAR_QUANTITY"] == QUANTITY


def test_gcov_auto_resolved_cube_equals_explicit(granules):
    cube = f"/science/LSAR/GCOV/metadata/radarGrid/{QUANTITY}"
    explicit = _read_window(granules, _open_interp(granules, "GCOV", cube)).ReadAsArray()
    auto_ds = _open_interp(granules, "GCOV")
    assert auto_ds.GetMetadataItem("NISAR_CUBE_PATH") == cube
    auto = _read_window(granules, auto_ds).ReadAsArray()
    gt = auto_ds.GetGeoTransform()
    assert explicit.shape == (round(WINDOW_M / -gt[5]), round(WINDOW_M / gt[1]))
    assert np.array_equal(explicit, auto, equal_nan=True)
    assert np.isfinite(explicit).mean() > 0.9
    assert 20.0 < np.nanmin(explicit) < np.nanmax(explicit) < 60.0


def test_gslc_reference_grid_default(granules):
    ds = _open_interp(granules, "GSLC")
    ref = gdal.OpenEx(granules.conn("GSLC"), gdal.OF_RASTER,
                      open_options=["FREQ=A", "POL=HH"])
    assert (ds.RasterXSize, ds.RasterYSize) == (ref.RasterXSize, ref.RasterYSize)
    assert ds.GetGeoTransform() == ref.GetGeoTransform()
    assert ds.GetSpatialRef().IsSame(ref.GetSpatialRef())
    md = ds.GetMetadata()
    assert md["NISAR_PRODUCT_TYPE"] == "GSLC"
    assert md["NISAR_CUBE_PATH"] == f"/science/LSAR/GSLC/metadata/radarGrid/{QUANTITY}"
    assert md["NISAR_REFERENCE_GRID"] == "/science/LSAR/GSLC/grids/frequencyA/HH"


def test_gslc_reference_grid_freq_b_pol(granules):
    ds = _open_interp(granules, "GSLC", FREQ="B", POL="HV")
    ref = gdal.OpenEx(granules.conn("GSLC"), gdal.OF_RASTER,
                      open_options=["FREQ=B", "POL=HV"])
    assert (ds.RasterXSize, ds.RasterYSize) == (ref.RasterXSize, ref.RasterYSize)
    assert ds.GetGeoTransform() == ref.GetGeoTransform()
    assert ds.GetMetadataItem("NISAR_REFERENCE_GRID") == "/science/LSAR/GSLC/grids/frequencyB/HV"


def test_gslc_matches_gcov_on_common_ground(granules):
    """Same acquisition, same DEM: GSLC (10x5 m) block-averaged to the GCOV posting
    (20 m) must reproduce the GCOV interpolation to well below the cube precision."""
    gslc = _read_window(granules, _open_interp(granules, "GSLC")).ReadAsArray()
    gcov = _read_window(granules, _open_interp(granules, "GCOV")).ReadAsArray()
    fy, fx = gslc.shape[0] // gcov.shape[0], gslc.shape[1] // gcov.shape[1]
    assert (fy * gcov.shape[0], fx * gcov.shape[1]) == gslc.shape
    gslc_coarse = gslc.reshape(gcov.shape[0], fy, gcov.shape[1], fx).mean(axis=(1, 3))
    assert np.nanmax(np.abs(gslc_coarse - gcov)) < 1e-3


def test_gslc_explicit_cube_path_equals_auto(granules):
    cube = f"/science/LSAR/GSLC/metadata/radarGrid/{QUANTITY}"
    explicit = _read_window(granules, _open_interp(granules, "GSLC", cube)).ReadAsArray()
    auto = _read_window(granules, _open_interp(granules, "GSLC")).ReadAsArray()
    assert np.array_equal(explicit, auto, equal_nan=True)


def test_missing_quantity_fails(granules):
    with pytest.raises(RuntimeError, match="Failed to open valid 3D coarse metadata cube"):
        gdal.OpenEx(granules.conn("GCOV"), gdal.OF_RASTER,
                    open_options=["QUANTITY=notACube", f"DEM_FILE={granules.dem}"])


# --- Level-1 (RSLC): interpolation in radar coordinates ---------------------------


def _l1_paths(product):
    """(geolocationGrid, slantRange group, zeroDopplerTime group, reference raster).
    RSLC keeps the time axis at swaths/ and range under frequencyA/; RIFG/RUNW nest both
    under frequencyA/interferogram/."""
    root = f"/science/LSAR/{product}"
    grid = root + "/metadata/geolocationGrid"
    if product == "RSLC":
        return grid, root + "/swaths/frequencyA", root + "/swaths", root + "/swaths/frequencyA/HH"
    ifg = root + "/swaths/frequencyA/interferogram"
    return grid, ifg, ifg, f"{ifg}/HH/{IFG_LAYER[product]}"


class L1Cube:
    """Geolocation-grid cubes + radar axes with an independent numpy trilinear kernel."""

    def __init__(self, granules, product="RSLC"):
        self.product = product
        self.grid, rgrp, tgrp, self.ref_path = _l1_paths(product)
        grid = self.grid
        (self.hgt, self.crange, self.ctime, self.srange, self.stime, self.dr, self.dt,
         self.cX, self.cY, self.cQ, epsg) = granules.hdf5_arrays(
            product,
            grid + "/heightAboveEllipsoid", grid + "/slantRange", grid + "/zeroDopplerTime",
            rgrp + "/slantRange", tgrp + "/zeroDopplerTime",
            rgrp + "/slantRangeSpacing", tgrp + "/zeroDopplerTimeSpacing",
            grid + "/coordinateX", grid + "/coordinateY", grid + "/" + QUANTITY, grid + "/epsg")
        self.epsg = int(np.asarray(epsg).ravel()[0])
        self.srs = osr.SpatialReference()
        self.srs.ImportFromEPSG(self.epsg)
        self.srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    @staticmethod
    def _node(axis, v):
        i = int(np.clip(np.searchsorted(axis, v) - 1, 0, len(axis) - 2))
        return i, float(np.clip((v - axis[i]) / (axis[i + 1] - axis[i]), 0.0, 1.0))

    def trilinear(self, cube, h, pixel, line):
        zi, wz = self._node(self.hgt, h)
        yi, wy = self._node(self.ctime, self.stime[line])
        xi, wx = self._node(self.crange, self.srange[pixel])
        c = 0.0
        for dz, fz in ((0, 1 - wz), (1, wz)):
            for dy, fy in ((0, 1 - wy), (1, wy)):
                for dx, fx in ((0, 1 - wx), (1, wx)):
                    c += cube[zi + dz, yi + dy, xi + dx] * fz * fy * fx
        return c

    def constant_dem(
        self, path, value, nodata=None, mask=None, alpha=None, geotransform=True, epsg=4326
    ):
        """Flat GeoTIFF covering the geolocation-grid footprint (cube-CRS bbox padded by
        1 deg / 50 km, projected to `epsg`). mask: fill value (0/255) for an internal
        per-dataset mask band; alpha: fill value (0/255) for an explicit second (alpha) band."""
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        ct = osr.CoordinateTransformation(self.srs, srs)
        pad = 1.0 if self.epsg == 4326 else 5e4
        xs = (self.cX.min() - pad, self.cX.max() + pad)
        ys = (self.cY.min() - pad, self.cY.max() + pad)
        pts = np.array([ct.TransformPoint(float(x), float(y))[:2] for x in xs for y in ys])
        x0, x1 = pts[:, 0].min(), pts[:, 0].max()
        y0, y1 = pts[:, 1].min(), pts[:, 1].max()
        with gdal.config_option("GDAL_TIFF_INTERNAL_MASK", "YES"):
            ds = gdal.GetDriverByName("GTiff").Create(
                path, 64, 64, 2 if alpha is not None else 1, gdal.GDT_Float32
            )
            if geotransform:
                ds.SetGeoTransform([x0, (x1 - x0) / 64, 0, y1, 0, -(y1 - y0) / 64])
            ds.SetProjection(srs.ExportToWkt())
            ds.GetRasterBand(1).Fill(value)
            if nodata is not None:
                ds.GetRasterBand(1).SetNoDataValue(nodata)
            if alpha is not None:
                ds.GetRasterBand(2).SetColorInterpretation(gdal.GCI_AlphaBand)
                ds.GetRasterBand(2).Fill(alpha)
            if mask is not None:
                ds.CreateMaskBand(gdal.GMF_PER_DATASET)
                ds.GetRasterBand(1).GetMaskBand().Fill(mask)
            ds = None
        return path


@pytest.fixture(scope="module")
def rslc_cube(granules):
    return L1Cube(granules)


@pytest.fixture(scope="module", params=IFG_PRODUCTS)
def ifg_cube(request, granules):
    return L1Cube(granules, request.param)


@pytest.fixture(scope="module")
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _read_rslc(granules, quantity=QUANTITY, dem=None, **oo):
    opts = [f"QUANTITY={quantity}", f"DEM_FILE={dem or granules.dem}"]
    opts += [f"{k}={v}" for k, v in oo.items()]
    ds = gdal.OpenEx(granules.conn("RSLC"), gdal.OF_RASTER, open_options=opts)
    return ds.ReadAsArray(*RSLC_WINDOW)


def _sample_points(n=150, seed=0):
    xoff, yoff, xs, ys = RSLC_WINDOW
    rng = np.random.default_rng(seed)
    return rng.integers(xoff, xoff + xs, n), rng.integers(yoff, yoff + ys, n)


def test_rslc_radar_grid_and_gcps(granules):
    ds = _open_interp(granules, "RSLC")
    ref = gdal.OpenEx(granules.conn("RSLC"), gdal.OF_RASTER,
                      open_options=["FREQ=A", "POL=HH"])
    assert (ds.RasterXSize, ds.RasterYSize) == (ref.RasterXSize, ref.RasterYSize)
    assert ds.GetRasterBand(1).DataType == gdal.GDT_Float32
    md = ds.GetMetadata()
    assert md["NISAR_PRODUCT_TYPE"] == "RSLC"
    assert md["NISAR_GRID_TYPE"] == "RADAR"
    assert md["NISAR_CUBE_PATH"] == f"{RSLC_GRID}/{QUANTITY}"
    assert md["NISAR_REFERENCE_GRID"] == "/science/LSAR/RSLC/swaths/frequencyA/HH"
    assert md["NISAR_GEOLOCATION_EPSG"] == "4326"
    assert md["DEM_NODATA_HEIGHT"] == "0"
    # radar grid: no geotransform, GCPs + GCP SRS passed through from the swath
    assert ds.GetGeoTransform(can_return_null=True) is None
    assert ds.GetGCPCount() == ref.GetGCPCount() > 0
    assert ds.GetGCPSpatialRef().IsSame(ref.GetGCPSpatialRef())
    assert ds.GetGCPSpatialRef().GetAuthorityCode(None) == "4326"
    a, b = ds.GetGCPs(), ref.GetGCPs()
    assert all((p.GCPPixel, p.GCPLine, p.GCPX, p.GCPY, p.GCPZ) ==
               (q.GCPPixel, q.GCPLine, q.GCPX, q.GCPY, q.GCPZ)
               for p, q in zip(a[::97], b[::97]))


def test_rslc_gcp_pixel_line_follow_spec(granules, rslc_cube):
    """GCP pixel/line come from the swath slantRange / zeroDopplerTime axes
    (spec: line = (t - zeroDopplerTime[0]) / zeroDopplerTimeSpacing), not PRF."""
    (dt,) = granules.hdf5_arrays("RSLC", "/science/LSAR/RSLC/swaths/zeroDopplerTimeSpacing")
    ref = gdal.OpenEx(granules.conn("RSLC"), gdal.OF_RASTER,
                      open_options=["FREQ=A", "POL=HH"])
    nt, nr = len(rslc_cube.ctime), len(rslc_cube.crange)
    assert ref.GetGCPCount() == nt * nr
    gcps = ref.GetGCPs()
    line = np.array([g.GCPLine for g in gcps]).reshape(nt, nr)
    pixel = np.array([g.GCPPixel for g in gcps]).reshape(nt, nr)
    stime, srange = rslc_cube.stime, rslc_cube.srange
    dr = srange[1] - srange[0]
    exp_line = (rslc_cube.ctime - stime[0]) / float(dt) + 0.5
    exp_pixel = (rslc_cube.crange - srange[0]) / dr + 0.5
    assert np.allclose(line, exp_line[:, None], atol=1e-6)
    assert np.allclose(pixel, exp_pixel[None, :], atol=1e-6)
    # the grid brackets the swath: its extreme lines land just outside [0, nlines)
    assert line.min() < 0.5 and line.max() > ref.RasterYSize - 0.5
    assert line.max() < ref.RasterYSize * 1.1
    # swath zeroDopplerTime is uniform at zeroDopplerTimeSpacing, so the last
    # swath line maps to (nlines - 1) + 0.5 in GCP line space
    assert np.isclose((stime[-1] - stime[0]) / float(dt), ref.RasterYSize - 1, atol=1e-6)


@pytest.mark.parametrize("height", [-500.0, 0.0, 1234.5, 4000.0])
@pytest.mark.parametrize("quantity", ["coordinateX", "coordinateY", QUANTITY])
def test_rslc_constant_dem_matches_trilinear(granules, rslc_cube, tmp_dir, height, quantity):
    """With a flat DEM the fixed-point height solve must land exactly on that height,
    so the driver output equals a direct trilinear lookup at (h, t, r)."""
    dem = rslc_cube.constant_dem(os.path.join(tmp_dir, f"dem_{height}.tif"), height)
    out = _read_rslc(granules, quantity, dem)
    cube = {"coordinateX": rslc_cube.cX, "coordinateY": rslc_cube.cY,
            QUANTITY: rslc_cube.cQ}[quantity]
    xoff, yoff = RSLC_WINDOW[:2]
    xs, ys = _sample_points()
    got = out[ys - yoff, xs - xoff]
    ref = np.array([rslc_cube.trilinear(cube, height, x, y) for x, y in zip(xs, ys)])
    tol = 2e-5 if quantity == QUANTITY else 1e-5  # float32 output precision
    assert np.abs(got - ref).max() < tol


def test_rslc_dem_nodata_height_default_and_override(granules, rslc_cube, tmp_dir):
    nodata = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_nd.tif"), -9999.0, nodata=-9999.0)
    flat0 = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_0.tif"), 0.0)
    flat1k = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_1k.tif"), 1000.0)
    assert np.array_equal(_read_rslc(granules, dem=nodata), _read_rslc(granules, dem=flat0))
    over = _read_rslc(granules, dem=nodata, DEM_NODATA_HEIGHT=1000)
    assert np.array_equal(over, _read_rslc(granules, dem=flat1k))
    assert np.abs(over - _read_rslc(granules, dem=flat0)).max() > 0.01


@pytest.mark.parametrize("bad", ["nan", "inf", "-inf"])
def test_rslc_dem_nodata_height_must_be_finite(granules, bad):
    with pytest.raises(RuntimeError, match="DEM_NODATA_HEIGHT must be a finite height"):
        _read_rslc(granules, DEM_NODATA_HEIGHT=bad)


def test_rslc_dem_without_geotransform_rejected(granules, rslc_cube, tmp_dir):
    dem = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_nogt.tif"), 0.0, geotransform=False)
    assert gdal.Open(dem).GetGeoTransform(can_return_null=True) is None
    with pytest.raises(RuntimeError, match="DEM has no affine geotransform"):
        _read_rslc(granules, dem=dem)


def test_rslc_dem_mask_band_honoured(granules, rslc_cube, tmp_dir):
    """A per-dataset mask (no nodata value) must hide DEM samples like nodata does."""
    masked = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_masked.tif"), 1000.0, mask=0)
    unmasked = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_unmasked.tif"), 1000.0, mask=255)
    flat0 = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_0b.tif"), 0.0)
    flat1k = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_1kb.tif"), 1000.0)
    ds = gdal.Open(masked)
    band = ds.GetRasterBand(1)
    assert band.GetNoDataValue() is None and band.GetMaskFlags() == gdal.GMF_PER_DATASET
    ds = None
    assert np.array_equal(_read_rslc(granules, dem=masked), _read_rslc(granules, dem=flat0))
    assert np.array_equal(_read_rslc(granules, dem=masked, DEM_NODATA_HEIGHT=1000),
                          _read_rslc(granules, dem=flat1k))
    assert np.array_equal(_read_rslc(granules, dem=unmasked), _read_rslc(granules, dem=flat1k))


def test_rslc_dem_mask_survives_reprojection(granules, rslc_cube, tmp_dir):
    """DEM in another CRS is warped lazily; its mask (no nodata value) must still
    route masked terrain to DEM_NODATA_HEIGHT."""
    masked = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_3857_masked.tif"), 1000.0,
                                    mask=0, epsg=3857)
    unmasked = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_3857.tif"), 1000.0, epsg=3857)
    flat20 = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_20.tif"), 20.0)
    flat1k = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_1kc.tif"), 1000.0)
    assert np.array_equal(_read_rslc(granules, dem=masked, DEM_NODATA_HEIGHT=20),
                          _read_rslc(granules, dem=flat20))
    # sanity: the reprojected DEM itself is used when valid (bilinear warp of a constant)
    assert np.allclose(_read_rslc(granules, dem=unmasked), _read_rslc(granules, dem=flat1k),
                       atol=2e-5)


@pytest.mark.parametrize("epsg", [4326, 3857])
@pytest.mark.parametrize("kind", ["alpha", "nodata+mask", "nodata+alpha"])
def test_rslc_dem_validity_combinations(granules, rslc_cube, tmp_dir, kind, epsg):
    """Explicit alpha bands, and per-dataset masks / alpha combined with a nodata value,
    must hide valid-looking heights in both the same-CRS and the warped path."""
    kw = {"alpha": 0} if "alpha" in kind else {"mask": 0}
    if kind.startswith("nodata"):
        kw["nodata"] = -9999.0
    hidden = rslc_cube.constant_dem(os.path.join(tmp_dir, f"dem_{kind}_{epsg}.tif"), 1000.0,
                                    epsg=epsg, **kw)
    flat20 = rslc_cube.constant_dem(os.path.join(tmp_dir, "dem_20v.tif"), 20.0)
    assert np.array_equal(_read_rslc(granules, dem=hidden, DEM_NODATA_HEIGHT=20),
                          _read_rslc(granules, dem=flat20))


def test_rslc_real_dem_self_consistent(granules, rslc_cube):
    """Terrain solve: the DEM height at the output (X, Y) must map back through the
    coordinate cubes to the same (X, Y), and give the same quantity value."""
    X = _read_rslc(granules, "coordinateX")
    Y = _read_rslc(granules, "coordinateY")
    Q = _read_rslc(granules)
    dem = gdal.Open(granules.dem)
    band = dem.GetRasterBand(1)
    inv = gdal.InvGeoTransform(dem.GetGeoTransform())
    xoff, yoff = RSLC_WINDOW[:2]
    xs, ys = _sample_points(60)
    worst = np.zeros(3)
    heights = []
    for x, y in zip(xs, ys):
        lon, lat = float(X[y - yoff, x - xoff]), float(Y[y - yoff, x - xoff])
        u = inv[0] + lon * inv[1] + lat * inv[2] - 0.5
        v = inv[3] + lon * inv[4] + lat * inv[5] - 0.5
        i, j = int(np.floor(u)), int(np.floor(v))
        blk = band.ReadAsArray(i, j, 2, 2).astype(float)
        fu, fv = u - i, v - j
        h = ((blk[0, 0] * (1 - fu) + blk[0, 1] * fu) * (1 - fv) +
             (blk[1, 0] * (1 - fu) + blk[1, 1] * fu) * fv)
        heights.append(h)
        worst = np.maximum(worst, [
            abs(rslc_cube.trilinear(rslc_cube.cX, h, x, y) - lon),
            abs(rslc_cube.trilinear(rslc_cube.cY, h, x, y) - lat),
            abs(rslc_cube.trilinear(rslc_cube.cQ, h, x, y) - Q[y - yoff, x - xoff])])
    assert np.ptp(heights) > 50.0  # the window is over real terrain
    assert worst[0] < 1e-5 and worst[1] < 1e-5 and worst[2] < 1e-3


def test_rslc_matches_gcov_on_common_ground(granules):
    """Same acquisition + DEM: RSLC pixels mapped to ground through coordinateX/Y
    must read the same incidence angle as the GCOV interpolation at that point."""
    X = _read_rslc(granules, "coordinateX")
    Y = _read_rslc(granules, "coordinateY")
    Q = _read_rslc(granules)
    gcov = _open_interp(granules, "GCOV")
    src = osr.SpatialReference()
    src.ImportFromEPSG(4326)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ct = osr.CoordinateTransformation(src, gcov.GetSpatialRef())
    inv = gdal.InvGeoTransform(gcov.GetGeoTransform())
    xoff, yoff = RSLC_WINDOW[:2]
    xs, ys = _sample_points(100, seed=1)
    diffs = []
    for x, y in zip(xs, ys):
        e, n, _ = ct.TransformPoint(float(X[y - yoff, x - xoff]), float(Y[y - yoff, x - xoff]))
        u = inv[0] + e * inv[1] + n * inv[2] - 0.5
        v = inv[3] + e * inv[4] + n * inv[5] - 0.5
        i, j = int(np.floor(u)), int(np.floor(v))
        blk = gcov.ReadAsArray(i, j, 2, 2).astype(float)
        if np.isnan(blk).any():
            continue
        fu, fv = u - i, v - j
        g = ((blk[0, 0] * (1 - fu) + blk[0, 1] * fu) * (1 - fv) +
             (blk[1, 0] * (1 - fu) + blk[1, 1] * fu) * fv)
        diffs.append(g - Q[y - yoff, x - xoff])
    assert len(diffs) > 50
    assert np.abs(diffs).max() < 1e-3


# --- Level-1 interferograms (RIFG/RUNW): nested interferogram/<POL>/<layer> grids -----


def _ifg_window(ds, xs=512, ys=200):
    return ((ds.RasterXSize - xs) // 2, (ds.RasterYSize - ys) // 2, xs, ys)


def _read_ifg(granules, cube, quantity=QUANTITY, dem=None, window=None, **oo):
    opts = [f"QUANTITY={quantity}", f"DEM_FILE={dem or granules.dem}"]
    opts += [f"{k}={v}" for k, v in oo.items()]
    ds = gdal.OpenEx(granules.conn(cube.product), gdal.OF_RASTER, open_options=opts)
    window = window or _ifg_window(ds)
    return ds.ReadAsArray(*window), window


def _bilinear_at(band, inv, x, y):
    u = inv[0] + x * inv[1] + y * inv[2] - 0.5
    v = inv[3] + x * inv[4] + y * inv[5] - 0.5
    i, j = int(np.floor(u)), int(np.floor(v))
    blk = band.ReadAsArray(i, j, 2, 2).astype(float)
    fu, fv = u - i, v - j
    return ((blk[0, 0] * (1 - fu) + blk[0, 1] * fu) * (1 - fv) +
            (blk[1, 0] * (1 - fu) + blk[1, 1] * fu) * fv)


def test_ifg_radar_grid_and_gcps(granules, ifg_cube):
    """Bare NISAR:file + QUANTITY routes to interferogram/HH/<layer> and inherits the
    nested slantRange/zeroDopplerTime axes for the GCPs."""
    p = ifg_cube.product
    ds = _open_interp(granules, p)
    ref = gdal.OpenEx(granules.conn(p, ifg_cube.ref_path), gdal.OF_RASTER)
    assert (ds.RasterXSize, ds.RasterYSize) == (ref.RasterXSize, ref.RasterYSize)
    assert (ds.RasterXSize, ds.RasterYSize) == (len(ifg_cube.srange), len(ifg_cube.stime))
    md = ds.GetMetadata()
    assert md["NISAR_PRODUCT_TYPE"] == p
    assert md["NISAR_GRID_TYPE"] == "RADAR"
    assert md["NISAR_CUBE_PATH"] == f"{ifg_cube.grid}/{QUANTITY}"
    assert md["NISAR_REFERENCE_GRID"] == ifg_cube.ref_path
    assert md["NISAR_GEOLOCATION_EPSG"] == str(ifg_cube.epsg)
    assert ds.GetGeoTransform(can_return_null=True) is None
    nt, nr = len(ifg_cube.ctime), len(ifg_cube.crange)
    assert ds.GetGCPCount() == ref.GetGCPCount() == nt * nr
    assert ds.GetGCPSpatialRef().GetAuthorityCode(None) == str(ifg_cube.epsg)
    gcps = ds.GetGCPs()
    line = np.array([g.GCPLine for g in gcps]).reshape(nt, nr)
    pixel = np.array([g.GCPPixel for g in gcps]).reshape(nt, nr)
    exp_line = (ifg_cube.ctime - ifg_cube.stime[0]) / float(ifg_cube.dt) + 0.5
    exp_pixel = (ifg_cube.crange - ifg_cube.srange[0]) / float(ifg_cube.dr) + 0.5
    assert np.allclose(line, exp_line[:, None], atol=1e-6)
    assert np.allclose(pixel, exp_pixel[None, :], atol=1e-6)
    assert line.min() < 0.5 < ref.RasterYSize - 0.5 < line.max() < ref.RasterYSize * 1.1
    assert np.isclose((ifg_cube.stime[-1] - ifg_cube.stime[0]) / float(ifg_cube.dt),
                      ref.RasterYSize - 1, atol=1e-6)


def test_ifg_explicit_pol_matches_default(granules, ifg_cube):
    a = _open_interp(granules, ifg_cube.product, FREQ="A", POL="HH")
    b = _open_interp(granules, ifg_cube.product)
    assert a.GetMetadataItem("NISAR_REFERENCE_GRID") == b.GetMetadataItem("NISAR_REFERENCE_GRID")
    win = _ifg_window(a, 256, 64)
    assert np.array_equal(a.ReadAsArray(*win), b.ReadAsArray(*win))


@pytest.mark.parametrize("dem_epsg", ["cube", 4326])
def test_ifg_constant_dem_matches_trilinear(granules, ifg_cube, tmp_dir, dem_epsg):
    """Flat DEM in the cube CRS (sampled directly) and in EPSG:4326 (warped) must both
    reproduce a direct trilinear lookup at (h, t, r)."""
    epsg = ifg_cube.epsg if dem_epsg == "cube" else dem_epsg
    dem = ifg_cube.constant_dem(
        os.path.join(tmp_dir, f"dem_{ifg_cube.product}_{epsg}.tif"), 1234.5, epsg=epsg)
    out, (xoff, yoff, xs, ys) = _read_ifg(granules, ifg_cube, dem=dem)
    rng = np.random.default_rng(0)
    px, py = rng.integers(xoff, xoff + xs, 100), rng.integers(yoff, yoff + ys, 100)
    ref = np.array([ifg_cube.trilinear(ifg_cube.cQ, 1234.5, x, y) for x, y in zip(px, py)])
    assert np.abs(out[py - yoff, px - xoff] - ref).max() < 2e-5


def test_ifg_real_dem_self_consistent(granules, ifg_cube):
    """Real (EPSG:4326) DEM against a UTM geolocation grid: the warped DEM is limited
    to the footprint (no global-warp warnings), and the terrain solve is consistent
    with the product's own digitalElevationModel layer."""
    warnings = []
    handler = lambda cls, num, msg: warnings.append(msg)
    gdal.PushErrorHandler(handler)
    try:
        X, win = _read_ifg(granules, ifg_cube, "coordinateX")
        Y, _ = _read_ifg(granules, ifg_cube, "coordinateY", window=win)
        Q, _ = _read_ifg(granules, ifg_cube, window=win)
    finally:
        gdal.PopErrorHandler()
    assert not [w for w in warnings if "Invalid latitude" in w]
    xoff, yoff, xs, ys = win
    ifg_grp = os.path.dirname(os.path.dirname(ifg_cube.ref_path))
    (dem_layer,) = granules.hdf5_arrays(ifg_cube.product, ifg_grp + "/digitalElevationModel")
    dem = gdal.Open(granules.dem)
    band = dem.GetRasterBand(1)
    inv = gdal.InvGeoTransform(dem.GetGeoTransform())
    ll = osr.SpatialReference()
    ll.ImportFromEPSG(4326)
    ll.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    ct = osr.CoordinateTransformation(ifg_cube.srs, ll)
    rng = np.random.default_rng(0)
    px, py = rng.integers(xoff, xoff + xs, 60), rng.integers(yoff, yoff + ys, 60)
    worst, dh = np.zeros(3), []
    for x, y in zip(px, py):
        ex, ny = float(X[y - yoff, x - xoff]), float(Y[y - yoff, x - xoff])
        lon, lat, _ = ct.TransformPoint(ex, ny)
        h = _bilinear_at(band, inv, lon, lat)
        dh.append(h - dem_layer[y, x])
        worst = np.maximum(worst, [abs(ifg_cube.trilinear(ifg_cube.cX, h, x, y) - ex),
                                   abs(ifg_cube.trilinear(ifg_cube.cY, h, x, y) - ny),
                                   abs(ifg_cube.trilinear(ifg_cube.cQ, h, x, y) -
                                       Q[y - yoff, x - xoff])])
    assert np.median(np.abs(dh)) < 0.5 and np.max(np.abs(dh)) < 2.0  # metres
    assert worst[0] < 2.0 and worst[1] < 2.0 and worst[2] < 1e-3  # metres, metres, degrees


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__] + sys.argv[1:]))
