# Copyright 2025, by the California Institute of Technology.
# ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.
#
# SPDX-License-Identifier: Apache-2.0

"""
NISAR_DUMP metadata-domain tests for the NISAR GDAL driver.

Exercises the NISAR_DUMP metadata domain (h5dump-style listing, available on
request without any open option; `-oo DUMP=YES` only advertises it to
`-mdd all` and never changes SUBDATASETS) in container mode on one granule per
product level: L1 RSLC, L2 GUNW, L2 GCOV and L3 SME2.

Granules are located through NASA Earthdata (CMR) with earthaccess and read
through the driver over HTTPS with `.netrc` credentials, unless a local copy
is found in NISAR_TEST_DATA_DIR.

    pytest -v -p no:cacheprovider conda-build/tests/test_dump.py

Environment overrides:
    NISAR_TEST_DATA_DIR  directory with local NISAR_L?_PR_<PRODUCT>_*.h5 files;
                         the first match per product is used instead of CMR
    NISAR_TEST_DUMP_<PRODUCT>  explicit path/URL for one product (RSLC, GUNW,
                         GCOV, SME2), bypassing both the local dir and CMR
    NISAR_TEST_ACCESS    "external" (HTTPS, default) or "direct" (S3, in-region)
"""

import glob
import os
import re

import pytest

try:
    import earthaccess
except ImportError:  # pragma: no cover
    earthaccess = None

from osgeo import gdal

gdal.UseExceptions()

PRODUCTS = ("RSLC", "GUNW", "GCOV", "SME2")
SHORT_NAMES = {
    "RSLC": "NISAR_L1_RSLC_BETA_V1",
    "GUNW": "NISAR_L2_GUNW_BETA_V1",
    "GCOV": "NISAR_L2_GCOV_BETA_V1",
    "SME2": "NISAR_L3_SME2_BETA_V1",
}
LEVEL = {"RSLC": "L1", "GUNW": "L2", "GCOV": "L2", "SME2": "L3"}
IDENT = "/science/LSAR/identification"


def _configure_gdal():
    gdal.SetConfigOption("GDAL_PAM_ENABLED", "NO")
    gdal.SetConfigOption("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
    if os.environ.get("NISAR_TEST_ACCESS", "external") == "direct":
        gdal.SetConfigOption("AWS_REGION", "us-west-2")
    else:
        gdal.SetConfigOption("GDAL_HTTP_NETRC", "YES")
        gdal.SetConfigOption("GDAL_HTTP_UNRESTRICTED_AUTH", "YES")
        gdal.SetConfigOption("GDAL_HTTP_TIMEOUT", "60")
        gdal.SetConfigOption("GDAL_HTTP_MAX_RETRY", "5")
        cookies = os.path.join(os.path.expanduser("~"), ".urs_cookies")
        gdal.SetConfigOption("GDAL_HTTP_COOKIEFILE", cookies)
        gdal.SetConfigOption("GDAL_HTTP_COOKIEJAR", cookies)


class Granules:
    def __init__(self):
        self.local_dir = os.environ.get("NISAR_TEST_DATA_DIR")
        self.access = os.environ.get("NISAR_TEST_ACCESS", "external")
        _configure_gdal()
        self._uris = {}

    def uri(self, product):
        if product in self._uris:
            return self._uris[product]
        explicit = os.environ.get(f"NISAR_TEST_DUMP_{product}")
        if explicit:
            self._uris[product] = explicit
            return explicit
        if self.local_dir:
            pattern = os.path.join(
                self.local_dir, f"NISAR_{LEVEL[product]}_PR_{product}_*.h5")
            found = sorted(glob.glob(pattern))
            if found:
                self._uris[product] = found[0]
                return found[0]
        if earthaccess is None:
            pytest.skip("earthaccess not installed and no local granule copy")
        try:
            earthaccess.login(strategy="netrc")
            results = earthaccess.search_data(
                short_name=SHORT_NAMES[product], count=1)
        except Exception as exc:  # credentials / network
            pytest.skip(f"earthaccess unavailable: {exc}")
        if not results:
            pytest.skip(f"no {product} granule found in CMR")
        links = [l for l in results[0].data_links(access=self.access)
                 if l.endswith(".h5")]
        if not links:
            pytest.skip(f"no {self.access} .h5 link for {product}")
        self._uris[product] = links[0]
        return links[0]

    def conn(self, product, path=None):
        s = f'NISAR:"{self.uri(product)}"'
        return s + ":" + path if path else s


@pytest.fixture(scope="module")
def granules():
    if gdal.GetDriverByName("NISAR") is None:
        pytest.skip("NISAR driver not available (check GDAL_DRIVER_PATH)")
    return Granules()


def _open(granules, product, **oo):
    opts = [f"{k}={v}" for k, v in oo.items()]
    return gdal.OpenEx(granules.conn(product), gdal.OF_RASTER, open_options=opts)


def _dump(ds):
    lines = ds.GetMetadata_List("NISAR_DUMP")
    assert lines, "NISAR_DUMP domain is empty"
    return lines


def _objects(lines, kind):
    """Paths of GROUP/DATASET headers in a dump."""
    prefix = f'{kind} "'
    return [l[len(prefix):l.index('"', len(prefix))]
            for l in lines if l.startswith(prefix)]


def _parents(lines):
    """Map object path -> enclosing GROUP path, by walking the brace structure.

    Object headers are unindented (`GROUP "p" {`, `DATASET "p" {`, ...) and a
    bare `}` closes the innermost open one; body lines are indented.
    Asserts the braces balance.
    """
    assert lines[0].startswith('HDF5 "') and lines[0].endswith(" {")
    stack, parents = [], {}
    for l in lines[1:-1]:
        if l == "}":
            stack.pop()
        elif l and not l[0].isspace():
            rest = l.split(" ", 1)[1]
            path = rest[1:rest.index('"', 1)]
            parents[path] = stack[-1] if stack else None
            stack.append(path)
    assert not stack, f"unclosed objects: {stack}"
    assert lines[-1] == "}"
    return parents


# --------------------------------------------------------------------------
# Default behaviour is untouched
# --------------------------------------------------------------------------

def test_open_options_registered():
    drv = gdal.GetDriverByName("NISAR")
    if drv is None:
        pytest.skip("NISAR driver not available")
    xml = drv.GetMetadataItem("DMD_OPENOPTIONLIST")
    for name in ("DUMP", "DUMP_ROOT", "DUMP_MODE"):
        assert f"name='{name}'" in xml


def _subdataset_descs(ds):
    sub = ds.GetMetadata("SUBDATASETS")
    descs = [v for k, v in sub.items() if k.endswith("_DESC")]
    assert descs
    return descs


@pytest.mark.parametrize("product", PRODUCTS)
def test_default_without_dump(granules, product):
    ds = _open(granules, product)
    assert "NISAR_DUMP" not in ds.GetMetadataDomainList()
    descs = _subdataset_descs(ds)
    assert not any("not openable" in d for d in descs)
    assert not any("(string" in d for d in descs)


# --------------------------------------------------------------------------
# HEADER dump
# --------------------------------------------------------------------------

@pytest.mark.parametrize("product", PRODUCTS)
def test_header_dump(granules, product):
    ds = _open(granules, product, DUMP="YES")
    assert "NISAR_DUMP" in ds.GetMetadataDomainList()
    lines = _dump(ds)

    assert lines[0].startswith('HDF5 "') and lines[0].endswith(" HEADER {")
    assert lines[-1] == "}"

    groups = _objects(lines, "GROUP")
    datasets = _objects(lines, "DATASET")
    assert "/science/LSAR" in groups
    assert IDENT in groups
    assert f"/science/LSAR/{product}" in groups
    assert f"{IDENT}/productType" in datasets
    assert all(p.startswith("/science/LSAR") for p in groups + datasets)

    # groups enclose their children (h5dump nesting), braces balance
    parents = _parents(lines)
    assert parents["/science/LSAR"] is None
    assert parents[IDENT] == "/science/LSAR"
    assert parents[f"{IDENT}/productType"] == IDENT
    assert parents[f"/science/LSAR/{product}"] == "/science/LSAR"

    # datatype / dataspace / attribute lines are present
    assert any(l.strip().startswith("DATATYPE  H5T_STRING") for l in lines)
    assert any(l.strip().startswith("DATATYPE  H5T_IEEE_F") for l in lines)
    assert any(l.strip().startswith("DATASPACE  SCALAR") for l in lines)
    assert any(l.strip().startswith("DATASPACE  SIMPLE") for l in lines)
    assert any(l.strip().startswith('ATTRIBUTE "description"') for l in lines)

    # HEADER mode prints attribute values but no dataset DATA blocks
    idx = lines.index(f'DATASET "{IDENT}/productType" {{')
    block = lines[idx:idx + 12]
    assert not any(l.startswith("   DATA {") for l in block)
    assert any(l.startswith("      DATA {") for l in block)  # attribute value


@pytest.mark.parametrize("product", PRODUCTS)
def test_dump_without_dump_option(granules, product):
    """-mdd NISAR_DUMP works without DUMP=YES (domain just isn't advertised)."""
    ds = _open(granules, product)
    assert "NISAR_DUMP" not in ds.GetMetadataDomainList()
    lines = _dump(ds)
    assert lines[0].endswith(" HEADER {") and lines[-1] == "}"
    assert IDENT in _objects(lines, "GROUP")
    assert f"{IDENT}/productType" in _objects(lines, "DATASET")

    ds = _open(granules, product, DUMP_MODE="FULL", DUMP_ROOT=IDENT)
    lines = _dump(ds)
    assert lines[0].endswith(" FULL {")
    assert all(p.startswith(IDENT) for p in _objects(lines, "GROUP") + _objects(lines, "DATASET"))
    assert any(l.startswith("   DATA {") for l in lines)


@pytest.mark.parametrize("product", PRODUCTS)
def test_dump_leaves_subdatasets_unchanged(granules, product):
    """DUMP=YES only advertises the domain; SUBDATASETS stays the raster-only
    inventory whose every entry can be opened."""
    plain = _open(granules, product).GetMetadata("SUBDATASETS")
    ds = _open(granules, product, DUMP="YES")
    assert ds.GetMetadata("SUBDATASETS") == plain
    descs = _subdataset_descs(ds)
    assert not any("not openable" in d for d in descs)
    assert f"{IDENT}/productType" not in " ".join(descs)
    assert any(d.endswith("(Float32)") or d.endswith("(complex, Float32)")
               for d in descs)
    # string datasets are still visible, via the dump
    assert f"{IDENT}/productType" in _objects(_dump(ds), "DATASET")


@pytest.mark.parametrize("product", PRODUCTS)
def test_dump_root_scoping(granules, product):
    ds = _open(granules, product, DUMP="YES", DUMP_ROOT=IDENT)
    lines = _dump(ds)
    groups = _objects(lines, "GROUP")
    datasets = _objects(lines, "DATASET")
    assert groups == [IDENT]
    assert datasets
    assert all(p.startswith(IDENT + "/") for p in datasets)
    assert "/science/LSAR" not in groups


def test_dump_root_file_root(granules):
    ds = _open(granules, "GCOV", DUMP="YES", DUMP_ROOT="/")
    lines = _dump(ds)
    groups = _objects(lines, "GROUP")
    assert groups[0] == "/"
    assert "/science" in groups
    assert "/science/LSAR" in groups
    parents = _parents(lines)
    assert parents["/"] is None
    assert parents["/science"] == "/"
    assert parents["/science/LSAR"] == "/science"


def test_dump_root_empty_rejected(granules):
    with pytest.raises(RuntimeError):
        _open(granules, "GCOV", DUMP_ROOT="")


def test_dump_root_missing(granules):
    with pytest.raises(RuntimeError):
        _open(granules, "GCOV", DUMP="YES", DUMP_ROOT="/no/such/group")


def test_dump_root_dataset_rejected(granules):
    with pytest.raises(RuntimeError):
        _open(granules, "GCOV", DUMP="YES", DUMP_ROOT=IDENT + "/productType")


def test_dump_mode_invalid(granules):
    with pytest.raises(RuntimeError):
        _open(granules, "GCOV", DUMP="YES", DUMP_MODE="BOGUS")


# --------------------------------------------------------------------------
# FULL dump
# --------------------------------------------------------------------------

def test_full_dump_values(granules):
    ds = _open(granules, "GCOV", DUMP="YES", DUMP_MODE="FULL", DUMP_ROOT=IDENT)
    lines = _dump(ds)
    assert lines[0].endswith(" FULL {")

    idx = lines.index(f'DATASET "{IDENT}/productType" {{')
    block = lines[idx:idx + 4]
    assert '   DATA { "GCOV" }' in block

    idx = lines.index(f'DATASET "{IDENT}/listOfFrequencies" {{')
    data = [l for l in lines[idx:idx + 6] if l.startswith("   DATA {")]
    assert data and '"A"' in data[0]


def test_full_dump_skips_large_arrays(granules):
    root = "/science/LSAR/GCOV/grids/frequencyA"
    ds = _open(granules, "GCOV", DUMP="YES", DUMP_MODE="FULL", DUMP_ROOT=root)
    lines = _dump(ds)
    idx = lines.index(f'DATASET "{root}/xCoordinates" {{')
    data = [l for l in lines[idx:idx + 4] if l.startswith("   DATA {")]
    assert data and "elements, not printed" in data[0]


def test_attribute_values_follow_element_cap(granules):
    """Attributes longer than NISAR_DUMP_MAX_ELEMENTS get an explicit marker
    instead of silently losing their DATA line (both modes)."""
    root = "/science/LSAR/GCOV/grids/frequencyA"
    with gdal.config_option("NISAR_DUMP_MAX_ELEMENTS", "1"):
        ds = _open(granules, "GCOV", DUMP_ROOT=root)
        lines = _dump(ds)
    idx = lines.index(f'DATASET "{root}/xCoordinates" {{')
    block = lines[idx:idx + 40]
    attr = block.index('   ATTRIBUTE "REFERENCE_LIST" {')
    data = [l for l in block[attr:attr + 4] if l.startswith("      DATA {")]
    assert data and re.fullmatch(r"      DATA \{ \(\d+ elements, not printed\) \}", data[0])
    assert not any(l.startswith("   DATA {") for l in block)  # HEADER mode


def test_links_and_aliases(tmp_path):
    """Soft links, external links and hard-link aliases are reported the way
    h5dump does (SOFTLINK/EXTERNAL_LINK/HARDLINK) instead of being dropped or
    dumped twice. Uses a synthetic granule, so it needs h5py."""
    h5py = pytest.importorskip("h5py")
    import numpy as np

    path = tmp_path / "links.h5"
    root = "/science/LSAR/RSLC"
    with h5py.File(path, "w") as f:
        ident = f.create_group("/science/LSAR/identification")
        ident.create_dataset("productType", data=np.bytes_(b"RSLC"))
        ident.create_dataset("productLevel", data=np.bytes_(b"L1"))
        f.create_dataset(f"{root}/swaths/frequencyA/HH",
                         data=np.zeros((4, 4), dtype=np.float32))
        meta = f.create_group(f"{root}/metadata")
        values = meta.create_dataset("values", data=np.arange(4, dtype=np.int32))
        meta["alias"] = values                       # hard link to a dataset
        f[f"{root}/metadataAlias"] = meta            # hard link to a group
        f[f"{root}/soft"] = h5py.SoftLink(f"{root}/metadata/values")
        f[f"{root}/dangling"] = h5py.SoftLink("/nowhere")
        f[f"{root}/ext"] = h5py.ExternalLink("other.h5", "/some/path")

    ds = gdal.OpenEx(f'NISAR:"{path}"', gdal.OF_RASTER,
                     open_options=[f"DUMP_ROOT={root}", "DUMP_MODE=FULL"])
    lines = _dump(ds)

    def block(header):
        idx = lines.index(header)
        return lines[idx:lines.index("}", idx) + 1]

    assert block(f'SOFTLINK "{root}/soft" {{') == [
        f'SOFTLINK "{root}/soft" {{', f'   LINKTARGET "{root}/metadata/values"', "}"]
    assert block(f'SOFTLINK "{root}/dangling" {{')[1] == '   LINKTARGET "/nowhere"'
    assert block(f'EXTERNAL_LINK "{root}/ext" {{') == [
        f'EXTERNAL_LINK "{root}/ext" {{', '   TARGETFILE "other.h5"',
        '   TARGETPATH "/some/path"', "}"]

    # The two names of the int32 dataset: one is dumped, the other is an alias.
    alias, values = block(f'DATASET "{root}/metadata/alias" {{'), block(
        f'DATASET "{root}/metadata/values" {{')
    dumped, aliased = (alias, values) if "   HARDLINK" in values[1] else (values, alias)
    assert "   DATA { 0, 1, 2, 3 }" in dumped
    assert aliased[1] == f'   HARDLINK {dumped[0][len("DATASET "):-2]}'
    assert len(aliased) == 3

    # The aliased group is not traversed a second time.
    assert block(f'GROUP "{root}/metadataAlias" {{') == [
        f'GROUP "{root}/metadataAlias" {{', f'   HARDLINK "{root}/metadata"', "}"]
    assert _objects(lines, "DATASET").count(f"{root}/metadataAlias/values") == 0
    assert _parents(lines)[f"{root}/soft"] == root


def _synthetic_granule(path, h5py, inst="LSAR"):
    import numpy as np

    root = f"/science/{inst}/RSLC"
    with h5py.File(path, "a") as f:
        ident = f.create_group(f"/science/{inst}/identification")
        ident.create_dataset("productType", data=np.bytes_(b"RSLC"))
        ident.create_dataset("productLevel", data=np.bytes_(b"L1"))
        f.create_dataset(f"{root}/swaths/frequencyA/HH",
                         data=np.zeros((4, 4), dtype=np.float32))
    return root


def test_string_cap_respects_utf8(tmp_path):
    """NISAR_DUMP_MAX_STRING_CHARS counts code points and never splits a
    multibyte UTF-8 sequence, for variable-length strings, attributes and
    the bounded read of fixed-length strings alike."""
    h5py = pytest.importorskip("h5py")
    import numpy as np

    path = tmp_path / "utf8.h5"
    root = _synthetic_granule(path, h5py)
    text = "\u00e9clair"                                   # 'é' is 2 bytes
    with h5py.File(path, "a") as f:
        meta = f.create_group(f"{root}/metadata")
        meta.create_dataset("vlen", data=text, dtype=h5py.string_dtype("utf-8"))
        meta.create_dataset("vlen1d", data=[text, "x"], dtype=h5py.string_dtype("utf-8"))
        meta.create_dataset("vlenbig", data=["x" * 100], dtype=h5py.string_dtype("utf-8"))
        meta.create_dataset("fixed", data=text.encode(),
                            dtype=h5py.string_dtype("utf-8", len(text.encode())))
        meta.attrs.create("attr", text, dtype=h5py.string_dtype("utf-8"))

    def value_lines(cap):
        with gdal.config_option("NISAR_DUMP_MAX_STRING_CHARS", str(cap)):
            ds = gdal.OpenEx(f'NISAR:"{path}"', gdal.OF_RASTER,
                             open_options=[f"DUMP_ROOT={root}/metadata", "DUMP_MODE=FULL"])
            lines = _dump(ds)
        return [ln.strip() for ln in lines if ln.lstrip().startswith("DATA {")]

    full = value_lines(64)
    assert sum(f'"{text}"' in ln for ln in full) == 4
    assert f'DATA {{ "{text}", "x" }}' in full
    assert 'DATA { "' + "x" * 64 + '..." (100 chars) }' in full

    capped = value_lines(1)
    assert len(capped) == 5
    # 101 VL bytes exceed the 1 * (4 * 1 + 1) byte budget; the rank-1 dataset
    # (10 bytes, budget 2 * 5) still fits although 'é' is two bytes.
    capped.remove("DATA { (variable-length string, 101 bytes, not printed) }")
    for ln in capped:
        assert "\u00e9..." in ln                       # cut after the whole 'é'
        assert "\ufffd" not in ln                      # no replacement char
    assert 'DATA { "\u00e9..." (6 chars), "x" }' in capped
    assert sum("(6 chars)" in ln for ln in capped) == 3  # code points, not bytes
    assert sum("(STRSIZE 7)" in ln for ln in capped) == 1  # bounded fixed-length read

    # The whole 'é' also survives when the cap lands exactly after it.
    assert sum("\u00e9c..." in ln for ln in value_lines(2)) == 4


def test_default_dump_root_follows_raster_instrument(tmp_path):
    """Opening an explicit raster path under the other instrument of a
    dual-instrument granule dumps that instrument, not the detected default."""
    h5py = pytest.importorskip("h5py")

    path = tmp_path / "dual.h5"
    _synthetic_granule(path, h5py, "LSAR")
    sroot = _synthetic_granule(path, h5py, "SSAR")

    def dump_root(desc, **oo):
        ds = gdal.OpenEx(desc, gdal.OF_RASTER,
                         open_options=[f"{k}={v}" for k, v in oo.items()])
        assert ds is not None
        return _dump(ds)[1]

    assert dump_root(f'NISAR:"{path}"') == 'GROUP "/science/LSAR" {'
    assert dump_root(f'NISAR:"{path}":{sroot}/swaths/frequencyA/HH') == \
        'GROUP "/science/SSAR" {'
    assert dump_root(f'NISAR:"{path}":{sroot}/swaths/frequencyA/HH',
                     DUMP_ROOT="/science/LSAR") == 'GROUP "/science/LSAR" {'
