// hdf5vfl.cpp
#include "hdf5vfl.h"
#include "cpl_port.h"
#include "cpl_vsi.h"

#include <algorithm>
#include <mutex>
#include <cstring> // For memset

namespace NisarVFL {

static std::mutex gMutex;
static hid_t hFileDriver = -1;

#define MAXADDR ((static_cast<haddr_t>(1) << (8 * sizeof(haddr_t) - 1)) - 1)

// --------------------------------------------------------------------------
// VFL State Structure
// --------------------------------------------------------------------------
typedef struct HDF5_vsil_t
{
    H5FD_t pub{}; /* must be first */
    VSILFILE *fp = nullptr;
    haddr_t eoa = 0;
    haddr_t eof = 0;
} HDF5_vsil_t;

// --------------------------------------------------------------------------
// Static Function Implementations (The GDAL <-> HDF5 Bridge)
// --------------------------------------------------------------------------

static H5FD_t *HDF5_vsil_open(const char *name, unsigned flags,
                              hid_t /*fapl_id*/, haddr_t /*maxaddr*/)
{
    const char *openFlags = "rb";
    if ((H5F_ACC_RDWR & flags))
        openFlags = "rb+";
    if ((H5F_ACC_TRUNC & flags) || (H5F_ACC_CREAT & flags))
        openFlags = "wb+";

    // Use GDAL's VSI layer to open the file (supports /vsicurl/, /vsis3/, etc.)
    VSILFILE *fp = VSIFOpenL(name, openFlags);
    if (!fp)
    {
        return nullptr;
    }
    if ((H5F_ACC_TRUNC & flags))
    {
        VSIFTruncateL(fp, 0);
    }

    HDF5_vsil_t *fh = new HDF5_vsil_t;
    memset(&fh->pub, 0, sizeof(fh->pub));
    if (!fh)
    {
        VSIFCloseL(fp);
        return nullptr;
    }
    fh->fp = fp;

    // Determine the end of the file
    VSIFSeekL(fh->fp, 0, SEEK_END);
    fh->eof = static_cast<haddr_t>(VSIFTellL(fh->fp));

    return reinterpret_cast<H5FD_t *>(fh);
}

static herr_t HDF5_vsil_close(H5FD_t *_file)
{
    HDF5_vsil_t *fh = reinterpret_cast<HDF5_vsil_t *>(_file);
    int ret = VSIFCloseL(fh->fp);
    delete fh;
    return ret;
}

static herr_t HDF5_vsil_query(const H5FD_t *, unsigned long *flags /* out */)
{
    *flags = H5FD_FEAT_AGGREGATE_METADATA | H5FD_FEAT_ACCUMULATE_METADATA |
             H5FD_FEAT_DATA_SIEVE | H5FD_FEAT_AGGREGATE_SMALLDATA;
    return 0;
}

static haddr_t HDF5_vsil_get_eoa(const H5FD_t *_file, H5FD_mem_t /*type*/)
{
    const HDF5_vsil_t *fh = reinterpret_cast<const HDF5_vsil_t *>(_file);
    return fh->eoa;
}

static herr_t HDF5_vsil_set_eoa(H5FD_t *_file, H5FD_mem_t /*type*/,
                                haddr_t addr)
{
    HDF5_vsil_t *fh = reinterpret_cast<HDF5_vsil_t *>(_file);
    fh->eoa = addr;
    return 0;
}

static haddr_t HDF5_vsil_get_eof(const H5FD_t *_file, H5FD_mem_t /* type */)
{
    const HDF5_vsil_t *fh = reinterpret_cast<const HDF5_vsil_t *>(_file);
    return fh->eof;
}

static herr_t HDF5_vsil_read(H5FD_t *_file, H5FD_mem_t /* type */,
                             hid_t /* dxpl_id */, haddr_t addr, size_t size,
                             void *buf /*out*/)
{
    HDF5_vsil_t *fh = reinterpret_cast<HDF5_vsil_t *>(_file);
    VSIFSeekL(fh->fp, static_cast<vsi_l_offset>(addr), SEEK_SET);
    return VSIFReadL(buf, size, 1, fh->fp) == 1 ? 0 : -1;
}

static herr_t HDF5_vsil_write(H5FD_t *_file, H5FD_mem_t /* type */,
                              hid_t /* dxpl_id */, haddr_t addr, size_t size,
                              const void *buf /*out*/)
{
    HDF5_vsil_t *fh = reinterpret_cast<HDF5_vsil_t *>(_file);
    VSIFSeekL(fh->fp, static_cast<vsi_l_offset>(addr), SEEK_SET);
    int ret = VSIFWriteL(buf, size, 1, fh->fp) == 1 ? 0 : -1;
    fh->eof = std::max(fh->eof, static_cast<haddr_t>(VSIFTellL(fh->fp)));
    return ret;
}

// NOTE: 'bool closing' is used here to comply with HDF5 2.1.0 (hbool_t deprecation)
static herr_t HDF5_vsil_truncate(H5FD_t *_file, hid_t /* dxpl_id*/,
                                 bool /*closing*/)
{
    HDF5_vsil_t *fh = reinterpret_cast<HDF5_vsil_t *>(_file);
    if (fh->eoa != fh->eof)
    {
        if (VSIFTruncateL(fh->fp, fh->eoa) < 0)
        {
            return -1;
        }
        fh->eof = fh->eoa;
    }
    return 0;
}


// --------------------------------------------------------------------------
// HDF5 2.1.0 Compatible Class Struct
// Exactly 40 fields to match H5FDdevelop.h
// --------------------------------------------------------------------------
static const H5FD_class_t HDF5_vsil_g = {
    H5FD_CLASS_VERSION,              /* 1: version */
    static_cast<H5FD_class_value_t>(513), /* 2: value (Reserved GDAL VSIL value) */
    "vsil",                          /* 3: name */
    MAXADDR,                         /* 4: maxaddr */
    H5F_CLOSE_WEAK,                  /* 5: fc_degree */
    nullptr,                         /* 6: terminate */
    nullptr,                         /* 7: sb_size */
    nullptr,                         /* 8: sb_encode */
    nullptr,                         /* 9: sb_decode */
    0,                               /* 10: fapl_size */
    nullptr,                         /* 11: fapl_get */
    nullptr,                         /* 12: fapl_copy */
    nullptr,                         /* 13: fapl_free */
    0,                               /* 14: dxpl_size */
    nullptr,                         /* 15: dxpl_copy */
    nullptr,                         /* 16: dxpl_free */
    HDF5_vsil_open,                  /* 17: open */
    HDF5_vsil_close,                 /* 18: close */
    nullptr,                         /* 19: cmp */
    HDF5_vsil_query,                 /* 20: query */
    nullptr,                         /* 21: get_type_map */
    nullptr,                         /* 22: alloc */
    nullptr,                         /* 23: free */
    HDF5_vsil_get_eoa,               /* 24: get_eoa */
    HDF5_vsil_set_eoa,               /* 25: set_eoa */
    HDF5_vsil_get_eof,               /* 26: get_eof */
    nullptr,                         /* 27: get_handle */
    HDF5_vsil_read,                  /* 28: read */
    HDF5_vsil_write,                 /* 29: write */
    nullptr,                         /* 30: read_vector */
    nullptr,                         /* 31: write_vector */
    nullptr,                         /* 32: read_selection */
    nullptr,                         /* 33: write_selection */
    nullptr,                         /* 34: flush */
    HDF5_vsil_truncate,              /* 35: truncate */
    nullptr,                         /* 36: lock */
    nullptr,                         /* 37: unlock */
    nullptr,                         /* 38: del */
    nullptr,                         /* 39: ctl */
    {                                /* 40: fl_map (H5FD_FLMAP_DICHOTOMY mapped out) */
        H5FD_MEM_SUPER,   /* default */
        H5FD_MEM_SUPER,   /* super */
        H5FD_MEM_SUPER,   /* btree */
        H5FD_MEM_DRAW,    /* draw */
        H5FD_MEM_DRAW,    /* gheap */
        H5FD_MEM_SUPER,   /* lheap */
        H5FD_MEM_SUPER    /* ohdr */
    }
};

// --------------------------------------------------------------------------
// Public API implementations exposed via hdf5vfl.h
// --------------------------------------------------------------------------

hid_t HDF5VFLGetFileDriver()
{
    std::lock_guard<std::mutex> oLock(gMutex);
    if (hFileDriver < 0)
    {
        hFileDriver = H5FDregister(&HDF5_vsil_g);
#if H5E_auto_t_vers == 2
        // Disable internal HDF5 error printing to terminal, leave to GDAL
        H5Eset_auto(H5E_DEFAULT, nullptr, nullptr);
#endif
    }
    return hFileDriver;
}

void HDF5VFLUnloadFileDriver()
{
    {
        std::lock_guard<std::mutex> oLock(gMutex);
        if (hFileDriver >= 0)
        {
            H5FDunregister(hFileDriver);
            hFileDriver = -1;
        }
    }
}

} // namespace NisarVFL
