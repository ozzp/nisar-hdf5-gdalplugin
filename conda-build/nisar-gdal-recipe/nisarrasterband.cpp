// nisarrasterband.cpp
/**************************************************************************************************************************/
/* Copyright 2025, by the California Institute of Technology.                                                             */
/* ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged.                                                */
/* Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.*/
/*                                                                                                                        */
/* This software may be subject to U.S. export control laws.                                                              */
/* By accepting this software, the user agrees to comply with all applicable U.S. export laws and regulations.            */
/* User has the responsibility to obtain export licenses, or other export authority as may be required                    */
/* before exporting such information to foreign countries or providing access to foreign persons.                         */
/**************************************************************************************************************************/

#include <thread>
#include <algorithm>   // For std::min
#include <vector>      // For std::vector
#include <cstring>     // For memset
#include <mutex>       // For std::lock_guard
#include <iomanip>     // for std::setprecision
#include <chrono>      // for timing instrumentation of H5Dread call
#include <atomic>

#include "hdf5.h"
#include "gdal.h"        // For CE_Failure, CE_None, GDALDataType
#include "cpl_conv.h"
#include "cpl_vsi.h"

#ifdef __AVX2__
#include <immintrin.h>
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#include <arm_neon.h>
#endif

#ifdef USE_ZLIB_NG
#include <zlib-ng.h>
#else
#include <zlib.h>
#endif

#include "nisarrasterband.h"
#include "nisaroverviewband.h"
#include "nisardataset.h"
#include "nisar_priv.h"

thread_local bool NisarRasterBand::bDisableOverviewRouting = false;

/************************************************************************/
/* ==================================================================== */
/*                            NisarRasterBand                         */
/* ==================================================================== */
/* This class inherits from GDALPamRasterBand and represents a single band */
/* within the NISAR dataset. It includes methods for reading           */
/* data blocks (IReadBlock) and retrieving NoData values (GetNoDataValue). */
/************************************************************************/
NisarRasterBand::NisarRasterBand( NisarDataset *poDSIn, int nBandIn ) :
      GDALPamRasterBand(),  //Call base class construction
      hH5Type(-1)           //Initialize custom member
{
    // Safety Check
    if (!poDSIn)
    {
        CPLError(CE_Fatal, CPLE_AppDefined, "NisarRasterBand constructor: Parent dataset pointer is NULL.");
        this->eDataType = GDT_Unknown;
        return; 
    }

    this->poDS = poDSIn;
    this->nBand = nBandIn;

    // Initialize base class members first
    this->nRasterXSize = poDSIn->GetRasterXSize();
    this->nRasterYSize = poDSIn->GetRasterYSize();

    NisarDataset *poGDS = static_cast<NisarDataset *>(poDSIn);
    this->eDataType = poGDS->eDataType;

    // Get HDF5 Dataset Handle
    hid_t hDatasetID = poGDS->GetDatasetHandle();
    if (hDatasetID < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined, "NisarRasterBand %d: Parent dataset handle is invalid.", nBandIn);
        return;
    }

    // Get and Store HDF5 Native Data Type Handle
    this->hH5Type = H5Dget_type(hDatasetID);
    if (this->hH5Type < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined, "NisarRasterBand %d: Failed to get HDF5 native datatype handle.", nBandIn);
    }

    // Determine Block Size (from HDF5 chunking)
    this->nBlockXSize = 512; // Default
    this->nBlockYSize = 512; // Default

    hid_t dcpl_id = H5Dget_create_plist(hDatasetID);
    if (dcpl_id >= 0)
    {
        H5D_layout_t layout = H5Pget_layout(dcpl_id);
        if (layout == H5D_CHUNKED)
        {
            int nFilters = H5Pget_nfilters(dcpl_id);
            for (int i = 0; i < nFilters; i++) {
                unsigned int flags;
                size_t cd_nelmts = 1;
                unsigned int cd_values[1] = {0}; // Allocate the buffer for the deflate level
                char name[128];
                
                H5Z_filter_t filter = H5Pget_filter2(dcpl_id, i, &flags, &cd_nelmts, cd_values, sizeof(name), name, nullptr);

                if (filter == H5Z_FILTER_DEFLATE) {
                    m_bIsDeflated = true;
                    // HDF5 stores the Zlib compression level in the first index of cd_values
                    if (cd_nelmts >= 1) {
                        m_nDeflateLevel = static_cast<int>(cd_values[0]);
                    }
                }
                if (filter == H5Z_FILTER_SHUFFLE) m_bIsShuffled = true;
            }
            hid_t hSpace = H5Dget_space(hDatasetID);
            if (hSpace >= 0) {
                int rank = H5Sget_simple_extent_ndims(hSpace);
                if (rank >= 2) {
                    std::vector<hsize_t> chunk_dims(rank);
                    if (H5Pget_chunk(dcpl_id, rank, chunk_dims.data()) == rank) {
                        this->nBlockXSize = static_cast<int>(chunk_dims[rank - 1]);
                        this->nBlockYSize = static_cast<int>(chunk_dims[rank - 2]);
                    }
                }
                H5Sclose(hSpace);
            }
        }
        else if (layout == H5D_CONTIGUOUS)
        {
            this->nBlockXSize = nRasterXSize;
            this->nBlockYSize = 1;
        }
        else //H5D_COMPACT or unknown
        {
            this->nBlockXSize = poGDS->GetRasterXSize();
            this->nBlockYSize = 1;
        }
        H5Pclose(dcpl_id);
    }

    // Create and cache HDF5 dataspace handles
    m_hFileSpaceID = H5Dget_space(hDatasetID);
    
    // Get the rank of the file dataspace
    int rank = -1;
    if( m_hFileSpaceID >= 0 )
        rank = H5Sget_simple_extent_ndims(m_hFileSpaceID);

    if (rank < 2)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "NisarRasterBand: Dataset rank is %d, but must be >= 2.", rank);
        // Set handles to invalid so IReadBlock will fail
        if (m_hFileSpaceID >= 0) H5Sclose(m_hFileSpaceID);
        m_hFileSpaceID = -1; 
        m_hMemSpaceID = -1;
        return;
    }

    // Create a memory dataspace with the *same rank* as the file dataspace
    std::vector<hsize_t> mem_dims(rank);
    
    // Set the block dimensions for Y and X
    mem_dims[rank - 2] = static_cast<hsize_t>(nBlockYSize);
    mem_dims[rank - 1] = static_cast<hsize_t>(nBlockXSize);
    
    // Set all higher dimensions to 1 (we will read one "slice" at a time)
    for (int i = 0; i < rank - 2; ++i)
    {
        mem_dims[i] = 1;
    }

    // Create the N-D memory dataspace
    m_hMemSpaceID = H5Screate_simple(rank, mem_dims.data(), NULL);

    // -------------------------------------------------------------
    // Pre-calculate Endianness (Avoids HDF5 lock during concurrent read)
    // -------------------------------------------------------------
    H5T_order_t fileOrder = H5Tget_order(hH5Type);
#ifdef CPL_IS_LSB
    m_bNeedsEndianSwap = (fileOrder == H5T_ORDER_BE);
#else
    m_bNeedsEndianSwap = (fileOrder == H5T_ORDER_LE);
#endif
    // 1. Calculate total blocks in the grid
    int nBlocksPerRow = (nRasterXSize + nBlockXSize - 1) / nBlockXSize;
    int nBlocksPerCol = (nRasterYSize + nBlockYSize - 1) / nBlockYSize;

    // Resize the class member vector!
    m_aoAllChunks.resize(nBlocksPerRow * nBlocksPerCol);

    // Initialize all chunks as missing (Sparse by default)
    for (int y = 0; y < nBlocksPerCol; ++y) {
        for (int x = 0; x < nBlocksPerRow; ++x) {
            int idx = y * nBlocksPerRow + x;
            m_aoAllChunks[idx].nBlockX = x;
            m_aoAllChunks[idx].nBlockY = y;
            m_aoAllChunks[idx].nOffset = 0;
            m_aoAllChunks[idx].nLength = 0;
            m_aoAllChunks[idx].bIsMissing = true; 
        }
    }

    // -------------------------------------------------------------
    // Check if we found and parsed Statistical Bounds in the Dataset Metadata
    // -------------------------------------------------------------
    // Because NisarDataset::Open() already copied all HDF5 attributes 
    // into the GDAL metadata dictionary, we just query the dictionary
    if (poGDS->GetMetadataItem("min_value") != nullptr && 
        poGDS->GetMetadataItem("max_value") != nullptr) {
        m_bHasMinMax = true;
    } else if (poGDS->GetMetadataItem("valid_min") != nullptr && 
               poGDS->GetMetadataItem("valid_max") != nullptr) {
        m_bHasMinMax = true;
    }

    // Read the max allowed virtual decimation from the environment (Default: 16)
    int nMaxVirtualDecimation = atoi(CPLGetConfigOption("NISAR_MAX_VIRTUAL_OVR", "16"));

    // Common power-of-two zoom levels
    int nFactors[] = {2, 4, 8, 16, 32, 64, 128};

    for (int factor : nFactors) {
        // STOP creating overviews if we hit the computational limit
        if (factor > nMaxVirtualDecimation) {
            CPLDebug("NISAR_OVERVIEW", "Capping virtual overviews at decimation %d. Skipping %d.", nMaxVirtualDecimation, factor);
            break;
        }

        // Only create an overview if it results in an image at least 1 pixel wide/high
        if (nRasterXSize / factor > 0 && nRasterYSize / factor > 0) {
            m_apoOverviews.push_back(std::make_unique<NisarOverviewBand>(this, factor));
        }
    }

    // 2. Define Context Struct for the C-Callback
    struct ChunkIterCtx {
        std::vector<NisarChunkInfo>* paoChunks;
        int nBlocksPerRow;
        int nBlockXSize;
        int nBlockYSize;
        int rank;
        int nBand;
    };
    
    ChunkIterCtx ctx = { &m_aoAllChunks, nBlocksPerRow, nBlockXSize, nBlockYSize, rank, nBand };

    // 3. Define the Stateless Lambda Callback
    // Note: Because this lambda captures nothing "[]", it implicitly casts to a C function pointer!
    H5D_chunk_iter_op_t chunk_cb = [](const hsize_t *offset, unsigned /*filter_mask*/, haddr_t addr, hsize_t size, void *op_data) -> int {
        ChunkIterCtx* pCtx = static_cast<ChunkIterCtx*>(op_data);
        
        int nBlockX = 0;
        int nBlockY = 0;
        
        // Translate HDF5 element offsets back to GDAL Block coordinates
        if (pCtx->rank == 3) {
            // If it's a 3D dataset, ensure we only process chunks belonging to THIS band (Z-index)
            if (offset[0] != static_cast<hsize_t>(pCtx->nBand - 1)) return 0; // Skip to next chunk
            nBlockY = static_cast<int>(offset[1] / pCtx->nBlockYSize);
            nBlockX = static_cast<int>(offset[2] / pCtx->nBlockXSize);
        } else if (pCtx->rank == 2) {
            nBlockY = static_cast<int>(offset[0] / pCtx->nBlockYSize);
            nBlockX = static_cast<int>(offset[1] / pCtx->nBlockXSize);
        } else {
            return -1; // Abort iteration on unexpected rank
        }

        // Calculate 1D index in our pre-allocated vector
        int idx = nBlockY * pCtx->nBlocksPerRow + nBlockX;
        
        // Map the physical address and mark it as physically existing!
        // We explicitly cast size() to int to prevent signed/unsigned compiler errors
        if (idx >= 0 && idx < static_cast<int>(pCtx->paoChunks->size())) {
            (*pCtx->paoChunks)[idx].nOffset = static_cast<vsi_l_offset>(addr);
            (*pCtx->paoChunks)[idx].nLength = static_cast<size_t>(size);
            (*pCtx->paoChunks)[idx].bIsMissing = false;
        }
        
        return 0; // Return 0 to tell HDF5 to keep iterating
    };

    // 4. Fire the Optimized Iterator
    // This blasts through the B-Tree in native C and populates our vector instantly.
    H5Dchunk_iter(hDatasetID, H5P_DEFAULT, chunk_cb, &ctx);

    // ====================================================================
    // 5. GENERATE THE SIDECAR (With Remote Target Tracking & Fallbacks)
    // ====================================================================
    
    // Track the target path dialect
    std::string osS3Url = GetStandardDatasetURI();

    // Safety Valve A: If the string is completely empty
    if (osS3Url.empty())
    {
        CPLError(CE_Warning, CPLE_AppDefined, 
                 "NISAR: GetStandardDatasetURI() resolved as empty! Applying structural local fallback.");
        
        // A standard local URI fallback ensures fsspec won't choke on an empty array element
        osS3Url = "file:///tmp/mock_nisar_dataset.h5";
    }
    // Safety Valve B: If it's a local file path (e.g., "/home/user/data.h5" or "C:\data.h5")
    else if (!STARTS_WITH_CI(osS3Url.c_str(), "s3://") && 
             !STARTS_WITH_CI(osS3Url.c_str(), "http://") && 
             !STARTS_WITH_CI(osS3Url.c_str(), "https://") &&
             !STARTS_WITH_CI(osS3Url.c_str(), "file://"))
    {
        CPLDebug("NISAR_ZARR", "Local file path tracking detected: %s", osS3Url.c_str());
        
        // fsspec loves explicit protocols. If it's a local file, prepend the file:// schema
        osS3Url = "file://" + osS3Url;
    }

    // Capture the runtime tracking states via GDAL console pipeline
    CPLDebug("NISAR_ZARR", "Sidecar Target URI verified as: %s", osS3Url.c_str());

    // Dynamically grab the HDF5 dataset path
    char szDatasetName[1024];
    ssize_t len = H5Iget_name(hDatasetID, szDatasetName, sizeof(szDatasetName));
    std::string osZarrGroup = (len > 0) ? szDatasetName : "unknown_dataset";
    
    // Safely format the output JSON file name 
    std::string osSafeName = osZarrGroup;
    std::replace(osSafeName.begin(), osSafeName.end(), '/', '_');
    std::string osOutJson = "/tmp/nisar_kerchunk" + osSafeName + ".json";

    CPLDebug("NISAR", "Exporting Virtual Zarr Sidecar with %zu parsed chunk slots.", m_aoAllChunks.size());
    
    // Fire the finalized, flat reference compiler
    WriteVirtualZarrSidecar(osS3Url, osZarrGroup, m_aoAllChunks, osOutJson);
}

NisarRasterBand::~NisarRasterBand()
{
    // Close the cached HDF5 objects
    if (m_hMemSpaceID >= 0) H5Sclose(m_hMemSpaceID);
    if (m_hFileSpaceID >= 0) H5Sclose(m_hFileSpaceID);

    if (hH5Type >= 0) {
        if (H5Tclose(hH5Type) < 0) {
             CPLError(CE_Warning, CPLE_AppDefined, "Failed to close HDF5 data type handle in ~NisarRasterBand.");
        }
    }
    if (m_bMaskBandOwned && m_poMaskBand) {
        delete m_poMaskBand;
    }
}

std::string NisarRasterBand::GetRawVSIPath() const
{
    // Extracts the physical file path from the GDAL description 
    // Example Inputs: 
    // "NISAR:s3://bucket/file.h5:/path/to/data"
    // "s3://bucket/file.h5"
    std::string sDesc = poDS->GetDescription();
    
    // 1. Strip "NISAR:" driver prefix
    if (STARTS_WITH_CI(sDesc.c_str(), "NISAR:")) {
        sDesc = sDesc.substr(6);
    }
    
    // 2. Strip the HDF5 internal path suffix (e.g., ":/science/LSAR/...")
    size_t nLastColon = sDesc.find_last_of(':');
    if (nLastColon != std::string::npos) {
        // Ensure the colon isn't part of a URL scheme (s3://, http://, https://)
        // We check if the colon is followed by "//"
        bool bIsUrlScheme = (sDesc.length() > nLastColon + 2 && 
                             sDesc[nLastColon + 1] == '/' && 
                             sDesc[nLastColon + 2] == '/');
                             
        if (!bIsUrlScheme) {
            sDesc = sDesc.substr(0, nLastColon); // Chop off the HDF5 path
        }
    }
    
    // 3. Strip surrounding quotes if GDAL added them
    if (sDesc.length() >= 2 && sDesc.front() == '"' && sDesc.back() == '"') {
        sDesc = sDesc.substr(1, sDesc.length() - 2);
    }
    
    // ---------------------------------------------------------
    // VSI Namespace Translation for VSIFOpenL
    // ---------------------------------------------------------
    // GDAL's VSIFOpenL requires /vsi... prefixes to route the I/O.
    // We seamlessly translate standard URLs into VSI paths here.
    
    if (STARTS_WITH_CI(sDesc.c_str(), "s3://")) {
        // Map "s3://bucket/file.h5" -> "/vsis3/bucket/file.h5"
        sDesc = "/vsis3/" + sDesc.substr(5);
    } 
    else if (STARTS_WITH_CI(sDesc.c_str(), "https://") || 
             STARTS_WITH_CI(sDesc.c_str(), "http://")) {
        // Map "https://domain.com/file.h5" -> "/vsicurl/https://domain.com/file.h5"
        // GDAL's vsicurl allows stacking the full URL after the prefix.
        sDesc = "/vsicurl/" + sDesc;
    }
    // If it's already a /vsi... path or a local file path, leave it untouched.

    return sDesc;
}

std::string NisarRasterBand::GetStandardDatasetURI() const
{
    // Strategy 1: Check the parent dataset's primary description field
    std::string sDesc = poDS->GetDescription();
    
    // Strategy 2: Fallback to the band's own description if dataset is empty
    if (sDesc.empty()) {
        sDesc = GetDescription();
    }

    // If we found a string, strip the driver prefixes and subdataset hooks
    if (!sDesc.empty()) 
    {
        // 1. Strip "NISAR:" driver prefix
        if (STARTS_WITH_CI(sDesc.c_str(), "NISAR:")) {
            sDesc = sDesc.substr(6);
        }
        
        // 2. Strip the HDF5 internal path suffix (e.g., ":/science/LSAR/...")
        size_t nLastColon = sDesc.find_last_of(':');
        if (nLastColon != std::string::npos) {
            // Protect the colon in "s3://" or "https://" URL schemes
            bool bIsUrlScheme = (sDesc.length() > nLastColon + 2 &&
                                 sDesc[nLastColon + 1] == '/' &&
                                 sDesc[nLastColon + 2] == '/');
                                 
            if (!bIsUrlScheme) {
                sDesc = sDesc.substr(0, nLastColon); // Chop off internal path
            }
        }
        
        // 3. Clean up quotes
        if (sDesc.length() >= 2 && sDesc.front() == '"' && sDesc.back() == '"') {
            sDesc = sDesc.substr(1, sDesc.length() - 2);
        }
    }
    
    return sDesc; 
}

bool NisarRasterBand::ProcessAndCopyChunk(const GByte* pSrcData, size_t nSrcSize, void* pDstData)
{
    size_t nElements = static_cast<size_t>(nBlockXSize) * nBlockYSize;
    int nElementSize = GDALGetDataTypeSizeBytes(eDataType);
    size_t nUncompressedSize = nElements * nElementSize;

    const GByte* pWorkingData = pSrcData;

    // Use a thread_local buffer. This allocates memory ONCE per thread,
    // and reuses it for every chunk without zero-initializing it!
    thread_local std::vector<GByte> tls_uncompressedData;

    if (m_bIsDeflated) {
        // resize() will not zero-initialize bytes if the capacity is already large enough
        if (tls_uncompressedData.size() < nUncompressedSize) {
            tls_uncompressedData.resize(nUncompressedSize);
        }

        // -------------------------------------------------------------
        // DECOMPRESSION ROUTING
        // -------------------------------------------------------------
        int zErr;
#ifdef USE_ZLIB_NG
        // The hardware-accelerated SIMD path
        z_size_t destLen = static_cast<z_size_t>(nUncompressedSize);
        zErr = zng_uncompress(
            tls_uncompressedData.data(),
            &destLen,
            pSrcData,
            static_cast<z_size_t>(nSrcSize)
        );
#else
        // The standard legacy zlib fallback path
        uLongf destLen = static_cast<uLongf>(nUncompressedSize);
        zErr = uncompress(
            tls_uncompressedData.data(),
            &destLen,
            pSrcData,
            static_cast<uLong>(nSrcSize)
        );
#endif

        if (zErr != Z_OK) return false;

        pWorkingData = tls_uncompressedData.data();
    }

    // -------------------------------------------------------------
    // Fused Un-Shuffle & Endianness Filter
    // -------------------------------------------------------------
    if (m_bIsShuffled && nElementSize > 1) {
        GByte* dst = static_cast<GByte*>(pDstData);

        // Invert plane reading order to get a "free" endian swap
        int p0 = 0, p1 = 1, p2 = 2, p3 = 3, p4 = 4, p5 = 5, p6 = 6, p7 = 7;
        if (m_bNeedsEndianSwap) {
            if (nElementSize == 2) { p0 = 1; p1 = 0; }
            else if (nElementSize == 4) { p0 = 3; p1 = 2; p2 = 1; p3 = 0; }
            else if (nElementSize == 8) { p0 = 7; p1 = 6; p2 = 5; p3 = 4; p4 = 3; p5 = 2; p6 = 1; p7 = 0; }
            
            // Flag as complete so the fallback GDALSwapWords block doesn't run
            m_bNeedsEndianSwap = false; 
        }

        if (nElementSize == 4) { // Float32 / Int32
            const GByte* src0 = pWorkingData + p0 * nElements;
            const GByte* src1 = pWorkingData + p1 * nElements;
            const GByte* src2 = pWorkingData + p2 * nElements;
            const GByte* src3 = pWorkingData + p3 * nElements;

            size_t j = 0;
            
#ifdef __AVX2__
            for (; j + 31 < nElements; j += 32) {
                __m256i v0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src0 + j));
                __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src1 + j));
                __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src2 + j));
                __m256i v3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src3 + j));

                __m256i v01_lo = _mm256_unpacklo_epi8(v0, v1);
                __m256i v01_hi = _mm256_unpackhi_epi8(v0, v1);
                __m256i v23_lo = _mm256_unpacklo_epi8(v2, v3);
                __m256i v23_hi = _mm256_unpackhi_epi8(v2, v3);

                __m256i v0123_0 = _mm256_unpacklo_epi16(v01_lo, v23_lo);
                __m256i v0123_1 = _mm256_unpackhi_epi16(v01_lo, v23_lo);
                __m256i v0123_2 = _mm256_unpacklo_epi16(v01_hi, v23_hi);
                __m256i v0123_3 = _mm256_unpackhi_epi16(v01_hi, v23_hi);

                __m256i out0 = _mm256_permute2x128_si256(v0123_0, v0123_1, 0x20);
                __m256i out1 = _mm256_permute2x128_si256(v0123_2, v0123_3, 0x20);
                __m256i out2 = _mm256_permute2x128_si256(v0123_0, v0123_1, 0x31);
                __m256i out3 = _mm256_permute2x128_si256(v0123_2, v0123_3, 0x31);

                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 0), out0);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 32), out1);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 64), out2);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 96), out3);
                
                dst += 128;
            }
#elif defined(__aarch64__) || defined(_M_ARM64)
            // ARM NEON Pipeline (Processes 16 elements = 64 output bytes per loop)
            for (; j + 15 < nElements; j += 16) {
                uint8x16_t v0 = vld1q_u8(src0 + j);
                uint8x16_t v1 = vld1q_u8(src1 + j);
                uint8x16_t v2 = vld1q_u8(src2 + j);
                uint8x16_t v3 = vld1q_u8(src3 + j);

                // Group the 4 independent byte planes into a structural layout
                uint8x16x4_t unshuffled = { v0, v1, v2, v3 };

                // Interleave and stream directly to application memory
                vst4q_u8(dst, unshuffled);
                dst += 64; 
            }
#endif
            // Scalar fallback/tail processing
            for (; j < nElements; ++j) {
                dst[0] = src0[j];
                dst[1] = src1[j];
                dst[2] = src2[j];
                dst[3] = src3[j];
                dst += 4;
            }
        } 
        else if (nElementSize == 8) { // CFloat32 / Float64
            const GByte* src0 = pWorkingData + p0 * nElements;
            const GByte* src1 = pWorkingData + p1 * nElements;
            const GByte* src2 = pWorkingData + p2 * nElements;
            const GByte* src3 = pWorkingData + p3 * nElements;
            const GByte* src4 = pWorkingData + p4 * nElements;
            const GByte* src5 = pWorkingData + p5 * nElements;
            const GByte* src6 = pWorkingData + p6 * nElements;
            const GByte* src7 = pWorkingData + p7 * nElements;

            size_t j = 0;

#if defined(__aarch64__) || defined(_M_ARM64)
            // ARM NEON Pipeline (Processes 16 elements = 128 output bytes per loop)
            for (; j + 15 < nElements; j += 16) {
                // 1. Load 16 bytes from each of the 8 planes
                uint8x16_t v0 = vld1q_u8(src0 + j);
                uint8x16_t v1 = vld1q_u8(src1 + j);
                uint8x16_t v2 = vld1q_u8(src2 + j);
                uint8x16_t v3 = vld1q_u8(src3 + j);
                uint8x16_t v4 = vld1q_u8(src4 + j);
                uint8x16_t v5 = vld1q_u8(src5 + j);
                uint8x16_t v6 = vld1q_u8(src6 + j);
                uint8x16_t v7 = vld1q_u8(src7 + j);

                // 2. Zip the lower and upper halves into 16-bit pairs
                uint8x16_t v01_lo = vzip1q_u8(v0, v1);
                uint8x16_t v01_hi = vzip2q_u8(v0, v1);
                uint8x16_t v23_lo = vzip1q_u8(v2, v3);
                uint8x16_t v23_hi = vzip2q_u8(v2, v3);
                uint8x16_t v45_lo = vzip1q_u8(v4, v5);
                uint8x16_t v45_hi = vzip2q_u8(v4, v5);
                uint8x16_t v67_lo = vzip1q_u8(v6, v7);
                uint8x16_t v67_hi = vzip2q_u8(v6, v7);

                // 3. Interleave the lower 8 elements into memory
                uint16x8x4_t out_lo;
                out_lo.val[0] = vreinterpretq_u16_u8(v01_lo);
                out_lo.val[1] = vreinterpretq_u16_u8(v23_lo);
                out_lo.val[2] = vreinterpretq_u16_u8(v45_lo);
                out_lo.val[3] = vreinterpretq_u16_u8(v67_lo);
                vst4q_u16(reinterpret_cast<uint16_t*>(dst), out_lo);

                // 4. Interleave the upper 8 elements into memory
                uint16x8x4_t out_hi;
                out_hi.val[0] = vreinterpretq_u16_u8(v01_hi);
                out_hi.val[1] = vreinterpretq_u16_u8(v23_hi);
                out_hi.val[2] = vreinterpretq_u16_u8(v45_hi);
                out_hi.val[3] = vreinterpretq_u16_u8(v67_hi);
                vst4q_u16(reinterpret_cast<uint16_t*>(dst + 64), out_hi);

                dst += 128;
            }
#endif
            // Highly unrolled sequential write loop
            for (size_t j = 0; j < nElements; ++j) {
                dst[0] = src0[j];
                dst[1] = src1[j];
                dst[2] = src2[j];
                dst[3] = src3[j];
                dst[4] = src4[j];
                dst[5] = src5[j];
                dst[6] = src6[j];
                dst[7] = src7[j];
                dst += 8;
            }
        }
        else if (nElementSize == 2) { // Int16 / UInt16
            const GByte* src0 = pWorkingData + p0 * nElements;
            const GByte* src1 = pWorkingData + p1 * nElements;
            
            for (size_t j = 0; j < nElements; ++j) {
                dst[0] = src0[j];
                dst[1] = src1[j];
                dst += 2;
            }
        }
    } 
    else {
        // Direct copy for 1-byte data types (QA Masks) or non-shuffled data
        memcpy(pDstData, pWorkingData, nUncompressedSize);
    }

    // -------------------------------------------------------------
    // Endianness Correction (Fallback for non-shuffled data)
    // -------------------------------------------------------------
    if (m_bNeedsEndianSwap && nElementSize > 1) {
#if defined(__aarch64__) || defined(_M_ARM64)
        if (nElementSize == 4) {
            uint32_t* ptr = static_cast<uint32_t*>(pDstData);
            size_t k = 0;
            // Process 4 Float32s (16 bytes) per cycle
            for (; k + 3 < nElements; k += 4) {
                uint8x16_t v = vld1q_u8(reinterpret_cast<uint8_t*>(ptr + k));
                // Instantly swap bytes 0-3, 4-7, 8-11, 12-15
                vst1q_u8(reinterpret_cast<uint8_t*>(ptr + k), vrev32q_u8(v));
            }
            // Tail swap handled by GDAL
            if (k < nElements) {
                GDALSwapWords(ptr + k, 4, static_cast<int>(nElements - k), 4);
            }
        }
        else {
            GDALSwapWords(pDstData, nElementSize, static_cast<int>(nElements), nElementSize);
        }
#else
        // Leverages native GDAL SIMD architecture
        GDALSwapWords(pDstData, nElementSize, static_cast<int>(nElements), nElementSize);
#endif
    }

    return true;
}

// --------------------------------------------------------------------
// Overview Overrides
// --------------------------------------------------------------------
int NisarRasterBand::GetOverviewCount() 
{
    if (bDisableOverviewRouting) return 0; // The Blindfold
    return static_cast<int>(m_apoOverviews.size());
}

GDALRasterBand* NisarRasterBand::GetOverview(int i) 
{
    if (bDisableOverviewRouting) return nullptr; // The Blindfold
    if (i < 0 || i >= static_cast<int>(m_apoOverviews.size())) {
        return nullptr;
    }
    // The compiler now knows this safely casts to GDALRasterBand*
    return m_apoOverviews[i].get();
}

int NisarRasterBand::GetMaskFlags()
{
    // Trigger discovery via GetMaskBand() to see if we populate m_poMaskBand
    GetMaskBand();

    // If we successfully created a custom NISAR mask band, declare it as shared.
    if (m_poMaskBand) {
        return GMF_PER_DATASET;
    }

    // Otherwise, per GDAL RFC 15, we declare that all pixels are valid.
    return GMF_ALL_VALID;
}

GDALRasterBand* NisarRasterBand::GetMaskBand()
{
    // Return cached if exists
    if (m_poMaskBand) return m_poMaskBand;

    //  Cast the dataset
    NisarDataset* poNisarDS = (NisarDataset*)poDS;
    if (!poNisarDS) return GDALPamRasterBand::GetMaskBand();

    // If user explicitly disabled masks via -oo MASK=NO, return "All Valid"
    if (!poNisarDS->m_bMaskEnabled) {
        return GDALPamRasterBand::GetMaskBand(); 
    }

    // Construct Mask Path
    // Instead of relying on metadata, we ask HDF5 for the true path of the current dataset.
    // get_hdf5_object_name is defined in nisar_priv.h
    std::string sBandPath = get_hdf5_object_name(poNisarDS->GetDatasetHandle());
    
    if (sBandPath.empty()) {
        // Fallback: If HDF5 name query fails, try metadata (though unlikely to be needed)
        const char* pszPath = poNisarDS->GetMetadataItem("HDF5_PATH");
        if (pszPath) sBandPath = pszPath;
    }

    if (sBandPath.empty()) return GDALPamRasterBand::GetMaskBand();

    // Find the parent group (e.g., remove "/HHHH" from ".../frequencyA/HHHH")
    size_t nLastSlash = sBandPath.find_last_of('/');
    if (nLastSlash == std::string::npos) return GDALPamRasterBand::GetMaskBand();

    // Construct sibling path: ".../frequencyA/mask"
    std::string sMaskPath = sBandPath.substr(0, nLastSlash) + "/mask";

    // Try to Open the Mask Dataset
    H5E_auto2_t old_func; void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    
    hid_t hMaskDS = H5Dopen2(poNisarDS->GetHDF5Handle(), sMaskPath.c_str(), H5P_DEFAULT);
    
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    if (hMaskDS < 0) {
        return GDALPamRasterBand::GetMaskBand(); // No mask found
    }

    // Determine Mask Logic Type
    NisarMaskType eMaskType = NisarMaskType::GCOV; // Default
    
    // Check product type 
    // We access m_sProductType directly since NisarRasterBand is a friend class
    if (poNisarDS->m_sProductType == "GUNW") {
        eMaskType = NisarMaskType::GUNW;
    }

    // Instantiate the correct class
    m_poMaskBand = new NisarHDF5MaskBand(poNisarDS, hMaskDS, eMaskType);
    m_bMaskBandOwned = true;

    return m_poMaskBand;
}

/***************************************************************************/
/*                             IReadBlock()                                */
/* This method reads a block of data from the HDF5 dataset.                */
/***************************************************************************/
CPLErr NisarRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage)
{
    // We lock to ensure the network arrays build cleanly, but we will 
    // manually drop this lock before we touch the GDAL Block Cache.
    std::unique_lock<std::mutex> oLock(m_oMegaFetchMutex);

    // -------------------------------------------------------------
    // ALIGNED PREFETCH EXPANSION (Mega-Fetch)
    // -------------------------------------------------------------
    int nPrefetchGrid = 24; 
    int nTotalBlocksX = (nRasterXSize + nBlockXSize - 1) / nBlockXSize;
    int nTotalBlocksY = (nRasterYSize + nBlockYSize - 1) / nBlockYSize;

    // Snap to rigid grid boundaries to prevent the "Creeping Window"
    int nFetchXMin = (nBlockXOff / nPrefetchGrid) * nPrefetchGrid;
    int nFetchYMin = (nBlockYOff / nPrefetchGrid) * nPrefetchGrid;
    
    int nFetchXMax = std::min(nFetchXMin + nPrefetchGrid - 1, nTotalBlocksX - 1);
    int nFetchYMax = std::min(nFetchYMin + nPrefetchGrid - 1, nTotalBlocksY - 1);

    std::vector<NisarChunkInfo> aoMissingChunks;
    std::vector<vsi_l_offset> anOffsets;
    std::vector<size_t> anSizes;

    // Scan the ALIGNED prefetch grid using the cached B-Tree vector
    for (int iY = nFetchYMin; iY <= nFetchYMax; iY++) {
        for (int iX = nFetchXMin; iX <= nFetchXMax; iX++) {
            
            // Exclude already cached blocks from network fetch arrays
            if (iX != nBlockXOff || iY != nBlockYOff) {
                GDALRasterBlock *poBlock = TryGetLockedBlockRef(iX, iY);
                if (poBlock != nullptr) {
                    poBlock->DropLock();
                    continue;
                }
            }

            int idx = iY * nTotalBlocksX + iX;
            if (idx >= 0 && idx < static_cast<int>(m_aoAllChunks.size())) {
                const auto& chunk = m_aoAllChunks[idx];
                if (!chunk.bIsMissing) {
                    aoMissingChunks.push_back({iX, iY, chunk.nOffset, chunk.nLength, false});
                    anOffsets.push_back(chunk.nOffset);
                    anSizes.push_back(chunk.nLength);
                } else {
                    aoMissingChunks.push_back({iX, iY, 0, 0, true});
                }
            } else {
                aoMissingChunks.push_back({iX, iY, 0, 0, true}); 
            }
        }
    }

    // Perform Concurrent Network I/O
    if (!anOffsets.empty()) {
        std::string sRawPath = GetRawVSIPath();
        VSILFILE* fp = VSIFOpenL(sRawPath.c_str(), "rb");
        
        if (fp) {
            vsi_l_offset nMinOffset = anOffsets[0];
            vsi_l_offset nMaxOffset = anOffsets[0] + anSizes[0];
            for (size_t i = 1; i < anOffsets.size(); i++) {
                if (anOffsets[i] < nMinOffset) nMinOffset = anOffsets[i];
                if (anOffsets[i] + anSizes[i] > nMaxOffset) nMaxOffset = anOffsets[i] + anSizes[i];
            }
            
            size_t nTotalSpan = static_cast<size_t>(nMaxOffset - nMinOffset);
            void* pMegaBuffer = nullptr;
            std::vector<void*> apData; 
            
            bool bIsMegaFetch = (nTotalSpan < 268435456); 

            // START NETWORK TIMING
            auto net_start_time = std::chrono::high_resolution_clock::now();

            if (bIsMegaFetch) {
                pMegaBuffer = CPLMalloc(nTotalSpan);
                VSIFSeekL(fp, nMinOffset, SEEK_SET);
                VSIFReadL(pMegaBuffer, 1, nTotalSpan, fp);
            } else {
                apData.resize(anOffsets.size(), nullptr);
                // FIX 1: RESTORE ACCURATE MEMORY ALLOCATION FOR MULTI-RANGE POINTERS
                for (size_t i = 0; i < anOffsets.size(); i++) {
                    apData[i] = CPLMalloc(anSizes[i]);
                }
                VSIFReadMultiRangeL(static_cast<int>(anOffsets.size()), apData.data(), anOffsets.data(), anSizes.data(), fp);
            }
            VSIFCloseL(fp);

            // Relinquish network lock early to free up standard I/O concurrency
            oLock.unlock();

            // STOP NETWORK TIMING
            auto net_end_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> net_elapsed = net_end_time - net_start_time;

            size_t nTotalDownloaded = bIsMegaFetch ? nTotalSpan : 0;
            if (!bIsMegaFetch) {
                for (size_t sz : anSizes) nTotalDownloaded += sz;
            }

            double dMegabytes = nTotalDownloaded / (1024.0 * 1024.0);
            double dSeconds = net_elapsed.count() / 1000.0;
            double dThroughputMBps = dMegabytes / dSeconds;

            CPLDebug("NISAR_NET_PERF", 
                     "[%s] Chunks: %d | Downloaded: %.2f MB | Throughput: %.2f MB/s",
                     bIsMegaFetch ? "MEGA-FETCH " : "MULTI-RANGE", 
                     static_cast<int>(anOffsets.size()), dMegabytes, dThroughputMBps);

            // 4. THREAD-ISOLATED DECOMPRESSION STAGING AREA
            std::atomic<bool> bSuccess{true};
            const size_t nExpectedBytes = static_cast<size_t>(nBlockXSize) * nBlockYSize * GDALGetDataTypeSizeBytes(eDataType);

            // Thread-safe storage container to house intermediate states
            struct DecompressedChunk {
                int nBlockX;
                int nBlockY;
                std::vector<GByte> osData;
                bool bIsTarget;
                bool bIsMissing;
                bool bValid = false;
            };

            const int nChunks = static_cast<int>(aoMissingChunks.size());
            std::vector<DecompressedChunk> aoOutputs(nChunks);

            int nNumThreads = std::thread::hardware_concurrency();
            if (nNumThreads <= 0) nNumThreads = 4;
            const char* pszNumThreads = CPLGetConfigOption("GDAL_NUM_THREADS", nullptr);
            if (pszNumThreads && !EQUAL(pszNumThreads, "ALL_CPUS")) {
                nNumThreads = atoi(pszNumThreads);
                if (nNumThreads <= 0) nNumThreads = 1;
            }
            int nThreadsToUse = std::min(nNumThreads, nChunks);

            std::vector<std::thread> workers;
            for (int t = 0; t < nThreadsToUse; ++t) {
                workers.emplace_back([this, t, nThreadsToUse, nChunks, &aoMissingChunks, pMegaBuffer, &apData, nMinOffset, nExpectedBytes, bIsMegaFetch, nBlockXOff, nBlockYOff, &aoOutputs, &bSuccess]() {
                    
                    for (int i = t; i < nChunks; i += nThreadsToUse) {
                        auto& chunk = aoMissingChunks[i];
                        auto& outChunk = aoOutputs[i];

                        outChunk.nBlockX = chunk.nBlockX;
                        outChunk.nBlockY = chunk.nBlockY;
                        outChunk.bIsMissing = chunk.bIsMissing;
                        outChunk.bIsTarget = (chunk.nBlockX == nBlockXOff && chunk.nBlockY == nBlockYOff);
                        
                        // Allocate dedicated memory array isolated inside this specific worker thread
                        outChunk.osData.resize(nExpectedBytes);

                        if (chunk.bIsMissing) {
                            memset(outChunk.osData.data(), 0, nExpectedBytes);
                            outChunk.bValid = true;
                        } else {
                            const GByte* pSrcBytes = nullptr;
                            if (bIsMegaFetch) {
                                size_t nChunkOffsetInBuffer = static_cast<size_t>(chunk.nOffset - nMinOffset);
                                pSrcBytes = static_cast<const GByte*>(pMegaBuffer) + nChunkOffsetInBuffer;
                            } else {
                                size_t validIdx = 0;
                                for(int k = 0; k < i; ++k) if(!aoMissingChunks[k].bIsMissing) validIdx++;
                                pSrcBytes = static_cast<const GByte*>(apData[validIdx]);
                            }
                            
                            // Safe SIMD Decompression executes completely in isolated thread memory spaces
                            bool bProcessSuccess = ProcessAndCopyChunk(pSrcBytes, chunk.nLength, outChunk.osData.data());
                            if (bProcessSuccess) {
                                outChunk.bValid = true;
                            } else {
                                memset(outChunk.osData.data(), 0, nExpectedBytes);
                                bSuccess = false; 
                            }
                        }
                    }
                });
            }

            // Sync all hardware worker threads
            for (auto& worker : workers) {
                if (worker.joinable()) worker.join();
            }

            // SAFE SINGLE-THREADED INJECTION INTO GDAL BLOCK CACHE
            // Running this on the main thread guarantees complete thread safety for GDAL
            for (int i = 0; i < nChunks; ++i) {
                auto& outChunk = aoOutputs[i];
                if (!outChunk.bValid) continue;

                if (outChunk.bIsTarget) {
                    // Direct delivery to GDAL application buffer
                    memcpy(pImage, outChunk.osData.data(), nExpectedBytes);
                } else {
                    // Neighborhood blocks are safely integrated into the cache sequentially
                    GDALRasterBlock* poBlock = GetLockedBlockRef(outChunk.nBlockX, outChunk.nBlockY, 1);
                    if (poBlock) {
                        memcpy(poBlock->GetDataRef(), outChunk.osData.data(), nExpectedBytes);
                        poBlock->DropLock();
                    }
                }
            }

            // Clean up original download memory allocations safely
            if (bIsMegaFetch) {
                CPLFree(pMegaBuffer);
            } else {
                for (void* p : apData) if (p) CPLFree(p);
            }
            
            return bSuccess ? CE_None : CE_Failure;
        }
    }
    
    memset(pImage, 0, static_cast<size_t>(nBlockXSize) * nBlockYSize * GDALGetDataTypeSizeBytes(eDataType));
    return CE_None;
}
// ====================================================================
// NisarHDF5MaskBand Implementation
// ====================================================================

NisarHDF5MaskBand::NisarHDF5MaskBand(NisarDataset* poDSIn, hid_t hMaskDS, NisarMaskType eType) :
    m_hMaskDS(hMaskDS),
    m_hMaskFileSpaceID(-1),
    m_eType(eType)
{
    this->poDS = poDSIn;
    this->nBand = 0; // Mask bands usually have nBand=0
    this->eDataType = GDT_Byte;
 
    // CACHE THE DATASPACE ONCE
    if (m_hMaskDS >= 0)
    {
        m_hMaskFileSpaceID = H5Dget_space(m_hMaskDS);
    }

    // Copy dimensions/blocking from the parent dataset
    this->nRasterXSize = poDSIn->GetRasterXSize();
    this->nRasterYSize = poDSIn->GetRasterYSize();
    
    // Get chunk size from HDF5 to optimize I/O
    hid_t hDAPL = H5Dget_create_plist(m_hMaskDS);
    if (H5Pget_layout(hDAPL) == H5D_CHUNKED) {
        hsize_t chunk_dims[2];
        H5Pget_chunk(hDAPL, 2, chunk_dims);
        this->nBlockXSize = static_cast<int>(chunk_dims[1]); // X is last dim
        this->nBlockYSize = static_cast<int>(chunk_dims[0]);
    } else {
        // Fallback if not chunked (unlikely for L2)
        this->nBlockXSize = this->nRasterXSize;
        this->nBlockYSize = 1;
    }
    H5Pclose(hDAPL);
}
// --------------------------------------------------------------------
// Statistics Overrides (Prevents Application from scanning the whole file)
// --------------------------------------------------------------------

CPLErr NisarRasterBand::GetStatistics(int bApproxOK, int bForce,
                                      double *pdfMin, double *pdfMax,
                                      double *pdfMean, double *pdfStdDev)
{
    // If Application allows approximations OR Data Products Producers provided the exact bounds
    if (bApproxOK || m_bHasMinMax) {
        
        // Grab the strings parsed by nisardataset.cpp
        const char* pszMin = poDS->GetMetadataItem("min_value");
        const char* pszMax = poDS->GetMetadataItem("max_value");
        
        // Fallback to valid_min/max if the calculated bounds aren't there
        if (!pszMin) pszMin = poDS->GetMetadataItem("valid_min");
        if (!pszMax) pszMax = poDS->GetMetadataItem("valid_max");

        // Convert the string to double (CPLAtof handles scientific notation perfectly)
        double dfMin = pszMin ? CPLAtof(pszMin) : 0.0;
        double dfMax = pszMax ? CPLAtof(pszMax) : 5.0; // Fallback radar max

        if (pdfMin) *pdfMin = dfMin;
        if (pdfMax) *pdfMax = dfMax;
        
        if (pdfMean) {
            const char* pszMean = poDS->GetMetadataItem("mean_value");
            *pdfMean = pszMean ? CPLAtof(pszMean) : (dfMax + dfMin) / 2.0; 
        }
        if (pdfStdDev) {
            const char* pszStdDev = poDS->GetMetadataItem("sample_stddev");
            *pdfStdDev = pszStdDev ? CPLAtof(pszStdDev) : (dfMax - dfMin) / 6.0; 
        }
        
        return CE_None;
    }

    // Deep scan fallback
    return GDALPamRasterBand::GetStatistics(bApproxOK, bForce, pdfMin, pdfMax, pdfMean, pdfStdDev);
}

// And the Min/Max overrides just call GetStatistics to avoid duplicating the CPLAtof logic!
double NisarRasterBand::GetMinimum(int* pbSuccess) {
    double dfMin = 0.0;
    GetStatistics(TRUE, FALSE, &dfMin, nullptr, nullptr, nullptr);
    if (pbSuccess) *pbSuccess = TRUE;
    return dfMin;
}

double NisarRasterBand::GetMaximum(int* pbSuccess) {
    double dfMax = 5.0;
    GetStatistics(TRUE, FALSE, nullptr, &dfMax, nullptr, nullptr);
    if (pbSuccess) *pbSuccess = TRUE;
    return dfMax;
}

NisarHDF5MaskBand::~NisarHDF5MaskBand()
{
    // CLEANLY RELEASE CACHED SPACE
    if (m_hMaskFileSpaceID >= 0)
    {
        H5Sclose(m_hMaskFileSpaceID);
        m_hMaskFileSpaceID = -1;
    }

    if (m_hMaskDS >= 0) {
        H5Dclose(m_hMaskDS);
    }
}
CPLErr NisarHDF5MaskBand::IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage)
{
    if (m_hMaskDS < 0 || m_hMaskFileSpaceID < 0) return CE_Failure;

    // Pre-fill the GDAL buffer with 0 (Invalid) to handle partial block padding
    int nFullBlockPixels = nBlockXSize * nBlockYSize;
    memset(pImage, 0, nFullBlockPixels);

    // Calculate offsets for HDF5 Hyperslab
    hsize_t offset[2] = {
        static_cast<hsize_t>(nBlockYOff) * nBlockYSize,
        static_cast<hsize_t>(nBlockXOff) * nBlockXSize
    };

    // Handle edge blocks
    int nRequestX = nBlockXSize;
    int nRequestY = nBlockYSize;

    if (offset[0] + nRequestY > static_cast<hsize_t>(nRasterYSize))
        nRequestY = nRasterYSize - offset[0];
    if (offset[1] + nRequestX > static_cast<hsize_t>(nRasterXSize))
        nRequestX = nRasterXSize - offset[1];

    hsize_t count[2] = { static_cast<hsize_t>(nRequestY), static_cast<hsize_t>(nRequestX) };

    // Create a memory space matching GDAL's full block dimensions
    hsize_t mem_dims[2] = { static_cast<hsize_t>(nBlockYSize), static_cast<hsize_t>(nBlockXSize) };
    hid_t hMemSpace = H5Screate_simple(2, mem_dims, nullptr);

    // If it's a partial block, select a hyperslab in the memory space
    if (nRequestX < nBlockXSize || nRequestY < nBlockYSize) {
        hsize_t mem_start[2] = {0, 0};
        H5Sselect_hyperslab(hMemSpace, H5S_SELECT_SET, mem_start, nullptr, count, nullptr);
    }

    hid_t hFileSpace = H5Scopy(m_hMaskFileSpaceID);
    H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count, nullptr);

    // Read into the buffer provided by GDAL
    GByte* pbyBuffer = static_cast<GByte*>(pImage);

    // INSTRUMENTATION START
    auto t_start = std::chrono::high_resolution_clock::now();

    herr_t status = H5Dread(m_hMaskDS, H5T_NATIVE_UINT8, hMemSpace, hFileSpace, H5P_DEFAULT, pbyBuffer);

    auto t_end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> t_diff = t_end - t_start;

    CPLDebug("NISAR_MASK_PERF", 
             "Mask H5Dread Block(X:%d, Y:%d) | RequestSize: %dx%d | Time: %.3f ms",
             nBlockXOff, nBlockYOff, nRequestX, nRequestY, t_diff.count());
    // INSTRUMENTATION END

    H5Sclose(hFileSpace);
    H5Sclose(hMemSpace);

    if (status < 0) return CE_Failure;

        // LUT Optimization for Mask Iteration
        // Static initialization ensures the LUT is built only once per application lifecycle
        if (m_eType == NisarMaskType::GCOV)
        {
#if defined(__aarch64__) || defined(_M_ARM64)
            // NEON Vectorized Thresholding (16 pixels per cycle)
            uint8x16_t v_min = vdupq_n_u8(1);
            uint8x16_t v_max = vdupq_n_u8(5);
            int i = 0;
            for (; i + 15 < nFullBlockPixels; i += 16) {
                uint8x16_t pixels = vld1q_u8(pbyBuffer + i);
                
                // Compare: pixels >= 1  (Returns 0xFF or 0x00)
                uint8x16_t mask1 = vcgeq_u8(pixels, v_min);
                // Compare: pixels <= 5  (Returns 0xFF or 0x00)
                uint8x16_t mask2 = vcleq_u8(pixels, v_max);
                
                // Bitwise AND combines the conditions. The result is instantly our 255/0 mask!
                vst1q_u8(pbyBuffer + i, vandq_u8(mask1, mask2));
            }
            // Scalar Tail
            for (; i < nFullBlockPixels; i++) {
                pbyBuffer[i] = (pbyBuffer[i] >= 1 && pbyBuffer[i] <= 5) ? 255 : 0;
            }
#else
            static const auto gcovLUT = []() {
            std::vector<GByte> lut(256, 0);
            for (int i = 1; i <= 5; i++) lut[i] = 255;
                return lut;
            }();

            for (int i = 0; i < nFullBlockPixels; i++) {
                pbyBuffer[i] = gcovLUT[pbyBuffer[i]];
            }
#endif
        }
        else if (m_eType == NisarMaskType::GUNW)
        {
            static const auto gunwLUT = []() {
                std::vector<GByte> lut(256, 0);
                for (int v = 1; v < 255; v++) {
                    int nRefSubswath = (v / 10) % 10;
                    int nSecSubswath = v % 10;
                    lut[v] = (nRefSubswath > 0 && nSecSubswath > 0) ? 255 : 0;
                }
                return lut;
                }();

            for (int i = 0; i < nFullBlockPixels; i++) {
                pbyBuffer[i] = gunwLUT[pbyBuffer[i]];
            }
        }
        return CE_None;
}
bool NisarRasterBand::WriteVirtualZarrSidecar(
    const std::string& osS3Url, 
    const std::string& osZarrGroupPath, // e.g., "science/LSAR/GCOV/grids/frequencyA/HHHH"
    const std::vector<NisarChunkInfo>& aoChunks,
    const std::string& osOutJsonPath) 
{
    // 1. Initialize the Root Object and Reference Dictionary
    CPLJSONObject oRoot;
    oRoot.Add("version", 1);
    
    CPLJSONObject oRefs;
    oRoot.Add("refs", oRefs);
    
    // Add the root Zarr group
    oRefs.Add(".zgroup", "{\"zarr_format\": 2}");
    
    // 2. Build the .zarray Metadata Dictionary
    CPLJSONObject oZarray;
    oZarray.Add("order", "C"); // Define C-contiguous memory layout
    oZarray.Add("zarr_format", 2);
    
    // Map GDAL eDataType to Zarr Format 2 Typestrings
    std::string osDtype; 
    switch(eDataType) {
        case GDT_Byte:     osDtype = "|u1"; break;
        case GDT_Int16:    osDtype = "<i2"; break;
        case GDT_UInt16:   osDtype = "<u2"; break;
        case GDT_Int32:    osDtype = "<i4"; break;
        case GDT_UInt32:   osDtype = "<u4"; break;
        case GDT_Int64:    osDtype = "<i8"; break;
        case GDT_UInt64:   osDtype = "<u8"; break;
        case GDT_Float32:  osDtype = "<f4"; break;
        case GDT_Float64:  osDtype = "<f8"; break;
        case GDT_CFloat32: osDtype = "<c8"; break; 
        case GDT_CFloat64: osDtype = "<c16"; break;
        default:           osDtype = "|V1"; break;
    }
    oZarray.Add("dtype", osDtype);

    CPLJSONArray oShape;
    oShape.Add(nRasterYSize); 
    oShape.Add(nRasterXSize);
    oZarray.Add("shape", oShape);
    
    CPLJSONArray oZChunks;
    oZChunks.Add(nBlockYSize); 
    oZChunks.Add(nBlockXSize);
    oZarray.Add("chunks", oZChunks);

    // Map the NoData Value to Zarr's fill_value
    int bHasNoData = 0;
    double dfNoData = GetNoDataValue(&bHasNoData);
    if (bHasNoData && std::isnan(dfNoData)) {
        oZarray.Add("fill_value", "NaN"); 
    } else if (bHasNoData) {
        oZarray.Add("fill_value", dfNoData);
    } else {
        oZarray.Add("fill_value", 0.0);
    }

    // 3. Map HDF5 Filters to Zarr Compressors
    if (m_bIsDeflated) {
        CPLJSONObject oCompressor;
        oCompressor.Add("id", "zlib");
        oCompressor.Add("level", m_nDeflateLevel); 
        oZarray.Add("compressor", oCompressor);
    } else {
        oZarray.AddNull("compressor");
    }

    if (m_bIsShuffled) {
        CPLJSONArray oFilters;
        CPLJSONObject oShuffle;
        oShuffle.Add("id", "shuffle");
        oShuffle.Add("elementsize", GDALGetDataTypeSizeBytes(eDataType));
        oFilters.Add(oShuffle);
        oZarray.Add("filters", oFilters);
    }

    // Convert the metadata sub-object to its internal string format
    std::string osZarrayStr = oZarray.Format(CPLJSONObject::PrettyFormat::Plain);
    
    // Clean up leading slash if present in dataset path
    std::string osCleanPath = osZarrGroupPath;
    if (osCleanPath.front() == '/') osCleanPath = osCleanPath.substr(1);
    
    // WORKAROUND STEP 1: Replace forward slashes with pipes to bypass CPLJSONObject's automatic nesting
    std::replace(osCleanPath.begin(), osCleanPath.end(), '/', '|');
    
    // Add the flat metadata record
    oRefs.Add(osCleanPath + "|.zarray", osZarrayStr);
 
    // Add the Xarray dimensions metadata
    CPLJSONObject oZattrs;
    CPLJSONArray oDimNames;
    oDimNames.Add("y");
    oDimNames.Add("x");
    oZattrs.Add("_ARRAY_DIMENSIONS", oDimNames);
    
    std::string osZattrsStr = oZattrs.Format(CPLJSONObject::PrettyFormat::Plain);
    oRefs.Add(osCleanPath + "|.zattrs", osZattrsStr);
    
    // 4. Iterate over your parsed B-Tree chunks and write the references
    for (const auto& chunk : aoChunks) 
    {
        if (chunk.bIsMissing) continue; 
        
        // Build the temporary flat string key using pipe notations
        std::string osChunkKey = CPLSPrintf("%s|%d.%d", osCleanPath.c_str(), chunk.nBlockY, chunk.nBlockX);
        
        CPLJSONArray oChunkRef;
        oChunkRef.Add(osS3Url);
        oChunkRef.Add(static_cast<GIntBig>(chunk.nOffset));
        oChunkRef.Add(static_cast<GIntBig>(chunk.nLength)); 
        
        oRefs.Add(osChunkKey, oChunkRef);
    }
    
    // 5. Serialize and transform the final JSON string
    std::string osFinalJson = oRoot.Format(CPLJSONObject::PrettyFormat::Pretty);
    
    // WORKAROUND STEP 2: Revert all temporary pipe markers back to true URL forward slashes
    std::replace(osFinalJson.begin(), osFinalJson.end(), '|', '/');
    
    // 6. Output to the filesystem
    VSILFILE* fp = VSIFOpenL(osOutJsonPath.c_str(), "wb");
    if (!fp) return false;
    VSIFWriteL(osFinalJson.data(), 1, osFinalJson.size(), fp);
    VSIFCloseL(fp);
    
    return true;
}
