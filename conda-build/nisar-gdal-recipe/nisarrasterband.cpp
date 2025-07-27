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

#include "nisarrasterband.h"
#include "nisardataset.h"
#include "hdf5.h"
#include "gdal.h"        // For CE_Failure, CE_None, GDALDataType
#include "cpl_conv.h"
#include <algorithm>   // For std::min
#include <vector>      // For std::vector
#include <cstring>     // For memset
#include <mutex>       // For std::lock_guard
#include <iomanip>     // for std::setprecision

// TODO:Move Static Helpers to nisar_priv.h to avoid code duplication
// --- Static Attribute Callback (Reads value, adds NAME=VALUE to list) ---
// (Same implementation as before - reads scalar string/int/float)
struct NISAR_AttrCallbackData {
    char ***ppapszList; // Pointer to the CSL list pointer being built
};
static herr_t NISAR_AttributeCallback(hid_t loc_id, const char *attr_name,
                                      const H5A_info_t * /*ainfo*/, void *op_data)
{
    NISAR_AttrCallbackData *data = static_cast<NISAR_AttrCallbackData*>(op_data);
    hid_t attr_id = -1;
    hid_t attr_type = -1;
    hid_t attr_space = -1;
    // hid_t mem_type = -1; // No longer needed for strings here
    std::string value_str;
    herr_t status = -1;
    bool bValueSet = false;

    attr_id = H5Aopen_by_name(loc_id, ".", attr_name, H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0) return 0; // Skip attribute on open failure

    attr_type = H5Aget_type(attr_id);
    if (attr_type < 0) { H5Aclose(attr_id); return 0; }

    attr_space = H5Aget_space(attr_id);
    if (attr_space < 0) { H5Tclose(attr_type); H5Aclose(attr_id); return 0; }

    H5T_class_t type_class = H5Tget_class(attr_type);
    hssize_t n_points = H5Sget_simple_extent_npoints(attr_space);
    size_t type_size = H5Tget_size(attr_type); // Size in bytes for fixed types

    if (n_points <= 0) {
        value_str = "(empty attribute)";
        bValueSet = true;
    } else if (type_class == H5T_STRING) {
        // --- Corrected String Handling ---
        char *pszReadVL = nullptr;
        char *pszReadFixed = nullptr;
        bool bIsVariable = H5Tis_variable_str(attr_type);

        if(bIsVariable) {
            // For VLEN strings, read using the FILE type (attr_type).
            // H5Aread will allocate memory pointed to by &pszReadVL.
            status = H5Aread(attr_id, attr_type, &pszReadVL); // Pass attr_type!
            if(status >= 0 && pszReadVL != nullptr) {
                 value_str = pszReadVL; // Copy content
                 bValueSet = true;
                 // Free using H5Dvlen_reclaim which requires type and space
                 // Make sure dataspace is scalar for single VLEN string attribute
                 if (H5Sget_simple_extent_type(attr_space) == H5S_SCALAR) {
                      status = H5Dvlen_reclaim(attr_type, attr_space, H5P_DEFAULT, &pszReadVL);
                      if (status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "H5Dvlen_reclaim failed for VLEN string attr '%s'.", attr_name); }
                 } else {
                      CPLError(CE_Warning, CPLE_AppDefined, "VLEN string attribute '%s' does not have scalar dataspace? Cannot reclaim reliably.", attr_name);
                      H5free_memory(pszReadVL); // Use H5free_memory as fallback if reclaim fails/inapplicable
                 }
                 pszReadVL = nullptr;
            } else { CPLError(CE_Warning, CPLE_FileIO, "Failed read VLEN string attr '%s'", attr_name); }
        } else if (type_size > 0 && n_points == 1) { // Fixed length string (scalar)
            pszReadFixed = (char *)VSIMalloc(type_size + 1); // Use VSI Alloc
            if (pszReadFixed) {
                 // Read using the file type (attr_type) into pre-allocated buffer
                 status = H5Aread(attr_id, attr_type, pszReadFixed);
                 if (status >= 0) {
                      // Ensure null termination based on HDF5 padding rules (or manually)
                      // H5T_STR_NULLTERM padding (default usually) means it should be there if size allows
                      // Manual termination is safest:
                      pszReadFixed[type_size] = '\0';
                      value_str = pszReadFixed;
                      bValueSet = true;
                 } else { CPLError(CE_Warning, CPLE_FileIO, "Failed read fixed string attr '%s'", attr_name); }
                 VSIFree(pszReadFixed); pszReadFixed = nullptr;
            } else { CPLError(CE_Failure, CPLE_OutOfMemory, "Malloc failed for fixed string attr '%s'", attr_name); }
        } else if (type_size > 0 && n_points > 1) { // Array of fixed strings
             value_str = CPLSPrintf("(array of fixed strings size %lld)", (long long)n_points);
             bValueSet = true;
             // TODO: Implement reading array of fixed strings if needed
        }
        // --- End Corrected String Handling ---

    } else if (type_class == H5T_INTEGER && n_points == 1) {
        // (Integer reading logic as before - seems OK)
        long long llVal = 0; status = H5Aread(attr_id, H5T_NATIVE_LLONG, &llVal); if (status >= 0) { value_str = CPLSPrintf("%lld", llVal); bValueSet = true; } else { CPLError(CE_Warning, CPLE_FileIO, "Failed read integer attr '%s'", attr_name); }
    } else if (type_class == H5T_FLOAT && n_points == 1) {
        // (Float reading logic as before - seems OK, uses ostringstream now)
         double dfVal = 0.0; status = H5Aread(attr_id, H5T_NATIVE_DOUBLE, &dfVal); if (status >= 0) { std::ostringstream oss; oss << std::scientific << std::setprecision(std::numeric_limits<double>::max_digits10); oss << dfVal; value_str = oss.str(); if (value_str == "nan" || value_str == "-nan") value_str = "nan"; else if (value_str == "inf") value_str = "inf"; else if (value_str == "-inf") value_str = "-inf"; bValueSet = true; } else { CPLError(CE_Warning, CPLE_FileIO, "Failed read float attr '%s'", attr_name); }
    } else if (n_points > 1) { // Handle other arrays
        // (Object Reference handling as before)
         if (type_class == H5T_REFERENCE || (type_class == H5T_VLEN && H5Tequal(H5Tget_super(attr_type), H5T_STD_REF_OBJ))) { value_str = CPLSPrintf("(object reference list size %lld)", (long long)n_points); }
         else { value_str = CPLSPrintf("(array attribute type %d size %lld)", type_class, (long long)n_points); }
        bValueSet = true;
    } else { // Other unhandled scalar types
        value_str = CPLSPrintf("(unhandled scalar type class %d)", type_class);
        bValueSet = true;
    }

    // Add NAME=VALUE to CSL list if value was obtained
    if (bValueSet) {
        *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), attr_name, value_str.c_str());
    } else { // Should only happen if H5Aread failed AND didn't log/set value_str
        *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), attr_name, "(Error reading attribute)");
    }

// Cleanup label
attr_cleanup:
    if (attr_type >= 0) H5Tclose(attr_type);
    if (attr_space >= 0) H5Sclose(attr_space);
    if (attr_id >= 0) H5Aclose(attr_id);
    return 0; // Continue H5Aiterate iteration
}

/************************************************************************/
/* ==================================================================== */
/*                            NisarRasterBand                         */
/* ==================================================================== */
/* This class inherits from GDALPamRasterBand and represents a single band */
/* within the NISAR dataset. It includes methods for reading           */
/* data blocks (IReadBlock) and retrieving NoData values (GetNoDataValue). */
/************************************************************************/


/************************************************************************/
/*                           NisarRasterBand()                         */
/************************************************************************/

NisarRasterBand::NisarRasterBand( NisarDataset *poDSIn, int nBandIn ) :
      GDALPamRasterBand(),  //Call base class construction
      hH5Type(-1),          //Initialize custom member
      m_bMetadataRead(false)// <<< Initialize metadata flag
      // m_MetadataMutex is default constructed
{
    // poDS and nBand member variables are inherited from GDALRasterBand
    // and should be automatically set by GDAL when poDS->SetBand() is called.
    // We use poDSIn and nBandIn here for initialization logic specific to this band.

    // --- Basic Member Initialization ---
    this->poDS = poDSIn;   // Store dataset pointer (inherited member)
    this->nBand = nBandIn; // Store band number (inherited member)

    // --- Validate Parent Dataset ---
    if (!this->poDS) { // Check the inherited poDS pointer after base constructor
        CPLError(CE_Fatal, CPLE_AppDefined, "NisarRasterBand constructor: Parent dataset pointer is NULL.");
        // Cannot proceed with initialization. Mark as invalid.
        this->eDataType = GDT_Unknown;
        this->nBlockXSize = 0;
        this->nBlockYSize = 0;
        this->hH5Type = -1;
        return; // Exit constructor in error state
    }

    NisarDataset *poGDS = reinterpret_cast<NisarDataset *>( this->poDS );


    // Set GDAL Data Type for this band
    // This relies on NisarDataset::Open having correctly set eDataType on poGDS
    if (poGDS->eDataType == GDT_Unknown) {
        CPLError(CE_Warning, CPLE_AppDefined, "NisarRasterBand constructor Band %d: Parent dataset has Unknown GDALDataType.", nBandIn);
    }
    this->eDataType = poGDS->eDataType; // Set inherited member


    // --- Get HDF5 Dataset Handle ---
    hid_t hDatasetID = poGDS->GetDatasetHandle(); // Use getter


    //  Get and Store HDF5 Native Data Type Handle
    if (hDatasetID >= 0) {
         this->hH5Type = H5Dget_type(hDatasetID); // Get a *copy* of the type
         if (this->hH5Type < 0) {
             CPLError(CE_Warning, CPLE_AppDefined, "NisarRasterBand %d: Failed to get HDF5 native datatype handle.", nBandIn);
             // Leave hH5Type as -1
         } else {
              CPLDebug("NISAR_Band", "Band %d: Stored HDF5 native data type handle.", nBandIn);
         }
    } else {
        CPLError(CE_Warning, CPLE_AppDefined, "NisarRasterBand %d: Cannot get HDF5 type, parent dataset handle is invalid.", nBandIn);
         this->hH5Type = -1; // Ensure it's marked invalid
    }


    // Determine Block Size (from HDF5 chunking)
    // Initialize inherited block size members with defaults
    this->nBlockXSize = 512;
    this->nBlockYSize = 512;

    if (hDatasetID >= 0) {
        hid_t dcpl_id = H5Dget_create_plist(hDatasetID);
        if (dcpl_id >= 0) {
            if (H5Pget_layout(dcpl_id) == H5D_CHUNKED) {
                CPLDebug("NISAR_Band", "H5D_CHUNKED is TRUE");
                hid_t dspace_id = H5Dget_space(hDatasetID);
                int rank = -1;
                if(dspace_id >= 0) {
                    rank = H5Sget_simple_extent_ndims(dspace_id);
                    H5Sclose(dspace_id);
                }

                if (rank >= 2) {
                     std::vector<hsize_t> chunk_dims(rank);
                     int chunk_rank = H5Pget_chunk(dcpl_id, rank, chunk_dims.data());

                     if (chunk_rank == rank) {
                         int chunkX = static_cast<int>(chunk_dims[rank - 1]);
                         int chunkY = static_cast<int>(chunk_dims[rank - 2]);
                         if (chunkX > 0 && chunkY > 0) {
                             this->nBlockXSize = chunkX; // Set member variable
                             this->nBlockYSize = chunkY; // Set member variable
                             CPLDebug("NISAR_Band", "Band %d: Using HDF5 chunk size for block size: %d x %d", nBandIn, nBlockXSize, nBlockYSize);
                         } else { /* Log warning, use default */ }
                     } else { /* Log warning, use default */ }
                } else { /* Log warning, use default */ }
            } else { // Not chunked
                 CPLDebug("NISAR_Band", "Band %d: Dataset not chunked. Using default block size %d x %d.", nBandIn, nBlockXSize, nBlockYSize);
            }
            H5Pclose(dcpl_id);
        } else { /* Log warning, use default */ }
    } else { /* Log debug, use default */ }

    // Ensure block sizes are positive
    if (this->nBlockXSize <= 0) this->nBlockXSize = 512;
    if (this->nBlockYSize <= 0) this->nBlockYSize = 512;

    // Log final determined block size
    CPLDebug("NISAR_Band", "Band %d: Final Block Size set to %d x %d", nBandIn, this->nBlockXSize, this->nBlockYSize);

    // Note: nRasterXSize and nRasterYSize members are set by GDAL core mechanism
    // when NisarDataset::Open calls poDS->SetBand( i, poBand );
}

NisarRasterBand::~NisarRasterBand()
{
    if (hH5Type >= 0) {
        if (H5Tclose(hH5Type) < 0) {
             CPLError(CE_Warning, CPLE_AppDefined, "Failed to close HDF5 data type handle in ~NisarRasterBand.");
        }
    }
}
// NisarRasterBand::GetMetadata
char **NisarRasterBand::GetMetadata( const char *pszDomain )
{
    // 1. Handle non-default domains via PAM first
    if (pszDomain != nullptr && !EQUAL(pszDomain, "")) {
         // Ensure PAM system is initialized for this band
         return GDALPamRasterBand::GetMetadata(pszDomain);
    }

    // 2. Handle default domain ("" or nullptr) with caching
    { // Scope for lock guard
        std::lock_guard<std::mutex> lock(m_MetadataMutex);

        // Check cache flag: Have we already read HDF5 attributes for this band?
        if (!m_bMetadataRead)
        {
            // No, read HDF5 attributes for the first time
            m_bMetadataRead = true; // Mark as attempted
            CPLDebug("NISAR_Band", "Band %d: Reading HDF5 attributes for default metadata domain.", nBand);

            NisarDataset *poGDS = reinterpret_cast<NisarDataset *>( this->poDS );
            hid_t hDatasetID = poGDS ? poGDS->GetDatasetHandle() : -1;

            if (hDatasetID >= 0) {
                // Build HDF5 attributes into a temporary list
                char **papszHDFMeta = nullptr;
                NISAR_AttrCallbackData callback_data;
                callback_data.ppapszList = &papszHDFMeta;
                hsize_t idx = 0;

                // Iterate attributes directly on this band's dataset handle
                H5Aiterate2(hDatasetID, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                            NISAR_AttributeCallback, &callback_data);

                if (papszHDFMeta != nullptr) {
                     // SetMetadata will merge papszHDFMeta with the internal PAM list
                     // for the default domain.
                     SetMetadata(papszHDFMeta);

                     // We destroy the temporary list structure itself.
                     CSLDestroy(papszHDFMeta);
                     papszHDFMeta = nullptr; // Mark as destroyed
                }
            } else {
                 CPLError(CE_Warning, CPLE_AppDefined, "Band %d: Cannot read HDF5 metadata, dataset handle invalid.", nBand);
            }
        } // End if !m_bMetadataRead
    } // Mutex unlocked here

    // Always return the (potentially updated) metadata list managed by PAM
    return GDALPamRasterBand::GetMetadata(pszDomain);
}
/***************************************************************************/
/*                             IReadBlock()                                */
/* This method reads a block of data from the HDF5 dataset.                */
/* TBD: implement actual data reading logic using HDF5 API calls (H5Dread) */
/***************************************************************************/

CPLErr NisarRasterBand::IReadBlock( int nBlockXOff, int nBlockYOff,
                                      void * pImage )
{
   // Cast parent dataset pointer (poDS is inherited from GDALRasterBand)
    NisarDataset *poGDS = reinterpret_cast<NisarDataset *>( this->poDS );

    // Initialize handles and return status
    hid_t hDatasetID = -1;
    hid_t hFileSpace = -1;
    hid_t hMemSpace = -1;
    herr_t status = -1; // HDF5 return status

    // --- Declare variables near the top ---
    int rank = -1;
    std::vector<hsize_t> start;
    std::vector<hsize_t> count;
    //std::vector<hsize_t> file_dims;
    int nActualBlockXSize = 0;
    int nActualBlockYSize = 0;
    hsize_t mem_dims[2] = {0, 0}; // <<< Declare and initialize mem_dims HERE

    // Validate essential inputs and handles
    // Checks for poGDS, hDatasetID, hH5Type as before
    if (!poGDS) { CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: Parent dataset pointer is NULL."); return CE_Failure; }
    hDatasetID = poGDS->GetDatasetHandle();
    if( hDatasetID < 0 ) { CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: Invalid HDF5 Dataset handle from parent."); return CE_Failure; }
    if( this->hH5Type < 0 ) { CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: Invalid HDF5 Datatype handle for band %d.", this->nBand); return CE_Failure; }

    // Get raster/block dimensions from member variables
    nRasterXSize = this->nRasterXSize;
    nRasterYSize = this->nRasterYSize;

    CPLDebug("NISAR_IReadBlock", "Band %d: Reading block (%d, %d), BlockSize %dx%d, RasterSize %dx%d",
             this->nBand, nBlockXOff, nBlockYOff, nBlockXSize, nBlockYSize, nRasterXSize, nRasterYSize);

    // Calculate necessary sizes and check if block is completely outside
    hsize_t start_offset_x = static_cast<hsize_t>(nBlockXOff) * nBlockXSize;
    hsize_t start_offset_y = static_cast<hsize_t>(nBlockYOff) * nBlockYSize;
    const size_t nDataTypeSize = GDALGetDataTypeSizeBytes(this->eDataType);
    const size_t nFullBlockBytes = static_cast<size_t>(nBlockXSize) * nBlockYSize * nDataTypeSize;

    if ( start_offset_x >= static_cast<hsize_t>(nRasterXSize) ||
         start_offset_y >= static_cast<hsize_t>(nRasterYSize) ||
         nDataTypeSize == 0 ) // Also check data type size is valid
    {
        CPLDebug("NISAR_IReadBlock", "Block (%d, %d) is outside bounds or data type size is zero. Filling buffer with 0.",
                 nBlockXOff, nBlockYOff );
        if (nFullBlockBytes > 0) memset(pImage, 0, nFullBlockBytes);
        goto success_cleanup; // Not an error
    }

    // Calculate actual block sizes to read (handles partial blocks)
    nActualBlockXSize = std::min(nBlockXSize, nRasterXSize - static_cast<int>(start_offset_x));
    nActualBlockYSize = std::min(nBlockYSize, nRasterYSize - static_cast<int>(start_offset_y));

    if( nActualBlockXSize <= 0 || nActualBlockYSize <= 0) {
         CPLDebug("NISAR_IReadBlock", "Calculated block read size is zero or negative (%d x %d). Filling buffer with 0.",
                  nActualBlockXSize, nActualBlockYSize);
         if (nFullBlockBytes > 0) memset(pImage, 0, nFullBlockBytes);
         goto success_cleanup;  // Not an error
    }

    // Prepare HDF5 Dataspaces
    hFileSpace = H5Dget_space(hDatasetID);
    if( hFileSpace < 0 ) { /* CPLError */ goto error_cleanup; }
    rank = H5Sget_simple_extent_ndims(hFileSpace);
    if( rank < 2 ) { /* CPLError */ goto error_cleanup; }

    start.resize(rank);
    count.resize(rank);
    start[rank - 1] = start_offset_x; start[rank - 2] = start_offset_y;
    count[rank - 1] = nActualBlockXSize; count[rank - 2] = nActualBlockYSize;
    if (rank > 2) { std::vector<hsize_t> file_dims(rank); if(H5Sget_simple_extent_dims(hFileSpace, file_dims.data(), nullptr)<0) { /* CPLError */ goto error_cleanup;} for(int i=0;i<rank-2;++i){ start[i]=0; count[i]=file_dims[i];} }

    status = H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, start.data(), NULL, count.data(), NULL);
    if( status < 0 ) { /* CPLError */ goto error_cleanup; }

    mem_dims[0] = static_cast<hsize_t>(nActualBlockYSize); // Assign to first element (Y dim)
    mem_dims[1] = static_cast<hsize_t>(nActualBlockXSize); // Assign to second element (X dim)
    hMemSpace = H5Screate_simple(2, mem_dims, NULL);
    if( hMemSpace < 0 ) { /* CPLError */ goto error_cleanup; }


    // DEBUGGING STEP: Pre-fill pImage buffer
    CPLDebug("NISAR_IReadBlock_DEBUG", "Pre-filling %dx%d block buffer (%lu bytes) with 0xAA pattern before H5Dread.",
             nBlockXSize, nBlockYSize, (unsigned long)nFullBlockBytes);
    memset(pImage, 0xAA, nFullBlockBytes);


    // Read the Data
    CPLDebug("NISAR_IReadBlock", "Band %d: Reading %dx%d hyperslab at offset %llu,%llu into buffer.",
             this->nBand, nActualBlockXSize, nActualBlockYSize,
             (unsigned long long)start[rank-1], (unsigned long long)start[rank-2]);

    status = H5Dread(hDatasetID, this->hH5Type, hMemSpace, hFileSpace, H5P_DEFAULT, pImage);

    if( status < 0 ) {
        CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: H5Dread failed for block %d, %d.", nBlockXOff, nBlockYOff);
        H5Eprint(H5E_DEFAULT, stderr);
        goto error_cleanup;
    }

    // Handle Partial Block Padding (Fill unused area with 0)
    if (nActualBlockXSize < nBlockXSize || nActualBlockYSize < nBlockYSize)
    {
        CPLDebug("NISAR_IReadBlock", "Band %d: Partial block read (%dx%d vs %dx%d). Padding unused buffer area with 0.",
                 this->nBand, nActualBlockXSize, nActualBlockYSize, nBlockXSize, nBlockYSize);

        GByte* pabyData = static_cast<GByte*>(pImage);
        const int nFillValue = 0; // Use integer 0 for memset

        // Pad columns to the right (if X is partial)
        if (nActualBlockXSize < nBlockXSize) {
            for(int iLine = 0; iLine < nActualBlockYSize; ++iLine) {
                GByte* pabyOffset = pabyData + (static_cast<size_t>(iLine) * nBlockXSize + nActualBlockXSize) * nDataTypeSize;
                size_t nBytesToPad = static_cast<size_t>(nBlockXSize - nActualBlockXSize) * nDataTypeSize;
                memset(pabyOffset, nFillValue, nBytesToPad);
            }
        }
        // Pad rows below (if Y is partial)
        if (nActualBlockYSize < nBlockYSize) {
            GByte* pabyOffset = pabyData + static_cast<size_t>(nActualBlockYSize) * nBlockXSize * nDataTypeSize;
            size_t nBytesToPad = static_cast<size_t>(nBlockYSize - nActualBlockYSize) * nBlockXSize * nDataTypeSize;
            memset(pabyOffset, nFillValue, nBytesToPad);
        }
    } // End partial block padding

// Cleanup and Return Success
success_cleanup:
    if(hMemSpace >= 0) H5Sclose(hMemSpace);
    if(hFileSpace >= 0) H5Sclose(hFileSpace);
    return CE_None;

// Cleanup and Return Failure
error_cleanup:
    if(hMemSpace >= 0) H5Sclose(hMemSpace);
    if(hFileSpace >= 0) H5Sclose(hFileSpace);
    return CE_Failure;
} // End IReadBlock

/************************************************************************/
/*                           GetNoDataValue()                           */
/* This method retrieves the NoData value from the HDF5 metadata if available.*/
/************************************************************************/

double NisarRasterBand::GetNoDataValue( int *pbSuccess )
{
    // TODO: Retrieve NoData value from HDF5 metadata if available
    if( pbSuccess )
        *pbSuccess = FALSE;

    return 0.0;
}
/*
char **NisarRasterBand::GetMetadata( const char * pszDomain )
{
    if( pszDomain != nullptr && !EQUAL(pszDomain, "") )
        return GDALRasterBand::GetMetadata(pszDomain);

    CPLStringList aosList;

    H5A_info_t attr_info;
    for (hsize_t i = 0; H5Aget_info(poDS->hDataset, i, &attr_info) >= 0; ++i) {
        hid_t hAttribute = H5Aopen(poDS->hDataset, attr_info.name, H5P_DEFAULT);
        if (hAttribute >= 0) {
            hid_t hDataType = H5Aget_type(hAttribute);
            size_t size = H5Tget_size(hDataType);
            if (H5Tget_class(hDataType) == H5T_STRING) {
                char *pszValue = new char[size + 1];
                herr_t hRet = H5Aread(hAttribute, hDataType, pszValue);
                if (hRet >= 0) {
                    pszValue[size] = '\0';
                    aosList.SetNameValue(attr_info.name, pszValue);
                }
                deletepszValue;
            } else if (H5Tget_class(hDataType) == H5T_INTEGER || H5Tget_class(hDataType) == H5T_FLOAT) {
                aosList.SetNameValue(attr_info.name, "(numeric)");
            }
            H5Aclose(hAttribute);
            H5Tclose(hDataType);
        }
    }

    return aosList.List();
}

GDALDataType NisarRasterBand::GetRasterDataType() const
{
    NisarDataset *poNisarDS = (NisarDataset *)poDS;
    hid_t hHDF5 = poNisarDS->hHDF5;
    GDALDataType eDataType = GDT_Unknown;
    herr_t status;
    hid_t hDataset = -1;
    hid_t hDataType = -1;

    // Construct the path to the specific band's dataset
    std::string datasetPath = "/science/LSAR/GSLC/grids/frequencyA/HH"; // Adjust based on band number or metadata

    hDataset = H5Dopen(hHDF5, datasetPath.c_str(), H5P_DEFAULT);
    if (hDataset < 0) {
        CPLError(CE_Warning, CPLE_OpenFailed, "Failed to open HDF5 dataset for band.");
        goto cleanup;
    }

    hDataType = H5Dget_type(hDataset);
    if (hDataType < 0) {
        CPLError(CE_Warning, CPLE_OpenFailed, "Failed to get HDF5 datatype for band.");
        goto cleanup;
    }

    H5T_class_t eHDF5Class = H5Tget_class(hDataType);
    size_t size = H5Tget_size(hDataType);

    if (eHDF5Class == H5T_FLOAT) {
        if (size == 4)
            eDataType = GDT_Float32;
        else if (size == 8)
            eDataType = GDT_Float64;
    } else if (eHDF5Class == H5T_INTEGER) {
        if (H5Tget_sign(hDataType) == H5T_SGN_NONE) { // Unsigned
            if (size == 1)
                eDataType = GDT_Byte;
            else if (size == 2)
                eDataType = GDT_UInt16;
            else if (size == 4)
                eDataType = GDT_UInt32;
            else if (size == 8)
                eDataType = GDT_UInt64;
        } else { // Signed
            if (size == 2)
                eDataType = GDT_Int16;
            else if (size == 4)
                eDataType = GDT_Int32;
            else if (size == 8)
                eDataType = GDT_Int64;
        }
    } else if (eHDF5Class == H5T_COMPOUND) {
        // Check for complex data type
        if (H5Tget_nmembers(hDataType) == 2) {
            hid_t hRealType = H5Tget_member_type(hDataType, 0);
            hid_t hImagType = H5Tget_member_type(hDataType, 1);
            if (H5Tequal(hRealType, hImagType) > 0) {
                size_t elem_size = H5Tget_size(hRealType);
                char *name1 = H5Tget_member_name(hDataType, 0);
                char *name2 = H5Tget_member_name(hDataType, 1);
                bool isReal = (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                bool isImaginary = (name2 && (name2[0] == 'i' || name2[0] == 'I'));
                H5free_memory(name1);
                H5free_memory(name2);

                if (isReal && isImaginary) {
                    if (H5Tequal(hRealType, H5T_NATIVE_FLOAT) > 0 && elem_size == 4)
                        eDataType = GDT_CFloat32;
                    else if (H5Tequal(hRealType, H5T_NATIVE_DOUBLE) > 0 && elem_size == 8)
                        eDataType = GDT_CFloat64;
                    else if (H5Tequal(hRealType, H5T_NATIVE_SHORT) > 0 && elem_size == 2)
                        eDataType = GDT_CInt16;
                    else if (H5Tequal(hRealType, H5T_NATIVE_INT) > 0 && elem_size == 4)
                        eDataType = GDT_CInt32;
                    // Add other complex types if needed (e.g., complex int64, complex float16)
                }
            }
            H5Tclose(hRealType);
            H5Tclose(hImagType);
        }
    }

cleanup:
    if (hDataType >= 0) H5Tclose(hDataType);
    if (hDataset >= 0) H5Dclose(hDataset);
    return eDataType;
}
*/
/*
GDALColorInterp NisarRasterBand::GetColorInterpretation()
{
    // For now, we'll just return undefined. You might need to add logic
    // to check for specific attributes or conventions in your NISAR files
    // to determine the color interpretation.
    return GCI_Undefined;
}
*/
