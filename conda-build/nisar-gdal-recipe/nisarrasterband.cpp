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
// Static Attribute Callback (Reads value, adds NAME=VALUE to list)
// reads scalar string/int/float
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
NisarRasterBand::NisarRasterBand( NisarDataset *poDSIn, int nBandIn ) :
      GDALPamRasterBand(),  //Call base class construction
      hH5Type(-1),          //Initialize custom member
      m_bMetadataRead(false)// <<< Initialize metadata flag
{
    this->poDS = poDSIn;
    this->nBand = nBandIn;

    if (!poDSIn)
    {
        CPLError(CE_Fatal, CPLE_AppDefined, "NisarRasterBand constructor: Parent dataset pointer is NULL.");
        this->eDataType = GDT_Unknown;
        return; // Exit constructor in an invalid state
    }

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
        if (H5Pget_layout(dcpl_id) == H5D_CHUNKED)
        {
            int rank = H5Sget_simple_extent_ndims(poGDS->hDataset);
            if (rank >= 2)
            {
                std::vector<hsize_t> chunk_dims(rank);
                if (H5Pget_chunk(dcpl_id, rank, chunk_dims.data()) == rank)
                {
                    this->nBlockXSize = static_cast<int>(chunk_dims[rank - 1]);
                    this->nBlockYSize = static_cast<int>(chunk_dims[rank - 2]);
                }
            }
        }
        H5Pclose(dcpl_id);
    }

    // Create and cache HDF5 dataspace handles
    m_hFileSpaceID = H5Dget_space(hDatasetID);

    hsize_t mem_dims[2] = {static_cast<hsize_t>(nBlockYSize),
                           static_cast<hsize_t>(nBlockXSize)};
    m_hMemSpaceID = H5Screate_simple(2, mem_dims, NULL);
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
}
// NisarRasterBand::GetMetadata
char **NisarRasterBand::GetMetadata( const char *pszDomain )
{
    // Handle non-default domains via PAM first
    if (pszDomain != nullptr && !EQUAL(pszDomain, "")) {
         // Ensure PAM system is initialized for this band
         return GDALPamRasterBand::GetMetadata(pszDomain);
    }

    // Handle default domain ("" or nullptr) with caching
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
/***************************************************************************/
CPLErr NisarRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage)
{
    // Validation and Setup
    NisarDataset *poGDS = reinterpret_cast<NisarDataset *>(this->poDS);
    if (!poGDS || poGDS->GetDatasetHandle() < 0 || this->hH5Type < 0 ||
        m_hFileSpaceID < 0 || m_hMemSpaceID < 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: Band is not properly initialized.");
        return CE_Failure;
    }

    const size_t nDataTypeSize = GDALGetDataTypeSizeBytes(this->eDataType);
    const size_t nFullBlockBytes = static_cast<size_t>(nBlockXSize) * nBlockYSize * nDataTypeSize;

    // Handle Off-Image Blocks
    hsize_t start_offset_x = static_cast<hsize_t>(nBlockXOff) * nBlockXSize;
    hsize_t start_offset_y = static_cast<hsize_t>(nBlockYOff) * nBlockYSize;

    if (start_offset_x >= static_cast<hsize_t>(nRasterXSize) ||
        start_offset_y >= static_cast<hsize_t>(nRasterYSize) ||
        nDataTypeSize == 0)
    {
        memset(pImage, 0, nFullBlockBytes);
        return CE_None;
    }

    // Read Data
    hid_t hDatasetID = poGDS->GetDatasetHandle();
    int rank = H5Sget_simple_extent_ndims(m_hFileSpaceID);
    if (rank < 2) return CE_Failure; // Should have been caught earlier

    // Calculate the actual number of pixels to read (for partial blocks).
    int nActualBlockXSize = std::min(nBlockXSize, nRasterXSize - static_cast<int>(start_offset_x));
    int nActualBlockYSize = std::min(nBlockYSize, nRasterYSize - static_cast<int>(start_offset_y));

    // Simplified partial block handling
    // Always pre-fill the entire buffer with the fill value (0).
    // H5Dread will then overwrite the valid data area.
    memset(pImage, 0, nFullBlockBytes);

    // Define the hyperslab (the block to read) in the HDF5 file.
    std::vector<hsize_t> file_start(rank);
    std::vector<hsize_t> file_count(rank);
    file_start[rank - 2] = start_offset_y;
    file_start[rank - 1] = start_offset_x;
    file_count[rank - 2] = nActualBlockYSize;
    file_count[rank - 1] = nActualBlockXSize;
    for (int i = 0; i < rank - 2; ++i) { // Handle higher dimensions
        file_start[i] = 0;
        file_count[i] = 1;
    }

    herr_t status = H5Sselect_hyperslab(m_hFileSpaceID, H5S_SELECT_SET, file_start.data(),
                                        nullptr, file_count.data(), nullptr);
    if (status < 0) return CE_Failure;

    // By default, write to the entire memory buffer.
    hid_t hMemSpace = m_hMemSpaceID;

    // If reading a partial block, select a smaller hyperslab in memory.
    // This tells H5Dread to place the partial data at the top-left
    // of the buffer, leaving the rest untouched (already filled with 0).
    if (nActualBlockXSize < nBlockXSize || nActualBlockYSize < nBlockYSize)
    {
        hsize_t mem_start[2] = {0, 0};
        hsize_t mem_count[2] = {static_cast<hsize_t>(nActualBlockYSize),
                                static_cast<hsize_t>(nActualBlockXSize)};
        status = H5Sselect_hyperslab(m_hMemSpaceID, H5S_SELECT_SET, mem_start,
                                     nullptr, mem_count, nullptr);
        if (status < 0) return CE_Failure;
    } else {
        // For a full block, select the entire memory space.
        H5Sselect_all(m_hMemSpaceID);
    }

    // Read the data from the file hyperslab to the memory hyperslab.
    status = H5Dread(hDatasetID, this->hH5Type, hMemSpace, m_hFileSpaceID,
                     H5P_DEFAULT, pImage);

    if (status < 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "IReadBlock: H5Dread failed for block %d, %d.",
                 nBlockXOff, nBlockYOff);
        H5Eprint(H5E_DEFAULT, stderr);
        return CE_Failure;
    }

    return CE_None;
}
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
