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
#include "nisar_priv.h"
#include "hdf5.h"
#include "gdal.h"        // For CE_Failure, CE_None, GDALDataType
#include "cpl_conv.h"
#include <algorithm>   // For std::min
#include <vector>      // For std::vector
#include <cstring>     // For memset
#include <mutex>       // For std::lock_guard
#include <iomanip>     // for std::setprecision

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

// NisarRasterBand::GetMetadata
char **NisarRasterBand::GetMetadata( const char *pszDomain )
{
    // First, let the PAM system load any existing metadata from .aux.xml
    GDALPamRasterBand::GetMetadata(pszDomain);

    // Handle non-default domains using only the PAM system
    if (pszDomain != nullptr && !EQUAL(pszDomain, "")) {
        return GDALPamRasterBand::GetMetadata(pszDomain);
    }

    // Handle the default domain with caching
    std::lock_guard<std::mutex> lock(m_MetadataMutex);

    // If we've already read the HDF5 attributes, just return what PAM has.
    if (m_bMetadataRead) {
        return GDALPamRasterBand::GetMetadata(pszDomain);
    }
    m_bMetadataRead = true; // Mark as read for this session

    CPLDebug("NISAR_Band", "Band %d: Reading HDF5 attributes for default metadata domain.", nBand);

    NisarDataset *poGDS = reinterpret_cast<NisarDataset *>( this->poDS );
    hid_t hDatasetID = poGDS ? poGDS->GetDatasetHandle() : -1;

    if (hDatasetID >= 0) {
        char **papszHDFMeta = nullptr;
        NISAR_AttrCallbackData callback_data;
        callback_data.ppapszList = &papszHDFMeta;
        
        // Band attributes are local and shouldn't have the full path.
        callback_data.pszPrefix = ""; 
	// Get the full HDF5 path of this band's dataset to use as a prefix.
        //std::string datasetPath = get_hdf5_object_name(hDatasetID);
        //callback_data.pszPrefix = datasetPath.c_str();

        hsize_t idx = 0;
        H5Aiterate2(hDatasetID, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                    NISAR_AttributeCallback, &callback_data);

        if (papszHDFMeta != nullptr) {
            // Merge the HDF5 attributes into this band's metadata list
            SetMetadata(papszHDFMeta);
            CSLDestroy(papszHDFMeta);
        }
    } else {
        CPLError(CE_Warning, CPLE_AppDefined, "Band %d: Cannot read HDF5 metadata, dataset handle invalid.", nBand);
    }

    // Return the final merged list from this band's PAM store
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

    if (rank == 3)
    {
        // 3D Case: [Band][Y][X]
        // GDAL Band indices are 1-based, HDF5 is 0-based.
        file_start[0] = static_cast<hsize_t>(this->nBand - 1);
        file_count[0] = 1;
    }
    else
    {
        // Fallback for > 3 dimensions or 2D
        for (int i = 0; i < rank - 2; ++i) {
            file_start[i] = 0;
            file_count[i] = 1;
        }
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
        // Create N-D memory selection
        std::vector<hsize_t> mem_start(rank, 0); // Start at {0, 0, ... 0}
        std::vector<hsize_t> mem_count(rank);

        // Set dimensions for Y and X
        mem_count[rank - 2] = static_cast<hsize_t>(nActualBlockYSize);
        mem_count[rank - 1] = static_cast<hsize_t>(nActualBlockXSize);

        // Set higher dimensions to 1
        for (int i = 0; i < rank - 2; ++i)
        {
            mem_count[i] = 1;
        }

        status = H5Sselect_hyperslab(m_hMemSpaceID, H5S_SELECT_SET, mem_start.data(),
                                     nullptr, mem_count.data(), nullptr);
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

// ====================================================================
// NisarHDF5MaskBand Implementation (Corrected Name)
// ====================================================================

NisarHDF5MaskBand::NisarHDF5MaskBand(NisarDataset* poDSIn, hid_t hMaskDS, NisarMaskType eType) :
    m_hMaskDS(hMaskDS),
    m_eType(eType) // FIX 3: This now works because eType is in the arguments
{
    this->poDS = poDSIn;
    this->nBand = 0; // Mask bands usually have nBand=0
    this->eDataType = GDT_Byte;

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

NisarHDF5MaskBand::~NisarHDF5MaskBand()
{
    if (m_hMaskDS >= 0) {
        H5Dclose(m_hMaskDS);
    }
}

CPLErr NisarHDF5MaskBand::IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage)
{
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
    
    // Read Raw Data
    hid_t hMemSpace = H5Screate_simple(2, count, nullptr);
    hid_t hFileSpace = H5Dget_space(m_hMaskDS);
    H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count, nullptr);

    // Read into the buffer provided by GDAL
    GByte* pbyBuffer = static_cast<GByte*>(pImage);
    
    herr_t status = H5Dread(m_hMaskDS, H5T_NATIVE_UINT8, hMemSpace, hFileSpace, H5P_DEFAULT, pbyBuffer);
    
    H5Sclose(hFileSpace);
    H5Sclose(hMemSpace);

    if (status < 0) return CE_Failure;

    int nPixels = nRequestX * nRequestY;
    
    // EXTENSIBLE VALIDITY LOGIC
    if (m_eType == NisarMaskType::GCOV)
    {
        for(int i = 0; i < nPixels; i++) {
            GByte v = pbyBuffer[i];
            // GCOV/GSLC: 1-5 is Valid. 0 is Invalid. 255 is Fill.
            // GDAL Standard: 255=Valid, 0=Invalid
            pbyBuffer[i] = (v >= 1 && v <= 5) ? 255 : 0;
        }
    }
    else if (m_eType == NisarMaskType::GUNW)
    {
        for(int i = 0; i < nPixels; i++) {
            GByte v = pbyBuffer[i];
            
            if (v == 255) {
                pbyBuffer[i] = 0; // Fill is Invalid
            } else {
                // Parse 3-digit decimal: Water|RefSub|SecSub
                // Example: 123 -> Water=1, Ref=2, Sec=3
                // Valid if BOTH subswaths are non-zero.
                int nRefSubswath = (v / 10) % 10;
                int nSecSubswath = v % 10;
                
                // GDAL Mask: 255=Valid, 0=Invalid
                pbyBuffer[i] = (nRefSubswath > 0 && nSecSubswath > 0) ? 255 : 0;
            }
        }
    }

    return CE_None;
}
