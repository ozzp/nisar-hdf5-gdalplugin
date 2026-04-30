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

/***************************************************************************/
/*                             IReadBlock()                                */
/* This method reads a block of data from the HDF5 dataset.                */
/***************************************************************************/
CPLErr NisarRasterBand::IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage)
{
    NisarDataset *poGDS = static_cast<NisarDataset *>(poDS);
    if (!poGDS || m_hFileSpaceID < 0 || m_hMemSpaceID < 0) return CE_Failure;

    hsize_t file_start[H5S_MAX_RANK];
    hsize_t file_count[H5S_MAX_RANK];
    hsize_t mem_start[H5S_MAX_RANK];
    hsize_t mem_count[H5S_MAX_RANK];

    const int rank = H5Sget_simple_extent_ndims(m_hFileSpaceID);
    const size_t nDataTypeSize = GDALGetDataTypeSizeBytes(eDataType);
    const size_t nFullBlockBytes = static_cast<size_t>(nBlockXSize) * nBlockYSize * nDataTypeSize;

    // Proper buffer filling
    memset(pImage, 0, nFullBlockBytes); 

    hsize_t start_x = static_cast<hsize_t>(nBlockXOff) * nBlockXSize;
    hsize_t start_y = static_cast<hsize_t>(nBlockYOff) * nBlockYSize;

    if (start_x >= static_cast<hsize_t>(nRasterXSize) || start_y >= static_cast<hsize_t>(nRasterYSize))
        return CE_None;

    int nActualX = std::min(nBlockXSize, nRasterXSize - static_cast<int>(start_x));
    int nActualY = std::min(nBlockYSize, nRasterYSize - static_cast<int>(start_y));

    hid_t hLocalFileSpace = H5Scopy(m_hFileSpaceID);
    hid_t hLocalMemSpace = H5Scopy(m_hMemSpaceID);

    for (int i = 0; i < rank; i++) { file_start[i] = 0; file_count[i] = 1; }
    file_start[rank - 2] = start_y;
    file_start[rank - 1] = start_x;
    file_count[rank - 2] = nActualY;
    file_count[rank - 1] = nActualX;

    if (rank == 3) {
        // Use indices for array assignment
        file_start[0] = static_cast<hsize_t>(nBand - 1);
        file_count[0] = 1;
    }

    H5Sselect_hyperslab(hLocalFileSpace, H5S_SELECT_SET, file_start, nullptr, file_count, nullptr);

    for (int i = 0; i < rank; i++) { mem_start[i] = 0; mem_count[i] = 1; }
    mem_count[rank - 2] = nActualY;
    mem_count[rank - 1] = nActualX;

    H5Sselect_hyperslab(hLocalMemSpace, H5S_SELECT_SET, mem_start, nullptr, mem_count, nullptr);

    herr_t status = H5Dread(poGDS->GetDatasetHandle(), hH5Type, hLocalMemSpace, 
                            hLocalFileSpace, H5P_DEFAULT, pImage);

    H5Sclose(hLocalFileSpace);
    H5Sclose(hLocalMemSpace);

    return (status < 0) ? CE_Failure : CE_None;
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

    hid_t hFileSpace = H5Dget_space(m_hMaskDS);
    H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count, nullptr);

    // Read into the buffer provided by GDAL
    GByte* pbyBuffer = static_cast<GByte*>(pImage);

    herr_t status = H5Dread(m_hMaskDS, H5T_NATIVE_UINT8, hMemSpace, hFileSpace, H5P_DEFAULT, pbyBuffer);

    H5Sclose(hFileSpace);
    H5Sclose(hMemSpace);

    if (status < 0) return CE_Failure;

    // Iterate over the FULL block, not just nPixels. 
    // This ensures padded pixels (which are 0) safely pass through the logic as Invalid.
    if (m_eType == NisarMaskType::GCOV)
    {
        for(int i = 0; i < nFullBlockPixels; i++) {
            GByte v = pbyBuffer[i];
            // GCOV/GSLC: 1-5 is Valid. 0 is Invalid. 255 is Fill.
            // GDAL Standard: 255=Valid, 0=Invalid
            pbyBuffer[i] = (v >= 1 && v <= 5) ? 255 : 0;
        }
    }
    else if (m_eType == NisarMaskType::GUNW)
    {
        for(int i = 0; i < nFullBlockPixels; i++) {
            GByte v = pbyBuffer[i];

            if (v == 255 || v == 0) {
                pbyBuffer[i] = 0; // Fill or missing is Invalid
            } else {
                int nRefSubswath = (v / 10) % 10;
                int nSecSubswath = v % 10;
                pbyBuffer[i] = (nRefSubswath > 0 && nSecSubswath > 0) ? 255 : 0;
            }
        }
    }

    return CE_None;
}
