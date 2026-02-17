// nisardataset.cpp
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

#include "nisardataset.h"
#include "nisarrasterband.h"

#include <sstream>  // For std::ostringstream
#include <iomanip>  // For std::setprecision
#include <vector>
#include <string>
#include <algorithm>

#include "cpl_port.h"
#include "cpl_conv.h"
#include "cpl_string.h"  // For CPLStrintf, EQUAL
#include "cpl_time.h"
#include "nisar_priv.h"
#include "hdf5.h"
#include "H5FDros3.h"  // For H5FD_ros3_fapl_t and H5FD_CURR_ROS3_FAPL_T_VERSION

// Helper to handle API change between 3.10 and 3.12
static std::string NisarGetExtension(const char* pszFilename)
{
#if GDAL_VERSION_MAJOR > 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
    return CPLGetExtensionSafe(pszFilename);
#else
    return std::string(CPLGetExtension(pszFilename));
#endif
}

/**
 * \brief Helper function to read a 1D array of HDF5 strings.
 *
 * Reads a 1D array of fixed-length strings and returns them
 * as a single, comma-separated std::string.
 * Returns an empty std::string on failure.
 */
std::string NisarDataset::ReadHDF5StringArrayAsList(hid_t hParentGroup,
                                                    const char *pszDatasetName)
{
    std::string sResult;
    hid_t hDataset = -1;
    hid_t hType = -1;
    hid_t hSpace = -1;
    hid_t hMemType = -1;
    char *pszBuffer = nullptr;
    hsize_t nStrings = 0;
    size_t nStringSize = 0; // Size of string in *file*
    size_t nMemStringSize = 0; // Size of string in *memory*
    int ndims = 0;

    std::ostringstream oss;

    // Suppress HDF5 errors for this read attempt
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open dataset
    hDataset = H5Dopen2(hParentGroup, pszDatasetName, H5P_DEFAULT);
    if (hDataset < 0)
    {
        goto cleanup_and_restore; // Failed to open dataset
    }

    // Get type and verify it's a string
    hType = H5Dget_type(hDataset);
    if (hType < 0 || H5Tget_class(hType) != H5T_STRING)
    {
        goto cleanup_and_restore; // Not a string
    }
    
    // Size of one string element IN THE FILE
    nStringSize = H5Tget_size(hType); 
    if (nStringSize == 0) nStringSize = 1;

    // Get space and verify it's 1D
    hSpace = H5Dget_space(hDataset);
    if (hSpace < 0)
    {
         goto cleanup_and_restore;
    }
    ndims = H5Sget_simple_extent_ndims(hSpace);
    if (ndims != 1)
    {
        // This helper only supports 1D arrays
        goto cleanup_and_restore;
    }

    // Get number of strings in the 1D array
    H5Sget_simple_extent_dims(hSpace, &nStrings, nullptr);
    if (nStrings == 0)
    {
        goto cleanup_and_restore; // Empty array
    }
    
    // We want to read each fixed-length string of size N
    // into a C-string buffer of size N+1 (for the null terminator).
    nMemStringSize = nStringSize + 1;
    
    hMemType = H5Tcopy(H5T_C_S1);
    H5Tset_size(hMemType, nMemStringSize);
        
    // Read the entire array of strings
    // HDF5 will read each nStringSize-byte string from the file
    // and place it into a (nStringSize+1)-byte slot in pszBuffer,
    // adding the null terminator for us.
    pszBuffer = (char *)CPLMalloc(nStrings * nMemStringSize);
    if (H5Dread(hDataset, hMemType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszBuffer) < 0)
    {
        goto cleanup_and_restore; // Read failed
    }

    // Iterate, and append
    for (hsize_t i = 0; i < nStrings; ++i)
    {
        // Point to the beginning of the i-th *null-terminated* string
        const char* pszSourceString = pszBuffer + (i * nMemStringSize);
        
        oss << pszSourceString;
        if (i < nStrings - 1)
        {
            oss << ","; // Add comma separator
        }
    }
    sResult = oss.str();

cleanup_and_restore:
    // Restore HDF5 error handling
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Clean up all resources
    CPLFree(pszBuffer);
    if (hMemType >= 0)
        H5Tclose(hMemType);
    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hType >= 0)
        H5Tclose(hType);
    if (hDataset >= 0)
        H5Dclose(hDataset);

    return sResult;
}
/**
 * \brief Helper function to read a scalar HDF5 string dataset.
 *
 * Assumes the dataset contains a single, fixed-length string.
 * Returns an empty std::string on failure.
 */
std::string NisarDataset::ReadHDF5StringDataset(hid_t hParentGroup,
                                                const char *pszDatasetName)
{
    std::string sResult;
    hid_t hDataset = -1;
    hid_t hType = -1;
    hid_t hSpace = -1;
    hid_t hMemType = -1;
    char *pszBuffer = nullptr;
    size_t nSize = 0; // Declared at top

    // Suppress HDF5 errors for this read attempt
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open dataset
    hDataset = H5Dopen2(hParentGroup, pszDatasetName, H5P_DEFAULT);
    if (hDataset < 0)
    {
        goto cleanup_and_restore; // Failed to open dataset
    }

    // Get type and verify it's a string
    hType = H5Dget_type(hDataset);
    if (hType < 0 || H5Tget_class(hType) != H5T_STRING)
    {
        goto cleanup_and_restore; // Not a string
    }

    // Get string size
    nSize = H5Tget_size(hType);
    if (nSize == 0)
    {
        nSize = 1; // Safety
    }

    // Get space and verify it's scalar
    hSpace = H5Dget_space(hDataset);
    if (hSpace < 0 || H5Sget_simple_extent_ndims(hSpace) != 0)
    {
        // Note: This helper only supports scalar strings, not arrays of strings
        goto cleanup_and_restore;
    }

    // Create memory type for reading
    hMemType = H5Tcopy(H5T_C_S1);
    H5Tset_size(hMemType, nSize + 1); // +1 for null terminator
    H5Tset_strpad(hMemType, H5T_STR_NULLTERM);  // Explicitly ask for Null Term

    // Read the string
    pszBuffer = (char *)CPLMalloc(nSize + 1);
    if (H5Dread(hDataset, hMemType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszBuffer) >= 0)
    {
        pszBuffer[nSize] = '\0'; // Ensure null termination
        sResult = pszBuffer;
    }

cleanup_and_restore:
    // Restore HDF5 error handling
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Clean up all resources
    CPLFree(pszBuffer);
    if (hMemType >= 0)
        H5Tclose(hMemType);
    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hType >= 0)
        H5Tclose(hType);
    if (hDataset >= 0)
        H5Dclose(hDataset);

    return sResult;
}

/************************************************************************/
/* Identify()                                                           */
/************************************************************************/

int NisarDataset::Identify(GDALOpenInfo *poOpenInfo)
{
    const char *pszPrefix = "NISAR:";
    size_t nPrefixLen = strlen(pszPrefix);
    
    // 1. Check for explicit driver prefix "NISAR:"
    if (EQUALN(poOpenInfo->pszFilename, pszPrefix, nPrefixLen))
    {
        // REQUIREMENT: Contain ".h5" anywhere, not necessarily at the end.
        // This supports both:
        //   "NISAR:product.h5"
        //   "NISAR:product.h5:/science/LSAR/..." (Subdataset path)
        CPLString osFilename = poOpenInfo->pszFilename;
        if (osFilename.ifind(".h5") != std::string::npos)
        {
            return TRUE;
        }
        // If it starts with NISAR: but doesn't look like an HDF5 file, reject it.
        return FALSE;
    }

    // 2. Check for standard .h5 file extension (Automatic Detection without prefix)
    // For files NOT starting with NISAR:, we strictly require the extension.
    if (!EQUAL(NisarGetExtension(poOpenInfo->pszFilename).c_str(), "h5"))
    {
        return FALSE;
    }

    // 3. Check if it's a remote file (Heuristic)
    // Remote files might not allow H5Fopen checks efficiently
    bool bIsRemote = STARTS_WITH_CI(poOpenInfo->pszFilename, "s3://") ||
                     STARTS_WITH_CI(poOpenInfo->pszFilename, "/vsis3/") ||
                     STARTS_WITH_CI(poOpenInfo->pszFilename, "http://") ||
                     STARTS_WITH_CI(poOpenInfo->pszFilename, "https://");

    if (bIsRemote)
    {
        // Heuristic: If it's a remote .h5 file and "NISAR" is in the name, claim it.
        CPLString osFilename = poOpenInfo->pszFilename;
        if (osFilename.ifind("NISAR") != std::string::npos)
        {
            return TRUE;
        }
        return FALSE; 
    }

    // 4. Local File Deep Check
    // It's a local .h5 file. Open it to verify it is actually NISAR data.
    
    // Suppress HDF5 errors
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Check if it's HDF5
    htri_t bIsHDF5 = H5Fis_hdf5(poOpenInfo->pszFilename);
    if (bIsHDF5 <= 0)
    {
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
        return FALSE;
    }

    // Check for NISAR structure
    bool bIsNisar = false;
    hid_t hFile = H5Fopen(poOpenInfo->pszFilename, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (hFile >= 0)
    {
        // Check for the signature NISAR groups
        if (H5Lexists(hFile, "/science/LSAR/identification", H5P_DEFAULT) > 0 ||
            H5Lexists(hFile, "/science/SSAR/identification", H5P_DEFAULT) > 0)
        {
            bIsNisar = true;
        }
        H5Fclose(hFile);
    }

    // Restore HDF5 errors
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    return bIsNisar;
}
/************************************************************************/
/* ReadIdentificationMetadata()                     */
/************************************************************************/
/**
 * \brief Reads essential identification metadata.
 *
 * This function is called from NisarDataset::Open() immediately after
 * the HDF5 file is opened. It finds the /science/[LSAR|SSAR]/identification
 * group and reads the productType and productLevel datasets to populate
 * the corresponding member variables (m_sProductType, m_sInst, etc.).
 * This is necessary for the Open() function to determine which
 * HDF5 dataset to open (e.g., L1 swaths vs L2 grids).
 */
void NisarDataset::ReadIdentificationMetadata()
{
    // Reset flags
    m_sInst.clear();
    m_sProductType.clear();
    m_bIsLevel1 = false;
    m_bIsLevel2 = false;

    if (hHDF5 < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "ReadIdentificationMetadata: HDF5 file is not open.");
        return;
    }

    hid_t hIdentGroup = -1;
    std::string sIdentPath;

    // Suppress HDF5 errors while probing for paths
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Check for LSAR
    if (H5Lexists(hHDF5, "/science/LSAR/identification", H5P_DEFAULT) > 0)
    {
        sIdentPath = "/science/LSAR/identification";
        m_sInst = "LSAR";
    }
    // Check for SSAR
    else if (H5Lexists(hHDF5, "/science/SSAR/identification", H5P_DEFAULT) > 0)
    {
        sIdentPath = "/science/SSAR/identification";
        m_sInst = "SSAR";
    }

    if (m_sInst.empty())
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Could not find /science/LSAR/identification or "
                 "/science/SSAR/identification group in file.");
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore
        return;
    }

    // Open the identified group
    hIdentGroup = H5Gopen2(hHDF5, sIdentPath.c_str(), H5P_DEFAULT);
    if (hIdentGroup < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Found identification path '%s' but failed to open it.",
                 sIdentPath.c_str());
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore
        return;
    }

    // Read productType
    m_sProductType = ReadHDF5StringDataset(hIdentGroup, "productType");

    // Read productLevel
    std::string sProductLevel = ReadHDF5StringDataset(hIdentGroup, "productLevel");

    // Set flags based on productLevel
    if (EQUAL(sProductLevel.c_str(), "L1"))
    {
        m_bIsLevel1 = true;
    }
    else if (EQUAL(sProductLevel.c_str(), "L2"))
    {
        m_bIsLevel2 = true;
    }

    // If productLevel was missing or weird, we force Level 2 for known grid types.
    if (EQUAL(m_sProductType.c_str(), "GUNW") || 
        EQUAL(m_sProductType.c_str(), "GCOV") || 
        EQUAL(m_sProductType.c_str(), "GSLC"))
    {
        m_bIsLevel2 = true;
        m_bIsLevel1 = false; 
    }

    // Restore HDF5 error handling
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Cleanup
    H5Gclose(hIdentGroup);

    // Log results
    CPLDebug("NISAR_DRIVER",
             "Identified Product: INST=%s, Type=%s, Level=%s (L1=%d, L2=%d)",
             m_sInst.c_str(), m_sProductType.c_str(), sProductLevel.c_str(),
             m_bIsLevel1, m_bIsLevel2);

    if (m_sProductType.empty() || sProductLevel.empty())
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Failed to read 'productType' or 'productLevel' from %s",
                 sIdentPath.c_str());
    }
}

static std::string ReadStringAttribute(hid_t hLocation, const char *pszAttrName)
{
    hid_t hAttr = H5Aopen(hLocation, pszAttrName, H5P_DEFAULT);
    if (hAttr < 0)
        return "";

    std::string attr_val = "";
    hid_t hAttrType = H5Aget_type(hAttr);
    if (hAttrType >= 0)
    {
        if (H5Tis_variable_str(hAttrType) > 0)
        {
            char *pszVal = nullptr;
            if (H5Aread(hAttr, hAttrType, &pszVal) >= 0 && pszVal)
            {
                attr_val = pszVal;
                H5free_memory(pszVal);
            }
        }
        else
        {  // Fixed-length string
            size_t nSize = H5Tget_size(hAttrType);
            char *pszVal = (char *)CPLMalloc(nSize + 1);
            if (H5Aread(hAttr, hAttrType, pszVal) >= 0)
            {
                pszVal[nSize] = '\0';
                attr_val = pszVal;
            }
            CPLFree(pszVal);
        }
        H5Tclose(hAttrType);
    }
    H5Aclose(hAttr);
    return attr_val;
}

// Helper to read a string attribute from an HDF5 object
static std::string ReadH5StringAttribute(hid_t hObjectID, const char *pszAttrName)
{
    // Suppress errors for this check
    H5E_auto2_t old_func; void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    if (H5Aexists(hObjectID, pszAttrName) <= 0)
    {
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
        return "";
    }
    
    // Use H5Aopen (modern API)
    hid_t hAttr = H5Aopen(hObjectID, pszAttrName, H5P_DEFAULT);
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore errors
    
    if (hAttr < 0) return "";

    std::string strVal;
    hid_t hAttrType = H5Aget_type(hAttr);
    hid_t hAttrSpace = H5Aget_space(hAttr);
    
    // --- CRITICAL FIX: Create a native memory type for reading strings ---
    // This ensures HDF5 handles the conversion from NULLPAD (File) to NULLTERM (Memory)
    hid_t hMemType = H5Tcopy(H5T_C_S1);

    if (H5Sget_simple_extent_npoints(hAttrSpace) == 1)
    {
        if (H5Tis_variable_str(hAttrType) > 0)
        {
            H5Tset_size(hMemType, H5T_VARIABLE);
            char *pszVal = nullptr;
            if (H5Aread(hAttr, hMemType, &pszVal) >= 0 && pszVal)
            {
                strVal = pszVal;
                H5free_memory(pszVal);
            }
        }
        else
        {
            size_t nSize = H5Tget_size(hAttrType);
            if (nSize > 0)
            {
                // Set memory size to nSize + 1 to accommodate the null terminator
                H5Tset_size(hMemType, nSize + 1);
                H5Tset_strpad(hMemType, H5T_STR_NULLTERM);

                char *pszVal = (char *)CPLMalloc(nSize + 1);
                
                // Read using hMemType (Memory), not hAttrType (File)
                if (H5Aread(hAttr, hMemType, pszVal) >= 0)
                {
                    pszVal[nSize] = '\0'; // Safety null termination
                    strVal = pszVal;
                }
                CPLFree(pszVal);
            }
        }
    }

    H5Tclose(hMemType);
    H5Sclose(hAttrSpace);
    H5Tclose(hAttrType);
    H5Aclose(hAttr);
    return strVal;
}

// Forward declaration for the helper function
static std::string get_hdf5_object_name(hid_t hObjID);

// DEFINE THE CONSTANT HERE
// TBD: Define default dataset for each product?
const char *DEFAULT_NISAR_HDF5_PATH =
    "/science/LSAR/GSLC/grids/frequencyA/HH";  // Example

// New Callback Struct and Function for SetMetadataItem
struct NISAR_AttrCallbackData_SetItem
{
    NisarDataset *poDS;  // Pointer to the dataset object
};

// Struct and Callback for Reading Identification Datasets
struct NISAR_IdentCallbackData
{
    char ***ppapszList;  // Pointer to the CSL list pointer being built
    hid_t hIdentGroup;   // Handle to the identification group
};

// Struct to pass data to H5Aiterate callback
struct MetadataAttrCallbackData
{
    char ***ppapszList;  // Pointer to the CSL list pointer in GetMetadata
    const char *pszObjectPath;  // Full path of the object being iterated
};

// Struct to pass data to H5Ovisit callback
struct MetadataVisitData
{
    char ***ppapszList;  // Pointer to the CSL list pointer in GetMetadata
    // Could add filters here, e.g., std::string rootPathFilter;
};

// Define struct to pass data to visitor
struct NISARVisitorData
{
    std::vector<std::string> *pFoundPaths;  // Pointer to list in Open()
    hid_t
        hStartingGroupID;  // Pass group/file ID for opening datasets inside visitor
    // Add other necessary data e.g., const char* pszRequiredPrefix;
};

// Callback for H5LiterateByName - reads scalar datasets in identification group
static herr_t NISAR_IdentificationDatasetCallback(hid_t group_id,
                                                  const char *member_name,
                                                  const H5L_info2_t * /*linfo*/,
                                                  void *op_data)
{
    NISAR_IdentCallbackData *data =
        static_cast<NISAR_IdentCallbackData *>(op_data);
    if (!data || !data->ppapszList || data->hIdentGroup < 0)
        return H5_ITER_ERROR;

    hid_t dset_id = -1;
    hid_t dtype = -1;
    hid_t dspace = -1;
    std::string value_str = "(Error reading dataset)";
    herr_t status = -1;
    bool bValueSet = false;

    // Check if the object is a dataset (optional, H5Dopen2 will fail otherwise)
    H5O_info2_t oinfo;
    if (H5Oget_info_by_name3(group_id, member_name, &oinfo, H5O_INFO_BASIC,
                             H5P_DEFAULT) < 0 ||
        oinfo.type != H5O_TYPE_DATASET)
    {
        return H5_ITER_CONT;  // Skip non-datasets
    }

    // Open the dataset
    dset_id = H5Dopen2(data->hIdentGroup, member_name, H5P_DEFAULT);
    if (dset_id < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "IdentCallback: Failed to open dataset '%s'", member_name);
        return H5_ITER_CONT;  // Continue
    }

    dspace = H5Dget_space(dset_id);
    dtype = H5Dget_type(dset_id);
    if (dspace < 0 || dtype < 0)
        goto ident_cleanup;

    // Check if it's a scalar dataset
    if (H5Sget_simple_extent_type(dspace) == H5S_SCALAR)
    {
        H5T_class_t type_class = H5Tget_class(dtype);
        size_t type_size = H5Tget_size(dtype);

        // Read Scalar Value (Primarily expecting strings)
        if (type_class == H5T_STRING)
        {
            char *pszReadVL = nullptr;
            char *pszReadFixed = nullptr;
            bool bIsVariable = H5Tis_variable_str(dtype);
            hid_t mem_type = H5Tcopy(H5T_C_S1);
            if (mem_type < 0)
                goto ident_cleanup;

            if (bIsVariable)
            {
                H5Tset_size(mem_type, H5T_VARIABLE);
                status = H5Dread(dset_id, mem_type, H5S_ALL, H5S_ALL,
                                 H5P_DEFAULT, &pszReadVL);
                if (status >= 0 && pszReadVL != nullptr)
                {
                    value_str = pszReadVL;
                    bValueSet = true;
                    H5free_memory(pszReadVL);
                }
                else
                {
                    CPLError(CE_Warning, CPLE_FileIO,
                             "Failed read VLEN string dataset '%s'",
                             member_name);
                }
            }
            else if (type_size > 0)
            {  // Fixed length string
                // We must allocate type_size + 1 in the MEMORY type to hold the null terminator.
                // If we use type_size, HDF5 will truncate the string to make room for \0.
                H5Tset_size(mem_type, type_size+1);
                H5Tset_strpad(mem_type, H5T_STR_NULLTERM);
                pszReadFixed = (char *)VSIMalloc(type_size + 1);
                if (pszReadFixed)
                {
                    status = H5Dread(dset_id, mem_type, H5S_ALL, H5S_ALL,
                                     H5P_DEFAULT, pszReadFixed);
                    if (status >= 0)
                    {
                        pszReadFixed[type_size] = '\0';
                        value_str = pszReadFixed;
                        bValueSet = true;
                    }
                    else
                    {
                        CPLError(CE_Warning, CPLE_FileIO,
                                 "Failed read fixed string dataset '%s'",
                                 member_name);
                    }
                    VSIFree(pszReadFixed);
                }
                else
                {
                    CPLError(CE_Failure, CPLE_OutOfMemory,
                             "Malloc failed for fixed string dataset '%s'",
                             member_name);
                }
            }
            if (mem_type >= 0)
            {
                H5Tclose(mem_type);
                mem_type = -1;
            }
        }
        // Add simple integer/float reads if needed for identification group
        else if (type_class == H5T_INTEGER)
        {
            long long llVal = 0;
            status = H5Dread(dset_id, H5T_NATIVE_LLONG, H5S_ALL, H5S_ALL,
                             H5P_DEFAULT, &llVal);
            if (status >= 0)
            {
                value_str = CPLSPrintf("%lld", llVal);
                bValueSet = true;
            }
            else
            {
                CPLError(CE_Warning, CPLE_FileIO,
                         "Failed read integer dataset '%s'", member_name);
            }
        }
        else if (type_class == H5T_FLOAT)
        {
            double dfVal = 0.0;
            status = H5Dread(dset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                             H5P_DEFAULT, &dfVal);
            if (status >= 0)
            {
                value_str = CPLSPrintf("%.18g", dfVal);
                bValueSet = true;
            }
            else
            {
                CPLError(CE_Warning, CPLE_FileIO,
                         "Failed read float dataset '%s'", member_name);
            }
        }
        else
        {
            value_str = "(unhandled scalar dataset type)";
            bValueSet = true;
        }
    }
    else
    {
        // Skip non-scalar datasets within identification group
        CPLDebug("NISAR_IDENT_CB",
                 "Skipping non-scalar dataset '%s' in identification group.",
                 member_name);
    }

    // If we successfully read a value, add NAME=VALUE to list
    if (bValueSet)
    {
        *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), member_name,
                                              value_str.c_str());
    }

ident_cleanup:
    if (dtype >= 0)
        H5Tclose(dtype);
    if (dspace >= 0)
        H5Sclose(dspace);
    if (dset_id >= 0)
        H5Dclose(dset_id);
    return 0;  // Continue H5Literate iteration
}

// Visitor callback function compatible with H5LiterateByName
static herr_t NISAR_FindDatasetsVisitor(
    hid_t obj_id /* ID of object being visited */,
    const char *name /* Full path of the object relative to hStartGroupID */,
    const H5O_info2_t *oinfo /* Object info */, void *op_data)
{
    // Mark obj_id as unused if not needed directly
    (void)obj_id;

    NISARVisitorData *data = static_cast<NISARVisitorData *>(op_data);
    // Validate the data pointer passed from the caller
    if (!data || !data->pFoundPaths)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Visitor callback received invalid op_data.");
        return H5_ITER_ERROR;  // Stop iteration
    }

    // Log entry for every object visited
    CPLDebug("NISAR_VISITOR_DETAIL", "Visiting object: Path='%s', Type=%d",
             name, (int)oinfo->type);

    // Filter 1: Only consider actual Datasets
    if (oinfo->type != H5O_TYPE_DATASET)
    {
        CPLDebug("NISAR_VISITOR_DETAIL", "--> Skipping '%s' (Not a dataset)",
                 name);
        return H5_ITER_CONT;  // Skip groups, named types, etc.
    }

    // Filter 2: Check if path starts with science/LSAR/
    // 'name' provided by H5Ovisit (when starting from root) is the full path *without* leading slash.
    const char *lsarPrefix = "science/LSAR/";
    const char *ssarPrefix = "science/SSAR/";

    // Check if path starts with *either* prefix
    if (strncmp(name, lsarPrefix, strlen(lsarPrefix)) != 0 &&
        strncmp(name, ssarPrefix, strlen(ssarPrefix)) != 0)
    {
        CPLDebug("NISAR_VISITOR_DETAIL",
                 "--> Skipping '%s' (Path does not start with %s or %s)", name,
                 lsarPrefix, ssarPrefix);
        return H5_ITER_CONT;  // Skip datasets outside the main science group
    }

    // Filter 3: Check Rank (>= 2 dimensions usually desired for rasters)
    // Try to get dataspace directly from the object ID provided by H5Ovisit
    hid_t dspace_id = -1;
    int rank = -1;

    // NOTE: H5Dget_space requires a *dataset* ID, not a generic object ID.
    // We still need to open it as a dataset first. Let's revert to H5Dopen2
    // but ensure the starting group ID passed in op_data is correct.
    // The error might be elsewhere. Let's try H5Dopen2 again.

    hid_t dset_id = -1;
    // Construct the full path with leading slash for H5Dopen2 from root
    std::string full_path = "/";
    full_path += name;

    // Use the hStartingGroupID from the visitor data, which should be the file handle
    dset_id = H5Dopen2(data->hStartingGroupID, full_path.c_str(), H5P_DEFAULT);
    if (dset_id < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Could not open dataset '%s' using H5Dopen2 during subdataset "
                 "discovery.",
                 full_path.c_str());
        return H5_ITER_CONT;  // Skip if cannot open
    }

    // Check if it's actually a dataset type after opening
    if (H5Iget_type(dset_id) != H5I_DATASET)
    {
        CPLDebug("NISAR_VISITOR_DETAIL",
                 "--> Skipping '%s' (Opened object is not H5I_DATASET)",
                 full_path.c_str());
        H5Dclose(dset_id);
        return H5_ITER_CONT;
    }

    dspace_id = H5Dget_space(dset_id);  // Use H5D function on dataset ID
    if (dspace_id >= 0)
    {
        rank = H5Sget_simple_extent_ndims(dspace_id);
        H5Sclose(dspace_id);  // Close dataspace handle
    }
    else
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Could not get dataspace for dataset '%s'.",
                 full_path.c_str());
        H5Dclose(dset_id);
        return H5_ITER_CONT;
    }

    // Close dataset handle opened for checks
    H5Dclose(dset_id);

    if (rank < 2)
    {
        CPLDebug("NISAR_VISITOR", "Skipping dataset '%s' (rank %d < 2)",
                 full_path.c_str(), rank);
        return H5_ITER_CONT;  // Skip scalar or 1D datasets
    }

    //  Store Result
    // If all filters passed, add the full path (including leading slash) to the list
    CPLDebug("NISAR_VISITOR", "Adding relevant dataset to list: %s (Rank: %d)",
             full_path.c_str(), rank);
    try
    {
        data->pFoundPaths->push_back(full_path);  // Store the full path
    }
    catch (const std::bad_alloc &)
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                 "Failed to store dataset path '%s'.", full_path.c_str());
        return H5_ITER_ERROR;  // Stop iteration on memory error
    }

    return H5_ITER_CONT;  // Continue iteration
}

static bool Read1DDoubleVec(hid_t hFile, const char *pszPath,
                            std::vector<double> &vec)
{
    hid_t hDset = H5Dopen2(hFile, pszPath, H5P_DEFAULT);
    if (hDset < 0)
    {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to open dataset: %s",
                 pszPath);
        return false;
    }

    hid_t hSpace = H5Dget_space(hDset);
    int nDims = H5Sget_simple_extent_ndims(hSpace);
    hsize_t dims[1];
    H5Sget_simple_extent_dims(hSpace, dims, nullptr);

    if (nDims != 1 || dims[0] == 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Dataset %s is not a non-empty 1D array.", pszPath);
        H5Sclose(hSpace);
        H5Dclose(hDset);
        return false;
    }

    try
    {
        vec.resize(dims[0]);
    }
    catch (const std::bad_alloc &)
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                 "Failed to allocate memory for %s", pszPath);
        H5Sclose(hSpace);
        H5Dclose(hDset);
        return false;
    }

    herr_t status = H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                            H5P_DEFAULT, vec.data());

    H5Sclose(hSpace);
    H5Dclose(hDset);

    if (status < 0)
    {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to read data from %s",
                 pszPath);
        return false;
    }
    return true;
}

char **NisarDataset::GetFileList()
{
    // Start with the default list from the parent class (which might include .aux.xml)
    char **papszFileList = GDALPamDataset::GetFileList();

    // Add the main HDF5 filename to the list
    if (pszFilename != nullptr)
    {
        // Only add the filename if it's not already in the list.
        if (CSLFindString(papszFileList, pszFilename) == -1)
        {
            papszFileList = CSLAddString(papszFileList, pszFilename);
        }
    }

    return papszFileList;
}

GDALDataType NisarDataset::GetGDALDataType(hid_t hH5Type)
{
    // Check Simple Native Types First
    // Use H5Tequal for robust comparison against predefined native types
    if (H5Tequal(hH5Type, H5T_NATIVE_FLOAT) > 0)
        return GDT_Float32;
    if (H5Tequal(hH5Type, H5T_NATIVE_DOUBLE) > 0)
        return GDT_Float64;
    if (H5Tequal(hH5Type, H5T_NATIVE_UINT8) > 0)
        return GDT_Byte;  // Standard GDAL Byte is often treated as unsigned
    if (H5Tequal(hH5Type, H5T_NATIVE_INT8) > 0)
        return GDT_Byte;  // Map signed char to GDT_Byte too? Review if GDT_Int8 needed.
    if (H5Tequal(hH5Type, H5T_NATIVE_INT16) > 0)
        return GDT_Int16;
    if (H5Tequal(hH5Type, H5T_NATIVE_UINT16) > 0)
        return GDT_UInt16;
    if (H5Tequal(hH5Type, H5T_NATIVE_INT32) > 0)
        return GDT_Int32;
    if (H5Tequal(hH5Type, H5T_NATIVE_UINT32) > 0)
        return GDT_UInt32;
    // Check if GDAL version supports 64-bit integers if needed
    if (H5Tequal(hH5Type, H5T_NATIVE_INT64) > 0)
        return GDT_Int64;
    if (H5Tequal(hH5Type, H5T_NATIVE_UINT64) > 0)
        return GDT_UInt64;
    // Add other simple native types if necessary (char, long, etc.)

    // Check if it's a Compound Type (Potentially Complex)
    H5T_class_t eHDF5Class = H5Tget_class(hH5Type);
    if (eHDF5Class == H5T_COMPOUND)
    {
        CPLDebug("NISAR_GetGDALDataType",
                 "Checking HDF5 Compound type for complex mapping.");
        GDALDataType eComplexType =
            GDT_Unknown;  // Variable to return if complex found

        // Check if it has exactly 2 members (common for complex: real, imag)
        if (H5Tget_nmembers(hH5Type) == 2)
        {
            hid_t hRealType =
                H5Tget_member_type(hH5Type, 0);  // Type of first member
            hid_t hImagType =
                H5Tget_member_type(hH5Type, 1);  // Type of second member
            char *name1 = nullptr;               // Member names
            char *name2 = nullptr;

            // Ensure we got valid type handles before proceeding
            if (hRealType >= 0 && hImagType >= 0)
            {
                // Check if both members have the same base data type
                if (H5Tequal(hRealType, hImagType) > 0)
                {
                    // Check member names convention ('r'/'R' and 'i'/'I')
                    name1 = H5Tget_member_name(hH5Type, 0);
                    name2 = H5Tget_member_name(hH5Type, 1);

                    // Check name validity and conventional naming for complex parts
                    bool isReal =
                        (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                    bool isImaginary =
                        (name2 && (name2[0] == 'i' || name2[0] == 'I'));

                    if (isReal && isImaginary)
                    {
                        size_t elem_size =
                            H5Tget_size(hRealType);  // Size of each component

                        // Map based on underlying HDF5 native type and size
                        if (H5Tequal(hRealType, H5T_NATIVE_FLOAT) > 0 &&
                            elem_size == sizeof(float))
                            eComplexType = GDT_CFloat32;
                        else if (H5Tequal(hRealType, H5T_NATIVE_DOUBLE) > 0 &&
                                 elem_size == sizeof(double))
                            eComplexType = GDT_CFloat64;
                        else if (H5Tequal(hRealType, H5T_NATIVE_SHORT) > 0 &&
                                 elem_size == sizeof(short))
                            eComplexType = GDT_CInt16;
                        else if (H5Tequal(hRealType, H5T_NATIVE_INT) > 0 &&
                                 elem_size == sizeof(int))
                            eComplexType = GDT_CInt32;
                        // Add other complex types if needed
                    }
                }  // end if types equal

                // Close the member type handles obtained from H5Tget_member_type
                H5Tclose(hRealType);
                hRealType = -1;
                H5Tclose(hImagType);
                hImagType = -1;
            }
            else
            { /* Handle error getting member types */
                if (hRealType >= 0)
                    H5Tclose(hRealType);
                if (hImagType >= 0)
                    H5Tclose(hImagType);
            }
            // Free memory allocated by H5Tget_member_name
            if (name1)
                H5free_memory(name1);
            if (name2)
                H5free_memory(name2);
        }  // end if nmembers == 2

        // If we identified a complex type, return it
        if (eComplexType != GDT_Unknown)
        {
            return eComplexType;
        }
        CPLDebug("NISAR_GetGDALDataType",
                 "Compound type did not match expected complex structure.");
    }  // end if compound

    // Handle Other HDF5 Types or Unknown
    // Example: Check for strings if needed (unlikely for main raster bands)
    // if (eHDF5Class == H5T_STRING) { return GDT_String; }

    CPLError(CE_Warning, CPLE_AppDefined,
             "NisarDataset::GetGDALDataType(): Unhandled or unsupported HDF5 "
             "data type (Class: %d).",
             (int)eHDF5Class);
    return GDT_Unknown;  // Default fallback if no mapping found
}

//------------------------------------------------------------------------------
// ReadGeoTransformAttribute (Helper)
//------------------------------------------------------------------------------
CPLErr NisarDataset::ReadGeoTransformAttribute(hid_t hObjectID,
                                               const char *pszAttrName,
                                               GDALGeoTransform &gt) const
{
    // Initialize handles and variables
    hid_t hAttr = -1;
    hid_t hAttrType = -1;
    hid_t hAttrSpace = -1;
    hssize_t nPoints = -1;
    CPLErr eErr = CE_Failure;  // Default to failure

    // Check attribute existence
    htri_t attrExists =
        H5Aexists_by_name(hObjectID, ".", pszAttrName, H5P_DEFAULT);
    if (attrExists <= 0)
    {
        if (attrExists < 0)
            CPLError(CE_Warning, CPLE_AppDefined,
                     "HDF5 error checking existence of attribute '%s'.",
                     pszAttrName);
        else
            CPLDebug("NISAR_ATTR",
                     "Attribute '%s' does not exist on object %lld.",
                     pszAttrName, (long long)hObjectID);
        goto cleanup;
    }

    // Open attribute
    hAttr = H5Aopen(hObjectID, pszAttrName, H5P_DEFAULT);
    if (hAttr < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "H5Aopen failed for attribute '%s'.", pszAttrName);
        goto cleanup;
    }

    // Check attribute type
    hAttrType = H5Aget_type(hAttr);
    if (hAttrType < 0 || !H5Tequal(hAttrType, H5T_NATIVE_DOUBLE))
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Attribute '%s' is not of type NATIVE_DOUBLE.", pszAttrName);
        goto cleanup;
    }

    // Check attribute space (dataspace size)
    hAttrSpace = H5Aget_space(hAttr);
    nPoints = (hAttrSpace >= 0) ? H5Sget_simple_extent_npoints(hAttrSpace) : -1;
    if (nPoints != 6)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Attribute '%s' does not have 6 elements (found %lld).",
                 pszAttrName, (long long)nPoints);
        goto cleanup;
    }

    // Read attribute data
    double adfTempGT[6];
    if (H5Aread(hAttr, H5T_NATIVE_DOUBLE, adfTempGT) < 0)
    {
        CPLError(CE_Warning, CPLE_FileIO, "H5Aread failed for attribute '%s'.",
                 pszAttrName);
        goto cleanup;
    }

    memcpy(gt.data(), adfTempGT, sizeof(double) * 6);

    eErr = CE_None;  // Success!

cleanup:  // Label for resource cleanup
    if (hAttrType >= 0)
        H5Tclose(hAttrType);
    if (hAttrSpace >= 0)
        H5Sclose(hAttrSpace);
    if (hAttr >= 0)
        H5Aclose(hAttr);
    return eErr;  // Return final status
}

/************************************************************************/
/*                            NisarDataset()                          */
/************************************************************************/

NisarDataset::NisarDataset()
    : hHDF5(H5I_INVALID_HID),
      hDataset(H5I_INVALID_HID),
      eDataType(GDT_Unknown),
      pszFilename(nullptr),
      papszSubDatasets(nullptr),
      m_poSRS(nullptr),  // Initialize SRS pointer
      m_bGotSRS(false),  // Initialize SRS flag
      m_bGotMetadata(false),
      m_papszGlobalMetadata(nullptr),  // Initialize global metadata cache
      m_bGotGlobalMetadata(false),     // Initialize global metadata flag
      m_bIsLevel1(false),
      m_bIsLevel2(false),
      m_bGotGeoTransform(false)
{

    // Initialize GT array cache if using caching pattern for it
     m_adfGeoTransform[0] = 0.0;
     m_adfGeoTransform[1] = 1.0;
     m_adfGeoTransform[2] = 0.0;
     m_adfGeoTransform[3] = 0.0;
     m_adfGeoTransform[4] = 0.0;
     m_adfGeoTransform[5] = 1.0;
}

/************************************************************************/
/*                            ~NisarDataset()                         */
/************************************************************************/

NisarDataset::~NisarDataset()
{
    // Flush PAM cache first
    FlushCache(true);

    // Destroy the subdataset list if it exists
    // CSLDestroy handles NULL input safely.
    CSLDestroy(papszSubDatasets);
    papszSubDatasets = nullptr;  // nullify after destroy

    // Close HDF5 handles if they are valid
    if (hDataset >= 0)
    {
        H5Dclose(hDataset);
        hDataset = -1;  // Reset handle ID after closing
    }

    if (hHDF5 >= 0)
    {
        H5Fclose(hHDF5);
        hHDF5 = -1;  // Reset handle ID *after* closing
    }

    // Free the filename buffer allocated in Open()
    CPLFree(pszFilename);
    pszFilename = nullptr;  // Good practice

    // Clean up SRS cache
    if (m_poSRS != nullptr)
    {
        m_poSRS->Release();  // Use OGR reference counting
        m_poSRS = nullptr;
    }

    // Clean up Metadata caches
    CSLDestroy(m_papszGlobalMetadata);  // Destroy global metadata list
    m_papszGlobalMetadata = nullptr;
}

/**
 * Reads the first and last N elements from a 1D HDF5 dataset and returns
 * them as a formatted string. Handles doubles and strings.
 */
static std::string Read1DArraySummary(hid_t hGroup, const char *pszDsetName,
                                      std::string &sUnitsOut, int nHeadTail = 3)
{
    hid_t hDset = -1, hSpace = -1, hMemSpace = -1, hDtype = -1,
          hNativeDtype = -1;
    std::string sResult = "";
    sUnitsOut = "";  // Initialize output parameter
    H5T_class_t type_class = H5T_NO_CLASS;

    // Suppress HDF5 errors for this operation, as the dataset might not exist
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hDset = H5Dopen2(hGroup, pszDsetName, H5P_DEFAULT);
    if (hDset < 0)
    {
        goto cleanup;  // Not found
    }

    sUnitsOut = ReadH5StringAttribute(hDset, "units");  // Read units early

    hSpace = H5Dget_space(hDset);
    hDtype = H5Dget_type(hDset);

    if (hSpace < 0 || hDtype < 0)
    {
        goto cleanup;  // Failed to get space or type
    }

    // Get Native Type and Class
    hNativeDtype = H5Tget_native_type(hDtype, H5T_DIR_ASCEND);
    if (hNativeDtype < 0)
        goto cleanup;  // Add check in case native type fails
    type_class = H5Tget_class(hNativeDtype);
    // ---

    if (H5Sget_simple_extent_ndims(hSpace) != 1)
    {
        sResult = "(non-1D array)";
    }
    else
    {
        hsize_t dims[1];
        H5Sget_simple_extent_dims(hSpace, dims, nullptr);
        hsize_t nTotalSize = dims[0];

        if (nTotalSize == 0)
        {
            sResult = "(empty)";
        }
        //Handle STRING type
        else if (type_class == H5T_STRING)
        {
            bool bIsVariable = H5Tis_variable_str(hNativeDtype);
            size_t nFixedSize = 0;
            if (!bIsVariable)
            {
                nFixedSize = H5Tget_size(hNativeDtype);
                if (nFixedSize == 0)
                {
                    sResult = "(invalid fixed string size)";
                    goto cleanup;
                }
            }

            // If the array is small, read and print all elements
            if (nTotalSize <= static_cast<hsize_t>(nHeadTail * 2))
            {
                if (bIsVariable)
                {
                    char **pszVals =
                        (char **)CPLMalloc(nTotalSize * sizeof(char *));
                    if (pszVals && H5Dread(hDset, hNativeDtype, H5S_ALL,
                                           H5S_ALL, H5P_DEFAULT, pszVals) >= 0)
                    {
                        for (hsize_t i = 0; i < nTotalSize; ++i)
                        {
                            sResult +=
                                (i == 0 ? "" : ", ") +
                                std::string(pszVals[i] ? pszVals[i] : "");
                        }
                        // Free variable length strings allocated by HDF5
                        H5Dvlen_reclaim(hNativeDtype, hSpace, H5P_DEFAULT,
                                        pszVals);
                    }
                    CPLFree(pszVals);
                }
                else
                {  // Fixed length
                    char *pszBuffer =
                        (char *)VSI_MALLOC_VERBOSE(nTotalSize * nFixedSize);
                    if (pszBuffer &&
                        H5Dread(hDset, hNativeDtype, H5S_ALL, H5S_ALL,
                                H5P_DEFAULT, pszBuffer) >= 0)
                    {
                        for (hsize_t i = 0; i < nTotalSize; ++i)
                        {
                            // Extract fixed-size string, ensure null termination
                            std::string sTemp(pszBuffer + i * nFixedSize,
                                              nFixedSize);
                            sResult += (i == 0 ? "" : ", ") + sTemp;
                        }
                    }
                    VSIFree(pszBuffer);
                }
            }
            // If the array is large, read head and tail
            else
            {
                hsize_t mem_dims[1] = {static_cast<hsize_t>(nHeadTail)};
                hMemSpace = H5Screate_simple(1, mem_dims, nullptr);
                hsize_t offset[1] = {0};
                hsize_t count[1] = {static_cast<hsize_t>(nHeadTail)};

                std::vector<std::string> s_head(nHeadTail);
                std::vector<std::string> s_tail(nHeadTail);

                // Read head
                H5Sselect_hyperslab(hSpace, H5S_SELECT_SET, offset, nullptr,
                                    count, nullptr);
                if (bIsVariable)
                {
                    char **pszValsHead =
                        (char **)CPLMalloc(nHeadTail * sizeof(char *));
                    if (pszValsHead &&
                        H5Dread(hDset, hNativeDtype, hMemSpace, hSpace,
                                H5P_DEFAULT, pszValsHead) >= 0)
                    {
                        for (int i = 0; i < nHeadTail; ++i)
                            s_head[i] = std::string(
                                pszValsHead[i] ? pszValsHead[i] : "");
                        H5Dvlen_reclaim(hNativeDtype, hMemSpace, H5P_DEFAULT,
                                        pszValsHead);
                    }
                    CPLFree(pszValsHead);
                }
                else
                {  // Fixed length
                    char *pszBufferHead =
                        (char *)VSI_MALLOC_VERBOSE(nHeadTail * nFixedSize);
                    if (pszBufferHead &&
                        H5Dread(hDset, hNativeDtype, hMemSpace, hSpace,
                                H5P_DEFAULT, pszBufferHead) >= 0)
                    {
                        for (int i = 0; i < nHeadTail; ++i)
                            s_head[i] = std::string(
                                pszBufferHead + i * nFixedSize, nFixedSize);
                    }
                    VSIFree(pszBufferHead);
                }

                // Read tail
                offset[0] = nTotalSize - nHeadTail;
                H5Sselect_hyperslab(hSpace, H5S_SELECT_SET, offset, nullptr,
                                    count, nullptr);
                if (bIsVariable)
                {
                    char **pszValsTail =
                        (char **)CPLMalloc(nHeadTail * sizeof(char *));
                    if (pszValsTail &&
                        H5Dread(hDset, hNativeDtype, hMemSpace, hSpace,
                                H5P_DEFAULT, pszValsTail) >= 0)
                    {
                        for (int i = 0; i < nHeadTail; ++i)
                            s_tail[i] = std::string(
                                pszValsTail[i] ? pszValsTail[i] : "");
                        H5Dvlen_reclaim(hNativeDtype, hMemSpace, H5P_DEFAULT,
                                        pszValsTail);
                    }
                    CPLFree(pszValsTail);
                }
                else
                {  // Fixed length
                    char *pszBufferTail =
                        (char *)VSI_MALLOC_VERBOSE(nHeadTail * nFixedSize);
                    if (pszBufferTail &&
                        H5Dread(hDset, hNativeDtype, hMemSpace, hSpace,
                                H5P_DEFAULT, pszBufferTail) >= 0)
                    {
                        for (int i = 0; i < nHeadTail; ++i)
                            s_tail[i] = std::string(
                                pszBufferTail + i * nFixedSize, nFixedSize);
                    }
                    VSIFree(pszBufferTail);
                }

                // Format the string
                for (int i = 0; i < nHeadTail; ++i)
                {
                    sResult += (i == 0 ? "" : ", ") + s_head[i];
                }
                sResult += " ...";
                for (int i = 0; i < nHeadTail; ++i)
                {
                    sResult += ", " + s_tail[i];
                }
            }
        }
        //Handle NUMERIC types (reads as double)
        else if (type_class == H5T_FLOAT || type_class == H5T_INTEGER)
        {
            // If the array is small, just read and print all elements
            if (nTotalSize <= static_cast<hsize_t>(nHeadTail * 2))
            {
                std::vector<double> v(nTotalSize);
                if (H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                            H5P_DEFAULT, v.data()) >= 0)
                {
                    for (size_t i = 0; i < v.size(); ++i)
                    {
                        sResult +=
                            CPLSPrintf(i == 0 ? "%.10g" : ", %.10g", v[i]);
                    }
                }
                else
                {
                    sResult = "(read error)";
                }
            }
            // If the array is large, read head and tail
            else
            {
                std::vector<double> v_head(nHeadTail);
                std::vector<double> v_tail(nHeadTail);
                hsize_t mem_dims[1] = {static_cast<hsize_t>(nHeadTail)};
                hMemSpace = H5Screate_simple(1, mem_dims, nullptr);

                // Read head
                hsize_t offset[1] = {0};
                hsize_t count[1] = {static_cast<hsize_t>(nHeadTail)};
                H5Sselect_hyperslab(hSpace, H5S_SELECT_SET, offset, nullptr,
                                    count, nullptr);
                herr_t head_status =
                    H5Dread(hDset, H5T_NATIVE_DOUBLE, hMemSpace, hSpace,
                            H5P_DEFAULT, v_head.data());

                // Read tail
                offset[0] = nTotalSize - nHeadTail;
                H5Sselect_hyperslab(hSpace, H5S_SELECT_SET, offset, nullptr,
                                    count, nullptr);
                herr_t tail_status =
                    H5Dread(hDset, H5T_NATIVE_DOUBLE, hMemSpace, hSpace,
                            H5P_DEFAULT, v_tail.data());

                if (head_status >= 0 && tail_status >= 0)
                {
                    // Format the string
                    for (int i = 0; i < nHeadTail; ++i)
                    {
                        sResult +=
                            CPLSPrintf(i == 0 ? "%.10g" : ", %.10g", v_head[i]);
                    }
                    sResult += " ...";
                    for (int i = 0; i < nHeadTail; ++i)
                    {
                        sResult += CPLSPrintf(", %.10g", v_tail[i]);
                    }
                }
                else
                {
                    sResult = "(read error)";
                }
            }
        }
        else  // --- Handle other types ---
        {
            sResult = "(unsupported data type)";
        }
    }

cleanup:
    // Restore errors first
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Close HDF5 handles
    if (hMemSpace >= 0)
        H5Sclose(hMemSpace);
    if (hNativeDtype >= 0)
        H5Tclose(hNativeDtype);
    if (hDtype >= 0)
        H5Tclose(hDtype);
    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hDset >= 0)
        H5Dclose(hDset);

    return sResult;
}

/************************************************************************/
/*                          GetMetadataDomainList()                     */
/************************************************************************/

char **NisarDataset::GetMetadataDomainList()
{
    CPLDebug("NISAR_DRIVER", "GetMetadataDomainList called");
    char **papszDomains = GDALPamDataset::GetMetadataDomainList();
    papszDomains =
        CSLAddString(papszDomains, "NISAR_GLOBAL");  // Add our global domain
    // Add SUBDATASETS if populated by Open
    if (papszSubDatasets != nullptr)
        papszDomains = CSLAddString(papszDomains, "SUBDATASETS");

    // Add DERIVED_SUBDATASETS if this is a raster dataset (not container)
    // and the type is numeric.
    // hDataset is > 0 for a specific raster, eDataType is set in Open()
    if (hDataset >= 0 && (eDataType > GDT_Unknown && eDataType < GDT_CInt16))
    {
        papszDomains = CSLAddString(papszDomains, "DERIVED_SUBDATASETS");
    }

    return papszDomains;
}

// Callback function for H5Literate to read metadata from scalar datasets
static herr_t NISAR_DatasetMetadataCallback(hid_t hGroup, const char *pszName,
                                            const H5L_info2_t * /*pLinkInfo*/,
                                            void *pOpData)
{
    NISAR_AttrCallbackData *data =
        static_cast<NISAR_AttrCallbackData *>(pOpData);

    hid_t hDset = -1, hDtype = -1, hSpace = -1;
    std::string value_str;
    std::string units_str;

    // Suppress errors for this operation, as some objects aren't datasets
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hid_t hObject = H5Oopen(hGroup, pszName, H5P_DEFAULT);
    if (hObject < 0)
        goto cleanup;

    H5O_info2_t oinfo;
    if (H5Oget_info(hObject, &oinfo, H5O_INFO_BASIC) < 0 ||
        oinfo.type != H5O_TYPE_DATASET)
    {
        H5Oclose(hObject);
        goto cleanup;  // Skip non-datasets (like groups)
    }

    hDset = hObject;  // It's a dataset, so we can use the object ID
    hDtype = H5Dget_type(hDset);
    hSpace = H5Dget_space(hDset);

    if (H5Sget_simple_extent_type(hSpace) == H5S_SCALAR)
    {
        switch (H5Tget_class(hDtype))
        {
            case H5T_INTEGER:
            {
                long long val = 0;
                if (H5Dread(hDset, H5T_NATIVE_LLONG, H5S_ALL, H5S_ALL,
                            H5P_DEFAULT, &val) >= 0)
                    value_str = std::to_string(val);
                break;
            }
            case H5T_FLOAT:
            {
                double val = 0.0;
                if (H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL,
                            H5P_DEFAULT, &val) >= 0)
                    value_str = CPLSPrintf("%.10g", val);
                break;
            }
            case H5T_STRING:
            {
                if (H5Tis_variable_str(hDtype) > 0)
                {
                    char *pszVal = nullptr;
                    if (H5Dread(hDset, hDtype, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                                &pszVal) >= 0 &&
                        pszVal)
                    {
                        value_str = pszVal;
                        H5free_memory(pszVal);
                    }
                }
                else
                {
                    size_t nSize = H5Tget_size(hDtype);
                    if (nSize > 0)
                    {
                        char *pszVal = (char *)CPLMalloc(nSize + 1);
                        if (H5Dread(hDset, hDtype, H5S_ALL, H5S_ALL,
                                    H5P_DEFAULT, pszVal) >= 0)
                        {
                            pszVal[nSize] = '\0';
                            value_str = pszVal;
                        }
                        CPLFree(pszVal);
                    }
                }
                break;
            }
            default:
                break;
        }

        // READ THE 'units' ATTRIBUTE
        //if (!value_str.empty()) {
        //    units_str = ReadH5StringAttribute(hDset, "units");
        //    if (!units_str.empty() && !EQUAL(units_str.c_str(), "unitless") && !EQUAL(units_str.c_str(), "1")) {
        //        value_str += " " + units_str; // Append the unit
        //    }
        //}
    }

    if (!value_str.empty())
    {
        std::string prefix = data->pszPrefix;
        if (!prefix.empty() && prefix.back() != '/')
        {
            prefix += "/";
        }
        std::string finalKey = prefix + pszName;

        // READ THE 'units' ATTRIBUTE and append to the key if it exists
        units_str = ReadH5StringAttribute(hDset, "units");
        if (!units_str.empty() && !EQUAL(units_str.c_str(), "unitless") &&
            !EQUAL(units_str.c_str(), "1"))
        {
            finalKey += " (" + units_str + ")";  // Append units to the key
        }

        *(data->ppapszList) = CSLSetNameValue(
            *(data->ppapszList), finalKey.c_str(), value_str.c_str());
    }

cleanup:
    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hDtype >= 0)
        H5Tclose(hDtype);
    if (hDset >= 0)
        H5Dclose(hDset);
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore errors
    return 0;
}

/************************************************************************/
/*                            GetMetadata()                             */
/************************************************************************/

char **NisarDataset::GetMetadata(const char *pszDomain)

{
    // Handle NISAR_GLOBAL Domain (Cached)
    if (pszDomain != nullptr && EQUAL(pszDomain, "NISAR_GLOBAL"))
    {
        std::lock_guard<std::mutex> lock(m_GlobalMetadataMutex);
        CPLDebug("NISAR_DRIVER",
                 "GetMetadata called for NISAR_GLOBAL domain. Cached=%d",
                 m_bGotGlobalMetadata);
        if (m_bGotGlobalMetadata)
        {
            return CSLDuplicate(m_papszGlobalMetadata);  // Return copy
        }
        m_bGotGlobalMetadata = true;
        CSLDestroy(m_papszGlobalMetadata);
        m_papszGlobalMetadata = nullptr;

        if (this->hHDF5 >= 0)
        {
            // Read attributes directly from the root group (file handle)
            CPLDebug("NISAR_DRIVER", "Reading metadata from root group ('/') "
                                     "for NISAR_GLOBAL domain.");
            NISAR_AttrCallbackData callback_data;
            callback_data.ppapszList = &m_papszGlobalMetadata;
            callback_data.pszPrefix = "";
            hsize_t idx = 0;
            // Iterate attributes on the root group (represented by the file handle)
            H5Aiterate2(this->hHDF5, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                        NISAR_AttributeCallback, &callback_data);
        }
        else
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "Cannot read NISAR_GLOBAL metadata: HDF5 file not open.");
        }
        CPLDebug("NISAR_DRIVER",
                 "Finished reading for NISAR_GLOBAL. Found %d items.",
                 CSLCount(m_papszGlobalMetadata));
        return CSLDuplicate(m_papszGlobalMetadata);  // Return copy
    }

    // Handle SUBDATASETS Domain
    if (pszDomain != nullptr && EQUAL(pszDomain, "SUBDATASETS"))
    {
        return CSLDuplicate(papszSubDatasets);  // Return copy
    }

    // Handle Default Domain ("" or nullptr)
    if (pszDomain == nullptr || EQUAL(pszDomain, ""))
    {
        std::lock_guard<std::mutex> lock(m_MetadataMutex);
        if (m_bGotMetadata)
        {
            return GDALPamDataset::GetMetadata(pszDomain);
        }
        m_bGotMetadata = true;
        CPLDebug("NISAR_DRIVER",
                 "GetMetadata('') attempting to load/merge HDF5 attributes.");
        TryLoadXML();

        char **papszHDFMetadata = nullptr;

        // Case 1: Level 1 Product (has GCPs)
        //if (GetGCPCount() > 0)
        if (m_bIsLevel1)
        {
            CPLDebug(
                "NISAR_DRIVER",
                "L1 product detected. Reading attributes and swath metadata.");
            std::string datasetPath = get_hdf5_object_name(this->hDataset);

            // Read scalar and array summary datasets from the parent 'swaths' group
            std::string productRootPath;
            std::string frequencyGroup = "frequencyA";  // Default

            // Find the product's root path (e.g., "/science/LSAR/RSLC")
            // m_sInst ("LSAR" or "SSAR") and m_sProductType ("RSLC", etc.)
            // were set during NisarDataset::Open()
            if (m_sInst.empty() || m_sProductType.empty())
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                "Dataset product info not set. Cannot find L1 metadata.");
            }
            else
            {
                productRootPath = "/science/" + m_sInst + "/" + m_sProductType;
            }

            if (!productRootPath.empty())
            {
                if (datasetPath.find("/frequencyB/") != std::string::npos)
                {
                    frequencyGroup = "frequencyB";
                }
                std::string swathBasePath =
                    productRootPath + "/swaths/" + frequencyGroup + "/";

                // List of SCALAR datasets to read
                const std::vector<std::string> scalar_datasets_to_read = {
                    "acquiredCenterFrequency",
                    "acquiredRangeBandwidth",
                    "listOfPolarizations",
                    "nominalAcquisitionPRF",
                    "numberOfSubSwaths",
                    "processedAzimuthBandwidth",
                    "processedCenterFrequency",
                    "processedRangeBandwidth",
                    "sceneCenterAlongTrackSpacing",
                    "sceneCenterGroundRangeSpacing",
                    "slantRangeSpacing"};

                // List of 1D ARRAY datasets to summarize
                const std::vector<std::string> array_datasets_to_summarize = {
                    "slantRange"
                    // We omit validSamples... datasets as they are 2D
                };

                // Open the parent swath group once
                hid_t hSwathGroup =
                    H5Gopen2(this->hHDF5, swathBasePath.c_str(), H5P_DEFAULT);
                if (hSwathGroup >= 0)
                {
                    CPLDebug("NISAR_DRIVER", "Reading scalar metadata from %s",
                             swathBasePath.c_str());

                    // Suppress errors for datasets that might be missing
                    H5E_auto2_t old_func;
                    void *old_client_data;
                    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
                    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

                    // Read scalar datasets
                    for (const auto &dset_name : scalar_datasets_to_read)
                    {
                        NISAR_AttrCallbackData callback_data;
                        callback_data.ppapszList = &papszHDFMetadata;
                        callback_data.pszPrefix = swathBasePath.c_str();
                        NISAR_DatasetMetadataCallback(hSwathGroup,
                                                      dset_name.c_str(),
                                                      nullptr, &callback_data);
                    }

                    // Read and summarize 1D array datasets
                    CPLDebug("NISAR_DRIVER",
                             "Reading 1D array summaries from %s",
                             swathBasePath.c_str());
                    for (const auto &dset_name : array_datasets_to_summarize)
                    {
                        std::string sUnits;
                        std::string summary = Read1DArraySummary(
                            hSwathGroup, dset_name.c_str(), sUnits);
                        if (!summary.empty())
                        {
                            std::string key = swathBasePath + dset_name;
                            // Add units to the key, just like in the scalar callback
                            if (!sUnits.empty() &&
                                !EQUAL(sUnits.c_str(), "unitless") &&
                                !EQUAL(sUnits.c_str(), "1"))
                            {
                                key += " (" + sUnits + ")";
                            }
                            papszHDFMetadata = CSLSetNameValue(
                                papszHDFMetadata, key.c_str(), summary.c_str());
                        }
                    }

                    H5Eset_auto2(H5E_DEFAULT, old_func,
                                 old_client_data);  // Restore errors
                    H5Gclose(
                        hSwathGroup);  // Close the group once after the loop
                }
            }
        }
        // Case 2: Level 2 Product (no GCPs, has a raster dataset open)
        else if (this->hDataset >= 0)
        {
            std::string datasetPath = get_hdf5_object_name(this->hDataset);

            // Determine the parent group path (e.g., /science/LSAR/GCOV/grids/frequencyA/ or /science/LSAR/GUNW/.../HH/)
            std::string gridsBasePath;
            size_t lastSlashPos = datasetPath.find_last_of('/');
            if (lastSlashPos != std::string::npos)
            {
                gridsBasePath = datasetPath.substr(
                    0, lastSlashPos + 1);  // Include trailing slash
            }
            else
            {
                CPLError(
                    CE_Warning, CPLE_AppDefined,
                    "Could not determine parent group path for L2 metadata.");
                gridsBasePath = "";  // Or handle error appropriately
            }

            if (!gridsBasePath.empty())
            {
                CPLDebug("NISAR_DRIVER",
                         "L2 dataset detected. Reading extended metadata "
                         "relative to: %s",
                         gridsBasePath.c_str());

                // Read Scalar Datasets
                const std::vector<std::string> scalar_datasets_to_read = {
                    //"xCoordinateSpacing",
                    //"yCoordinateSpacing",
                    "numberOfSubSwaths"  // May not exist for all L2
                    // Add others if needed
                };

                // Read 1D Array Datasets
                const std::vector<std::string> array_datasets_to_summarize = {
                    "listOfCovarianceTerms",
                    "listOfPolarizations"  // May not exist for all L2
                    //"xCoordinates",
                    //"yCoordinates"
                };

                // Open the parent grids group once for scalar/array reads
                hid_t hGridsGroup =
                    H5Gopen2(this->hHDF5, gridsBasePath.c_str(), H5P_DEFAULT);
                if (hGridsGroup >= 0)
                {
                    H5E_auto2_t old_func_dset;
                    void *old_client_data_dset;
                    H5Eget_auto2(H5E_DEFAULT, &old_func_dset,
                                 &old_client_data_dset);
                    H5Eset_auto2(
                        H5E_DEFAULT, nullptr,
                        nullptr);  // Suppress errors for missing datasets

                    // Read scalar datasets relative to the group
                    CPLDebug("NISAR_DRIVER", "Reading scalar datasets from %s",
                             gridsBasePath.c_str());
                    for (const auto &dset_name : scalar_datasets_to_read)
                    {
                        NISAR_AttrCallbackData callback_data;
                        callback_data.ppapszList = &papszHDFMetadata;
                        callback_data.pszPrefix = gridsBasePath.c_str();
                        NISAR_DatasetMetadataCallback(hGridsGroup,
                                                      dset_name.c_str(),
                                                      nullptr, &callback_data);
                    }

                    // Read and summarize 1D array datasets relative to the group
                    CPLDebug("NISAR_DRIVER",
                             "Reading 1D array summaries from %s",
                             gridsBasePath.c_str());
                    for (const auto &dset_name : array_datasets_to_summarize)
                    {
                        std::string sUnits;
                        // Assumes Read1DArraySummary takes group handle
                        std::string summary = Read1DArraySummary(
                            hGridsGroup, dset_name.c_str(), sUnits);
                        if (!summary.empty())
                        {
                            std::string key = gridsBasePath + dset_name;
                            // Add units to the key
                            if (!sUnits.empty() &&
                                !EQUAL(sUnits.c_str(), "unitless") &&
                                !EQUAL(sUnits.c_str(), "1"))
                            {
                                key += " (" + sUnits + ")";
                            }
                            papszHDFMetadata = CSLSetNameValue(
                                papszHDFMetadata, key.c_str(), summary.c_str());
                        }
                    }

                    H5Eset_auto2(H5E_DEFAULT, old_func_dset,
                                 old_client_data_dset);  // Restore errors
                    H5Gclose(hGridsGroup);
                }
                else
                {
                    CPLError(CE_Warning, CPLE_OpenFailed,
                             "Could not open grids group for metadata: %s",
                             gridsBasePath.c_str());
                }
            }
        }

        // Case 3: Container Dataset (no raster bands open)
        else
        {
            NISAR_AttrCallbackData callback_data;
            callback_data.ppapszList = &papszHDFMetadata;
            hsize_t idx = 0;

            // Iteration 1: Read attributes from the root group
            CPLDebug("NISAR_DRIVER", "Reading metadata from root group ('/').");
            callback_data.pszPrefix = "";  // Use an empty prefix for the root
            H5Aiterate2(this->hHDF5, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                        NISAR_AttributeCallback, &callback_data);
            // For a container, m_sInst might not be set.
            // We must find the correct identification group.
            std::string sIdentPath;
            if (H5Lexists(this->hHDF5, "/science/LSAR/identification", H5P_DEFAULT) > 0)
            {
                sIdentPath = "/science/LSAR/identification";
            }
            else if (H5Lexists(this->hHDF5, "/science/SSAR/identification", H5P_DEFAULT) > 0)
            {
                sIdentPath = "/science/SSAR/identification";
            }

            if (!sIdentPath.empty())
            {
                CPLDebug("NISAR_DRIVER",
                         "Reading default metadata from root group ('%s') for container dataset.",
                          sIdentPath.c_str());
                hid_t hIdentGroup = H5Gopen2(this->hHDF5, sIdentPath.c_str(), H5P_DEFAULT);
                if (hIdentGroup >= 0)
                {
                    callback_data.pszPrefix = sIdentPath.c_str();
                    idx = 0; //Reset the iterator index
                    H5Literate(hIdentGroup, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                        NISAR_DatasetMetadataCallback, &callback_data);

                    // Clean up the handle
                    H5Gclose(hIdentGroup);
                }
            }
            else
            {
                CPLDebug("NISAR_DRIVER", "No identification group found for container dataset.");
            }
        }

        // Now, merge the collected HDF5 attributes into PAM
        if (papszHDFMetadata != nullptr)
        {
            CPLDebug("NISAR_DRIVER",
                     "Merging %d HDF5 attributes into PAM default domain.",
                     CSLCount(papszHDFMetadata));
            SetMetadata(papszHDFMetadata);
            CSLDestroy(papszHDFMetadata);
        }

        return GDALPamDataset::GetMetadata(pszDomain);
    }  // End default domain handling

    // Handle other domains via PAM
    TryLoadXML();
    return GDALPamDataset::GetMetadata(pszDomain);
}

// ====================================================================
// METADATA IMPLEMENTATION
// ====================================================================

// Structure to pass state into the H5Ovisit callback
struct NISAR_MetadataVisitData
{
    NisarDataset* poDS;
    char** papszMetadata; // CSL list being built
    std::string sPrefix;  // Optional prefix if needed
};

// Callback for H5Ovisit to recursively read metadata
herr_t NisarDataset::MetadataVisitCallback(hid_t hGroup, const char *name, const H5O_info2_t *info, void *op_data)
{
    NISAR_MetadataVisitData* data = static_cast<NISAR_MetadataVisitData*>(op_data);
    
    if (EQUAL(name, ".")) return 0;
    if (info->type != H5O_TYPE_DATASET) return 0; 

    // Identify Key Name early to skip expensive reads
    std::string sKey = name;
    // Normalize path separators to underscores for the key
    std::replace(sKey.begin(), sKey.end(), '/', '_');

    // FILTER: Skip massive configuration dumps
    if (sKey.find("runConfigurationContents") != std::string::npos) {
        return 0; // Skip entirely
    }

    hid_t hDset = H5Dopen2(hGroup, name, H5P_DEFAULT);
    if (hDset < 0) return 0;

    hid_t hSpace = H5Dget_space(hDset);
    int nRank = H5Sget_simple_extent_ndims(hSpace);
    hid_t hType = H5Dget_type(hDset);
    H5T_class_t type_class = H5Tget_class(hType);
    
    bool bShouldRead = false;

    // Logic: Read Scalars and 1-element Strings
    if (nRank == 0) {
        bShouldRead = true; 
    }
    else if (type_class == H5T_STRING && nRank <= 1) {
        if (nRank == 1) {
            hsize_t dims[1];
            H5Sget_simple_extent_dims(hSpace, dims, nullptr);
            if (dims[0] == 1) bShouldRead = true; 
        } else {
             bShouldRead = true;
        }
    }

    if (bShouldRead)
    {
        std::string sValue;

        if (type_class == H5T_STRING && nRank == 1) {
            sValue = data->poDS->ReadHDF5StringArrayAsList(hGroup, name);
        }
        else {
             sValue = data->poDS->ReadHDF5StringDataset(hGroup, name);
        }
        
        if (sValue.empty() && type_class != H5T_STRING) {
            double dfVal;
            if (H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &dfVal) >= 0) {
                sValue = CPLSPrintf("%.15g", dfVal);
            }
        }

        if (!sValue.empty())
        {
            // SANITIZE: Remove Newlines
            if (sValue.find('\n') != std::string::npos || sValue.find('\r') != std::string::npos) {
                std::replace(sValue.begin(), sValue.end(), '\n', ' ');
                std::replace(sValue.begin(), sValue.end(), '\r', ' ');
            }

            // START UNIT READING BLOCK
            std::string sUnits = ReadH5StringAttribute(hDset, "units");
            if (!sUnits.empty() && 
                !EQUAL(sUnits.c_str(), "unitless") && 
                !EQUAL(sUnits.c_str(), "1"))
            {
                sKey += " (" + sUnits + ")";
            }
            // END UNIT READING BLOCK

            data->papszMetadata = CSLSetNameValue(data->papszMetadata, sKey.c_str(), sValue.c_str());
        }
    }

    if (hType >= 0) H5Tclose(hType);
    if (hSpace >= 0) H5Sclose(hSpace);
    if (hDset >= 0) H5Dclose(hDset);

    return 0; 
}

void NisarDataset::InitializeMetadataMap()
{
    // Only map if product type is known
    if (!m_sInst.empty() && !m_sProductType.empty()) {
        m_oMetadataMap["ATTITUDE"]     = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/attitude", "NISAR_ATTITUDE" };
        m_oMetadataMap["CALIBRATIONINFORMATION"]  = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/calibrationInformation", "NISAR_CALIBRATIONINFORMATION" };
        m_oMetadataMap["CEOSANALYSISREADYDATA"]     = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/ceosAnalysisReadyData", "NISAR_CEOSANALYSISREADYDATA" };
        m_oMetadataMap["ORBIT"]        = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/orbit", "NISAR_ORBIT" };
        m_oMetadataMap["PROCESSINGINFORMATION"]   = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/processingInformation", "NISAR_PROCESSINGINFORMATION" };
        m_oMetadataMap["RADARGRID"]   = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/radarGrid", "NISAR_RADARGRID" };
        m_oMetadataMap["SOURCEDATA"]       = { "/science/" + m_sInst + "/" + m_sProductType + "/metadata/sourceData", "NISAR_SOURCEDATA" };
    }
}

void NisarDataset::LoadMetadataDomain(const std::string& sKeyword)
{
    auto it = m_oMetadataMap.find(sKeyword);
    if (it == m_oMetadataMap.end()) {
        CPLDebug("NISAR_DRIVER", "Requested metadata keyword '%s' not recognized.", sKeyword.c_str());
        return;
    }

    const std::string& sH5Path = it->second.sHDF5Path;
    const std::string& sDomain = it->second.sGDALDomain;

    // Suppress errors temporarily in case the group doesn't exist
    H5E_auto2_t old_func; void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hid_t hGroup = H5Gopen2(hHDF5, sH5Path.c_str(), H5P_DEFAULT);
    
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore

    if (hGroup < 0) {
        CPLDebug("NISAR_DRIVER", "Metadata group not found: %s", sH5Path.c_str());
        return;
    }

    CPLDebug("NISAR_DRIVER", "Loading metadata domain: %s from %s", sDomain.c_str(), sH5Path.c_str());

    NISAR_MetadataVisitData visitData;
    visitData.poDS = this;
    visitData.papszMetadata = nullptr;

    // Recursively visit all objects in this group
    H5Ovisit(hGroup, H5_INDEX_NAME, H5_ITER_NATIVE, MetadataVisitCallback, &visitData, H5O_INFO_BASIC);

    // Apply the gathered list to the dataset
    if (visitData.papszMetadata) {
        SetMetadata(visitData.papszMetadata, sDomain.c_str());
        CSLDestroy(visitData.papszMetadata);
    }

    H5Gclose(hGroup);
}

/************************************************************************/
/*                                Open()                                */
/* This static method is responsible for opening a NISAR HDF5 file and  */
/* creating a NisarDataset object. It performs the following steps:    */
/* Checks if the file can be opened in read-only mode.                  */
/* Opens the HDF5 file using H5Fopen                                    */
/* Identifies the relevant HDF5 dataset(s) within the file.             */
/* Creates a NisarDataset object and populates its properties          */
/* (dimensions, data type, etc.) using HDF5 API calls.                  */
/* Creates raster bands (NisarRasterBand) for the dataset.             */
/************************************************************************/
// *********************************************************************
// ** IMPORTANT AWS AUTHENTICATION FOR ROS3 VFD:                      **
// ** This driver relies on the HDF5 ROS3 VFD picking up AWS          **
// ** credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,          **
// ** AWS_SESSION_TOKEN) and region (AWS_REGION or                    **
// ** HDF5_ROS3_AWS_REGION) from ENVIRONMENT VARIABLES.               **
// ** It does NOT directly read ~/.aws/credentials or handle profiles.**
// ** To use credentials from a profile (e.g., 'saml-pub'), ensure    **
// ** these environment variables are set correctly *before* running  **
// ** the GDAL command                                                **
// *********************************************************************
// ******************************************************************
// ** CRITICAL WARNING: AWS_SESSION_TOKEN IS *NOT* SUPPORTED       **
// ** by the H5FD_ros3_fapl_t struct in HDF5 <= 1.14.2             **
// ** If a session token is required (e.g., for SAML/IAM temp      **
// ** credentials), this configuration method WILL FAIL silently   **
// ** or with signature errors later.                              **
// ******************************************************************
GDALDataset *NisarDataset::Open(GDALOpenInfo *poOpenInfo)
{
    // Extract Filename and Subdataset Path
    const char *pszFullInput = poOpenInfo->pszFilename;
    const char *pszDataIdentifier = nullptr;  // Part after "NISAR:" prefix
    char *pszActualFilename =
        nullptr;  // Buffer for isolated filename (local path or /vsis3/ path)
    const char *pszSubdatasetPath =
        nullptr;  // Points to HDF5 path part, or NULL
    const char *pszPrefix = "NISAR:";
    size_t nPrefixLen = strlen(pszPrefix);

    // Check for and strip the driver prefix if present
    if (EQUALN(pszFullInput, pszPrefix, nPrefixLen))
    {
        pszDataIdentifier = pszFullInput + nPrefixLen;
        CPLDebug("NISAR_DRIVER",
                 "Identified 'NISAR:' prefix. Actual filename+subdataset: %s",
                 pszDataIdentifier);
    }
    else
    {
        // No "NISAR:" prefix. This means Identify() was successful.
        // The entire string is the data identifier (filename + optional subdataset).
        pszDataIdentifier = pszFullInput;
        CPLDebug("NISAR_DRIVER",
                 "No 'NISAR:' prefix found (Identify() succeeded). Using full string: %s",
                 pszDataIdentifier);
    }

    // Handle empty filename after prefix removal
    if (pszDataIdentifier == nullptr || pszDataIdentifier[0] == '\0')
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "Empty filename is provided after 'NISAR:' prefix in '%s'",
                 pszFullInput);
        return nullptr;
    }

    // Now, parse pszDataIdentifier to separate filename and HDF5 path.
    const char *pszLastColon = strrchr(pszDataIdentifier, ':');

    // Check if the last colon is part of a URI scheme (e.g., "s3://").
    // If it is, then there is no subdataset path provided in the string.
    if (pszLastColon != nullptr && pszLastColon[1] == '/' &&
        pszLastColon[2] == '/')
    {
        // The colon is part of a '://' scheme. The whole string is the filename.
        pszActualFilename = CPLStrdup(pszDataIdentifier);
        pszSubdatasetPath = nullptr;
        CPLDebug("NISAR_DRIVER",
                 "URI detected with no subdataset path. Filename: %s",
                 pszActualFilename);
    }
    // Check if a colon was found at all.
    else if (pszLastColon != nullptr)
    {
        // A colon was found, and it's not part of a '://' scheme.
        // Treat it as the subdataset separator.
        pszSubdatasetPath = pszLastColon + 1;
        size_t nFilenameLen = pszLastColon - pszDataIdentifier;
        pszActualFilename = (char *)CPLMalloc(nFilenameLen + 1);
        strncpy(pszActualFilename, pszDataIdentifier, nFilenameLen);
        pszActualFilename[nFilenameLen] = '\0';

        CPLDebug("NISAR_DRIVER", "HDF5 path specified. Filename: %s, Path: %s",
                 pszActualFilename, pszSubdatasetPath);
    }
    else
    {
        // No colon found at all. The whole string is the filename.
        pszActualFilename = CPLStrdup(pszDataIdentifier);
        pszSubdatasetPath = nullptr;
        CPLDebug("NISAR_DRIVER", "No colon separator found. Filename: %s",
                 pszActualFilename);
    }

    // Final check on extracted filename
    if (pszActualFilename == nullptr || pszActualFilename[0] == '\0')
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "Could not determine filename from input: %s", pszFullInput);
        CPLFree(pszActualFilename);  // Free if allocated by CPLMalloc above
        return nullptr;
    }

    CPLDebug("NISAR_DRIVER", "Parsed Filename/Path Part: %s",
             pszActualFilename);
    CPLDebug("NISAR_DRIVER", "Parsed HDF5 Subdataset Path: %s",
             pszSubdatasetPath ? pszSubdatasetPath : "(none specified)");

    // Basic check: Does it have an .h5 extension? (Weak check)
    if (!EQUAL(NisarGetExtension(pszActualFilename).c_str(), "h5"))
    {
        return nullptr;
    }

    // Open the HDF5 file and check for specific NISAR metadata/groups.
    // This requires opening the file. If the structure
    // isn't right, clean up and return nullptr.

    // Check Access Mode
    // This driver currently only supports reading.
    if (poOpenInfo->eAccess == GA_Update)
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "The NISAR driver does not support update access to existing "
                 "datasets.");
        CPLFree(pszActualFilename);
        return nullptr;
    }

    // Prepare Base FAPL & Filename for Pass 1
    hid_t fapl_id_pass1 = H5P_DEFAULT;
    const char *filenameForH5Fopen = pszActualFilename;
    std::string final_url_storage;
    bool bIsS3 = false;
    const char *s3_path_part = nullptr;
    bool bNeedToCloseFapl1 = false;

    // Check for GDAL S3 path OR direct S3 URL
    if (STARTS_WITH_CI(pszActualFilename, "/vsis3/"))
    {
        bIsS3 = true;
        CPLDebug("NISAR_DRIVER",
                 "Detected /vsis3/ path, configuring HDF5 ROS3 VFD.");
        // Translate /vsis3/bucket/key to s3://bucket/key for HDF5 ROS3 VFD
        s3_path_part = pszActualFilename + strlen("/vsis3/");
        if (*s3_path_part == '\0')
        { /* Error handling */
            CPLError(
                CE_Failure, CPLE_OpenFailed,
                "Invalid S3 path: missing bucket/key after /vsis3/ in '%s'",
                pszActualFilename);
            CPLFree(pszActualFilename);
            return nullptr;
        }
    }
    else if (STARTS_WITH_CI(pszActualFilename, "s3://"))
    {
        bIsS3 = true;
        CPLDebug("NISAR_DRIVER",
                 "Detected direct s3:// path, configuring HDF5 ROS3 VFD.");
        s3_path_part = pszActualFilename;  // Includes s3:// prefix initially
        // filenameForH5Fopen is already pszActualFilename (which starts with s3://)
    }
    else
    {
        CPLDebug("NISAR_DRIVER",
                 "Assuming local file path, using default HDF5 FAPL.");
    }

    // Configure FAPL for ROS3 if an S3 path was detected
    if (bIsS3)
    {

        fapl_id_pass1 = H5Pcreate(H5P_FILE_ACCESS);  // Create new FAPL

        if (fapl_id_pass1 < 0)
        {
            // Error handling
            CPLFree(pszActualFilename);
            return nullptr;
        }

        // Prepare ROS3 VFD Configuration Struct
        H5FD_ros3_fapl_t ros3_fapl_conf;
        // Initialize the struct. Using memset is safest.
        memset(&ros3_fapl_conf, 0, sizeof(H5FD_ros3_fapl_t));
        // Set the structure version (use HDF5 macro)
        ros3_fapl_conf.version = H5FD_CURR_ROS3_FAPL_T_VERSION;
        ros3_fapl_conf.authenticate = TRUE;  // Assume authentication needed

        const char *env_region = getenv("AWS_REGION");
        // In H5FDros3.h, the struct members are declared like: char aws_region[H5FD_ROS3_MAX_REGION_LEN + 1];
        // So, strncpy should use the full MAX_LEN. The struct already has space for null terminator.
 	if (env_region != nullptr && strlen(env_region) > 0)
        {
            strncpy(ros3_fapl_conf.aws_region, env_region,
                    H5FD_ROS3_MAX_REGION_LEN);
            ros3_fapl_conf.aws_region[H5FD_ROS3_MAX_REGION_LEN] =
                '\0';  //Ensure null termination
            CPLDebug("NISAR_DRIVER", "ROS3 Config: Using Region: %s",
                     ros3_fapl_conf.aws_region);
        }
        else
        {
            // cleanup and return
            CPLError(CE_Failure, CPLE_AppDefined,
                     "AWS_REGION environment variable not set, needed for HDF5 "
                     "ROS3 VFD / HTTPS URL.");
            H5Pclose(fapl_id_pass1);
            CPLFree(pszActualFilename);
            return nullptr;
        }

        // Get Credentials (Optional for struct, VFD might use env/role if missing)
        const char *env_key_id = getenv("AWS_ACCESS_KEY_ID");
        const char *env_secret = getenv("AWS_SECRET_ACCESS_KEY");

        if (env_key_id && *env_key_id)
        {
            strncpy(ros3_fapl_conf.secret_id, env_key_id,
                    H5FD_ROS3_MAX_SECRET_ID_LEN);
            ros3_fapl_conf.secret_id[H5FD_ROS3_MAX_SECRET_ID_LEN] = '\0';
            CPLDebug("NISAR_DRIVER",
                     "ROS3 Config: Setting Secret ID (Key ID) from env var.");
        }
        if (env_secret && *env_secret)
        {
            strncpy(ros3_fapl_conf.secret_key, env_secret,
                    H5FD_ROS3_MAX_SECRET_KEY_LEN);
            ros3_fapl_conf.secret_key[H5FD_ROS3_MAX_SECRET_KEY_LEN] = '\0';
            CPLDebug("NISAR_DRIVER",
                     "ROS3 Config: Setting Secret Key from env var.");
        }

        // Configure FAPL using H5Pset_fapl_ros3 ***
        herr_t status = H5Pset_fapl_ros3(fapl_id_pass1, &ros3_fapl_conf);
        if (status < 0)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass1);
            CPLFree(pszActualFilename);
            return nullptr;
        }
        CPLDebug("NISAR_DRIVER",
                 "Configured HDF5 FAPL using H5Pset_fapl_ros3.");

        //SET SESSION TOKEN (if available)
        const char *env_token =
            getenv("AWS_SESSION_TOKEN");  // Check if present
        if (env_token != nullptr && strlen(env_token) > 0)
        {
            CPLDebug("NISAR_DRIVER",
                     "AWS_SESSION_TOKEN found, attempting to set it on FAPL.");
            status = H5Pset_fapl_ros3_token(fapl_id_pass1, env_token);
            if (status < 0)
            {
                CPLError(CE_Failure, CPLE_AppDefined,
                         "H5Pset_fapl_ros3_token failed.");
                H5Eprint(H5E_DEFAULT, stderr);
                H5Pclose(fapl_id_pass1);
                CPLFree(pszActualFilename);
                return nullptr;
            }
            CPLDebug(
                "NISAR_DRIVER",
                "Successfully set session token using H5Pset_fapl_ros3_token.");
        }
        else
        {
            CPLDebug("NISAR_DRIVER",
                     "AWS_SESSION_TOKEN environment variable not set or empty. "
                     "Proceeding without setting token.");
        }
        // END TOKEN SETTING LOGIC

        // Construct HTTPS URL (Workaround for HDF5 1.14.6 scheme bug)
        const char *path_after_scheme = s3_path_part;
        if (STARTS_WITH_CI(s3_path_part, "s3://"))
        {
            path_after_scheme +=
                strlen("s3://");  // Skip the scheme part if present
        }
        // Now path_after_scheme points to "bucket/key..."

        std::string bucket_name;
        std::string object_key;
        // Parse bucketAndKey = "bucket/key/path..."
        const char *first_slash = strchr(path_after_scheme, '/');
        if (first_slash == nullptr)
        {  // Only bucket name? Invalid S3 path for object.
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Invalid S3 path: missing object key in '%s'",
                     pszActualFilename);
            // cleanup and return
            H5Pclose(fapl_id_pass1);
            CPLFree(pszActualFilename);
            return nullptr;
        }
        else
        {
            bucket_name.assign(path_after_scheme,
                               first_slash - path_after_scheme);
            object_key.assign(first_slash + 1);
        }
        if (bucket_name.empty())
        {  // Check for empty bucket name
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Invalid S3 path: empty bucket name parsed from '%s'",
                     path_after_scheme);
            H5Pclose(fapl_id_pass1);
            CPLFree(pszActualFilename);
            return nullptr;
        }

        // Construct HTTPS URL (basic version, no special endpoint handling)
        final_url_storage = "https://";
        final_url_storage += bucket_name;
        final_url_storage += ".s3.";
        final_url_storage += env_region;
        final_url_storage +=
            ".amazonaws.com/";  // Assuming standard AWS endpoint
        final_url_storage += object_key;

        filenameForH5Fopen =
            final_url_storage.c_str();  // Use the HTTPS URL for H5Fopen
        CPLDebug("NISAR_DRIVER", "Constructed HTTPS URL for HDF5: %s",
                 filenameForH5Fopen);
    }  // end of if(bIsS3)
    else
    {  //local file
        fapl_id_pass1 = H5P_DEFAULT;
        CPLDebug("NISAR_DRIVER",
                 "Assuming local file path, using default HDF5 FAPL.");
    }

    // Pass 1: Open, Get Page Size, Close
    hsize_t actual_page_size = 0;  // Store the result here
    hid_t hHDF5_pass1 = -1;
    hid_t fcpl_id = -1;

    CPLDebug("NISAR_DRIVER", "Attempting H5Fopen (Pass 1) to get page size: %s",
             filenameForH5Fopen);
    H5E_auto2_t old_func;
    void *old_client_data;  // Suppress errors
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    hHDF5_pass1 = H5Fopen(filenameForH5Fopen, H5F_ACC_RDONLY, fapl_id_pass1);
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore errors

    if (hHDF5_pass1 < 0)
    {
        CPLError(CE_Warning, CPLE_OpenFailed,
                 "H5Fopen failed (Pass 1) for '%s'. Cannot determine optimal "
                 "page buffer. Proceeding with defaults.",
                 filenameForH5Fopen);
        // Don't fail the whole open yet, just proceed without optimization.
        // Fallback: use a reasonable default page size guess for buffer calc later if needed
        actual_page_size = 4 * 1024;
    }
    else
    {
        // Pass 1 open succeeded, get FCPL and Page Size
        fcpl_id = H5Fget_create_plist(hHDF5_pass1);
        if (fcpl_id < 0)
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "H5Fget_create_plist failed (Pass 1). Using default page "
                     "size for buffer calculation.");
            actual_page_size = 4 * 1024;  // Fallback
        }
        else
        {
            if (H5Pget_file_space_page_size(fcpl_id, &actual_page_size) < 0)
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                         "H5Pget_file_space_page_size failed (Pass 1). Using "
                         "default page size.");
                actual_page_size = 4 * 1024;  // Fallback
            }
            else if (actual_page_size == 0)
            {
                CPLError(CE_Warning, CPLE_AppDefined,
                         "H5Pget_file_space_page_size returned 0 (Pass 1). "
                         "Using default page size.");
                actual_page_size = 4 * 1024;  // Fallback
            }
            H5Pclose(fcpl_id);  // Close FCPL handle
        }
        // Close the file from Pass 1
        H5Fclose(hHDF5_pass1);
        CPLDebug("NISAR_DRIVER",
                 "Determined actual file page size (or fallback): %lu bytes.",
                 (unsigned long)actual_page_size);
    }
    // We no longer need fapl_id_pass1, close if needed
    if (bNeedToCloseFapl1)
    {
        H5Pclose(fapl_id_pass1);
        bNeedToCloseFapl1 = false;
        fapl_id_pass1 = H5P_DEFAULT;
    }

    // Prepare Optimized FAPL for Pass 2
    hid_t fapl_id_pass2 = H5P_DEFAULT;
    bool bNeedToCloseFapl2 = false;

    // Only create optimized FAPL for S3 for now
    if (bIsS3)
    {

        fapl_id_pass2 = H5Pcreate(H5P_FILE_ACCESS);  // Create new FAPL

        if (fapl_id_pass2 < 0)
        {
            // Error handling
            CPLFree(pszActualFilename);
            bNeedToCloseFapl2 = true;
            return nullptr;
        }

        // Reconfigure ROS3 VFD Configuration settings
        H5FD_ros3_fapl_t ros3_fapl_conf;  // Reuse struct name
        // Initialize the struct. Using memset is safest.
        memset(&ros3_fapl_conf, 0, sizeof(H5FD_ros3_fapl_t));
        // Set the structure version (use HDF5 macro)
        ros3_fapl_conf.version = H5FD_CURR_ROS3_FAPL_T_VERSION;
        ros3_fapl_conf.authenticate = TRUE;  // Assume authentication needed

        const char *env_region = getenv("AWS_REGION");
        // In H5FDros3.h, the struct members are declared like: char aws_region[H5FD_ROS3_MAX_REGION_LEN + 1];
        // So, strncpy should use the full MAX_LEN. The struct already has space for null terminator.
        if (env_region != nullptr || strlen(env_region) > 0)
        {
            strncpy(ros3_fapl_conf.aws_region, env_region,
                    H5FD_ROS3_MAX_REGION_LEN);
            ros3_fapl_conf.aws_region[H5FD_ROS3_MAX_REGION_LEN] =
                '\0';  //Ensure null termination
            CPLDebug("NISAR_DRIVER", "ROS3 Config: Using Region: %s",
                     ros3_fapl_conf.aws_region);
        }
        else
        {
            // cleanup and return
            CPLError(CE_Failure, CPLE_AppDefined,
                     "AWS_REGION environment variable not set, needed for HDF5 "
                     "ROS3 VFD / HTTPS URL.");
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        }

        // Get Credentials (Optional for struct, VFD might use env/role if missing)
        const char *env_key_id = getenv("AWS_ACCESS_KEY_ID");
        const char *env_secret = getenv("AWS_SECRET_ACCESS_KEY");

        if (env_key_id && *env_key_id)
        {
            strncpy(ros3_fapl_conf.secret_id, env_key_id,
                    H5FD_ROS3_MAX_SECRET_ID_LEN);
            ros3_fapl_conf.secret_id[H5FD_ROS3_MAX_SECRET_ID_LEN] = '\0';
            CPLDebug("NISAR_DRIVER",
                     "ROS3 Config: Setting Secret ID (Key ID) from env var.");
        }
        if (env_secret && *env_secret)
        {
            strncpy(ros3_fapl_conf.secret_key, env_secret,
                    H5FD_ROS3_MAX_SECRET_KEY_LEN);
            ros3_fapl_conf.secret_key[H5FD_ROS3_MAX_SECRET_KEY_LEN] = '\0';
            CPLDebug("NISAR_DRIVER",
                     "ROS3 Config: Setting Secret Key from env var.");
        }

        // Configure FAPL using H5Pset_fapl_ros3 ***
        herr_t status = H5Pset_fapl_ros3(fapl_id_pass2, &ros3_fapl_conf);
        if (status < 0)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        }
        CPLDebug("NISAR_DRIVER",
                 "Configured HDF5 FAPL using H5Pset_fapl_ros3.");

        //SET SESSION TOKEN (if available)
        const char *env_token =
            getenv("AWS_SESSION_TOKEN");  // Check if present
        if (env_token != nullptr && strlen(env_token) > 0)
        {
            CPLDebug("NISAR_DRIVER",
                     "AWS_SESSION_TOKEN found, attempting to set it on FAPL.");
            status = H5Pset_fapl_ros3_token(fapl_id_pass2, env_token);
            if (status < 0)
            {
                CPLError(CE_Failure, CPLE_AppDefined,
                         "H5Pset_fapl_ros3_token failed.");
                H5Eprint(H5E_DEFAULT, stderr);
                H5Pclose(fapl_id_pass2);
                CPLFree(pszActualFilename);
                return nullptr;
            }
            CPLDebug(
                "NISAR_DRIVER",
                "Successfully set session token using H5Pset_fapl_ros3_token.");
        }
        else
        {
            CPLDebug("NISAR_DRIVER",
                     "AWS_SESSION_TOKEN environment variable not set or empty. "
                     "Proceeding without setting token.");
        }
        // END TOKEN SETTING LOGIC

        // Calculate and set optimized page buffer size based on actual_page_size
        if (actual_page_size > 0)
        {
            size_t target_buffer_bytes = 16 * 1024 * 1024;  // Target 16 MB
            unsigned int num_pages_in_buffer =
                (target_buffer_bytes + actual_page_size - 1) / actual_page_size;
            if (num_pages_in_buffer == 0)
                num_pages_in_buffer = 1;
            size_t page_buffer_bytes =
                num_pages_in_buffer * actual_page_size;  // Exact multiple
            CPLDebug("NISAR_DRIVER",
                     "Setting OPTIMIZED HDF5 page buffer: %u pages, Total=%lu "
                     "bytes.",
                     num_pages_in_buffer, (unsigned long)page_buffer_bytes);
            herr_t page_status =
                H5Pset_page_buffer_size(fapl_id_pass2, page_buffer_bytes, 0, 0);
            if (page_status < 0)
            {
                CPLError(
                    CE_Warning, CPLE_AppDefined,
                    "Failed to set optimized HDF5 page buffer size."); /* Continue anyway */
            }
        }
        else
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "Could not use actual page size for buffer calculation.");
        }

        status = H5Pset_fapl_ros3(fapl_id_pass2, &ros3_fapl_conf);
        if (status < 0)
        {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        }
        CPLDebug("NISAR_DRIVER",
                 "Re-configured FAPL for Pass 2 with ROS3 settings.");

        // Construct HTTPS URL (Workaround for HDF5 1.14.6 scheme bug)
        const char *path_after_scheme = s3_path_part;
        if (STARTS_WITH_CI(s3_path_part, "s3://"))
        {
            path_after_scheme +=
                strlen("s3://");  // Skip the scheme part if present
        }
        // Now path_after_scheme points to "bucket/key..."

        std::string bucket_name;
        std::string object_key;
        // Parse bucketAndKey = "bucket/key/path..."
        const char *first_slash = strchr(path_after_scheme, '/');
        if (first_slash == nullptr)
        {  // Only bucket name? Invalid S3 path for object.
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Invalid S3 path: missing object key in '%s'",
                     pszActualFilename);
            // cleanup and return
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        }
        else
        {
            bucket_name.assign(path_after_scheme,
                               first_slash - path_after_scheme);
            object_key.assign(first_slash + 1);
        }
        if (bucket_name.empty())
        {  // Check for empty bucket name
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Invalid S3 path: empty bucket name parsed from '%s'",
                     path_after_scheme);
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        }

        // Construct HTTPS URL (basic version, no special endpoint handling)
        final_url_storage = "https://";
        final_url_storage += bucket_name;
        final_url_storage += ".s3.";
        final_url_storage += env_region;
        final_url_storage +=
            ".amazonaws.com/";  // Assuming standard AWS endpoint
        final_url_storage += object_key;

        filenameForH5Fopen =
            final_url_storage.c_str();  // Use the HTTPS URL for H5Fopen
        CPLDebug("NISAR_DRIVER", "Constructed HTTPS URL for HDF5: %s",
                 filenameForH5Fopen);
    }
    else
    {  // Local file
        fapl_id_pass2 = H5P_DEFAULT;
        CPLDebug("NISAR_DRIVER", "Using default FAPL for Pass 2 (local file).");
    }

    // Pass 2: Open HDF5 file
    hid_t hHDF5 = -1;
    CPLDebug("NISAR_DRIVER", "Attempting H5Fopen (Pass 2) with optimized FAPL: %s",
             filenameForH5Fopen);
 
    // Suppress errors for this open attempt
    H5E_auto2_t old_func_pass2;
    void *old_client_data_pass2;
    H5Eget_auto2(H5E_DEFAULT, &old_func_pass2, &old_client_data_pass2);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    hHDF5 = H5Fopen(filenameForH5Fopen, H5F_ACC_RDONLY, fapl_id_pass2);
    H5Eset_auto2(H5E_DEFAULT, old_func_pass2, old_client_data_pass2); // Restore
 
    // Close FAPL for Pass 2, it's not needed after open
    if (bNeedToCloseFapl2)
    {
        H5Pclose(fapl_id_pass2);
        bNeedToCloseFapl2 = false;
    }
 
    if (hHDF5 < 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
              "H5Fopen failed (Pass 2) for '%s'.", filenameForH5Fopen);
        CPLFree(pszActualFilename);
        return nullptr;
    }
 
    CPLDebug("NISAR_DRIVER", "H5Fopen (Pass 2) successful.");
 
    // Create the NisarDataset object
    NisarDataset *poDS = nullptr;
    try
    {
        poDS = new NisarDataset();
    }
    catch (const std::bad_alloc &)
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
              "Failed to allocate NisarDataset object.");
        H5Fclose(hHDF5);
        CPLFree(pszActualFilename);
        return nullptr;
    }

    const char* pszMaskOpt = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "MASK");
    if (pszMaskOpt && !CPLTestBool(pszMaskOpt)) {
        poDS->m_bMaskEnabled = false;
        CPLDebug("NISAR_DRIVER", "Masking disabled by user request (-oo MASK=NO).");
    }
 
    // Assign handles and critical info to the dataset object
    // The NisarDataset destructor must handle closing/freeing these
    poDS->hHDF5 = hHDF5;
    poDS->pszFilename = pszActualFilename;
    pszActualFilename = nullptr; // Ownership transferred to poDS
 
    // Read identification metadata to populate m_sProductType, m_bIsLevel1, etc.
    // This MUST be done before the "Priority 2" logic below.
    poDS->ReadIdentificationMetadata();
 
     // Declare variables needed for dataset path logic
     const char *pathToOpen = nullptr;
     std::string sConstructedPath;

     // Determine which dataset to open.
     // Priority 1: A specific HDF5 path (NISAR:file.h5:/path/to/data)
     if (pszSubdatasetPath != nullptr)
     {
         pathToOpen = pszSubdatasetPath;
         CPLDebug("NISAR_DRIVER",
                  "Priority 1: Opening specified HDF5 dataset path: %s",
                  pathToOpen);
     }
     else
     {
         // Priority 2: Open Options (-oo INST=... -oo FREQ=... -oo POL=...)
         const char *pszInst = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "INST");
         const char *pszFreq = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "FREQ");
         const char *pszPol = CSLFetchNameValue(poOpenInfo->papszOpenOptions, "POL");

        if (pszInst || pszFreq || pszPol)
        {
            // Use defaults for any missing options
            if (pszInst == nullptr) pszInst = "LSAR";
            if (pszFreq == nullptr) pszFreq = "A";
            
            // We default POL *after* checking GCOV vs. non-GCOV
            // if (pszPol == nullptr) ... 

            CPLDebug("NISAR_DRIVER", "Priority 2: Using OpenOptions: INST=%s, FREQ=%s, POL=%s",
                     pszInst, pszFreq, pszPol ? pszPol : "(default)");

            // Validate Open Options
            if (!EQUAL(pszInst, "LSAR") && !EQUAL(pszInst, "SSAR"))
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid INST open option: '%s'. Must be LSAR or SSAR.", pszInst);
                delete poDS;
                return nullptr;
            }
            if (!EQUAL(pszFreq, "A") && !EQUAL(pszFreq, "B"))
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid FREQ open option: '%s'. Must be A or B.", pszFreq);
                delete poDS;
                return nullptr;
            }

            // Check if product type was identified (MUST do this before POL validation)
            if (poDS->m_sProductType.empty())
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Could not determine NISAR product type (e.g., GSLC, RSLC) from metadata. "
                         "Cannot construct path from Open Options.");
                delete poDS;
                return nullptr;
            }

            // BEGIN METADATA-BASED POL VALIDATION
            // Create uppercase versions of INST & FREQ for path building
            std::string sInstUpper = pszInst;
            std::string sFreqUpper = pszFreq;
            for(auto& c : sInstUpper) c = toupper(c);
            for(auto& c : sFreqUpper) c = toupper(c);

            // Build the base path to the metadata group
            std::string sMetadataGroupPath = "/science/";
            sMetadataGroupPath += sInstUpper;
            sMetadataGroupPath += "/";
            sMetadataGroupPath += poDS->m_sProductType; // "GCOV", "RSLC", etc.

            if (poDS->m_bIsLevel1)
            {
                sMetadataGroupPath += "/swaths/frequency";
            }
            else if (poDS->m_bIsLevel2)
            {
                sMetadataGroupPath += "/grids/frequency";
            }
            else
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Unknown product level for '%s'. Cannot determine if path is 'swaths' or 'grids'.",
                         poDS->m_sProductType.c_str());
                delete poDS;
                return nullptr;
            }
            
            sMetadataGroupPath += sFreqUpper; // e.g., .../frequencyA

            // Determine which metadata list to read
            std::string sMetadataDatasetName;
            const bool bIsGCOV = EQUAL(poDS->m_sProductType.c_str(), "GCOV");

            // This ensures we have a modifiable string for uppercasing.
            std::string sPol;
            
            if (bIsGCOV)
            {
                sMetadataDatasetName = "listOfCovarianceTerms";
                if (pszPol == nullptr) pszPol = "HHHH"; // Default GCOV to HHHH
            }
            else
            {
                sMetadataDatasetName = "listOfPolarizations";
                if (pszPol == nullptr) pszPol = "HH"; // Default others to HH
            }

            // Create a modifiable, uppercased string for the path
            std::string sPolUpper = pszPol;
            for(auto& c : sPolUpper) c = toupper(c);

            //  Read the metadata dataset
            std::string sPolList;
            hid_t hMetadataGroup = -1;
            
            // Suppress HDF5 errors during this read attempt
            H5E_auto2_t old_func_md; void *old_client_data_md;
            H5Eget_auto2(H5E_DEFAULT, &old_func_md, &old_client_data_md);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

            hMetadataGroup = H5Gopen2(poDS->hHDF5, sMetadataGroupPath.c_str(), H5P_DEFAULT);
            if (hMetadataGroup >= 0)
            {
                // Use the helper we defined earlier to read the scalar string
                sPolList = poDS->ReadHDF5StringDataset(hMetadataGroup, sMetadataDatasetName.c_str());
                if (sPolList.empty())
                {
                    // If scalar fails, try to read as 1D ARRAY (for RSLC, GSLC)
                    CPLDebug("NISAR_DRIVER", 
                             "Failed to read %s as scalar string, trying as 1D array...",
                              sMetadataDatasetName.c_str());
                    sPolList = poDS->ReadHDF5StringArrayAsList(hMetadataGroup, sMetadataDatasetName.c_str());
                }
                H5Gclose(hMetadataGroup);
            }
            
            // Restore HDF5 error handling
            H5Eset_auto2(H5E_DEFAULT, old_func_md, old_client_data_md);

            if (sPolList.empty())
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Failed to read polarization list '%s' from '%s'.",
                         sMetadataDatasetName.c_str(), sMetadataGroupPath.c_str());
                delete poDS; return nullptr;
            }

            // Split and check the list
            // Assumes list is comma-separated: "HHHH, HVHV, VHVH, VVVV"
            char **papszPolList = CSLTokenizeString2(sPolList.c_str(), ",", CSLT_STRIPLEADSPACES | CSLT_STRIPENDSPACES);
            bool bPolFound = false;
            for (int i = 0; papszPolList && papszPolList[i] != nullptr; ++i)
            {
                if (EQUAL(papszPolList[i], pszPol))
                {
                    bPolFound = true;
                    break;
                }
            }

            if (!bPolFound)
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid POL open option: '%s' is not in the list of "
                         "available polarizations for this product. "
                         "Available polarizations are: [%s]",
                         pszPol, sPolList.c_str());
                //CPLDebug("NISAR_DRIVER", "Available polarizations are: [%s]", sPolList.c_str());
                CSLDestroy(papszPolList);
                delete poDS; return nullptr;
            }
            
            CSLDestroy(papszPolList);
            //  END METADATA-BASED POL VALIDATION

            // Store options in dataset
            poDS->m_sInst = pszInst;
            poDS->m_sFreq = pszFreq;
            poDS->m_sPol = pszPol;

            // Uppercase POL for GCOV/RSLC paths
            for(auto& c : sPol) c = toupper(c);

            //Construct the path
            // We already built the base path, just add the final component
            sConstructedPath = sMetadataGroupPath;
            sConstructedPath += "/";
            sConstructedPath += sPolUpper; // Add the now-validated polarization

            pathToOpen = sConstructedPath.c_str();
            CPLDebug("NISAR_DRIVER", "Constructed HDF5 dataset path from OpenOptions: %s", pathToOpen);
        }
        else
        {
            // Priority 3: No specific subdataset requested - Perform Discovery
            CPLDebug("NISAR_DRIVER", "Priority 3: No specific HDF5 dataset path or OpenOptions. "
                                    "Running subdataset discovery.");

             poDS->hDataset = -1; // No specific dataset opened yet

             // Prepare visitor data
             NISARVisitorData visitor_data;
             std::vector<std::string> found_paths_vector;
             visitor_data.pFoundPaths = &found_paths_vector;
             visitor_data.hStartingGroupID = poDS->hHDF5;

             CPLDebug("NISAR_DRIVER", "Starting H5Ovisit to find subdatasets.");
             herr_t visit_status =

             H5Ovisit(poDS->hHDF5,  // Starting object ID (root group/file)
                       H5_INDEX_NAME, H5_ITER_NATIVE,  // Index and order
                       NISAR_FindDatasetsVisitor,     // callback function
                       (void *)&visitor_data,         // User data
                       H5O_INFO_BASIC);  // Fields for oinfo struct in callback
             if (visit_status < 0)
             {
                 CPLError(CE_Warning, CPLE_AppDefined,
                          "HDF5 visit failed during subdataset search.");
             }
             else
             {
                 CPLDebug("NISAR_DRIVER",
                          "H5Ovisit completed. Found %d potential subdatasets.",
                           (int)found_paths_vector.size());
             }

             // Check if the visitor found any relevant datasets
             if (!found_paths_vector.empty())
             {
                 char **papszMetadataList = nullptr;
                 int nSubdatasets = 0;

                 for (const std::string &hdf5_path : found_paths_vector)
                 {
                     nSubdatasets++;
                     // Construct SUBDATASET_n_NAME
                     std::string name_key =
                         CPLSPrintf("SUBDATASET_%d_NAME", nSubdatasets);
                     std::string name_val = pszPrefix;
                     name_val += "\"";
                     name_val += poDS->pszFilename;
                     name_val += "\":";
                     name_val += hdf5_path;
                     papszMetadataList = CSLSetNameValue(
                         papszMetadataList, name_key.c_str(), name_val.c_str());

                     // Construct SUBDATASET_n_DESC
                     std::string desc_key =
                         CPLSPrintf("SUBDATASET_%d_DESC", nSubdatasets);
                     std::string desc_val = "[";
                     hid_t hSubDataset =
                     H5Dopen2(poDS->hHDF5, hdf5_path.c_str(), H5P_DEFAULT);
                    if (hSubDataset >= 0)
                    {
                         hid_t hSubSpace = H5Dget_space(hSubDataset);
                         hid_t hSubType = H5Dget_type(hSubDataset);
                         int nSubDim = (hSubSpace >= 0)
                         ? H5Sget_simple_extent_ndims(hSubSpace) : -1;
                         if (nSubDim > 0)
                         {
                            hsize_t adimsSub[H5S_MAX_RANK];
                             H5Sget_simple_extent_dims(hSubSpace, adimsSub, nullptr);
                             // Format dimensions (Example: YxX for 2D)
                             for (int i = 0; i < nSubDim; ++i)
                             {
                                 desc_val += CPLSPrintf(
                                     "%llu%s", (unsigned long long)adimsSub[i],
                                 (i < nSubDim - 1) ? "x" : "");
                             }
                         }
                         else if (nSubDim == 0)
                         {

                            desc_val += "scalar";
                         }
                         else
                         {
                             desc_val += "?";
                         }
                         desc_val += "]";
                         GDALDataType eSubDataType =
                            (hSubType >= 0)
                                 ? NisarDataset::GetGDALDataType(hSubType)
                                 : GDT_Unknown;
                         std::string sDataTypeDesc = "(unknown)";
                         if (eSubDataType != GDT_Unknown)
                         {
                             sDataTypeDesc = "(";
                             if (GDALDataTypeIsComplex(eSubDataType))
                                 sDataTypeDesc += "complex, ";
                             sDataTypeDesc += GDALGetDataTypeName(
                                                GDALGetNonComplexDataType(eSubDataType));
                             sDataTypeDesc += ")";
                         }
                         desc_val += " " + hdf5_path + " " + sDataTypeDesc;
                         if (hSubType >= 0)
                             H5Tclose(hSubType);
                         if (hSubSpace >= 0)
                             H5Sclose(hSubSpace);
                             H5Dclose(hSubDataset);
                    }
                    else
                    {
                        desc_val += "?] " + hdf5_path + " (Error opening)";
                    }

                    papszMetadataList = CSLSetNameValue(papszMetadataList, 
                                                        desc_key.c_str(),
                                                        desc_val.c_str());
                }  // End loop

                 // Store the generated list in papszSubDatasets member
                 CSLDestroy(
                     poDS->papszSubDatasets);  // Clear any previous just in case
                     poDS->papszSubDatasets = papszMetadataList;  // Assign the new list
                     // Also set it in the metadata domain for immediate visibility
                     poDS->SetMetadata(poDS->papszSubDatasets, "SUBDATASETS");
                     // Configure poDS as a container dataset
                     poDS->nRasterXSize = 0;
                     poDS->nRasterYSize = 0;
                     poDS->nBands = 0;
                     CPLDebug("NISAR_DRIVER",
                          "Populated SUBDATASETS metadata for %d datasets.",
                          nSubdatasets);

                 // Return the container dataset
                 poDS->TryLoadXML();
                 poDS->SetDescription(poOpenInfo->pszFilename);
                 return poDS;  // <<<< EXIT POINT 1: Return container dataset
            }
            else
            {
                // No subdatasets found
                CPLDebug("NISAR_DRIVER", "No subdatasets found during discovery.");
                 delete poDS;
                 return nullptr;
             }
        } // End Priority 3
    } // End Priority 2

    // Prepare DAPL with Chunk Cache
    poDS->hDataset = -1;
    hid_t dapl_id = H5P_DEFAULT;  // Default DAPL initially
    bool bNeedToCloseDapl = false;

    dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
    if (dapl_id >= 0)
    {
        bNeedToCloseDapl = true;
        size_t rdcc_nslots, rdcc_nbytes;
        double rdcc_w0;
        H5Pget_chunk_cache(dapl_id, &rdcc_nslots, &rdcc_nbytes,
                           &rdcc_w0);  // Get defaults
        // Set desired cache size (e.g., 128 MB) - TODO: Make configurable
        size_t new_cache_size_mb =
            CPLScanLong(CPLGetConfigOption("NISAR_CHUNK_CACHE_SIZE_MB", "512"),
                        512);  // Read from config option
        size_t new_nbytes = new_cache_size_mb * 1024 * 1024;
        size_t new_nslots = std::max(
            (size_t)10009, rdcc_nslots * 4);  // Heuristic, prime num often good
        herr_t cache_status =
            H5Pset_chunk_cache(dapl_id, new_nslots, new_nbytes, rdcc_w0);
        if (cache_status < 0)
        {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "Failed to set HDF5 chunk cache.");
            H5Pclose(dapl_id);
            dapl_id = H5P_DEFAULT;
            bNeedToCloseDapl = false;
        }
        else
        {
            CPLDebug("NISAR_DRIVER",
                     "Set HDF5 chunk cache: slots=%lu, size=%lu bytes (%.0f "
                     "MB), w0=%.2f",
                     (unsigned long)new_nslots, (unsigned long)new_nbytes,
                     (double)new_cache_size_mb, rdcc_w0);
        }
    }
    else
    {
        CPLError(CE_Warning, CPLE_AppDefined, "Failed to copy default DAPL.");
        dapl_id = H5P_DEFAULT;
    }

    // Suppress HDF5 errors for this existence check
    H5E_auto2_t old_func_exists;
    void *old_client_data_exists;
    H5Eget_auto2(H5E_DEFAULT, &old_func_exists, &old_client_data_exists);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Check if the link to the dataset exists
    htri_t bExists = H5Lexists(poDS->hHDF5, pathToOpen, H5P_DEFAULT);

    // Restore HDF5 error handling
    H5Eset_auto2(H5E_DEFAULT, old_func_exists, old_client_data_exists);

    if (bExists <= 0)
    {
        // Path does not exist. Fail gracefully with a clear error.
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "The HDF5 dataset '%s' does not exist in the file '%s'. "
                 "Please check the path or Open Options.",
                 pathToOpen, poDS->pszFilename);
    
        // Clean up the DAPL if we created it
        if (bNeedToCloseDapl)
            H5Pclose(dapl_id);

        delete poDS; // This closes hHDF5 and frees pszFilename
        return nullptr;
    }
    // Open Target HDF5 Dataset
    CPLDebug("NISAR_DRIVER", "Attempting to open HDF5 dataset: %s", pathToOpen);
    poDS->hDataset = H5Dopen2(poDS->hHDF5, pathToOpen, dapl_id); //Use configured dapl_id
    if (bNeedToCloseDapl)
        H5Pclose(dapl_id);

    if (poDS->hDataset < 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
	 	            "H5Dopen2 failed for dataset '%s'.", pathToOpen);
        delete poDS; // Destructor handles hHDF5 and pszActualFilename
        return nullptr;
    }

    CPLDebug("NISAR_DRIVER", "Successfully Opened Subdataset: %s", pathToOpen);

    // Get HDF5 native data type
    hid_t hH5DataType = H5Dget_type(poDS->hDataset);
    if (hH5DataType < 0)
    {  // Check if H5Dget_type itself failed
        CPLError(CE_Failure, CPLE_AppDefined,
                 "H5Dget_type failed for dataset '%s'.", pathToOpen);
        delete poDS;  // poDS destructor handles cleanup of handles & filename
        return nullptr;
    }

    // H5Dget_type succeeded, now map the type

    // Map to GDAL data type using static helper function (MUST BE IMPLEMENTED)
    poDS->eDataType = NisarDataset::GetGDALDataType(hH5DataType);

    // Close the HDF5 type handle now that we're done with it
    // It's safe to close here because GetGDALDataType should have finished using it.
    H5Tclose(hH5DataType);
    hH5DataType =
        -1;  // Mark as closed for safety, although not strictly needed here

    // Check if the mapping was successful (type is known and supported by driver)
    if (poDS->eDataType == GDT_Unknown)
    {
        // Use a specific error message for mapping failure
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Unsupported HDF5 data type encountered in dataset '%s'. "
                 "Please update NisarDataset::GetGDALDataType.",
                 pathToOpen);
        delete poDS;
        return nullptr;
    }

    // Log the successfully determined type
    CPLDebug("NISAR_DRIVER", "Dataset GDAL Data Type: %s",
             GDALGetDataTypeName(poDS->eDataType));

    // Get dimensions
    hid_t hDataspace = H5Dget_space(poDS->hDataset);
    if (hDataspace < 0)
    {  // Check return value
        CPLError(CE_Failure, CPLE_AppDefined,
                 "H5Dget_space failed for dataset '%s'.", pathToOpen);
        delete poDS;
        return nullptr;
    }
    const int nDims = H5Sget_simple_extent_ndims(hDataspace);

    // Rank Check
    // Require at least 2 dimensions for raster interpretation
    if (nDims < 2)
    {
        // Provide more specific error
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Dataset '%s' has rank %d, requires rank >= 2 for raster "
                 "interpretation.",
                 pathToOpen, nDims);
        H5Sclose(hDataspace);
        delete poDS;
        return nullptr;
    }

    // Dimension Array Size
    // Use H5S_MAX_RANK for safety or allocate std::vector based on nDims
    hsize_t adims[H5S_MAX_RANK];
    H5Sget_simple_extent_dims(hDataspace, adims, nullptr);
///////////////////////////////////
    // --------------------------------------------------------------------
    // Dimension Assignment & Band Logic
    // --------------------------------------------------------------------
    int nBandsToCreate = 1;

    if (nDims == 3)
    {
        // 3D Case: [Bands, Y, X]
        // This handles the Radar Grid metadata cubes (e.g. 21 x 720 x 748)
        nBandsToCreate     = static_cast<int>(adims[0]);
        poDS->nRasterYSize = static_cast<int>(adims[1]);
        poDS->nRasterXSize = static_cast<int>(adims[2]);
        
        CPLDebug("NISAR_DRIVER", "Detected 3D Dataset: %d Bands x %d Y x %d X", 
                 nBandsToCreate, poDS->nRasterYSize, poDS->nRasterXSize);
    }
    else if (nDims == 2)
    {
        // Standard 2D Case: [Y, X]
        nBandsToCreate     = 1;
        poDS->nRasterYSize = static_cast<int>(adims[0]);
        poDS->nRasterXSize = static_cast<int>(adims[1]);
    }
    else
    {
        // Fallback for > 3 dimensions (Take last two as spatial)
        // Or you can return an error here if you want to be strict.
        nBandsToCreate     = 1; 
        poDS->nRasterYSize = static_cast<int>(adims[nDims - 2]);
        poDS->nRasterXSize = static_cast<int>(adims[nDims - 1]);
        
        CPLDebug("NISAR_DRIVER", "Rank %d detected. Treating as 2D using last two dimensions.", nDims);
    }

    H5Sclose(hDataspace); // Close handle
    
    // Sanity Checks
    if (poDS->nRasterXSize <= 0 || poDS->nRasterYSize <= 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Invalid raster dimensions (%d x %d) read from dataset '%s'.",
                 poDS->nRasterXSize, poDS->nRasterYSize, pathToOpen);
        delete poDS;
        return nullptr;
    }

    CPLDebug("NISAR_DRIVER", "Dataset Dimensions: %d x %d (Bands: %d)", 
             poDS->nRasterXSize, poDS->nRasterYSize, nBandsToCreate);

    // Create Bands
    for (int i = 0; i < nBandsToCreate; i++)
    {
        // GDAL Band indices are 1-based (1, 2, 3...)
        poDS->SetBand(i + 1, new NisarRasterBand(poDS, i + 1));
    }
////////////////////////////////////////
    if (poDS->m_bIsLevel2)
    {
        CPLDebug("NISAR_DRIVER", "Level 2 product detected. Georeferencing "
 	                             "will use GeoTransform and SRS.");
    }
    else if (poDS->m_bIsLevel1)
    {
        CPLDebug(
	    "NISAR_DRIVER",
 	    "Level 1 product detected. Will generate GCPs for georeferencing.");
 	    // Pass the discovered product group to the function
 	    if (poDS->GenerateGCPsFromGeolocationGrid(poDS->m_sProductType.c_str()) !=
 	        CE_None)
 	    {
 	        CPLError(CE_Warning, CPLE_AppDefined,
 	                 "Failed to generate GCPs for Level 1 product.");
 	        // Decide if this should be a fatal error or just a warning
 	    }
    }
    else
    {
 	    CPLError(
 	        CE_Warning, CPLE_AppDefined,
 	        "Unknown NISAR product structure. Georeferencing may be absent.");
    }

    // Band Creation Check
    // We only run this legacy logic if bands haven't been created yet.
    // (The 3D/2D logic earlier in this function should have already called SetBand)
    if (poDS->nBands == 0)
    {
        // Determine Band Count 
        if (GDALDataTypeIsComplex(poDS->eDataType))
        {
            poDS->nBands = 1;  // Convention: Represent complex as one GDAL band
            CPLDebug("NISAR_DRIVER",
                     "Data type is complex, setting Band Count to: %d (single "
                     "complex band convention)",
                     poDS->nBands);
        }
        else
        {
            poDS->nBands = 1;  // Default for non-complex types
            CPLDebug("NISAR_DRIVER",
                     "Data type is not complex, setting Band Count to: %d",
                     poDS->nBands);
        }

        // Note: Further logic may be needed for other multi-component datasets if supported later.

        // Create raster bands
        for (int i = 1; i <= poDS->nBands; i++)
        {
            NisarRasterBand *poBand = nullptr;
            try
            {
                // Create the band object
                poBand = new NisarRasterBand(poDS, i);
            }
            catch (const std::bad_alloc &)
            {
                CPLError(CE_Failure, CPLE_OutOfMemory,
                         "Failed to allocate NisarRasterBand object for band %d.",
                         i);
                delete poDS;
                return nullptr;
            }

            poDS->SetBand(i, poBand);  // Add band to dataset (poDS takes ownership)
        }
    }

    // Final Setup
    poDS->SetDescription(
        poOpenInfo
            ->pszFilename);  // Use original full input path as description
    if (pathToOpen)          // Store the actual HDF5 path opened
        poDS->SetMetadataItem("HDF5_PATH", pathToOpen);

    // Add DERIVED_SUBDATASETS Metadata
    // We only do this if we opened a specific dataset (hDataset >= 0)
    // and not for the container (which has hDataset < 0).
    if (poDS->hDataset >= 0)
    {
        // Construct a connection string that points EXACTLY to this subdataset
        // Format: NISAR:filename.h5:hdf5_path
        // This ensures the Derived driver re-opens the specific band, not the generic container.
        std::string sTargetString = "NISAR:";
        sTargetString += poDS->pszFilename; 
        sTargetString += ":";
        sTargetString += pathToOpen; // pathToOpen is the HDF5 internal path defined earlier

        bool bIsComplex = GDALDataTypeIsComplex(poDS->eDataType);
        // Check if it's a numeric type (not string or unknown)
        bool bIsNumeric = (poDS->eDataType > GDT_Unknown && poDS->eDataType < GDT_CInt16);

        if (bIsComplex)
        {
            CPLDebug("NISAR_DRIVER", "Advertising complex DERIVED_SUBDATASETS for %s", sTargetString.c_str());
            
            // Note: We use sTargetString.c_str() here, NOT pszFullDatasetName
            poDS->SetMetadataItem("AMPLITUDE", 
                CPLSPrintf("DERIVED_SUBDATASET:AMPLITUDE:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("PHASE", 
                CPLSPrintf("DERIVED_SUBDATASET:PHASE:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("REAL", 
                CPLSPrintf("DERIVED_SUBDATASET:REAL:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("IMAG", 
                CPLSPrintf("DERIVED_SUBDATASET:IMAG:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("INTENSITY", 
                CPLSPrintf("DERIVED_SUBDATASET:INTENSITY:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("CONJ", 
                CPLSPrintf("DERIVED_SUBDATASET:CONJ:\"%s\"", sTargetString.c_str()), "DERIVED_SUBDATASETS");
        }

        // LOGAMPLITUDE is available for both complex and non-complex numeric types
        if (bIsNumeric)
        {
            CPLDebug("NISAR_DRIVER",
                     "Advertising LOGAMPLITUDE DERIVED_SUBDATASET for %s",
                     sTargetString.c_str());
            poDS->SetMetadataItem(
                "LOGAMPLITUDE",
                CPLSPrintf("DERIVED_SUBDATASET:LOGAMPLITUDE:\"%s\"", 
                            sTargetString.c_str()), 
                           "DERIVED_SUBDATASETS");
        }
    }
    // End DERIVED_SUBDATASETS

    ///////////////////////////////////////////////////////////////////////
    // Mask Layer Support
    // 
    // Check if the dataset being opened is the mask layer.
    // We check both the input path variable and the dataset description/metadata.
    //
    bool bIsMask = false;
    
    // Check Explicit path variable from Open() logic
    if (pathToOpen && std::string(pathToOpen).find("/mask") != std::string::npos) {
        bIsMask = true;
    }
    
    // Check Fallback check against stored HDF5 path metadata
    if (!bIsMask) {
        const char* pszH5Path = poDS->GetMetadataItem("HDF5_PATH");
        if (pszH5Path && std::string(pszH5Path).find("/mask") != std::string::npos) {
            bIsMask = true;
        }
    }

    if (bIsMask)
    {
        CPLDebug("NISAR_DRIVER", "Applying NISAR Mask Layer metadata (NoData=255, Categories) to band.");

        GDALRasterBand* poBand = poDS->GetRasterBand(1);
        if (poBand)
        {
            // Set Fill Value (255)
            // This allows GIS software to make the border transparent automatically.
            poBand->SetNoDataValue(255.0);

            // Set Category Names (Classes 0-5)
            // This allows 'gdalinfo' and GIS legends to show text instead of numbers.
            char** papszCategories = nullptr;
            papszCategories = CSLAddString(papszCategories, "Invalid or partially focused"); // 0
            papszCategories = CSLAddString(papszCategories, "Valid (Sub-swath 1)");        // 1
            papszCategories = CSLAddString(papszCategories, "Valid (Sub-swath 2)");        // 2
            papszCategories = CSLAddString(papszCategories, "Valid (Sub-swath 3)");        // 3
            papszCategories = CSLAddString(papszCategories, "Valid (Sub-swath 4)");        // 4
            papszCategories = CSLAddString(papszCategories, "Valid (Sub-swath 5)");        // 5
            
            poBand->SetCategoryNames(papszCategories);
            CSLDestroy(papszCategories);

            // Set Color Interpretation to Palette
            // This is the standard GDAL hint that "Category Names" are present.
            poBand->SetColorInterpretation(GCI_PaletteIndex);
        }
    }

    poDS->TryLoadXML();  // Initialize PAM system (loads .aux.xml if present)

    CPLDebug("NISAR_DRIVER",
             "Finished NisarDataset::Open successfully for dataset '%s'",
             pathToOpen);

    // Initialize any other metadata, etc.
    // Load Requested Metadata
    if (!poDS->m_sInst.empty() && !poDS->m_sProductType.empty()) 
    {
        poDS->InitializeMetadataMap();

        const char* pszMetaOption = poOpenInfo->papszOpenOptions 
                                    ? CSLFetchNameValue(poOpenInfo->papszOpenOptions, "METADATA") 
                                    : nullptr;

        if (pszMetaOption)
        {
            if (EQUAL(pszMetaOption, "ALL"))
            {
                // Load everything in the map
                for (auto const& [key, val] : poDS->m_oMetadataMap) {
                    poDS->LoadMetadataDomain(key);
                }
            }
            else
            {
                // Split comma-separated list
                char **papszTokens = CSLTokenizeString2(pszMetaOption, ",", CSLT_STRIPLEADSPACES | CSLT_STRIPENDSPACES);
                for (int i = 0; papszTokens && papszTokens[i] != nullptr; i++) {
                    std::string sToken = papszTokens[i];
                    // Manual uppercase loop
                    for (auto & c: sToken) c = toupper(c);
                    poDS->LoadMetadataDomain(sToken);
                }
                CSLDestroy(papszTokens);
            }
        }
    }

    return poDS;
}

/************************************************************************/
/*                                GetGeoTransform()                     */
/* Read geotransform parameters from the HDF5 file (as attributes)      */
/* Set the adfGeoTransform array.                                       */
/* If no geotransform is found, return CE_None and set the transform to */
/* an identity transform.                                               */
/************************************************************************/
#if GDAL_VERSION_MAJOR > 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
CPLErr NisarDataset::GetGeoTransform(GDALGeoTransform &oGT) const
{
#else
CPLErr NisarDataset::GetGeoTransform(double *padfTransform)
{
    // Legacy Signature: create an temporary object to hold the result
    GDALGeoTransform oGT;
    // Initialize identity
    oGT[0] = 0.0; oGT[1] = 1.0; oGT[2] = 0.0;
    oGT[3] = 0.0; oGT[4] = 0.0; oGT[5] = 1.0;
#endif

    // Check PAM (Persistent Aux Metadata) - Highest Priority
    // We must handle the parent call differently because the parent signature 
    // also changes between versions.
#if GDAL_VERSION_MAJOR > 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
    if (GDALPamDataset::GetGeoTransform(oGT) == CE_None) return CE_None;
#else
    if (GDALPamDataset::GetGeoTransform(padfTransform) == CE_None) return CE_None;
#endif

    // Check Internal Cache
    {
        std::lock_guard<std::mutex> lock(m_GeoTransformMutex);
        if (m_bGotGeoTransform) {
            memcpy(oGT.data(), m_adfGeoTransform, sizeof(double) * 6);
            // LEGACY ADAPTER WRITE-BACK
            #if GDAL_VERSION_MAJOR < 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
            if (padfTransform)
                memcpy(padfTransform, oGT.data(), sizeof(double) * 6);
            #endif
            return CE_None;
        }
    }

    // Early Exit for Non-Grid Data
    // If we have GCPs (Level 1 / Swaths), we should not return a GeoTransform.
    if (const_cast<NisarDataset *>(this)->GetGCPCount() > 0) return CE_Failure;
    if (hDataset < 0) return CE_Failure;

    CPLDebug("NISAR_DRIVER", "GetGeoTransform: Cache miss. Calculating...");

    // Try reading explicit 'GeoTransform' attribute (Generic Fallback)
    if (ReadGeoTransformAttribute(this->hDataset, "GeoTransform", oGT) == CE_None) {
        std::lock_guard<std::mutex> lock(m_GeoTransformMutex);
        m_bGotGeoTransform = true;
        memcpy(m_adfGeoTransform, oGT.data(), sizeof(double) * 6);
        // LEGACY ADAPTER WRITE-BACK
        #if GDAL_VERSION_MAJOR < 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
        if (padfTransform)
            memcpy(padfTransform, oGT.data(), sizeof(double) * 6);
        #endif
        return CE_None;
    }

    // Calculate from Coordinate Arrays (Walk-Up Discovery)
    // This logic handles:
    // Case A: Main Image Grids
    //    Path: .../science/LSAR/GCOV/grids/frequencyA/HHHH
    //    Coords: .../science/LSAR/GCOV/grids/frequencyA/xCoordinates
    //
    // Calibration Grids (Noise, etc.)
    //    Path: .../metadata/calibrationInformation/frequencyA/noiseEquivalentBackscatter/HH
    //    Coords: .../metadata/calibrationInformation/frequencyA/noiseEquivalentBackscatter/xCoordinates
    
    // Only attempt this for Level 2 products (GCOV, GUNW) which use defined grids
    if (m_bIsLevel2)
    {
        std::string sCurrentPath = get_hdf5_object_name(this->hDataset);
        
        // Safety Check: Only scan for coordinates if we are in a known grid-like hierarchy
        bool bIsStandardGrid = (sCurrentPath.find("/grids/") != std::string::npos);
        bool bIsCalibrationGrid = (sCurrentPath.find("/calibrationInformation/") != std::string::npos);
        bool bIsRadarGrid       = (sCurrentPath.find("/radarGrid/") != std::string::npos);

        if (bIsStandardGrid || bIsCalibrationGrid || bIsRadarGrid)
        {
            std::string sCoordsRoot;
            std::string sSearchPath = sCurrentPath;

            // 
            // We strip one path component at a time to find the 'folder' containing xCoordinates
            while (sSearchPath.length() > 1)
            {
                // Strip the last component (e.g., remove "/HH")
                size_t nLastSlash = sSearchPath.find_last_of('/');
                if (nLastSlash == std::string::npos || nLastSlash == 0) break;

                sSearchPath = sSearchPath.substr(0, nLastSlash); 

                // Check for sibling coordinates
                std::string sTestX = sSearchPath + "/xCoordinates";
                
                // Fast existence check
                H5E_auto2_t old_func; void *old_client_data;
                H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
                H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr); // Suppress errors
                htri_t exists = H5Lexists(this->hHDF5, sTestX.c_str(), H5P_DEFAULT);
                H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore errors

                if (exists > 0) {
                    sCoordsRoot = sSearchPath;
                    CPLDebug("NISAR_DRIVER", "GetGeoTransform: Found coordinate arrays at: %s", sCoordsRoot.c_str());
                    break;
                }
            }

            if (!sCoordsRoot.empty()) 
            {
                // Open and Read Coordinates
                std::string sXCoordsPath = sCoordsRoot + "/xCoordinates";
                std::string sYCoordsPath = sCoordsRoot + "/yCoordinates";

                hid_t hX = H5Dopen2(this->hHDF5, sXCoordsPath.c_str(), H5P_DEFAULT);
                hid_t hY = H5Dopen2(this->hHDF5, sYCoordsPath.c_str(), H5P_DEFAULT);

                if (hX >= 0 && hY >= 0) 
                {
                    // Get Dimensions
                    hsize_t dimsX[1] = {0}, dimsY[1] = {0};
                    hid_t hSpaceX = H5Dget_space(hX);
                    H5Sget_simple_extent_dims(hSpaceX, dimsX, nullptr);
                    H5Sclose(hSpaceX);

                    hid_t hSpaceY = H5Dget_space(hY);
                    H5Sget_simple_extent_dims(hSpaceY, dimsY, nullptr);
                    H5Sclose(hSpaceY);

                    // Proceed only if we have valid arrays (>= 2 points needed for resolution)
                    if (dimsX[0] >= 2 && dimsY[0] >= 2) 
                    {
                        // Helper Lambda to read scalar double at index
                        auto ReadVal = [](hid_t hD, hsize_t idx) -> double {
                            double val = 0.0;
                            hsize_t count[1] = {1};
                            hsize_t start[1] = {idx};
                            hid_t hMem = H5Screate_simple(1, count, nullptr);
                            hid_t hFile = H5Dget_space(hD);
                            H5Sselect_hyperslab(hFile, H5S_SELECT_SET, start, nullptr, count, nullptr);
                            if (H5Dread(hD, H5T_NATIVE_DOUBLE, hMem, hFile, H5P_DEFAULT, &val) < 0) val = 0.0;
                            H5Sclose(hFile); H5Sclose(hMem);
                            return val;
                        };

                        double dfXStart = ReadVal(hX, 0);
                        double dfXEnd   = ReadVal(hX, dimsX[0] - 1);
                        double dfYStart = ReadVal(hY, 0);
                        double dfYEnd   = ReadVal(hY, dimsY[0] - 1);

                        // Calculate GeoTransform
                        // Calculate Pixel Size (Resolution)
                        // Note: We use the array size (dims - 1) for the span.
                        double resX = (dfXEnd - dfXStart) / static_cast<double>(dimsX[0] - 1);
                        double resY = (dfYEnd - dfYStart) / static_cast<double>(dimsY[0] - 1);

                        // Calculate Top-Left Corner (Origin)
                        // NISAR coordinates represent Pixel Centers. 
                        // GDAL GeoTransform[0],[3] represents Top-Left Corner.
                        // We must shift by half a pixel.
                        double dfULX = dfXStart - (0.5 * resX);
                        double dfULY = dfYStart - (0.5 * resY);

                        double adfCalcGT[6];
                        adfCalcGT[0] = dfULX;       // Top-Left X
                        adfCalcGT[1] = resX;        // W-E Resolution
                        adfCalcGT[2] = 0.0;         // Rotation (0)
                        adfCalcGT[3] = dfULY;       // Top-Left Y
                        adfCalcGT[4] = 0.0;         // Rotation (0)
                        adfCalcGT[5] = resY;        // N-S Resolution


                        // Clean up HDF5 handles
                        H5Dclose(hX); 
                        H5Dclose(hY);

                        // Cache and Return
                        {
                            std::lock_guard<std::mutex> lock(m_GeoTransformMutex);
                            m_bGotGeoTransform = true;
                            memcpy(m_adfGeoTransform, adfCalcGT, sizeof(double) * 6);
                            memcpy(oGT.data(), m_adfGeoTransform, sizeof(double) * 6);
                        }

                        CPLDebug("NISAR_DRIVER", "GetGeoTransform: Derived from coordinates. Origin=(%.2f, %.2f) Res=(%.6f, %.6f)",
                                 adfCalcGT[0], adfCalcGT[3], adfCalcGT[1], adfCalcGT[5]);

                        // LEGACY ADAPTER WRITE-BACK
                        #if GDAL_VERSION_MAJOR < 3 || (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
                        if (padfTransform)
                           memcpy(padfTransform, oGT.data(), sizeof(double) * 6);
                        #endif

                        return CE_None;
                    }
                }
                
                // Cleanup if we failed inside the block
                if (hX >= 0) H5Dclose(hX);
                if (hY >= 0) H5Dclose(hY);
            }
        }
        else 
        {
            CPLDebug("NISAR_DRIVER", "GetGeoTransform: Path '%s' is not recognized as a grid or calibration layer. Skipping coordinate discovery.", sCurrentPath.c_str());
        }
    }

    return CE_Failure;
}
//------------------------------------------------------------------------------
// NisarDataset::GetSpatialRef
//------------------------------------------------------------------------------
const OGRSpatialReference *NisarDataset::GetSpatialRef() const
{
    // Use Mutex Holder for RAII-style locking
    // It acquires lock on construction, releases on destruction (end of scope)
    std::lock_guard<std::mutex> lock(
        m_SRSMutex);  // Use std::lock_guard and correct member name

    // Check cache flag - if already computed, return cached pointer
    if (m_bGotSRS)
    {
        return m_poSRS;  // This could be nullptr if previous attempt failed
    }

    // Mark as attempted (even if we fail below, don't try again)
    m_bGotSRS = true;

    CPLDebug("NISAR_DRIVER", "NisarDataset::GetSpatialRef() called.");

    // Check if this is a container dataset
    if (hDataset < 0)
    {
        // CPLError(CE_Warning, CPLE_AppDefined, "GetSpatialRef: Invalid main dataset handle. Cannot determine SRS.");
        // This is a container dataset, it has no SRS. Return quietly.
        return nullptr;  // Return null for container or if handle invalid
    }
    // Could also check nRasterXSize == 0, but hDataset < 0 is sufficient

    // Initialize local variables
    OGRSpatialReference *poSRS =
        nullptr;                    // Pointer to the SRS object we might create
    hid_t hProjectionDataset = -1;  // Handle for the 'projection' dataset
    hid_t hAttribute =
        -1;                // Handle for attributes 'epsg_code' or 'spatial_ref'
    hid_t hAttrType = -1;  // Handle for attribute type
    herr_t status = -1;
    long long epsg_code = 0;  // For reading EPSG code
    char *pszWKT_Alloc =
        nullptr;  // For reading WKT string attribute (fixed length)
    char *pszWKT_VL =
        nullptr;  // For reading WKT string attribute (variable length)
    std::string parentGroupPath;

    // Path construction variables
    std::string datasetPath;
    std::string projectionPath;

    // Temporary pointer for newly created SRS object during processing
    OGRSpatialReference *poTmpSRS = nullptr;

    // Dynamically find the product's root path
    datasetPath = get_hdf5_object_name(this->hDataset);

    // Find the last '/' to get the parent group path
    size_t lastSlashPos = datasetPath.find_last_of('/');
    if (lastSlashPos == std::string::npos)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Could not determine parent group for projection.");
        goto cleanup;  // Or return failure
    }
    parentGroupPath = datasetPath.substr(0, lastSlashPos);

    // The projection dataset is assumed to be in the same group
    projectionPath = parentGroupPath + "/projection";

    CPLDebug("NISAR_DRIVER", "Attempting to open projection dataset: %s",
             projectionPath.c_str());

    // Wrap the HDF5 call that might fail in error suppression
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hProjectionDataset =
        H5Dopen2(this->hHDF5, projectionPath.c_str(), H5P_DEFAULT);

    H5Eset_auto2(H5E_DEFAULT, old_func,
                 old_client_data);  // RESTORE HANDLER IMMEDIATELY

    if (hProjectionDataset < 0)
    {
        CPLDebug("NISAR_DRIVER",
                 "GetSpatialRef: Optional 'projection' dataset not found.");
        goto cleanup;  // Now safe to jump
    }

    // Try Reading EPSG Code Attribute
    hAttribute = H5Aopen(hProjectionDataset, "epsg_code", H5P_DEFAULT);

    if (hAttribute >= 0)
    {  // Found an EPSG attribute
        CPLDebug("NISAR_DRIVER", "Found potential EPSG attribute.");
        hAttrType = H5Aget_type(hAttribute);
        if (hAttrType >= 0 && H5Tget_class(hAttrType) == H5T_INTEGER)
        {
            // Read as appropriate integer type (try LLONG first)
            status = H5Aread(hAttribute, H5T_NATIVE_LLONG, &epsg_code);
            if (status < 0)
            {  // Try INT if LLONG fails?
                status = H5Aread(hAttribute, H5T_NATIVE_INT,
                                 &epsg_code);  // Read into long long still ok
            }

            if (status >= 0 && epsg_code > 0)
            {
                CPLDebug("NISAR_DRIVER", "Read EPSG code: %lld", epsg_code);
                poTmpSRS = new OGRSpatialReference();  // Create SRS object
                // SetAuthority requires GDAL >= 3.x ? Use importFromEPSG instead
                if (poTmpSRS->importFromEPSG(static_cast<int>(epsg_code)) ==
                    OGRERR_NONE)
                {
                    m_poSRS = poTmpSRS;  //Assign to member
                    poTmpSRS = nullptr;  // Prevent deletion
                    CPLDebug("NISAR_DRIVER", "Assigned SRS from EPSG...");
                    CPLDebug("NISAR_DRIVER", "Successfully imported EPSG:%d.",
                             (int)epsg_code);
                    goto cleanup;  //Jump to cleanup AFTER successful assignment
                }
                else
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "Failed to import EPSG code %lld into "
                             "OGRSpatialReference.",
                             epsg_code);
                    delete poTmpSRS;
                    poSRS = nullptr;  // Delete failed object
                                      // Continue below to try WKT attribute
                }
            }
            else
            {
                CPLError(CE_Warning, CPLE_FileIO,
                         "Failed to read valid EPSG code from attribute.");
            }
        }
        else
        {
            CPLError(CE_Warning, CPLE_FileIO,
                     "EPSG attribute is not an integer type.");
        }

        // Close attribute type handle if it was opened successfully
        if (hAttrType >= 0)
        {
            H5Tclose(hAttrType);
            hAttrType = -1;  // Reset handle ID after closing
        }
        // Close attribute handle if it was opened successfully
        if (hAttribute >= 0)
        {
            H5Aclose(hAttribute);
            hAttribute = -1;  // Reset handle ID after closing
        }
    }
    else
    {
        CPLDebug("NISAR_DRIVER", "EPSG attribute not found.");
    }

    // Try Reading WKT Attribute if EPSG failed or not found
    if (m_poSRS == nullptr)
    {
        CPLDebug("NISAR_DRIVER",
                 "Attempting to read WKT from 'spatial_ref' attribute.");
        hAttribute = H5Aopen(hProjectionDataset, "spatial_ref",
                             H5P_DEFAULT);  // Try "spatial_ref"

        if (hAttribute >= 0)
        {
            hAttrType = H5Aget_type(hAttribute);
            if (hAttrType >= 0 && H5Tget_class(hAttrType) == H5T_STRING)
            {
                //H5T_str_t ePad = H5T_STR_NULLTERM; // Default, check?
                //H5T_cset_t eCset = H5T_CSET_ASCII;    // Default, check?
                bool bIsVariable = H5Tis_variable_str(hAttrType);

                if (bIsVariable)
                {  // Variable length string
                    status = H5Aread(
                        hAttribute, hAttrType,
                        &pszWKT_VL);  // Reads into char* allocated by HDF5
                    if (status >= 0 && pszWKT_VL != nullptr)
                    {
                        poTmpSRS = new OGRSpatialReference();
                        // Use const_cast ONLY if importFromWkt needs non-const char* (older GDAL?)
                        // Modern GDAL takes const char*. pszWKT_VL is char* here.
                        if (poTmpSRS->importFromWkt(pszWKT_VL) == OGRERR_NONE)
                        {
                            m_poSRS = poTmpSRS;  //Assign to member
                            poTmpSRS = nullptr;
                            CPLDebug("NISAR_DRIVER",
                                     "Successfully imported WKT from "
                                     "variable-length attribute.");
                            goto cleanup;
                        }
                        else
                        {
                            CPLError(
                                CE_Warning, CPLE_AppDefined,
                                "Failed to import WKT from variable-length "
                                "'spatial_ref' attribute.");
                            delete poTmpSRS;
                            poSRS = nullptr;
                        }
                        // Free the variable length string buffer allocated by H5Aread
                        // Need the memory dataspace and type to reclaim? Docs say yes.
                        hid_t vl_space = H5Aget_space(hAttribute);
                        if (vl_space >= 0)
                        {
                            H5Dvlen_reclaim(
                                hAttrType, vl_space, H5P_DEFAULT,
                                &pszWKT_VL);  // Pass address of pointer
                            H5Sclose(vl_space);
                        }
                        else
                        {
                            CPLFree(pszWKT_VL);
                        }  // Fallback? Unsafe. Needs H5Dvlen_reclaim.
                        pszWKT_VL = nullptr;  // Mark as freed
                    }
                    else
                    {
                        CPLError(CE_Warning, CPLE_FileIO,
                                 "Failed to read variable-length 'spatial_ref' "
                                 "attribute.");
                    }
                }
                else
                {  // Fixed length string
                    size_t nWktLen = H5Tget_size(hAttrType);
                    if (nWktLen > 0)
                    {
                        pszWKT_Alloc = (char *)VSIMalloc(
                            nWktLen + 1);  // Use VSI for GDAL consistency
                        if (pszWKT_Alloc)
                        {
                            status =
                                H5Aread(hAttribute, hAttrType, pszWKT_Alloc);
                            if (status >= 0)
                            {
                                pszWKT_Alloc[nWktLen] =
                                    '\0';  // Ensure null termination
                                poTmpSRS = new OGRSpatialReference();
                                if (poTmpSRS->importFromWkt(pszWKT_Alloc) !=
                                    OGRERR_NONE)
                                {
                                    CPLError(CE_Warning, CPLE_AppDefined,
                                             "Failed to import WKT from "
                                             "fixed-length 'spatial_ref' "
                                             "attribute.");
                                    delete poTmpSRS;
                                    poTmpSRS = nullptr;
                                }
                                else
                                {
                                    CPLDebug("NISAR_DRIVER",
                                             "Successfully imported WKT from "
                                             "fixed-length attribute.");
                                    m_poSRS = poTmpSRS;
                                    poTmpSRS = nullptr;
                                    goto cleanup;
                                }
                            }
                            else
                            {
                                CPLError(CE_Warning, CPLE_FileIO,
                                         "Failed to read fixed-length "
                                         "'spatial_ref' attribute.");
                            }
                            VSIFree(pszWKT_Alloc);
                            pszWKT_Alloc = nullptr;  // Free allocated buffer
                        }
                        else
                        {
                            CPLError(CE_Failure, CPLE_OutOfMemory,
                                     "Failed to allocate buffer for WKT.");
                        }
                    }
                    else
                    {
                        CPLError(CE_Warning, CPLE_AppDefined,
                                 "Fixed-length WKT attribute has zero size.");
                    }
                }
            }
            else
            {
                CPLError(CE_Warning, CPLE_FileIO,
                         "'spatial_ref' attribute is not a string type.");
            }

            // Close attribute handles if they were opened
            if (hAttrType >= 0)
            {
                H5Tclose(hAttrType);
                hAttrType = -1;
            }
            // Close attribute handle if it was opened successfully
            if (hAttribute >= 0)
            {
                H5Aclose(hAttribute);
                hAttribute = -1;  // Reset handle ID after closing
            }
        }
        else
        {
            CPLDebug("NISAR_DRIVER",
                     "WKT attribute ('spatial_ref') not found.");
        }
    }  // end if pSRS == nullptr

cleanup:
    // Close handles opened within this function
    if (hAttribute >= 0)
        H5Aclose(
            hAttribute);  // Should be closed already above, but just in case
    if (hAttrType >= 0)
        H5Tclose(hAttrType);  // Should be closed already above
    if (hProjectionDataset >= 0)
        H5Dclose(hProjectionDataset);

    // Free any potentially allocated WKT buffers if error occurred before free
    if (pszWKT_Alloc)
        VSIFree(pszWKT_Alloc);
    // Note: Cannot easily free pszWKT_VL here if H5Dvlen_reclaim failed earlier.
    // H5Dvlen_reclaim should be called right after successful importFromWkt.

    // If poTmpSRS was allocated but not assigned to m_poSRS (e.g., import failed), delete it
    if (poTmpSRS != nullptr)
    {
        delete poTmpSRS;
    }

    if (m_poSRS == nullptr)
    {
        CPLDebug("NISAR_DRIVER",
                 "GetSpatialRef: Could not find valid SRS information.");
    }
    else
    {
        m_poSRS->SetAxisMappingStrategy(
            OAMS_TRADITIONAL_GIS_ORDER);  // Ensure consistent axis order
        CPLDebug("NISAR_DRIVER", "GetSpatialRef returning cached SRS object.");
    }

    // Mutex is automatically unlocked when holderD goes out of scope
    return m_poSRS;  // Return pointer to the cached member variable (const*)
}

/**
 * Reads a 2D slice from a 3D HDF5 dataset into a 1D vector using hyperslab selection.
 *
 * This function is designed to extract a 2D plane of data from a 3D cube, which is
 * necessary for handling NISAR's geolocation grids that are structured as
 * (height, azimuth_time, slant_range).
 *
 * @param hLocation The hid_t of the file or group containing the dataset.
 * @param pszPath The name of the 3D dataset to read from.
 * @param vec The output std::vector<double> where the flattened 2D slice will be stored.
 * @param nSliceIndex The index of the slice to extract from the first dimension (height).
 * @return `true` on success, `false` on failure.
 */
static bool Read2DSliceAsVec(hid_t hLocation, const char *pszPath,
                             std::vector<double> &vec, int nSliceIndex = 0)
{
    hid_t hDset = -1;
    hid_t hFileSpace = -1;
    hid_t hMemSpace = -1;
    bool bSuccess = false;
    hsize_t dims[3];
    hsize_t offset[3];
    hsize_t count[3];
    hsize_t mem_dims[2];

    hDset = H5Dopen2(hLocation, pszPath, H5P_DEFAULT);
    if (hDset < 0)
    {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to open dataset: %s",
                 pszPath);
        goto cleanup;
    }

    hFileSpace = H5Dget_space(hDset);
    if (hFileSpace < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined, "Failed to get filespace for: %s",
                 pszPath);
        goto cleanup;
    }

    if (H5Sget_simple_extent_ndims(hFileSpace) != 3)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Dataset '%s' is not 3-dimensional as expected for a metadata "
                 "cube.",
                 pszPath);
        goto cleanup;
    }

    H5Sget_simple_extent_dims(hFileSpace, dims,
                              nullptr);  // Get dims: [height, azimuth, range]

    if (nSliceIndex < 0 || static_cast<hsize_t>(nSliceIndex) >= dims[0])
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Slice index %d is out of bounds for dataset: %s", nSliceIndex,
                 pszPath);
        goto cleanup;
    }

    // Hyperslab Selection in the File
    // Define the starting offset of our slice in the 3D cube [height, azimuth, range].
    offset[0] = static_cast<hsize_t>(nSliceIndex);
    offset[1] = 0;
    offset[2] = 0;

    // Define the size of the block to read: 1 slice in height, all of azimuth, all of range.
    count[0] = 1;
    count[1] = dims[1];
    count[2] = dims[2];

    if (H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count,
                            nullptr) < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Failed to select hyperslab for: %s", pszPath);
        goto cleanup;
    }

    // Dataspace in Memory
    // Create a 2D dataspace for memory that matches the dimensions of our slice [azimuth, range].
    mem_dims[0] = dims[1];
    mem_dims[1] = dims[2];
    hMemSpace = H5Screate_simple(2, mem_dims, nullptr);
    if (hMemSpace < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Failed to create memory space for slice.");
        goto cleanup;
    }

    try
    {
        vec.resize(dims[1] * dims[2]);
    }
    catch (const std::bad_alloc &)
    {
        CPLError(CE_Failure, CPLE_OutOfMemory,
                 "Failed to allocate memory for slice from %s", pszPath);
        goto cleanup;
    }

    if (H5Dread(hDset, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpace, H5P_DEFAULT,
                vec.data()) < 0)
    {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to read data slice from %s",
                 pszPath);
        vec.clear();
        goto cleanup;
    }

    bSuccess = true;

cleanup:
    if (hMemSpace >= 0)
        H5Sclose(hMemSpace);
    if (hFileSpace >= 0)
        H5Sclose(hFileSpace);
    if (hDset >= 0)
        H5Dclose(hDset);
    return bSuccess;
}

CPLErr
NisarDataset::GenerateGCPsFromGeolocationGrid(const char *pszProductGroup)
{
    // DECLARE ALL VARIABLES AT THE TOP
    CPLErr eErr = CE_Failure;
    hid_t hGridGroup = -1;
    hid_t hAttr = -1;
    hid_t hScalarDset = -1;
    OGRSpatialReference *poCRS = nullptr;
    std::vector<GDAL_GCP> gcp_list;
    char *pszStartTimeStr = nullptr;

    hid_t hEpsgDset;
    long long epsg_code = 0;
    std::vector<double> x_coords, y_coords, slant_ranges, azimuth_times;
    double startingRange = 0.0, rangePixelSpacing = 0.0, prf = 0.0,
           scene_start_time = 0.0;
    double gcp_unix_time;

    double time_epoch = 0.0;
    hid_t hAzimuthTimeDset = -1;
    std::string time_units;
    int nEpochYear, nEpochMonth, nEpochDay, nEpochHour, nEpochMin, nEpochSec;
    struct tm epoch_tm;

    hid_t hSlantRangeDset = -1;
    hid_t hMemSpace = -1;
    hid_t hFileSpace = -1;
    hid_t hStrType = -1;

    hsize_t mem_dims[1];
    hsize_t offset[1];
    hsize_t count[1];

    bool bStartTimeAllocatedByHDF5 = false;

    // Declare all path strings here, using member variables
    // m_sInst ("LSAR" or "SSAR") was set in NisarDataset::Open()
    std::string sStartTimePath =
        "/science/" + m_sInst + "/identification/zeroDopplerStartTime";

    std::string sGridPath = "/science/" + m_sInst + "/" + pszProductGroup +
                            "/metadata/geolocationGrid";

    std::string sSwathPath =
        "/science/" + m_sInst + "/" + pszProductGroup + "/swaths/frequencyA/";

    std::string sStartingRangePath = sSwathPath + "startingRange";
    std::string sSlantRangeSpacingPath = sSwathPath + "slantRangeSpacing";
    std::string sPulseRepetitionFrequencyPath =
        sSwathPath + "nominalAcquisitionPRF";
    std::string sSlantRangePath = sSwathPath + "slantRange";

    // Open the geolocationGrid group
    hGridGroup = H5Gopen2(hHDF5, sGridPath.c_str(), H5P_DEFAULT);
    if (hGridGroup < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Could not open geolocationGrid group at %s",
                 sGridPath.c_str());
        goto cleanup;
    }

    // Read the EPSG code from the scalar DATASET
    hEpsgDset = H5Dopen2(hGridGroup, "epsg", H5P_DEFAULT);
    if (hEpsgDset < 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to open 'epsg' dataset in geolocationGrid.");
        goto cleanup;
    }

    // Use H5T_NATIVE_INT to match the Int32 spec. Reading into a long long is safe.
    if (H5Dread(hEpsgDset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &epsg_code) < 0 ||
        epsg_code <= 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to read a valid EPSG code from 'epsg' dataset.");
        //H5Dclose(hEpsgDset); // Close handle even on read failure
        goto cleanup;
    }

    //H5Dclose(hEpsgDset); // Close handle on success
    CPLDebug("NISAR_DRIVER", "Read EPSG code %lld from dataset.", epsg_code);

    // Read the grid vectors into memory
    if (!Read2DSliceAsVec(hGridGroup, "coordinateX", x_coords) ||
        !Read2DSliceAsVec(hGridGroup, "coordinateY", y_coords) ||
        !Read1DDoubleVec(hGridGroup, "slantRange", slant_ranges) ||
        !Read1DDoubleVec(hGridGroup, "zeroDopplerTime", azimuth_times))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to read one or more geolocation grid datasets.");
        goto cleanup;
    }
    CPLDebug(
        "NISAR_DRIVER",
        "Read geolocation grid arrays. Azimuth points: %zu, Range points: %zu",
        azimuth_times.size(), slant_ranges.size());

    // Get the time epoch for the azimuth grid
    time_epoch = 0.0;
    hAzimuthTimeDset = H5Dopen2(hGridGroup, "zeroDopplerTime", H5P_DEFAULT);
    if (hAzimuthTimeDset < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to open geolocationGrid/zeroDopplerTime dataset.");
        goto cleanup;
    }
    time_units = ReadH5StringAttribute(hAzimuthTimeDset, "units");
    //H5Dclose(hAzimuthTimeDset);

    if (sscanf(time_units.c_str(), "seconds since %d-%d-%dT%d:%d:%d",
               &nEpochYear, &nEpochMonth, &nEpochDay, &nEpochHour, &nEpochMin,
               &nEpochSec) == 6)
    {
        struct tm epoch_tm;
        memset(&epoch_tm, 0, sizeof(epoch_tm));
        epoch_tm.tm_year = nEpochYear - 1900;
        epoch_tm.tm_mon = nEpochMonth - 1;
        epoch_tm.tm_mday = nEpochDay;
        epoch_tm.tm_hour = nEpochHour;
        epoch_tm.tm_min = nEpochMin;
        epoch_tm.tm_sec = nEpochSec;
        time_epoch = static_cast<double>(CPLYMDHMSToUnixTime(&epoch_tm));
    }
    else
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Could not parse time epoch from units: %s",
                 time_units.c_str());
        goto cleanup;
    }

    // Read scalar parameters for pixel/line conversion

    // Read the first value from the "slantRange" array dataset
    hSlantRangeDset = H5Dopen2(hHDF5, sSlantRangePath.c_str(), H5P_DEFAULT);
    if (hSlantRangeDset < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to open 'slantRange' dataset.");
        goto cleanup;
    }

    // Create a memory space for a single double value
    mem_dims[0] = 1;
    hMemSpace = H5Screate_simple(1, mem_dims, nullptr);
    if (hMemSpace < 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to create memory space for 'slantRange' read.");
        goto cleanup;
    }

    // Select the first element from the file's dataspace
    hFileSpace = H5Dget_space(hSlantRangeDset);
    if (hFileSpace < 0)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Failed to get filespace for 'slantRange' dataset.");
        goto cleanup;
    }
    offset[0] = 0;
    count[0] = 1;
    H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count,
                        nullptr);

    // Read the single value
    if (H5Dread(hSlantRangeDset, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpace,
                H5P_DEFAULT, &startingRange) < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to read first element from 'slantRange' dataset.");
        goto cleanup;
    }

    CPLDebug("NISAR_DRIVER", "Read startingRange: %.10g", startingRange);

    // Read slantRangeSpacing
    hScalarDset = H5Dopen2(hHDF5, sSlantRangeSpacingPath.c_str(), H5P_DEFAULT);
    if (hScalarDset < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to open slantRangeSpacing.");
        goto cleanup;
    }
    if (H5Dread(hScalarDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &rangePixelSpacing) < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read slantRangeSpacing.");
        goto cleanup;
    }
    //H5Dclose(hScalarDset);
    //hScalarDset = -1;

    // Read processedPulseRepetitionFrequency
    hScalarDset =
        H5Dopen2(hHDF5, sPulseRepetitionFrequencyPath.c_str(), H5P_DEFAULT);
    if (hScalarDset < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to open processedPulseRepetitionFrequency.");
        goto cleanup;
    }
    if (H5Dread(hScalarDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &prf) < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to read processedPulseRepetitionFrequency.");
        goto cleanup;
    }
    //H5Dclose(hScalarDset);
    //hScalarDset = -1;

    // Read and parse the Scene Start Time STRING
    hScalarDset = H5Dopen2(hHDF5, sStartTimePath.c_str(), H5P_DEFAULT);
    if (hScalarDset >= 0)
    {
        hStrType = H5Dget_type(hScalarDset);
        if (hStrType >= 0)
        {
            if (H5Tis_variable_str(hStrType) > 0)
            {  // Handle variable-length
                // NOTE: This H5Dread allocates memory that must be freed with H5free_memory
                H5Dread(hScalarDset, hStrType, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                        &pszStartTimeStr);
                if (pszStartTimeStr != nullptr)
                {
                    bStartTimeAllocatedByHDF5 = true;
                }
            }
            else
            {  //Handle fixed-length
                size_t nSize = H5Tget_size(hStrType);
                if (nSize > 0)
                {
                    // Allocate with CPLMalloc, free with CPLFree in cleanup block
                    pszStartTimeStr = (char *)CPLMalloc(nSize + 1);
                    if (H5Dread(hScalarDset, hStrType, H5S_ALL, H5S_ALL,
                                H5P_DEFAULT, pszStartTimeStr) >= 0)
                    {
                        pszStartTimeStr[nSize] =
                            '\0';  // Ensure null termination
                    }
                    else
                    {
                        CPLFree(pszStartTimeStr);
                        pszStartTimeStr = nullptr;  // Reset on failure
                    }
                }
            }
            //H5Tclose(hStrType);
        }
        //H5Dclose(hScalarDset);
        //hScalarDset = -1;
    }

    if (pszStartTimeStr == nullptr || pszStartTimeStr[0] == '\0')
    {
        CPLError(CE_Failure, CPLE_FileIO,
                 "Failed to read a valid zeroDopplerStartTime string.");
        goto cleanup;
    }

    int nYear, nMonth, nDay, nHour, nMin;
    double dfSec;
    if (sscanf(pszStartTimeStr, "%d-%d-%dT%d:%d:%lf", &nYear, &nMonth, &nDay,
               &nHour, &nMin, &dfSec) == 6)
    {
        struct tm brokendown_time;
        memset(&brokendown_time, 0,
               sizeof(brokendown_time));  // Important: zero out the struct
        brokendown_time.tm_year = nYear - 1900;
        brokendown_time.tm_mon = nMonth - 1;
        brokendown_time.tm_mday = nDay;
        brokendown_time.tm_hour = nHour;
        brokendown_time.tm_min = nMin;
        scene_start_time =
            static_cast<double>(CPLYMDHMSToUnixTime(&brokendown_time)) + dfSec;
        CPLDebug("NISAR_DRIVER",
                 "Parsed start time %s to %f seconds since epoch.",
                 pszStartTimeStr, scene_start_time);
    }
    else
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Could not parse zeroDopplerStartTime string: %s",
                 pszStartTimeStr);
        goto cleanup;
    }

    // Create the CRS from the EPSG code
    poCRS = new OGRSpatialReference();
    if (poCRS->importFromEPSG(static_cast<int>(epsg_code)) != OGRERR_NONE)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to import EPSG:%lld.",
                 epsg_code);
        goto cleanup;
    }
    poCRS->SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);

    // Build the GCP list
    try
    {
        gcp_list.reserve(azimuth_times.size() * slant_ranges.size());
        for (size_t i = 0; i < azimuth_times.size(); ++i)
        {
            for (size_t j = 0; j < slant_ranges.size(); ++j)
            {
                GDAL_GCP gcp;
                size_t grid_index = i * slant_ranges.size() + j;

                gcp.dfGCPX = x_coords.at(grid_index);
                gcp.dfGCPY = y_coords.at(grid_index);
                gcp.dfGCPZ = 0.0;

                gcp.dfGCPPixel =
                    ((slant_ranges.at(j) - startingRange) / rangePixelSpacing) +
                    0.5;
                // Convert grid azimuth time to Unix time before subtracting
                gcp_unix_time = time_epoch + azimuth_times.at(i);
                gcp.dfGCPLine =
                    ((gcp_unix_time - scene_start_time) * prf) + 0.5;

                gcp.pszId = CPLStrdup(CPLSPrintf("%zu", gcp_list.size() + 1));
                gcp.pszInfo = CPLStrdup("");

                gcp_list.push_back(gcp);
            }
        }
    }
    catch (const std::exception &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Exception while building GCP list: %s", e.what());
        goto cleanup;
    }

    // Set the GCPs on the dataset
    this->SetGCPs(gcp_list.size(), gcp_list.data(), poCRS);
    CPLDebug("NISAR_DRIVER", "Successfully set %zu GCPs on the dataset.",
             gcp_list.size());
    eErr = CE_None;  // Success!

cleanup:
    // Clean up all resources
    if (pszStartTimeStr)
    {
        if (bStartTimeAllocatedByHDF5)
        {
            H5free_memory(pszStartTimeStr);
        }
        else
        {
            CPLFree(pszStartTimeStr);
        }
    }
    if (hScalarDset >= 0)
        H5Dclose(hScalarDset);
    if (hGridGroup >= 0)
        H5Gclose(hGridGroup);
    if (hEpsgDset >= 0)
        H5Dclose(hEpsgDset);
    if (hSlantRangeDset >= 0)
        H5Dclose(hSlantRangeDset);
    if (hMemSpace >= 0)
        H5Sclose(hMemSpace);
    if (hFileSpace >= 0)
        H5Sclose(hFileSpace);
    if (hStrType >= 0)
        H5Tclose(hStrType);
    if (poCRS)
        poCRS->Release();

    // The GCP list is now owned by the dataset, so we do NOT free it here.

    return eErr;
}
