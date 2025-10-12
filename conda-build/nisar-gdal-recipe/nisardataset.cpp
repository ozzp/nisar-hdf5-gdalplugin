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

#include <sstream>   // For std::ostringstream
#include <iomanip>   // For std::setprecision

#include "cpl_port.h"
#include "cpl_conv.h"
#include "cpl_time.h"
#include "nisar_priv.h"
#include "H5FDros3.h" // For H5FD_ros3_fapl_t and H5FD_CURR_ROS3_FAPL_T_VERSION

static std::string ReadStringAttribute(hid_t hLocation, const char* pszAttrName)
{
    hid_t hAttr = H5Aopen(hLocation, pszAttrName, H5P_DEFAULT);
    if (hAttr < 0) return "";

    std::string attr_val = "";
    hid_t hAttrType = H5Aget_type(hAttr);
    if (hAttrType >= 0)
    {
        if (H5Tis_variable_str(hAttrType) > 0) {
            char *pszVal = nullptr;
            if (H5Aread(hAttr, hAttrType, &pszVal) >= 0 && pszVal) {
                attr_val = pszVal;
                H5free_memory(pszVal);
            }
        } else { // Fixed-length string
            size_t nSize = H5Tget_size(hAttrType);
            char *pszVal = (char *)CPLMalloc(nSize + 1);
            if (H5Aread(hAttr, hAttrType, pszVal) >= 0) {
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

// Forward declaration for the helper function
static std::string get_hdf5_object_name(hid_t hObjID);

// DEFINE THE CONSTANT HERE
// TBD: Define default dataset for each product?
const char *DEFAULT_NISAR_HDF5_PATH = "/science/LSAR/GSLC/grids/frequencyA/HH"; // Example

// New Callback Struct and Function for SetMetadataItem
struct NISAR_AttrCallbackData_SetItem {
    NisarDataset* poDS; // Pointer to the dataset object
};

// Struct and Callback for Reading Identification Datasets
struct NISAR_IdentCallbackData {
    char ***ppapszList; // Pointer to the CSL list pointer being built
    hid_t hIdentGroup;   // Handle to the identification group
};


// Struct to pass data to H5Aiterate callback
struct MetadataAttrCallbackData {
    char ***ppapszList;        // Pointer to the CSL list pointer in GetMetadata
    const char *pszObjectPath; // Full path of the object being iterated
};

// Struct to pass data to H5Ovisit callback
struct MetadataVisitData {
    char ***ppapszList; // Pointer to the CSL list pointer in GetMetadata
    // Could add filters here, e.g., std::string rootPathFilter;
};

// Define struct to pass data to visitor
struct NISARVisitorData {
    std::vector<std::string>* pFoundPaths; // Pointer to list in Open()
    hid_t hStartingGroupID; // Pass group/file ID for opening datasets inside visitor
    // Add other necessary data e.g., const char* pszRequiredPrefix;
};

// Callback for H5LiterateByName - reads scalar datasets in identification group
static herr_t NISAR_IdentificationDatasetCallback(hid_t group_id, const char *member_name,
                                                  const H5L_info2_t * /*linfo*/, void *op_data)
{
    NISAR_IdentCallbackData *data = static_cast<NISAR_IdentCallbackData*>(op_data);
    if (!data || !data->ppapszList || data->hIdentGroup < 0) return H5_ITER_ERROR;

    hid_t dset_id = -1;
    hid_t dtype = -1;
    hid_t dspace = -1;
    std::string value_str = "(Error reading dataset)";
    herr_t status = -1;
    bool bValueSet = false;

    // Check if the object is a dataset (optional, H5Dopen2 will fail otherwise)
    H5O_info2_t oinfo;
    if (H5Oget_info_by_name3(group_id, member_name, &oinfo, H5O_INFO_BASIC, H5P_DEFAULT) < 0 || oinfo.type != H5O_TYPE_DATASET) {
        return H5_ITER_CONT; // Skip non-datasets
    }

    // Open the dataset
    dset_id = H5Dopen2(data->hIdentGroup, member_name, H5P_DEFAULT);
    if (dset_id < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "IdentCallback: Failed to open dataset '%s'", member_name);
        return H5_ITER_CONT; // Continue
    }

    dspace = H5Dget_space(dset_id);
    dtype = H5Dget_type(dset_id);
    if (dspace < 0 || dtype < 0) goto ident_cleanup;

    // Check if it's a scalar dataset
    if (H5Sget_simple_extent_type(dspace) == H5S_SCALAR) {
        H5T_class_t type_class = H5Tget_class(dtype);
        size_t type_size = H5Tget_size(dtype);

        // Read Scalar Value (Primarily expecting strings)
        if (type_class == H5T_STRING) {
            char *pszReadVL = nullptr; char *pszReadFixed = nullptr;
            bool bIsVariable = H5Tis_variable_str(dtype);
            hid_t mem_type = H5Tcopy(H5T_C_S1); if(mem_type < 0) goto ident_cleanup;

            if(bIsVariable) {
                H5Tset_size(mem_type, H5T_VARIABLE);
                status = H5Dread(dset_id, mem_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &pszReadVL);
                if(status >= 0 && pszReadVL != nullptr) { value_str = pszReadVL; bValueSet = true; H5free_memory(pszReadVL); }
                else { CPLError(CE_Warning, CPLE_FileIO, "Failed read VLEN string dataset '%s'", member_name); }
            } else if (type_size > 0) { // Fixed length string
                H5Tset_size(mem_type, type_size); H5Tset_strpad(mem_type, H5T_STR_NULLTERM);
                pszReadFixed = (char *)VSIMalloc(type_size + 1);
                if (pszReadFixed) {
                     status = H5Dread(dset_id, mem_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszReadFixed);
                     if (status >= 0) { pszReadFixed[type_size] = '\0'; value_str = pszReadFixed; bValueSet = true; }
                     else { CPLError(CE_Warning, CPLE_FileIO, "Failed read fixed string dataset '%s'", member_name); }
                     VSIFree(pszReadFixed);
                } else { CPLError(CE_Failure, CPLE_OutOfMemory, "Malloc failed for fixed string dataset '%s'", member_name); }
            }
            if (mem_type >= 0) {
	        H5Tclose(mem_type);
	        mem_type = -1;
            }
        }
        // Add simple integer/float reads if needed for identification group
        else if (type_class == H5T_INTEGER) {
             long long llVal = 0; status = H5Dread(dset_id, H5T_NATIVE_LLONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, &llVal);
             if(status >= 0) { value_str = CPLSPrintf("%lld", llVal); bValueSet = true; }
             else { CPLError(CE_Warning, CPLE_FileIO, "Failed read integer dataset '%s'", member_name); }
        }
        else if (type_class == H5T_FLOAT) {
             double dfVal = 0.0; status = H5Dread(dset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &dfVal);
             if(status >= 0) { value_str = CPLSPrintf("%.18g", dfVal); bValueSet = true; }
             else { CPLError(CE_Warning, CPLE_FileIO, "Failed read float dataset '%s'", member_name); }
        }
        else {
             value_str = "(unhandled scalar dataset type)"; bValueSet = true;
        }
    } else {
        // Skip non-scalar datasets within identification group
        CPLDebug("NISAR_IDENT_CB", "Skipping non-scalar dataset '%s' in identification group.", member_name);
    }

    // If we successfully read a value, add NAME=VALUE to list
    if (bValueSet) {
        *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), member_name, value_str.c_str());
    }

ident_cleanup:
    if (dtype >= 0) H5Tclose(dtype);
    if (dspace >= 0) H5Sclose(dspace);
    if (dset_id >= 0) H5Dclose(dset_id);
    return 0; // Continue H5Literate iteration
}

// Visitor callback function compatible with H5LiterateByName
static herr_t NISAR_FindDatasetsVisitor(hid_t obj_id /* ID of object being visited */,
                                        const char *name /* Full path of the object relative to hStartGroupID */,
                                        const H5O_info2_t *oinfo /* Object info */,
                                        void *op_data)
{
   // Mark obj_id as unused if not needed directly
    (void)obj_id;

    NISARVisitorData *data = static_cast<NISARVisitorData *>(op_data);
    // Validate the data pointer passed from the caller
    if (!data || !data->pFoundPaths ) {
        CPLError(CE_Failure, CPLE_AppDefined, "Visitor callback received invalid op_data.");
        return H5_ITER_ERROR; // Stop iteration
    }

    // Log entry for every object visited
    CPLDebug("NISAR_VISITOR_DETAIL", "Visiting object: Path='%s', Type=%d", name, (int)oinfo->type);

    // Filter 1: Only consider actual Datasets
    if (oinfo->type != H5O_TYPE_DATASET) {
        CPLDebug("NISAR_VISITOR_DETAIL", "--> Skipping '%s' (Not a dataset)", name);
        return H5_ITER_CONT; // Skip groups, named types, etc.
    }

    // Filter 2: Check if path starts with science/LSAR/
    // 'name' provided by H5Ovisit (when starting from root) is the full path *without* leading slash.
    const char* requiredPrefix = "science/LSAR/";
    if (strncmp(name, requiredPrefix, strlen(requiredPrefix)) != 0) {
         CPLDebug("NISAR_VISITOR_DETAIL", "--> Skipping '%s' (Path does not start with %s)", name, requiredPrefix);
         return H5_ITER_CONT; // Skip datasets outside the main science group
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
    if (dset_id < 0) {
         CPLError(CE_Warning, CPLE_AppDefined, "Could not open dataset '%s' using H5Dopen2 during subdataset discovery.", full_path.c_str());
         return H5_ITER_CONT; // Skip if cannot open
    }

    // Check if it's actually a dataset type after opening
    if (H5Iget_type(dset_id) != H5I_DATASET) {
         CPLDebug("NISAR_VISITOR_DETAIL", "--> Skipping '%s' (Opened object is not H5I_DATASET)", full_path.c_str());
         H5Dclose(dset_id);
         return H5_ITER_CONT;
    }

    dspace_id = H5Dget_space(dset_id); // Use H5D function on dataset ID
    if (dspace_id >= 0) {
        rank = H5Sget_simple_extent_ndims(dspace_id);
        H5Sclose(dspace_id); // Close dataspace handle
    } else {
         CPLError(CE_Warning, CPLE_AppDefined, "Could not get dataspace for dataset '%s'.", full_path.c_str());
         H5Dclose(dset_id);
         return H5_ITER_CONT;
    }

    // Close dataset handle opened for checks
    H5Dclose(dset_id);

    if (rank < 2) {
        CPLDebug("NISAR_VISITOR", "Skipping dataset '%s' (rank %d < 2)", full_path.c_str(), rank);
        return H5_ITER_CONT; // Skip scalar or 1D datasets
    }

    //  Store Result
    // If all filters passed, add the full path (including leading slash) to the list
    CPLDebug("NISAR_VISITOR", "Adding relevant dataset to list: %s (Rank: %d)", full_path.c_str(), rank);
    try {
         data->pFoundPaths->push_back(full_path); // Store the full path
    } catch (const std::bad_alloc&) {
         CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to store dataset path '%s'.", full_path.c_str());
         return H5_ITER_ERROR; // Stop iteration on memory error
    }

    return H5_ITER_CONT; // Continue iteration
}

static bool Read1DDoubleVec(hid_t hFile, const char* pszPath, std::vector<double>& vec)
{
    hid_t hDset = H5Dopen2(hFile, pszPath, H5P_DEFAULT);
    if (hDset < 0) {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to open dataset: %s", pszPath);
        return false;
    }

    hid_t hSpace = H5Dget_space(hDset);
    int nDims = H5Sget_simple_extent_ndims(hSpace);
    hsize_t dims[1];
    H5Sget_simple_extent_dims(hSpace, dims, nullptr);

    if (nDims != 1 || dims[0] == 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "Dataset %s is not a non-empty 1D array.", pszPath);
        H5Sclose(hSpace);
        H5Dclose(hDset);
        return false;
    }

    try {
        vec.resize(dims[0]);
    } catch (const std::bad_alloc&) {
        CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate memory for %s", pszPath);
        H5Sclose(hSpace);
        H5Dclose(hDset);
        return false;
    }

    herr_t status = H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, vec.data());

    H5Sclose(hSpace);
    H5Dclose(hDset);

    if (status < 0) {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to read data from %s", pszPath);
        return false;
    }
    return true;
}

GDALDataType NisarDataset::GetGDALDataType(hid_t hH5Type)
{
    // Check Simple Native Types First
    // Use H5Tequal for robust comparison against predefined native types
    if( H5Tequal(hH5Type, H5T_NATIVE_FLOAT) > 0 )  return GDT_Float32;
    if( H5Tequal(hH5Type, H5T_NATIVE_DOUBLE) > 0 ) return GDT_Float64;
    if( H5Tequal(hH5Type, H5T_NATIVE_UINT8) > 0 )  return GDT_Byte; // Standard GDAL Byte is often treated as unsigned
    if( H5Tequal(hH5Type, H5T_NATIVE_INT8) > 0 )   return GDT_Byte; // Map signed char to GDT_Byte too? Review if GDT_Int8 needed.
    if( H5Tequal(hH5Type, H5T_NATIVE_INT16) > 0 )  return GDT_Int16;
    if( H5Tequal(hH5Type, H5T_NATIVE_UINT16) > 0 ) return GDT_UInt16;
    if( H5Tequal(hH5Type, H5T_NATIVE_INT32) > 0 )  return GDT_Int32;
    if( H5Tequal(hH5Type, H5T_NATIVE_UINT32) > 0 ) return GDT_UInt32;
    // Check if GDAL version supports 64-bit integers if needed
    if( H5Tequal(hH5Type, H5T_NATIVE_INT64) > 0 )  return GDT_Int64;
    if( H5Tequal(hH5Type, H5T_NATIVE_UINT64) > 0 ) return GDT_UInt64;
    // Add other simple native types if necessary (char, long, etc.)

    // Check if it's a Compound Type (Potentially Complex)
    H5T_class_t eHDF5Class = H5Tget_class(hH5Type);
    if (eHDF5Class == H5T_COMPOUND) {
        CPLDebug("NISAR_GetGDALDataType", "Checking HDF5 Compound type for complex mapping.");
        GDALDataType eComplexType = GDT_Unknown; // Variable to return if complex found

        // Check if it has exactly 2 members (common for complex: real, imag)
        if (H5Tget_nmembers(hH5Type) == 2) {
            hid_t hRealType = H5Tget_member_type(hH5Type, 0); // Type of first member
            hid_t hImagType = H5Tget_member_type(hH5Type, 1); // Type of second member
            char *name1 = nullptr; // Member names
            char *name2 = nullptr;

            // Ensure we got valid type handles before proceeding
            if (hRealType >= 0 && hImagType >= 0) {
                // Check if both members have the same base data type
                if (H5Tequal(hRealType, hImagType) > 0) {
                    // Check member names convention ('r'/'R' and 'i'/'I')
                    name1 = H5Tget_member_name(hH5Type, 0);
                    name2 = H5Tget_member_name(hH5Type, 1);

                    // Check name validity and conventional naming for complex parts
                    bool isReal = (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                    bool isImaginary = (name2 && (name2[0] == 'i' || name2[0] == 'I'));

                    if (isReal && isImaginary) {
                        size_t elem_size = H5Tget_size(hRealType); // Size of each component

                        // Map based on underlying HDF5 native type and size
                        if (H5Tequal(hRealType, H5T_NATIVE_FLOAT) > 0 && elem_size == sizeof(float))
                            eComplexType = GDT_CFloat32;
                        else if (H5Tequal(hRealType, H5T_NATIVE_DOUBLE) > 0 && elem_size == sizeof(double))
                            eComplexType = GDT_CFloat64;
                        else if (H5Tequal(hRealType, H5T_NATIVE_SHORT) > 0 && elem_size == sizeof(short))
                            eComplexType = GDT_CInt16;
                        else if (H5Tequal(hRealType, H5T_NATIVE_INT) > 0 && elem_size == sizeof(int))
                            eComplexType = GDT_CInt32;
                        // Add other complex types if needed
                    }
                } // end if types equal

                // Close the member type handles obtained from H5Tget_member_type
                 H5Tclose(hRealType); hRealType = -1;
                 H5Tclose(hImagType); hImagType = -1;
            } else { /* Handle error getting member types */
                 if(hRealType >= 0) H5Tclose(hRealType);
                 if(hImagType >= 0) H5Tclose(hImagType);
            }
            // Free memory allocated by H5Tget_member_name
            if(name1) H5free_memory(name1);
            if(name2) H5free_memory(name2);
        } // end if nmembers == 2

        // If we identified a complex type, return it
        if (eComplexType != GDT_Unknown) {
             return eComplexType;
        }
        CPLDebug("NISAR_GetGDALDataType", "Compound type did not match expected complex structure.");
    } // end if compound

    // Handle Other HDF5 Types or Unknown
    // Example: Check for strings if needed (unlikely for main raster bands)
    // if (eHDF5Class == H5T_STRING) { return GDT_String; }

    CPLError(CE_Warning, CPLE_AppDefined,
             "NisarDataset::GetGDALDataType(): Unhandled or unsupported HDF5 data type (Class: %d).", (int)eHDF5Class);
    return GDT_Unknown; // Default fallback if no mapping found
}

//------------------------------------------------------------------------------
// get_hdf5_object_name (Static Helper)
//------------------------------------------------------------------------------
static std::string get_hdf5_object_name(hid_t hObjID) {
    ssize_t name_len_signed;
    size_t name_len;
    char *name_buffer = nullptr;
    std::string object_name = "";

    if (hObjID < 0) return object_name; // Return empty string for invalid handle

    // Call 1: Get length
    name_len_signed = H5Iget_name(hObjID, nullptr, 0);
    if (name_len_signed <= 0) { // Error or empty name (e.g., root group "/")
        // For root group H5Iget_name might return 1 for "/"? Test needed.
        // If truly empty or error, return empty string.
         if (name_len_signed < 0)
              CPLError(CE_Warning, CPLE_AppDefined, "H5Iget_name failed to get object name length.");
         return object_name;
    }

    name_len = static_cast<size_t>(name_len_signed);
    name_buffer = (char *)CPLMalloc(name_len + 1); // Use CPLMalloc
    if (name_buffer == nullptr) {
        CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate %lu bytes for HDF5 object name.", (unsigned long)(name_len + 1));
        return object_name; // Return empty string
    }

    // Call 2: Get name
    if (H5Iget_name(hObjID, name_buffer, name_len + 1) < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "H5Iget_name failed to retrieve object name.");
        CPLFree(name_buffer);
        return object_name; // Return empty string
    }

    name_buffer[name_len] = '\0'; // Ensure null termination
    object_name = name_buffer; // Copy to std::string
    CPLFree(name_buffer); // Free CPL buffer
    return object_name;
}


//------------------------------------------------------------------------------
// ReadGeoTransformAttribute (Static Helper)
//------------------------------------------------------------------------------
static CPLErr ReadGeoTransformAttribute(hid_t hLocationID, const char *pszAttrName, double *padfTransform)
{
    // Initialize handles and variables
    hid_t hAttr = -1;
    hid_t hAttrType = -1;
    hid_t hAttrSpace = -1;
    hssize_t nPoints = -1;
    CPLErr eErr = CE_Failure; // Default to failure

    // Check attribute existence
    htri_t attrExists = H5Aexists_by_name(hLocationID, ".", pszAttrName, H5P_DEFAULT);
    if (attrExists <= 0) {
        if (attrExists < 0) CPLError(CE_Warning, CPLE_AppDefined, "HDF5 error checking existence of attribute '%s'.", pszAttrName);
        else CPLDebug("NISAR_ATTR", "Attribute '%s' does not exist on object %lld.", pszAttrName, (long long)hLocationID);
        goto cleanup;
    }

    // Open attribute
    hAttr = H5Aopen(hLocationID, pszAttrName, H5P_DEFAULT);
    if (hAttr < 0) { CPLError(CE_Warning, CPLE_AppDefined, "H5Aopen failed for attribute '%s'.", pszAttrName); goto cleanup; }

    // Check attribute type
    hAttrType = H5Aget_type(hAttr);
    if (hAttrType < 0 || !H5Tequal(hAttrType, H5T_NATIVE_DOUBLE)) { CPLError(CE_Warning, CPLE_AppDefined, "Attribute '%s' is not of type NATIVE_DOUBLE.", pszAttrName); goto cleanup; }

    // Check attribute space (dataspace size)
    hAttrSpace = H5Aget_space(hAttr);
    nPoints = (hAttrSpace >= 0) ? H5Sget_simple_extent_npoints(hAttrSpace) : -1;
    if (nPoints != 6) { CPLError(CE_Warning, CPLE_AppDefined, "Attribute '%s' does not have 6 elements (found %lld).", pszAttrName, (long long)nPoints); goto cleanup; }

    // Read attribute data
    if (H5Aread(hAttr, H5T_NATIVE_DOUBLE, padfTransform) < 0) { CPLError(CE_Warning, CPLE_FileIO, "H5Aread failed for attribute '%s'.", pszAttrName); goto cleanup; }

    eErr = CE_None; // Success!

cleanup: // Label for resource cleanup
    if(hAttrType >= 0) H5Tclose(hAttrType);
    if(hAttrSpace >= 0) H5Sclose(hAttrSpace);
    if (hAttr >= 0) H5Aclose(hAttr);
    return eErr; // Return final status
}

/************************************************************************/
/*                            NisarDataset()                          */
/************************************************************************/

NisarDataset::NisarDataset() :
    hHDF5(H5I_INVALID_HID),
    hDataset(H5I_INVALID_HID),
    eDataType(GDT_Unknown),
    pszFilename(nullptr),
    papszSubDatasets(nullptr),
    m_poSRS(nullptr),             // Initialize SRS pointer
    m_bGotSRS(false),             // Initialize SRS flag
    m_bGotMetadata(false),
    m_papszGlobalMetadata(nullptr), // Initialize global metadata cache
    m_bGotGlobalMetadata(false)     // Initialize global metadata flag
{

    // Initialize GT array cache if using caching pattern for it
    // m_adfGeoTransform[0] = 0.0; m_adfGeoTransform[1] = 1.0; m_adfGeoTransform[2] = 0.0;
    // m_adfGeoTransform[3] = 0.0; m_adfGeoTransform[4] = 0.0; m_adfGeoTransform[5] = 1.0;
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
    CSLDestroy( papszSubDatasets );
    papszSubDatasets = nullptr; // nullify after destroy

    // Close HDF5 handles if they are valid
    if( hDataset >= 0 )
    {
        H5Dclose(hDataset);
        hDataset = -1; // Reset handle ID after closing
    }

    if( hHDF5 >= 0 )
    {
        H5Fclose(hHDF5);
        hHDF5 = -1; // Reset handle ID *after* closing
    }

    // Free the filename buffer allocated in Open()
    CPLFree(pszFilename);
    pszFilename = nullptr; // Good practice

    // Clean up SRS cache
    if (m_poSRS != nullptr) {
        m_poSRS->Release(); // Use OGR reference counting
        m_poSRS = nullptr;
    }

    // Clean up Metadata caches
    CSLDestroy(m_papszGlobalMetadata); // Destroy global metadata list
    m_papszGlobalMetadata = nullptr;
}

/************************************************************************/
/*                          GetMetadataDomainList()                     */
/************************************************************************/

char **NisarDataset::GetMetadataDomainList()
{
     CPLDebug("NISAR_DRIVER", "GetMetadataDomainList called");
     char **papszDomains = GDALPamDataset::GetMetadataDomainList();
     papszDomains = CSLAddString(papszDomains, "NISAR_GLOBAL"); // Add our global domain
     // Add SUBDATASETS if populated by Open
     if(papszSubDatasets != nullptr) papszDomains = CSLAddString(papszDomains, "SUBDATASETS");
     return papszDomains;
}

/************************************************************************/
/*                            GetMetadata()                             */
/************************************************************************/

char **NisarDataset::GetMetadata( const char *pszDomain )

{
    // Handle NISAR_GLOBAL Domain (Cached)
    if (pszDomain != nullptr && EQUAL(pszDomain, "NISAR_GLOBAL")) {
        std::lock_guard<std::mutex> lock(m_GlobalMetadataMutex);
        CPLDebug("NISAR_DRIVER", "GetMetadata called for NISAR_GLOBAL domain. Cached=%d", m_bGotGlobalMetadata);
        if (m_bGotGlobalMetadata) {
            return CSLDuplicate(m_papszGlobalMetadata); // Return copy
        }
        m_bGotGlobalMetadata = true;
        CSLDestroy(m_papszGlobalMetadata);
        m_papszGlobalMetadata = nullptr;

        if (this->hHDF5 >= 0) {
            // *** FIX: Read attributes directly from the root group (file handle) ***
            CPLDebug("NISAR_DRIVER", "Reading metadata from root group ('/') for NISAR_GLOBAL domain.");
            NISAR_AttrCallbackData callback_data;
            callback_data.ppapszList = &m_papszGlobalMetadata;
            hsize_t idx = 0;
            // Iterate attributes on the root group (represented by the file handle)
            H5Aiterate2(this->hHDF5, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                        NISAR_AttributeCallback, &callback_data);
        } else { CPLError(CE_Warning, CPLE_AppDefined, "Cannot read NISAR_GLOBAL metadata: HDF5 file not open."); }
        CPLDebug("NISAR_DRIVER", "Finished reading for NISAR_GLOBAL. Found %d items.", CSLCount(m_papszGlobalMetadata));
        return CSLDuplicate(m_papszGlobalMetadata); // Return copy
    }

    // Handle SUBDATASETS Domain
    if (pszDomain != nullptr && EQUAL(pszDomain, "SUBDATASETS")) {
         return CSLDuplicate(papszSubDatasets); // Return copy
    }

    // Handle Default Domain ("" or nullptr)
    if (pszDomain == nullptr || EQUAL(pszDomain, "")) {
        std::lock_guard<std::mutex> lock(m_MetadataMutex);
        if (m_bGotMetadata) {
             CPLDebug("NISAR_DRIVER", "GetMetadata('') returning cached PAM list.");
             return GDALPamDataset::GetMetadata(pszDomain);
        }
        m_bGotMetadata = true;
        CPLDebug("NISAR_DRIVER", "GetMetadata('') attempting to load/merge HDF5 attributes.");
        TryLoadXML();

        char **papszHDFMetadata = nullptr;
        NISAR_AttrCallbackData callback_data;
        callback_data.ppapszList = &papszHDFMetadata;

        if (this->hHDF5 < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Cannot read default metadata: HDF5 file handle invalid."); }
        else if (this->hDataset < 0) {
             // Container Dataset Case: Read attributes from ROOT group
             CPLDebug("NISAR_DRIVER", "Reading default metadata from root group ('/') for container dataset.");
             hsize_t idx = 0;
             // *** FIX: Iterate attributes on the root group (file handle) ***
             herr_t iter_status = H5Aiterate2(this->hHDF5, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                         NISAR_AttributeCallback, &callback_data);
             CPLDebug("NISAR_DRIVER", "Finished iterating root attributes (Status: %d). Found %d items for container default metadata.",
                      (int)iter_status, CSLCount(papszHDFMetadata));
             // Fall through to merge papszHDFMetadata into PAM
        } else {
            // Specific Dataset Case: Read attributes from dataset and related objects
            CPLDebug("NISAR_DRIVER", "Reading HDF5 attributes for default metadata domain from specific dataset and related objects.");
            std::string datasetPath = get_hdf5_object_name(this->hDataset);
            std::string groupPath = "";
            // ... (calculate groupPath) ...
            if (!datasetPath.empty()) { if (const char* lastSlash = strrchr(datasetPath.c_str(), '/')) { if (lastSlash == datasetPath.c_str()) groupPath = "/"; else groupPath = datasetPath.substr(0, lastSlash - datasetPath.c_str()); if(groupPath.empty()) groupPath = "/"; } else { groupPath = "/"; } }
            else { CPLError(CE_Warning, CPLE_AppDefined, "Cannot determine dataset path for metadata reading."); }

            std::vector<std::pair<std::string, hid_t>> objects_to_scan;
            objects_to_scan.push_back({datasetPath, this->hDataset});
            hid_t hProjection = -1, hXCoord = -1, hYCoord = -1;
            if (!groupPath.empty()) {
                 // ... (Open projection, xCoords, yCoords, add to objects_to_scan if successful) ...
                 std::string projectionPath = groupPath + (groupPath == "/" ? "" : "/") + "projection"; hProjection = H5Dopen2(this->hHDF5, projectionPath.c_str(), H5P_DEFAULT); if (hProjection >= 0) objects_to_scan.push_back({projectionPath, hProjection});
                 std::string xCoordPath = groupPath + (groupPath == "/" ? "" : "/") + "xCoordinates"; hXCoord = H5Dopen2(this->hHDF5, xCoordPath.c_str(), H5P_DEFAULT); if (hXCoord >= 0) objects_to_scan.push_back({xCoordPath, hXCoord});
                 std::string yCoordPath = groupPath + (groupPath == "/" ? "" : "/") + "yCoordinates"; hYCoord = H5Dopen2(this->hHDF5, yCoordPath.c_str(), H5P_DEFAULT); if (hYCoord >= 0) objects_to_scan.push_back({yCoordPath, hYCoord});
            }

            // Iterate attributes for each relevant object
            for (const auto& pair : objects_to_scan) {
                 const std::string& obj_path = pair.first;
                 hid_t obj_id = pair.second;
                 if(obj_id < 0 || obj_path.empty()) continue;
                 CPLDebug("NISAR_DRIVER", "Reading attributes from: %s", obj_path.c_str());
                 char** papszObjAttrs = nullptr;
                 NISAR_AttrCallbackData obj_callback_data;
                 obj_callback_data.ppapszList = &papszObjAttrs;
                 hsize_t idx = 0;
                 H5Aiterate2(obj_id, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                             NISAR_AttributeCallback, &obj_callback_data);

                 // Add formatted attributes (OBJECT_PATH#NAME=VALUE) to the main HDF5 list
                 for (int i=0; papszObjAttrs != nullptr && papszObjAttrs[i] != nullptr; ++i) {
                      char* pszKey = nullptr; const char* pszValue = CPLParseNameValue(papszObjAttrs[i], &pszKey);
                      if (pszKey && pszValue) { std::string finalKey = obj_path + "#" + pszKey; papszHDFMetadata = CSLSetNameValue(papszHDFMetadata, finalKey.c_str(), pszValue); }
                      CPLFree(pszKey);
                 }
                 CSLDestroy(papszObjAttrs);
            }
            // Close handles opened in the loop
            if(hProjection >= 0) H5Dclose(hProjection); if(hXCoord >= 0) H5Dclose(hXCoord); if(hYCoord >= 0) H5Dclose(hYCoord);
        } // End specific dataset case

        // Now, merge the collected HDF5 attributes (papszHDFMetadata) into PAM
        if (papszHDFMetadata != nullptr) {
             CPLDebug("NISAR_DRIVER", "Merging %d HDF5 attributes into PAM default domain.", CSLCount(papszHDFMetadata));
             SetMetadata(papszHDFMetadata);
             CSLDestroy(papszHDFMetadata);
        }

        // Return the final merged list managed by PAM
        return GDALPamDataset::GetMetadata(pszDomain);
    } // End default domain handling

    // Handle other domains via PAM
    TryLoadXML();
    return GDALPamDataset::GetMetadata(pszDomain);
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
GDALDataset *NisarDataset::Open( GDALOpenInfo * poOpenInfo ) {
    // Extract Filename and Subdataset Path
    const char *pszFullInput = poOpenInfo->pszFilename;
    const char *pszDataIdentifier = nullptr; // Part after "NISAR:" prefix
    char *pszActualFilename = nullptr;       // Buffer for isolated filename (local path or /vsis3/ path)
    const char *pszSubdatasetPath = nullptr; // Points to HDF5 path part, or NULL
    const char *pszPrefix = "NISAR:";
    size_t nPrefixLen = strlen(pszPrefix);

    // Check for and strip the driver prefix if present
    if (EQUALN(pszFullInput, pszPrefix, nPrefixLen)) {
        pszDataIdentifier = pszFullInput + nPrefixLen;
        CPLDebug("NISAR_DRIVER", "Identified 'NISAR:' prefix. Actual filename+subdataset: %s", pszDataIdentifier);
    }
    else {
        // If the prefix is not used, the full string is the filename.
        // This might happen if GDAL identifies the file by other means
        // and calls this driver's Open function directly.
        pszDataIdentifier = pszFullInput;
        CPLDebug("NISAR_DRIVER", "No 'NISAR:' prefix found. Using full filename: %s", pszDataIdentifier);
    }

    // Handle empty filename after prefix removal
    if (pszDataIdentifier == nullptr || pszDataIdentifier[0] == '\0') {
         CPLError( CE_Failure, CPLE_OpenFailed,
                  "Empty filename is provided after 'NISAR:' prefix in '%s'", pszFullInput );
        return nullptr;
    }

    // Now, parse pszDataIdentifier to separate filename and HDF5 path
    const char *pszLastColon = strrchr(pszDataIdentifier, ':');

    if (pszLastColon == nullptr) {
        // No colon found; assume entire string is the filename
        // Use CPLStrdup to ensure pszActualFilename is always allocated and needs freeing
        pszActualFilename = CPLStrdup(pszDataIdentifier);
        pszSubdatasetPath = nullptr; // No specific path provided
        CPLDebug("NISAR_DRIVER", "No HDF5 path specified in input. Filename: %s", pszActualFilename);
    } else {
        // Colon found. Split into filename and path.
        // Subdataset path starts AFTER the last colon
        pszSubdatasetPath = pszLastColon + 1;

        // Filename is the part BEFORE the last colon. Copy it.
        size_t nFilenameLen = pszLastColon - pszDataIdentifier;
        pszActualFilename = (char *)CPLMalloc(nFilenameLen + 1);
        strncpy(pszActualFilename, pszDataIdentifier, nFilenameLen);
        pszActualFilename[nFilenameLen] = '\0';

        // Handle case where path part is empty (e.g., "NISAR:file.h5:")
        if (pszSubdatasetPath[0] == '\0') {
            CPLError(CE_Warning, CPLE_AppDefined,
                     "Empty HDF5 path provided after colon for '%s'. Using default.", pszActualFilename);
            pszSubdatasetPath = nullptr; // Treat as unspecified
        }

        CPLDebug("NISAR_DRIVER", "HDF5 path specified in input. Filename: %s, Path: %s",
                 pszActualFilename, pszSubdatasetPath ? pszSubdatasetPath : "(none)");
    }

    // Final check on extracted filename
    if (pszActualFilename == nullptr || pszActualFilename[0] == '\0') {
        CPLError(CE_Failure, CPLE_OpenFailed, "Could not determine filename from input: %s", pszFullInput);
        CPLFree(pszActualFilename); // Free if allocated by CPLMalloc above
        return nullptr;
    }
 
    CPLDebug("NISAR_DRIVER", "Parsed Filename/Path Part: %s", pszActualFilename);
    CPLDebug("NISAR_DRIVER", "Parsed HDF5 Subdataset Path: %s", pszSubdatasetPath ? pszSubdatasetPath : "(none specified)");

    // Basic check: Does it have an .h5 extension? (Weak check)
    //if (!EQUAL(CPLGetExtension(pszActualFilename), "h5")) {
    if (!EQUAL(CPLGetExtensionSafe(pszActualFilename).c_str(), "h5")) {
         return nullptr;
    }

    // Open the HDF5 file and check for specific NISAR metadata/groups.
    // This requires opening the file. If the structure
    // isn't right, clean up and return nullptr.

    // Check Access Mode
    // This driver currently only supports reading.
    if( poOpenInfo->eAccess == GA_Update ) {
        CPLError( CE_Failure, CPLE_NotSupported,
                  "The NISAR driver does not support update access to existing datasets." );
        CPLFree(pszActualFilename);
        return nullptr;
    }
    
    // Prepare Base FAPL & Filename for Pass 1 
    hid_t fapl_id_pass1 = H5P_DEFAULT;
    const char* filenameForH5Fopen = pszActualFilename;
    std::string final_url_storage;
    bool bIsS3 = false;
    const char* s3_path_part = nullptr;
    bool bNeedToCloseFapl1 = false; 
 
    // Prepare for H5Fopen: Handle Local vs S3 & Configure FAPL
    //hid_t hHDF5 = -1;                      // Handle for the opened HDF5 file
    //hid_t fapl_id = H5P_DEFAULT;           // File Access Property List, default for local
    //const char* filenameForH5Fopen = pszActualFilename; // Use parsed name by default
    //std::string final_url_storage;
    //bool bIsS3 = false;                    // Flag to track if we configured for S3
    //const char* s3_path_part = nullptr; // Will point to "bucket/key..." or "s3://bucket/key..."

    // Check for GDAL S3 path OR direct S3 URL
    if (STARTS_WITH_CI(pszActualFilename, "/vsis3/")) {
        bIsS3 = true;
        CPLDebug("NISAR_DRIVER", "Detected /vsis3/ path, configuring HDF5 ROS3 VFD.");
        // Translate /vsis3/bucket/key to s3://bucket/key for HDF5 ROS3 VFD
        s3_path_part = pszActualFilename + strlen("/vsis3/");
        if (*s3_path_part == '\0') { /* Error handling */ 
            CPLError(CE_Failure, CPLE_OpenFailed, "Invalid S3 path: missing bucket/key after /vsis3/ in '%s'", pszActualFilename);
            CPLFree(pszActualFilename); 
            return nullptr;
        }
    }
    else if (STARTS_WITH_CI(pszActualFilename, "s3://")) {
        bIsS3 = true;
        CPLDebug("NISAR_DRIVER", "Detected direct s3:// path, configuring HDF5 ROS3 VFD.");
        s3_path_part = pszActualFilename; // Includes s3:// prefix initially
        // filenameForH5Fopen is already pszActualFilename (which starts with s3://)
    }
    else {
        CPLDebug("NISAR_DRIVER", "Assuming local file path, using default HDF5 FAPL.");
        // Keep bIsS3 = false;
        // Keep fapl_id = H5P_DEFAULT;
        // Keep filenameForH5Fopen = pszActualFilename;
    }

    // Configure FAPL for ROS3 if an S3 path was detected
    if (bIsS3) {

        fapl_id_pass1 = H5Pcreate(H5P_FILE_ACCESS); // Create new FAPL

        if (fapl_id_pass1 < 0) { 
            // Error handling
            CPLFree(pszActualFilename); 
            bNeedToCloseFapl1 = true;
            return nullptr; 
        }

        // Prepare ROS3 VFD Configuration Struct
        H5FD_ros3_fapl_t ros3_fapl_conf;
        // Initialize the struct. Using memset is safest.
        memset(&ros3_fapl_conf, 0, sizeof(H5FD_ros3_fapl_t));
        // Set the structure version (use HDF5 macro)
        ros3_fapl_conf.version = H5FD_CURR_ROS3_FAPL_T_VERSION;
        ros3_fapl_conf.authenticate = TRUE; // Assume authentication needed

        const char* env_region = getenv("AWS_REGION");
        // In H5FDros3.h, the struct members are declared like: char aws_region[H5FD_ROS3_MAX_REGION_LEN + 1];
        // So, strncpy should use the full MAX_LEN. The struct already has space for null terminator.
        if (env_region != nullptr || strlen(env_region) > 0) {
            strncpy(ros3_fapl_conf.aws_region, env_region, H5FD_ROS3_MAX_REGION_LEN);
            ros3_fapl_conf.aws_region[H5FD_ROS3_MAX_REGION_LEN] = '\0'; //Ensure null termination
            CPLDebug("NISAR_DRIVER", "ROS3 Config: Using Region: %s", ros3_fapl_conf.aws_region);
        }
        else {
            // cleanup and return
            CPLError(CE_Failure, CPLE_AppDefined, "AWS_REGION environment variable not set, needed for HDF5 ROS3 VFD / HTTPS URL.");
            H5Pclose(fapl_id_pass1); 
            CPLFree(pszActualFilename); 
            return nullptr;
        }

        // Get Credentials (Optional for struct, VFD might use env/role if missing)
        const char* env_key_id = getenv("AWS_ACCESS_KEY_ID");
        const char* env_secret = getenv("AWS_SECRET_ACCESS_KEY");

        if (env_key_id && *env_key_id) {
             strncpy(ros3_fapl_conf.secret_id, env_key_id, H5FD_ROS3_MAX_SECRET_ID_LEN);
             ros3_fapl_conf.secret_id[H5FD_ROS3_MAX_SECRET_ID_LEN] = '\0';
             CPLDebug("NISAR_DRIVER", "ROS3 Config: Setting Secret ID (Key ID) from env var.");
        }
        if (env_secret && *env_secret) {
             strncpy(ros3_fapl_conf.secret_key, env_secret, H5FD_ROS3_MAX_SECRET_KEY_LEN);
             ros3_fapl_conf.secret_key[H5FD_ROS3_MAX_SECRET_KEY_LEN] = '\0';
             CPLDebug("NISAR_DRIVER", "ROS3 Config: Setting Secret Key from env var.");
        }

        // Configure FAPL using H5Pset_fapl_ros3 ***
        herr_t status = H5Pset_fapl_ros3(fapl_id_pass1, &ros3_fapl_conf);
        if (status < 0) {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass1); 
            CPLFree(pszActualFilename); 
            return nullptr;
        }
        CPLDebug("NISAR_DRIVER", "Configured HDF5 FAPL using H5Pset_fapl_ros3.");

        //SET SESSION TOKEN (if available)
        const char* env_token  = getenv("AWS_SESSION_TOKEN"); // Check if present
        if (env_token != nullptr && strlen(env_token) > 0) {
            CPLDebug("NISAR_DRIVER", "AWS_SESSION_TOKEN found, attempting to set it on FAPL.");
            status = H5Pset_fapl_ros3_token(fapl_id_pass1, env_token);
            if (status < 0) {
                CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3_token failed.");
                H5Eprint(H5E_DEFAULT, stderr);
                H5Pclose(fapl_id_pass1); CPLFree(pszActualFilename); return nullptr;
            }
            CPLDebug("NISAR_DRIVER", "Successfully set session token using H5Pset_fapl_ros3_token.");
        } 
        else {
            CPLDebug("NISAR_DRIVER", "AWS_SESSION_TOKEN environment variable not set or empty. Proceeding without setting token.");
        }
        // END TOKEN SETTING LOGIC

        // Construct HTTPS URL (Workaround for HDF5 1.14.6 scheme bug)
        const char* path_after_scheme = s3_path_part;
        if (STARTS_WITH_CI(s3_path_part, "s3://")) {
             path_after_scheme += strlen("s3://"); // Skip the scheme part if present
        }
        // Now path_after_scheme points to "bucket/key..."
        
        std::string bucket_name;
        std::string object_key;
        // Parse bucketAndKey = "bucket/key/path..."
        const char* first_slash = strchr(path_after_scheme, '/');
        if (first_slash == nullptr) { // Only bucket name? Invalid S3 path for object.
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid S3 path: missing object key in '%s'", pszActualFilename);
            // cleanup and return
            H5Pclose(fapl_id_pass1);
            CPLFree(pszActualFilename);
            return nullptr;
        } else {
            bucket_name.assign(path_after_scheme, first_slash - path_after_scheme);
            object_key.assign(first_slash + 1);
        }
        if (bucket_name.empty()) { // Check for empty bucket name
             CPLError(CE_Failure, CPLE_AppDefined, "Invalid S3 path: empty bucket name parsed from '%s'", path_after_scheme);
             H5Pclose(fapl_id_pass1);
             CPLFree(pszActualFilename);
             return nullptr;
        }

        // Construct HTTPS URL (basic version, no special endpoint handling)
        final_url_storage = "https://";
        final_url_storage += bucket_name;
        final_url_storage += ".s3.";
        final_url_storage += env_region;
        final_url_storage += ".amazonaws.com/"; // Assuming standard AWS endpoint
        final_url_storage += object_key;
         
        filenameForH5Fopen = final_url_storage.c_str(); // Use the HTTPS URL for H5Fopen
        CPLDebug("NISAR_DRIVER", "Constructed HTTPS URL for HDF5: %s", filenameForH5Fopen);
    } // end of if(bIsS3)    
    else { //local file
        fapl_id_pass1 = H5P_DEFAULT;
        CPLDebug("NISAR_DRIVER", "Assuming local file path, using default HDF5 FAPL.");
    }

    // Pass 1: Open, Get Page Size, Close
    hsize_t actual_page_size = 0; // Store the result here
    hid_t hHDF5_pass1 = -1;
    hid_t fcpl_id = -1;

    CPLDebug("NISAR_DRIVER", "Attempting H5Fopen (Pass 1) to get page size: %s", filenameForH5Fopen);
    H5E_auto2_t old_func; void *old_client_data; // Suppress errors
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data); H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    hHDF5_pass1 = H5Fopen( filenameForH5Fopen, H5F_ACC_RDONLY, fapl_id_pass1 );
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore errors

    if (hHDF5_pass1 < 0) {
        CPLError(CE_Warning, CPLE_OpenFailed, "H5Fopen failed (Pass 1) for '%s'. Cannot determine optimal page buffer. Proceeding with defaults.", filenameForH5Fopen);
        // Don't fail the whole open yet, just proceed without optimization.
        // Fallback: use a reasonable default page size guess for buffer calc later if needed
        actual_page_size = 4 * 1024;
    } else {
        // Pass 1 open succeeded, get FCPL and Page Size
        fcpl_id = H5Fget_create_plist(hHDF5_pass1);
        if (fcpl_id < 0) {
            CPLError(CE_Warning, CPLE_AppDefined, "H5Fget_create_plist failed (Pass 1). Using default page size for buffer calculation.");
            actual_page_size = 4 * 1024; // Fallback
        } else {
            if (H5Pget_file_space_page_size(fcpl_id, &actual_page_size) < 0) {
                 CPLError(CE_Warning, CPLE_AppDefined, "H5Pget_file_space_page_size failed (Pass 1). Using default page size.");
                 actual_page_size = 4 * 1024; // Fallback
            } else if (actual_page_size == 0) {
                 CPLError(CE_Warning, CPLE_AppDefined, "H5Pget_file_space_page_size returned 0 (Pass 1). Using default page size.");
                 actual_page_size = 4 * 1024; // Fallback
            }
            H5Pclose(fcpl_id); // Close FCPL handle
        }
        // Close the file from Pass 1
        H5Fclose(hHDF5_pass1);
        CPLDebug("NISAR_DRIVER", "Determined actual file page size (or fallback): %lu bytes.", (unsigned long)actual_page_size);
    }
    // We no longer need fapl_id_pass1, close if needed
    if (bNeedToCloseFapl1) { H5Pclose(fapl_id_pass1); bNeedToCloseFapl1 = false; fapl_id_pass1 = H5P_DEFAULT; } 

    // Prepare Optimized FAPL for Pass 2
    hid_t fapl_id_pass2 = H5P_DEFAULT;
    bool bNeedToCloseFapl2 = false;

    // Only create optimized FAPL for S3 for now
    if (bIsS3) {

        fapl_id_pass2 = H5Pcreate(H5P_FILE_ACCESS); // Create new FAPL

        if (fapl_id_pass2 < 0) { 
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
        ros3_fapl_conf.authenticate = TRUE; // Assume authentication needed

        const char* env_region = getenv("AWS_REGION");
        // In H5FDros3.h, the struct members are declared like: char aws_region[H5FD_ROS3_MAX_REGION_LEN + 1];
        // So, strncpy should use the full MAX_LEN. The struct already has space for null terminator.
        if (env_region != nullptr || strlen(env_region) > 0) {
            strncpy(ros3_fapl_conf.aws_region, env_region, H5FD_ROS3_MAX_REGION_LEN);
            ros3_fapl_conf.aws_region[H5FD_ROS3_MAX_REGION_LEN] = '\0'; //Ensure null termination
            CPLDebug("NISAR_DRIVER", "ROS3 Config: Using Region: %s", ros3_fapl_conf.aws_region);
        }
        else {
            // cleanup and return
            CPLError(CE_Failure, CPLE_AppDefined, "AWS_REGION environment variable not set, needed for HDF5 ROS3 VFD / HTTPS URL.");
            H5Pclose(fapl_id_pass2); 
            CPLFree(pszActualFilename); 
            return nullptr;
        }

        // Get Credentials (Optional for struct, VFD might use env/role if missing)
        const char* env_key_id = getenv("AWS_ACCESS_KEY_ID");
        const char* env_secret = getenv("AWS_SECRET_ACCESS_KEY");

        if (env_key_id && *env_key_id) {
             strncpy(ros3_fapl_conf.secret_id, env_key_id, H5FD_ROS3_MAX_SECRET_ID_LEN);
             ros3_fapl_conf.secret_id[H5FD_ROS3_MAX_SECRET_ID_LEN] = '\0';
             CPLDebug("NISAR_DRIVER", "ROS3 Config: Setting Secret ID (Key ID) from env var.");
        }
        if (env_secret && *env_secret) {
             strncpy(ros3_fapl_conf.secret_key, env_secret, H5FD_ROS3_MAX_SECRET_KEY_LEN);
             ros3_fapl_conf.secret_key[H5FD_ROS3_MAX_SECRET_KEY_LEN] = '\0';
             CPLDebug("NISAR_DRIVER", "ROS3 Config: Setting Secret Key from env var.");
        }

        // Configure FAPL using H5Pset_fapl_ros3 ***
        herr_t status = H5Pset_fapl_ros3(fapl_id_pass2, &ros3_fapl_conf);
        if (status < 0) {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass2); 
            CPLFree(pszActualFilename); 
            return nullptr;
        }
        CPLDebug("NISAR_DRIVER", "Configured HDF5 FAPL using H5Pset_fapl_ros3.");


        //SET SESSION TOKEN (if available)
        const char* env_token  = getenv("AWS_SESSION_TOKEN"); // Check if present
        if (env_token != nullptr && strlen(env_token) > 0) {
            CPLDebug("NISAR_DRIVER", "AWS_SESSION_TOKEN found, attempting to set it on FAPL.");
            status = H5Pset_fapl_ros3_token(fapl_id_pass2, env_token);
            if (status < 0) {
                CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3_token failed.");
                H5Eprint(H5E_DEFAULT, stderr);
                H5Pclose(fapl_id_pass2); CPLFree(pszActualFilename); return nullptr;
            }
            CPLDebug("NISAR_DRIVER", "Successfully set session token using H5Pset_fapl_ros3_token.");
        } 
        else {
            CPLDebug("NISAR_DRIVER", "AWS_SESSION_TOKEN environment variable not set or empty. Proceeding without setting token.");
        }
        // END TOKEN SETTING LOGIC

        

        // Calculate and set optimized page buffer size based on actual_page_size
        if (actual_page_size > 0) {
            size_t target_buffer_bytes = 16 * 1024 * 1024; // Target 16 MB
            unsigned int num_pages_in_buffer = (target_buffer_bytes + actual_page_size - 1) / actual_page_size;
            if (num_pages_in_buffer == 0) num_pages_in_buffer = 1;
            size_t page_buffer_bytes = num_pages_in_buffer * actual_page_size; // Exact multiple
            CPLDebug("NISAR_DRIVER", "Setting OPTIMIZED HDF5 page buffer: %u pages, Total=%lu bytes.", num_pages_in_buffer, (unsigned long)page_buffer_bytes);
            herr_t page_status = H5Pset_page_buffer_size(fapl_id_pass2, page_buffer_bytes, 0, 0);
            if (page_status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Failed to set optimized HDF5 page buffer size."); /* Continue anyway */ }
         } else {
              CPLError(CE_Warning, CPLE_AppDefined, "Could not use actual page size for buffer calculation.");
         }

        status = H5Pset_fapl_ros3(fapl_id_pass2, &ros3_fapl_conf);
        if (status < 0) {
            CPLError(CE_Failure, CPLE_AppDefined, "H5Pset_fapl_ros3 failed.");
            // Optional: H5Eprint(H5E_DEFAULT, stderr);
            H5Pclose(fapl_id_pass2); CPLFree(pszActualFilename); return nullptr;
        }
        CPLDebug("NISAR_DRIVER", "Re-configured FAPL for Pass 2 with ROS3 settings.");

        // Construct HTTPS URL (Workaround for HDF5 1.14.6 scheme bug)
        const char* path_after_scheme = s3_path_part;
        if (STARTS_WITH_CI(s3_path_part, "s3://")) {
             path_after_scheme += strlen("s3://"); // Skip the scheme part if present
        }
        // Now path_after_scheme points to "bucket/key..."
        
        std::string bucket_name;
        std::string object_key;
        // Parse bucketAndKey = "bucket/key/path..."
        const char* first_slash = strchr(path_after_scheme, '/');
        if (first_slash == nullptr) { // Only bucket name? Invalid S3 path for object.
            CPLError(CE_Failure, CPLE_AppDefined, "Invalid S3 path: missing object key in '%s'", pszActualFilename);
            // cleanup and return
            H5Pclose(fapl_id_pass2);
            CPLFree(pszActualFilename);
            return nullptr;
        } else {
            bucket_name.assign(path_after_scheme, first_slash - path_after_scheme);
            object_key.assign(first_slash + 1);
        }
        if (bucket_name.empty()) { // Check for empty bucket name
             CPLError(CE_Failure, CPLE_AppDefined, "Invalid S3 path: empty bucket name parsed from '%s'", path_after_scheme);
             H5Pclose(fapl_id_pass2);
             CPLFree(pszActualFilename);
             return nullptr;
        }

        // Construct HTTPS URL (basic version, no special endpoint handling)
        final_url_storage = "https://";
        final_url_storage += bucket_name;
        final_url_storage += ".s3.";
        final_url_storage += env_region;
        final_url_storage += ".amazonaws.com/"; // Assuming standard AWS endpoint
        final_url_storage += object_key;
         
        filenameForH5Fopen = final_url_storage.c_str(); // Use the HTTPS URL for H5Fopen
        CPLDebug("NISAR_DRIVER", "Constructed HTTPS URL for HDF5: %s", filenameForH5Fopen);

    } else { // Local file
         fapl_id_pass2 = H5P_DEFAULT;
         CPLDebug("NISAR_DRIVER", "Using default FAPL for Pass 2 (local file).");
    }

    //  Pass 2: Re-Open HDF5 File with Optimized FAPL
    hid_t hHDF5 = -1; // This will be the final handle stored in poDS
    CPLDebug("NISAR_DRIVER", "Attempting H5Fopen (Pass 2 - Optimized) with filename: %s", filenameForH5Fopen);
    hHDF5 = H5Fopen( filenameForH5Fopen, H5F_ACC_RDONLY, fapl_id_pass2 );
    if (bNeedToCloseFapl2) { H5Pclose(fapl_id_pass2); } // Close optimized FAPL

    // Check H5Fopen result for Pass 2
    if( hHDF5 < 0 ) {
         CPLError( CE_Failure, CPLE_OpenFailed, "H5Fopen failed (Pass 2) for '%s'.", filenameForH5Fopen);
         H5Eprint(H5E_DEFAULT, stderr); // Print details on final open failure
         CPLFree(pszActualFilename);
         return nullptr;
    }
    CPLDebug("NISAR_DRIVER", "Successfully RE-opened HDF5 file handle with optimized FAPL: %s", filenameForH5Fopen);

    // PRODUCT LEVEL DETECTION (GENERALIZED)
    bool isLevel1 = false;
    bool isLevel2 = false;
    std::string productType = "";
    std::string productLevelGroup = "";

    // First, try to read the productType dataset for an explicit definition.
    hid_t hIdentGroup = H5Gopen2(hHDF5, "/science/LSAR/identification", H5P_DEFAULT);
    if (hIdentGroup >= 0)
    {
        CPLDebug("NISAR_DRIVER", "Successfully opened /science/LSAR/identification group.");
        hid_t hDset = H5Dopen2(hIdentGroup, "productType", H5P_DEFAULT);
        if(hDset >= 0) {
            hid_t hStrType = H5Dget_type(hDset);
            if (hStrType >= 0) {
                // This logic correctly handles both fixed and variable-length strings
                if (H5Tis_variable_str(hStrType) > 0) {
                    char* pszVal = nullptr;
                    if (H5Dread(hDset, hStrType, H5S_ALL, H5S_ALL, H5P_DEFAULT, &pszVal) >= 0 && pszVal) {
                        productType = pszVal;
                        H5free_memory(pszVal);
                    }
                } else {
                    size_t nSize = H5Tget_size(hStrType);
                    if (nSize > 0) {
                        char* pszVal = (char *)CPLMalloc(nSize + 1);
                        if (H5Dread(hDset, hStrType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszVal) >= 0) {
                            pszVal[nSize] = '\0';
                            productType = pszVal;
                        }
                        CPLFree(pszVal);
                    }
                }
                H5Tclose(hStrType);
            }
            H5Dclose(hDset);
        }
        H5Gclose(hIdentGroup);
    }

    // Now, use the result of our search to set the flags and the product group name.
    if (!productType.empty()) {
        CPLDebug("NISAR_DRIVER", "Read productType: %s", productType.c_str());
        if (productType == "RSLC" || productType == "RIFG" || productType == "RUNW") {
            isLevel1 = true;
            productLevelGroup = productType; // Use the actual product type for path building
        }
        else if (productType == "GSLC" || productType == "GUNW" ||
                 productType == "GOFF" || productType == "GCOV") {
            isLevel2 = true;
            productLevelGroup = productType; // Use the actual product type for path building
        }
    }
    // If we couldn't read productType, fall back to checking for common group paths.
    else {
        CPLDebug("NISAR_DRIVER", "Could not determine productType. Falling back to path-based detection.");
        
	// Temporarily disable HDF5 error printing for these checks
	H5E_auto2_t old_func;
	void *old_client_data;
	H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
	H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

        if (H5Lexists(hHDF5, "/science/LSAR/RSLC", H5P_DEFAULT) > 0) {
            isLevel1 = true;
            productLevelGroup = "RSLC";
        } else if (H5Lexists(hHDF5, "/science/LSAR/GSLC", H5P_DEFAULT) > 0) {
            isLevel2 = true;
            productLevelGroup = "GSLC";
        } else if (H5Lexists(hHDF5, "/science/LSAR/GCOV", H5P_DEFAULT) > 0) {
            isLevel2 = true;
            productLevelGroup = "GCOV";
        }
        // NOTE: Add other checks for RIFG, GUNW, etc.

        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data); // Restore error handler
    }

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

    // Open the HDF5 File
    // Temporarily suppress HDF5 error stack printing to avoid noise if file is not HDF5
    //H5E_auto2_t old_func;
    //void *old_client_data;
    //H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    //H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
 
    // Identification Check (Post-Open)
    htri_t groupExists = H5Lexists(hHDF5, "/science/LSAR", H5P_DEFAULT);
    if (groupExists <= 0) {
       CPLDebug("NISAR_DRIVER", "File '%s' lacks expected '/science/LSAR' group. Not treating as NISAR.", filenameForH5Fopen);
       H5Fclose(hHDF5); CPLFree(pszActualFilename); return nullptr;
    }
    CPLDebug("NISAR_DRIVER", "Found '/science/LSAR' group, proceeding.");

  // Store the raw open options in the dataset's metadata
  // This makes them visible in gdalinfo under "Open_Options" domain
  //poDS->SetMetadata( papszOptions, "OPEN_OPTIONS" );
   

    hid_t hDataset = -1;
    const char *pathToOpen = nullptr;  
    NisarDataset *poDS = nullptr;

    if (pszSubdatasetPath == nullptr) {
        // CASE 1: No specific subdataset requested - Perform Discovery
        CPLDebug("NISAR_DRIVER", "No specific HDF5 dataset path provided. Running subdataset discovery.");

        // Create preliminary dataset object to hold results and file handle
        try { 
            poDS = new NisarDataset();
       	} catch(...) { 
            H5Fclose(hHDF5);
	    CPLFree(pszActualFilename);
	    return nullptr;
       	}
        poDS->hHDF5 = hHDF5; // Keep file handle
        poDS->pszFilename = pszActualFilename; // Takes ownership
        poDS->hDataset = -1; // No specific dataset opened yet

        // Prepare visitor data
        NISARVisitorData visitor_data;
        std::vector<std::string> found_paths_vector;
        visitor_data.pFoundPaths = &found_paths_vector;
	visitor_data.hStartingGroupID = poDS->hHDF5;

        CPLDebug("NISAR_DRIVER", "Starting H5Ovisit to find subdatasets.");
	herr_t visit_status = H5Ovisit(poDS->hHDF5, // Starting object ID (root group/file)
                                       H5_INDEX_NAME, H5_ITER_NATIVE, // Index and order
                                       NISAR_FindDatasetsVisitor,    // callback function
                                       (void*)&visitor_data,       // User data
                                       H5O_INFO_BASIC);            // Fields for oinfo struct in callback
        if (visit_status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "HDF5 visit failed during subdataset search."); }
        else { CPLDebug("NISAR_DRIVER", "H5Ovisit completed. Found %d potential subdatasets.", (int)found_paths_vector.size()); }

        // Check if the visitor found any relevant datasets
        if (!found_paths_vector.empty()) {
            char **papszMetadataList = nullptr;
            int nSubdatasets = 0;

            for (const std::string& hdf5_path : found_paths_vector) {
                nSubdatasets++;
                // Construct SUBDATASET_n_NAME
                std::string name_key = CPLSPrintf("SUBDATASET_%d_NAME", nSubdatasets);
                std::string name_val = pszPrefix; name_val += "\""; name_val += poDS->pszFilename; name_val += "\":"; name_val += hdf5_path;
                papszMetadataList = CSLSetNameValue(papszMetadataList, name_key.c_str(), name_val.c_str());

                // Construct SUBDATASET_n_DESC
                std::string desc_key = CPLSPrintf("SUBDATASET_%d_DESC", nSubdatasets);
                std::string desc_val = "[";
                hid_t hSubDataset = H5Dopen2(poDS->hHDF5, hdf5_path.c_str(), H5P_DEFAULT);
                if (hSubDataset >= 0) {
                     hid_t hSubSpace = H5Dget_space(hSubDataset); hid_t hSubType = H5Dget_type(hSubDataset);
                     int nSubDim = (hSubSpace >= 0) ? H5Sget_simple_extent_ndims(hSubSpace) : -1;
                     if(nSubDim > 0) {
                         hsize_t adimsSub[H5S_MAX_RANK]; H5Sget_simple_extent_dims(hSubSpace, adimsSub, nullptr);
                         // Format dimensions (Example: YxX for 2D)
                         for(int i=0; i<nSubDim; ++i) { 
                             desc_val += CPLSPrintf("%llu%s", (unsigned long long)adimsSub[i], (i<nSubDim-1)?"x":"");
                         }
                     } else if (nSubDim == 0) { 
                         desc_val += "scalar"; 
                     } else { 
                         desc_val += "?"; 
                     } 
                     desc_val += "]";
                     GDALDataType eSubDataType = (hSubType >= 0) ? NisarDataset::GetGDALDataType(hSubType) : GDT_Unknown;
                     std::string sDataTypeDesc = "(unknown)"; 
                     if(eSubDataType!=GDT_Unknown) { 
                         sDataTypeDesc = "("; 
                         if(GDALDataTypeIsComplex(eSubDataType)) 
                             sDataTypeDesc += "complex, "; 
                         sDataTypeDesc += GDALGetDataTypeName(GDALGetNonComplexDataType(eSubDataType)); 
                         sDataTypeDesc += ")";
                     }
                     desc_val += " " + hdf5_path + " " + sDataTypeDesc;
                     if(hSubType >= 0) 
                         H5Tclose(hSubType); 
                     if(hSubSpace >= 0) 
                         H5Sclose(hSubSpace); 
                     H5Dclose(hSubDataset);
                } else { 
                    desc_val += "?] " + hdf5_path + " (Error opening)";
                }
                papszMetadataList = CSLSetNameValue(papszMetadataList, desc_key.c_str(), desc_val.c_str());
            } // End loop

	    // Store the generated list in papszSubDatasets member
            CSLDestroy(poDS->papszSubDatasets); // Clear any previous just in case
            poDS->papszSubDatasets = papszMetadataList; // Assign the new list
            // Also set it in the metadata domain for immediate visibility
            poDS->SetMetadata(poDS->papszSubDatasets, "SUBDATASETS");

	    // Configure poDS as a container dataset
            poDS->nRasterXSize = 0; poDS->nRasterYSize = 0; poDS->nBands = 0;
            CPLDebug("NISAR_DRIVER", "Populated SUBDATASETS metadata for %d datasets.", nSubdatasets);

            // Return the container dataset
            poDS->TryLoadXML();
            poDS->SetDescription(poOpenInfo->pszFilename);
            return poDS; // <<<< EXIT POINT 1: Return container dataset
	}  // Exit Point 1: Returned container dataset
	    	
    } // End handling of pszSubdatasetPath == nullptr !!!!ozp

  
    if (pszSubdatasetPath != nullptr) {
        pathToOpen = pszSubdatasetPath;
        CPLDebug("NISAR_DRIVER", "Attempting to open specified HDF5 dataset path: %s", pathToOpen);
    } else {
        //Subdataset Discovery goes here TODO: Implement
        pathToOpen = DEFAULT_NISAR_HDF5_PATH;
        CPLDebug("NISAR_DRIVER", "No specific HDF5 dataset path: %s", pathToOpen);
    }
    if (!pathToOpen || !*pathToOpen) {
        //Error handling
        H5Fclose(hHDF5);
        CPLFree(pszActualFilename); 
        return nullptr;
    }

    // Prepare DAPL with Chunk Cache
    hDataset = -1;
    hid_t dapl_id = H5P_DEFAULT; // Default DAPL initially
    bool bNeedToCloseDapl = false;

    dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
    if (dapl_id >= 0) {
         bNeedToCloseDapl = true;
         size_t rdcc_nslots, rdcc_nbytes; double rdcc_w0;
         H5Pget_chunk_cache(dapl_id, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0); // Get defaults
         // Set desired cache size (e.g., 128 MB) - TODO: Make configurable
         size_t new_cache_size_mb = CPLScanLong(CPLGetConfigOption("NISAR_CHUNK_CACHE_SIZE_MB", "512"), 512); // Read from config option
         size_t new_nbytes = new_cache_size_mb * 1024 * 1024;
         size_t new_nslots = std::max((size_t)10009, rdcc_nslots * 4); // Heuristic, prime num often good
         herr_t cache_status = H5Pset_chunk_cache(dapl_id, new_nslots, new_nbytes, rdcc_w0);
         if (cache_status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Failed to set HDF5 chunk cache."); H5Pclose(dapl_id); dapl_id = H5P_DEFAULT; bNeedToCloseDapl = false;}
         else { CPLDebug("NISAR_DRIVER", "Set HDF5 chunk cache: slots=%lu, size=%lu bytes (%.0f MB), w0=%.2f", (unsigned long)new_nslots, (unsigned long)new_nbytes, (double)new_cache_size_mb, rdcc_w0); }
    } else { CPLError(CE_Warning, CPLE_AppDefined, "Failed to copy default DAPL."); dapl_id = H5P_DEFAULT; }

    

    // Open Target HDF5 Dataset
    CPLDebug("NISAR_DRIVER", "Attempting to open HDF5 dataset: %s", pathToOpen);
    hDataset = H5Dopen2( hHDF5, pathToOpen, dapl_id ); //Use configured dapl_id
    if (bNeedToCloseDapl) 
        H5Pclose(dapl_id);

    if( hDataset < 0 )
    {
        CPLError(CE_Failure, CPLE_OpenFailed, "H5Dopen2 failed for dataset '%s'.", pathToOpen);
        H5Fclose(hHDF5);
        CPLFree(pszActualFilename); // Free buffer if ownership not transferred
        return nullptr;
    }

    CPLDebug("NISAR_DRIVER", "Successfully Opened Subdataset: %s", pathToOpen);

    // Create a new NisarDataset object
    poDS = nullptr;

    try {
        poDS = new NisarDataset();
    } catch (const std::bad_alloc&) {
        CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate NisarDataset.");
        H5Dclose(hDataset); H5Fclose(hHDF5); CPLFree(pszActualFilename); return nullptr;
    }

    // Store handles and filename (transfer ownership of pszActualFilename)
    poDS->hHDF5 = hHDF5;
    poDS->hDataset = hDataset;
    // Assuming pszFilename member exists in NisarDataset class
    poDS->pszFilename = pszActualFilename; // Destructor MUST CPLFree this

    // Get dataset information (dimensions, datatype, etc.)
    // using HDF5 API calls and set the corresponding
    // properties in poDS (nRasterXSize, nRasterYSize, etc.).

    // Get HDF5 native data type
    hid_t hH5DataType = H5Dget_type(hDataset);
    if (hH5DataType < 0) { // Check if H5Dget_type itself failed
        CPLError(CE_Failure, CPLE_AppDefined, "H5Dget_type failed for dataset '%s'.", pathToOpen);
        delete poDS; // poDS destructor handles cleanup of handles & filename
        return nullptr;
    }

    // H5Dget_type succeeded, now map the type

    // Map to GDAL data type using static helper function (MUST BE IMPLEMENTED)
    poDS->eDataType = NisarDataset::GetGDALDataType(hH5DataType);

    // Close the HDF5 type handle now that we're done with it
    // It's safe to close here because GetGDALDataType should have finished using it.
    H5Tclose(hH5DataType);
    hH5DataType = -1; // Mark as closed for safety, although not strictly needed here

    // Check if the mapping was successful (type is known and supported by driver)
    if(poDS->eDataType == GDT_Unknown) {
        // Use a specific error message for mapping failure
        CPLError(CE_Failure, CPLE_AppDefined, "Unsupported HDF5 data type encountered in dataset '%s'. Please update NisarDataset::GetGDALDataType.", pathToOpen);
        delete poDS;
        return nullptr;
    }

    // Log the successfully determined type
    CPLDebug("NISAR_DRIVER", "Dataset GDAL Data Type: %s", GDALGetDataTypeName(poDS->eDataType)); 

   // Get dimensions
    hid_t hDataspace = H5Dget_space( hDataset );
     if (hDataspace < 0) { // Check return value
          CPLError(CE_Failure, CPLE_AppDefined, "H5Dget_space failed for dataset '%s'.", pathToOpen);
          delete poDS;
          return nullptr;
     }
    const int nDims = H5Sget_simple_extent_ndims( hDataspace );

    // Rank Check
    // Require at least 2 dimensions for raster interpretation
    if( nDims < 2 )
    {
        // Provide more specific error
        CPLError(CE_Failure, CPLE_AppDefined, "Dataset '%s' has rank %d, requires rank >= 2 for raster interpretation.", pathToOpen, nDims);
        H5Sclose(hDataspace);
        delete poDS;
        return nullptr;
    }

    // Dimension Array Size
    // Use H5S_MAX_RANK for safety or allocate std::vector based on nDims
    hsize_t adims[H5S_MAX_RANK];
    H5Sget_simple_extent_dims( hDataspace, adims, nullptr );

    // Dimension Assignment
    // Use indices based on rank, assuming HDF5 [..., Y, X] -> GDAL X=last, Y=second-last
    poDS->nRasterXSize = static_cast<int>(adims[nDims-1]);
    poDS->nRasterYSize = static_cast<int>(adims[nDims-2]);
    H5Sclose(hDataspace); // Close dataspace handle

    // Sanity check dimensions
    if (poDS->nRasterXSize <= 0 || poDS->nRasterYSize <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Invalid raster dimensions (%d x %d) read from dataset '%s'.",
                 poDS->nRasterXSize, poDS->nRasterYSize, pathToOpen);
        delete poDS; return nullptr;
    }
    CPLDebug("NISAR_DRIVER", "Dataset Dimensions: %d x %d", poDS->nRasterXSize, poDS->nRasterYSize);

    if (isLevel2) {
        CPLDebug("NISAR_DRIVER", "Level 2 product detected. Georeferencing will use GeoTransform and SRS.");
    }
    else if (isLevel1) {
        CPLDebug("NISAR_DRIVER", "Level 1 product detected. Will generate GCPs for georeferencing.");
        // Pass the discovered product group to the function
        if (poDS->GenerateGCPsFromGeolocationGrid(productLevelGroup.c_str()) != CE_None) {
            CPLError(CE_Warning, CPLE_AppDefined, "Failed to generate GCPs for Level 1 product.");
            // Decide if this should be a fatal error or just a warning
        }
    }
    else {
        CPLError(CE_Warning, CPLE_AppDefined, "Unknown NISAR product structure. Georeferencing may be absent.");
    }

    // Determine Band Count
    if (GDALDataTypeIsComplex(poDS->eDataType)) {
       poDS->nBands = 1; // Convention: Represent complex as one GDAL band
       CPLDebug("NISAR_DRIVER", "Data type is complex, setting Band Count to: %d (single complex band convention)", poDS->nBands);
    } else {
       poDS->nBands = 1; // Default for non-complex types
       CPLDebug("NISAR_DRIVER", "Data type is not complex, setting Band Count to: %d", poDS->nBands);
    }

    // Note: Further logic may be needed for other multi-component datasets if supported later.

       // Create raster bands
    for( int i = 1; i <= poDS->nBands; i++ ) {
        NisarRasterBand* poBand = nullptr;
        try {
             // Create the band object - Constructor will handle its own setup now
             poBand = new NisarRasterBand( poDS, i );
        } catch(const std::bad_alloc&) {
             CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate NisarRasterBand object for band %d.", i);
             delete poDS; return nullptr;
        }

        poDS->SetBand( i, poBand ); // Add band to dataset (poDS takes ownership)
    }

    // Final Setup
    poDS->SetDescription(poOpenInfo->pszFilename); // Use original full input path as description
    if(pathToOpen) // Store the actual HDF5 path opened
        poDS->SetMetadataItem("HDF5_PATH", pathToOpen);
    poDS->TryLoadXML(); // Initialize PAM system (loads .aux.xml if present)

    CPLDebug("NISAR_DRIVER", "Finished NisarDataset::Open successfully for dataset '%s'", pathToOpen);


    // Initialize any other metadata, etc.

    return poDS;
}
/************************************************************************/
/*                                GetGeoTransform()                     */
/* Read geotransform parameters from the HDF5 file (as attributes)      */
/* Set the adfGeoTransform array.                                       */
/* If no geotransform is found, return CE_None and set the transform to */
/* an identity transform.                                               */
/************************************************************************/
CPLErr NisarDataset::GetGeoTransform( double * padfTransform )
{
    CPLDebug("NISAR_DRIVER", "NisarDataset::GetGeoTransform() called.");

    if (GetGCPCount() > 0)
    {
        // If we have GCPs, return the default "not found" transform.
        // GDAL will use the GCPs instead.
        return GDALPamDataset::GetGeoTransform(padfTransform); // This returns the default failure case
    }

    CPLDebug("NISAR_DRIVER", "NisarDataset::GetGeoTransform() Initialize output. ");

    // Initialize output and local variables
    // Default identity transform (required by GDAL if specific transform not found)
    padfTransform[0] = 0.0;  padfTransform[1] = 1.0;  padfTransform[2] = 0.0;
    padfTransform[3] = 0.0;  padfTransform[4] = 0.0;  padfTransform[5] = 1.0;
    
    // Check if this is a container dataset (no raster bands)
    // If nRasterXSize is 0 (set during subdataset discovery), return default GT.
    // Also check if the main dataset handle is valid.
    if (nRasterXSize == 0 || hDataset < 0) {
         CPLDebug("NISAR_DRIVER", "GetGeoTransform called on container dataset or invalid handle. Returning default identity.");
         // It's not an error for a container to lack a GeoTransform
         return CE_None;
    }

    // Handles opened in this function
    hid_t hGroup           = -1;
    hid_t hAttribute       = -1; // If using ReadGeoTransformAttribute helper
    hid_t hXCoords         = -1;
    hid_t hYCoords         = -1;
    hid_t hXSpacing        = -1;
    hid_t hYSpacing        = -1;
    hid_t hMemSpace        = -1;
    hid_t hFileSpaceX      = -1;
    hid_t hFileSpaceY      = -1;

    // Variables for coordinate/spacing method
    std::string datasetPath;
    const char* lastSlash = nullptr;
    std::string groupPath;
    std::string xCoordinatesPath;
    std::string yCoordinatesPath;
    std::string xCoordinateSpacingPath;
    std::string yCoordinateSpacingPath;
    double xSpacing = 0.0;
    double ySpacing = 0.0;
    double xCoord = 0.0;
    double yCoord = 0.0; // TODO: Re-investigate why this read 4.94e-324 previously
    hsize_t count1d[1] = {0};
    hsize_t offset1d[1] = {0};

    CPLErr eErr = CE_Failure; // Status flag, default to failure until success


    // Method 1: Try reading standard 'GeoTransform' attribute
    // IMPORTANT: Verify attribute name ("GeoTransform") from NISAR
    const char *pszGeoTransformAttrName = "GeoTransform";

    if (this->hDataset >= 0) {
         eErr = ReadGeoTransformAttribute(this->hDataset, pszGeoTransformAttrName, padfTransform);
         if (eErr == CE_None) {
             CPLDebug("NISAR_DRIVER", "Read '%s' attribute from dataset.", pszGeoTransformAttrName);
             goto cleanup; // Found it, we're done (attribute takes precedence)
         }
    } else {
         CPLError(CE_Failure, CPLE_AppDefined, "GetGeoTransform: Invalid main dataset handle (hDataset).");
         eErr = CE_Failure; // Ensure correct return status
         goto cleanup; // Cannot proceed
    }

    //  Method 2: Fallback to reading coordinate/spacing datasets
    CPLDebug("NISAR_DRIVER", "Attribute '%s' not found or failed. Falling back to coordinate/spacing datasets.", pszGeoTransformAttrName);

    // Get the full path of the main dataset to find its parent group
    datasetPath = get_hdf5_object_name(this->hDataset); // Uses static helper
    if (datasetPath.empty() || datasetPath == "/") { // Check if getting name failed or is root
         CPLError(CE_Warning, CPLE_AppDefined, "GetGeoTransform: Could not get valid path for main dataset or dataset is at root. Cannot find relative coordinate datasets.");
         eErr = CE_Failure; // Cannot proceed with this method
         goto cleanup;
    }

    // Calculate parent group path
    lastSlash = strrchr(datasetPath.c_str(), '/'); // Use standard C func
    if (lastSlash == datasetPath.c_str()) { // Only one slash at beginning
        groupPath = "/";
    } else if (lastSlash != nullptr) {
        groupPath = datasetPath.substr(0, lastSlash - datasetPath.c_str());
         if(groupPath.empty()){ // Should not happen if datasetPath != "/"
             groupPath = "/";
         }
    } else { // No slash found - should not happen for valid dataset paths
         CPLError(CE_Warning, CPLE_AppDefined, "GetGeoTransform: Could not determine parent group path for '%s'.", datasetPath.c_str());
         eErr = CE_Failure;
         goto cleanup;
    }
    CPLDebug("NISAR_DRIVER", "Derived parent group path: %s", groupPath.c_str());


    // Construct full paths for coordinate/spacing datasets
    // *** IMPORTANT: Verify these dataset names ("xCoordinates", etc.) from NISAR Spec ***
    xCoordinatesPath       = groupPath + (groupPath == "/" ? "" : "/") + "xCoordinates";
    yCoordinatesPath       = groupPath + (groupPath == "/" ? "" : "/") + "yCoordinates";
    xCoordinateSpacingPath = groupPath + (groupPath == "/" ? "" : "/") + "xCoordinateSpacing";
    yCoordinateSpacingPath = groupPath + (groupPath == "/" ? "" : "/") + "yCoordinateSpacing";
    CPLDebug("NISAR_DRIVER", "Attempting to read: %s, %s, %s, %s",
             xCoordinatesPath.c_str(), yCoordinatesPath.c_str(),
             xCoordinateSpacingPath.c_str(), yCoordinateSpacingPath.c_str());

    // Open the four required datasets (relative to the main file handle hHDF5)
    // Using H5Dopen2 for consistency
    hXCoords = H5Dopen2(this->hHDF5, xCoordinatesPath.c_str(), H5P_DEFAULT);
    hYCoords = H5Dopen2(this->hHDF5, yCoordinatesPath.c_str(), H5P_DEFAULT);
    hXSpacing = H5Dopen2(this->hHDF5, xCoordinateSpacingPath.c_str(), H5P_DEFAULT);
    hYSpacing = H5Dopen2(this->hHDF5, yCoordinateSpacingPath.c_str(), H5P_DEFAULT);

    // Check if all datasets were opened successfully
    if (hXCoords < 0 || hYCoords < 0 || hXSpacing < 0 || hYSpacing < 0) {
        CPLError(CE_Warning, CPLE_OpenFailed, "GetGeoTransform: Failed to open one or more coordinate/spacing datasets under '%s'.", groupPath.c_str());
        eErr = CE_Failure; // Mark as failure
        goto cleanup;
    }

    // Read spacing values (assuming they are scalar doubles)
    herr_t status;
    status = H5Dread(hXSpacing, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &xSpacing); // Assign value
    if (status < 0) { CPLError(CE_Warning, CPLE_FileIO, "Failed to read '%s'.", xCoordinateSpacingPath.c_str()); eErr = CE_Failure; goto cleanup; }
    status = H5Dread(hYSpacing, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &ySpacing); // Assign value
    if (status < 0) { CPLError(CE_Warning, CPLE_FileIO, "Failed to read '%s'.", yCoordinateSpacingPath.c_str()); eErr = CE_Failure; goto cleanup; }
    CPLDebug("NISAR_DRIVER", "Read Spacing: X=%.10g, Y=%.10g", xSpacing, ySpacing);

    // Read the first coordinate value from X and Y datasets
    count1d[0] = 1;   // Assign value
    offset1d[0] = 0;  // Assign value
    hMemSpace = H5Screate_simple(1, count1d, nullptr);
    if (hMemSpace < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Failed to create memory space."); eErr = CE_Failure; goto cleanup;}

    // Read X Coordinate (first element)
    hFileSpaceX = H5Dget_space(hXCoords);
    if (hFileSpaceX < 0 || H5Sget_simple_extent_ndims(hFileSpaceX) == 0 ) { CPLError(CE_Warning, CPLE_AppDefined, "Failed to get valid dataspace for '%s'.", xCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup; }
    status = H5Sselect_hyperslab(hFileSpaceX, H5S_SELECT_SET, offset1d, nullptr, count1d, nullptr);
    if (status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Failed select hyperslab for '%s'.", xCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup;}
    status = H5Dread(hXCoords, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpaceX, H5P_DEFAULT, &xCoord); // Assign value
    if (status < 0) { CPLError(CE_Warning, CPLE_FileIO, "Failed to read first element of '%s'.", xCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup;}
    CPLDebug("NISAR_DRIVER", "Read first X Coordinate: %.10g", xCoord);

    // Read Y Coordinate (first element)
    hFileSpaceY = H5Dget_space(hYCoords);
     if (hFileSpaceY < 0 || H5Sget_simple_extent_ndims(hFileSpaceY) == 0 ) { CPLError(CE_Warning, CPLE_AppDefined, "Failed to get valid dataspace for '%s'.", yCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup; }
    status = H5Sselect_hyperslab(hFileSpaceY, H5S_SELECT_SET, offset1d, nullptr, count1d, nullptr);
     if (status < 0) { CPLError(CE_Warning, CPLE_AppDefined, "Failed select hyperslab for '%s'.", yCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup;}
    // TODO: Still investigate why this read 4.94e-324 before! Initialize yCoord=-9999? H5Eprint?
    status = H5Dread(hYCoords, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpaceY, H5P_DEFAULT, &yCoord); // Assign value
    if (status < 0) { CPLError(CE_Warning, CPLE_FileIO, "Failed to read first element of '%s'.", yCoordinatesPath.c_str()); eErr = CE_Failure; goto cleanup;}
    CPLDebug("NISAR_DRIVER", "Read first Y Coordinate: %.10g", yCoord); // CHECK THIS VALUE


    // Calculate GeoTransform
    // *** IMPORTANT: Verify pixel convention (center vs. corner) from NISAR Spec ***
    // This assumes coordinates represent the CENTER of the top-left pixel (0,0)
    padfTransform[0] = xCoord - 0.5 * xSpacing;   // Top Left X (UL corner)
    padfTransform[1] = xSpacing;                  // W-E pixel resolution
    padfTransform[2] = 0.0;                       // Rotation X
    padfTransform[3] = yCoord - 0.5 * ySpacing;   // Top Left Y (UL corner)
    padfTransform[4] = 0.0;                       // Rotation Y
    padfTransform[5] = ySpacing;                  // N-S pixel resolution (MUST be negative for North-up)

    // Check if Y spacing is negative (North-up); if not, issue warning or adjust.
    if (ySpacing > 0) {
         CPLError(CE_Warning, CPLE_AppDefined, "yCoordinateSpacing is positive (%.10g), expected negative for North-up image convention. GeoTransform might be incorrect.", ySpacing);
         // Optionally flip it: padfTransform[5] = -ySpacing; padfTransform[3] = yCoord + 0.5 * ySpacing; ?? Check carefully!
    }

    CPLDebug("NISAR_DRIVER", "Calculated GeoTransform: GT[0]=%.10g, GT[1]=%.10g, GT[3]=%.10g, GT[5]=%.10g",
             padfTransform[0], padfTransform[1], padfTransform[3], padfTransform[5]);
    eErr = CE_None; // Mark success for coordinate method

cleanup:
    // Close handles opened *within this function*
    if (hAttribute >= 0) H5Aclose(hAttribute);
    if (hFileSpaceY >= 0) H5Sclose(hFileSpaceY);
    if (hFileSpaceX >= 0) H5Sclose(hFileSpaceX);
    if (hMemSpace >= 0) H5Sclose(hMemSpace);
    if (hYSpacing >= 0) H5Dclose(hYSpacing);
    if (hXSpacing >= 0) H5Dclose(hXSpacing);
    if (hYCoords >= 0) H5Dclose(hYCoords);
    if (hXCoords >= 0) H5Dclose(hXCoords);
    if (hGroup >= 0) H5Gclose(hGroup);
    // DO NOT close this->hHDF5 or this->hDataset

    // Return CE_None if successful OR if non-fatal errors occurred (e.g., attribute/coords not found)
    // In those non-fatal cases, the default identity transform set at the start will be used.
    if (eErr == CE_None) {
         CPLDebug("NISAR_DRIVER", "GetGeoTransform returning successfully (found transform or using default).");
    } else {
         CPLDebug("NISAR_DRIVER", "GetGeoTransform failed to find/read coordinates/spacing or attribute. Using default identity transform.");
         // Ensure default identity is set if eErr != CE_None
         padfTransform[0] = 0.0; padfTransform[1] = 1.0; padfTransform[2] = 0.0;
         padfTransform[3] = 0.0; padfTransform[4] = 0.0; padfTransform[5] = 1.0;
    }
    return CE_None; // Return CE_None as per GDAL convention unless fatal error occurred
}


//------------------------------------------------------------------------------
// NisarDataset::GetSpatialRef
//------------------------------------------------------------------------------
const OGRSpatialReference* NisarDataset::GetSpatialRef() const
{
    // Use Mutex Holder for RAII-style locking
    // It acquires lock on construction, releases on destruction (end of scope)
    std::lock_guard<std::mutex> lock(m_SRSMutex); // Use std::lock_guard and correct member name


    // Check cache flag - if already computed, return cached pointer
    if( m_bGotSRS ) {
        return m_poSRS; // This could be nullptr if previous attempt failed
    }

    // Mark as attempted (even if we fail below, don't try again)
    m_bGotSRS = true;
 
    CPLDebug("NISAR_DRIVER", "NisarDataset::GetSpatialRef() called.");
    
    // --- Check if this is a container dataset ---
    if (hDataset < 0) {
         CPLError(CE_Warning, CPLE_AppDefined, "GetSpatialRef: Invalid main dataset handle. Cannot determine SRS.");
         return nullptr; // Return null for container or if handle invalid
    }
     // Could also check nRasterXSize == 0, but hDataset < 0 is sufficient

    // Initialize local variables
    OGRSpatialReference *poSRS = nullptr; // Pointer to the SRS object we might create
    hid_t hProjectionDataset = -1; // Handle for the 'projection' dataset
    hid_t hAttribute = -1;         // Handle for attributes 'epsg_code' or 'spatial_ref'
    hid_t hAttrType = -1;          // Handle for attribute type
    herr_t status = -1;
    long long epsg_code = 0;       // For reading EPSG code
    char *pszWKT_Alloc = nullptr; // For reading WKT string attribute (fixed length)
    char *pszWKT_VL = nullptr;    // For reading WKT string attribute (variable length)

    // Path construction variables
    std::string datasetPath;
    const char* lastSlash = nullptr;
    std::string groupPath;
    std::string projectionPath;

    // Temporary pointer for newly created SRS object during processing
    OGRSpatialReference *poTmpSRS = nullptr;

    // ADD ERROR SUPPRESSION
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Determine Path to Projection Info
    // Need to find the group containing the main dataset first
    if (this->hDataset < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "GetSpatialRef: Invalid main dataset handle.");
        return nullptr;
    }
    datasetPath = get_hdf5_object_name(this->hDataset);
    if (datasetPath.empty() || datasetPath == "/") {
        CPLError(CE_Warning, CPLE_AppDefined, "GetSpatialRef: Could not get path of main dataset or dataset is at root. Cannot find projection dataset.");
        // Error handled inside helper or return empty
        goto cleanup;
    }
    lastSlash = strrchr(datasetPath.c_str(), '/'); // Use standard C func
    //groupPath;
    if (lastSlash == datasetPath.c_str()) groupPath = "/";
    else if (lastSlash != nullptr) { groupPath = datasetPath.substr(0, lastSlash - datasetPath.c_str()); if(groupPath.empty()) groupPath = "/"; }
    else { groupPath = "/"; } // Fallback? Should error?

    // IMPORTANT: Verify dataset name ("projection") from NISAR Spec
    projectionPath = groupPath + (groupPath == "/" ? "" : "/") + "projection";
    CPLDebug("NISAR_DRIVER", "Attempting to open projection dataset: %s", projectionPath.c_str());

    // Open the Projection "Dataset" (often just holds attributes)
    hProjectionDataset = H5Dopen2( this->hHDF5, projectionPath.c_str(), H5P_DEFAULT );
    if (hProjectionDataset < 0) {
        //No CPLError here, as this is expected for all L1 products
        //CPLError(CE_Warning, CPLE_OpenFailed, "GetSpatialRef: Failed to open projection dataset: '%s'. No SRS available.", projectionPath.c_str());
        CPLDebug("NISAR_DRIVER", "GetSpatialRef: Optional 'projection' dataset not found for this product, this is expected for Level 1 Product.");
        goto cleanup; // Go directly to cleanup, will return nullptr
    }

    // RESTORE ERROR HANDLER
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Try Reading EPSG Code Attribute
    hAttribute = H5Aopen(hProjectionDataset, "epsg_code", H5P_DEFAULT);

    if (hAttribute >= 0) { // Found an EPSG attribute
        CPLDebug("NISAR_DRIVER", "Found potential EPSG attribute.");
        hAttrType = H5Aget_type(hAttribute);
        if (hAttrType >= 0 && H5Tget_class(hAttrType) == H5T_INTEGER) {
            // Read as appropriate integer type (try LLONG first)
            status = H5Aread(hAttribute, H5T_NATIVE_LLONG, &epsg_code);
            if (status < 0) { // Try INT if LLONG fails?
                 status = H5Aread(hAttribute, H5T_NATIVE_INT, &epsg_code); // Read into long long still ok
            }

            if (status >= 0 && epsg_code > 0) {
                CPLDebug("NISAR_DRIVER", "Read EPSG code: %lld", epsg_code);
                poTmpSRS = new OGRSpatialReference(); // Create SRS object
                // SetAuthority requires GDAL >= 3.x ? Use importFromEPSG instead
                if (poTmpSRS->importFromEPSG(static_cast<int>(epsg_code)) == OGRERR_NONE) {
                     m_poSRS = poTmpSRS;  //Assign to member
                     poTmpSRS = nullptr;  // Prevent deletion
                     CPLDebug("NISAR_DRIVER", "Assigned SRS from EPSG...");
                     CPLDebug("NISAR_DRIVER", "Successfully imported EPSG:%d.", (int)epsg_code);
                     goto cleanup;  //Jump to cleanup AFTER successful assignment
                } else {
                     CPLError(CE_Warning, CPLE_AppDefined, "Failed to import EPSG code %lld into OGRSpatialReference.", epsg_code);
                     delete poTmpSRS; poSRS = nullptr; // Delete failed object
                     // Continue below to try WKT attribute
                }
            } else { CPLError(CE_Warning, CPLE_FileIO, "Failed to read valid EPSG code from attribute."); }
        } else { CPLError(CE_Warning, CPLE_FileIO, "EPSG attribute is not an integer type."); }

        // Close attribute type handle if it was opened successfully
        if (hAttrType >= 0) {
            H5Tclose(hAttrType);
            hAttrType = -1; // Reset handle ID after closing
        }
        // Close attribute handle if it was opened successfully
        if (hAttribute >= 0) {
            H5Aclose(hAttribute);
            hAttribute = -1; // Reset handle ID after closing
        }
    } else { CPLDebug("NISAR_DRIVER", "EPSG attribute not found."); }


    // Try Reading WKT Attribute if EPSG failed or not found
    if (poSRS == nullptr) {
        CPLDebug("NISAR_DRIVER", "Attempting to read WKT from 'spatial_ref' attribute.");
        hAttribute = H5Aopen(hProjectionDataset, "spatial_ref", H5P_DEFAULT); // Try "spatial_ref"

        if (hAttribute >= 0) {
            hAttrType = H5Aget_type(hAttribute);
            if (hAttrType >= 0 && H5Tget_class(hAttrType) == H5T_STRING) {
                 //H5T_str_t ePad = H5T_STR_NULLTERM; // Default, check?
                 //H5T_cset_t eCset = H5T_CSET_ASCII;    // Default, check?
                 bool bIsVariable = H5Tis_variable_str(hAttrType);

                 if (bIsVariable) { // Variable length string
                     status = H5Aread(hAttribute, hAttrType, &pszWKT_VL); // Reads into char* allocated by HDF5
                     if (status >= 0 && pszWKT_VL != nullptr) {
                         poTmpSRS = new OGRSpatialReference();
                         // Use const_cast ONLY if importFromWkt needs non-const char* (older GDAL?)
                         // Modern GDAL takes const char*. pszWKT_VL is char* here.
                         if (poTmpSRS->importFromWkt(pszWKT_VL) == OGRERR_NONE) {
                              m_poSRS = poTmpSRS; //Assign to member
                              poTmpSRS = nullptr;
                              CPLDebug("NISAR_DRIVER", "Successfully imported WKT from variable-length attribute.");
                              goto cleanup;
                         } else {
                              CPLError(CE_Warning, CPLE_AppDefined, "Failed to import WKT from variable-length 'spatial_ref' attribute.");
                              delete poTmpSRS; 
                              poSRS = nullptr;
                         }
                         // Free the variable length string buffer allocated by H5Aread
                         // Need the memory dataspace and type to reclaim? Docs say yes.
                         hid_t vl_space = H5Aget_space(hAttribute);
                         if (vl_space >= 0) {
                              H5Dvlen_reclaim(hAttrType, vl_space, H5P_DEFAULT, &pszWKT_VL); // Pass address of pointer
                              H5Sclose(vl_space);
                         } else { CPLFree(pszWKT_VL); } // Fallback? Unsafe. Needs H5Dvlen_reclaim.
                         pszWKT_VL = nullptr; // Mark as freed

                     } else { CPLError(CE_Warning, CPLE_FileIO, "Failed to read variable-length 'spatial_ref' attribute."); }
                 } else { // Fixed length string
                     size_t nWktLen = H5Tget_size(hAttrType);
                     if(nWktLen > 0) {
                          pszWKT_Alloc = (char *)VSIMalloc(nWktLen + 1); // Use VSI for GDAL consistency
                          if (pszWKT_Alloc) {
                              status = H5Aread(hAttribute, hAttrType, pszWKT_Alloc);
                              if (status >= 0) {
                                  pszWKT_Alloc[nWktLen] = '\0'; // Ensure null termination
                                  poSRS = new OGRSpatialReference();
                                  if (poSRS->importFromWkt(pszWKT_Alloc) != OGRERR_NONE) {
                                       CPLError(CE_Warning, CPLE_AppDefined, "Failed to import WKT from fixed-length 'spatial_ref' attribute.");
                                       delete poSRS; poSRS = nullptr;
                                  } else {
                                       CPLDebug("NISAR_DRIVER", "Successfully imported WKT from fixed-length attribute.");
                                  }
                              } else { CPLError(CE_Warning, CPLE_FileIO, "Failed to read fixed-length 'spatial_ref' attribute."); }
                              VSIFree(pszWKT_Alloc); pszWKT_Alloc = nullptr; // Free allocated buffer
                          } else { CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate buffer for WKT."); }
                     } else { CPLError(CE_Warning, CPLE_AppDefined, "Fixed-length WKT attribute has zero size."); }
                 }
            } else { CPLError(CE_Warning, CPLE_FileIO, "'spatial_ref' attribute is not a string type."); }

            // Close attribute handles if they were opened
            if(hAttrType >= 0) {
                H5Tclose(hAttrType); 
                hAttrType = -1;
            }
            // Close attribute handle if it was opened successfully
            if (hAttribute >= 0) {
                H5Aclose(hAttribute);
                hAttribute = -1; // Reset handle ID after closing
            }
        } else { CPLDebug("NISAR_DRIVER", "WKT attribute ('spatial_ref') not found."); }
    } // end if pSRS == nullptr

cleanup:
    // Close handles opened within this function
    if (hAttribute >= 0) H5Aclose(hAttribute); // Should be closed already above, but just in case
    if (hAttrType >= 0) H5Tclose(hAttrType);   // Should be closed already above
    if (hProjectionDataset >= 0) H5Dclose(hProjectionDataset);

    // Free any potentially allocated WKT buffers if error occurred before free
    if (pszWKT_Alloc) VSIFree(pszWKT_Alloc);
    // Note: Cannot easily free pszWKT_VL here if H5Dvlen_reclaim failed earlier.
    // H5Dvlen_reclaim should be called right after successful importFromWkt.

    // If poTmpSRS was allocated but not assigned to m_poSRS (e.g., import failed), delete it
    if (poTmpSRS != nullptr) {
        delete poTmpSRS;
    }

    if(m_poSRS == nullptr) {
         CPLDebug("NISAR_DRIVER", "GetSpatialRef: Could not find valid SRS information.");
    } else {
         m_poSRS->SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER); // Ensure consistent axis order
         CPLDebug("NISAR_DRIVER", "GetSpatialRef returning cached SRS object.");
    }

    // Mutex is automatically unlocked when holderD goes out of scope
    return m_poSRS; // Return pointer to the cached member variable (const*)
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
static bool Read2DSliceAsVec(hid_t hLocation, const char* pszPath,
                             std::vector<double>& vec, int nSliceIndex = 0)
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
    if (hDset < 0) {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to open dataset: %s", pszPath);
        goto cleanup;
    }

    hFileSpace = H5Dget_space(hDset);
    if (hFileSpace < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "Failed to get filespace for: %s", pszPath);
        goto cleanup;
    }

    if (H5Sget_simple_extent_ndims(hFileSpace) != 3) {
        CPLError(CE_Warning, CPLE_AppDefined, "Dataset '%s' is not 3-dimensional as expected for a metadata cube.", pszPath);
        goto cleanup;
    }

    H5Sget_simple_extent_dims(hFileSpace, dims, nullptr); // Get dims: [height, azimuth, range]

    if (nSliceIndex < 0 || static_cast<hsize_t>(nSliceIndex) >= dims[0]) {
        CPLError(CE_Warning, CPLE_AppDefined, "Slice index %d is out of bounds for dataset: %s", nSliceIndex, pszPath);
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

    if (H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count, nullptr) < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "Failed to select hyperslab for: %s", pszPath);
        goto cleanup;
    }

    // Dataspace in Memory
    // Create a 2D dataspace for memory that matches the dimensions of our slice [azimuth, range].
    mem_dims[0] = dims[1];
    mem_dims[1] = dims[2];
    hMemSpace = H5Screate_simple(2, mem_dims, nullptr);
    if (hMemSpace < 0) {
        CPLError(CE_Warning, CPLE_AppDefined, "Failed to create memory space for slice.");
        goto cleanup;
    }

    try {
        vec.resize(dims[1] * dims[2]);
    } catch (const std::bad_alloc&) {
        CPLError(CE_Failure, CPLE_OutOfMemory, "Failed to allocate memory for slice from %s", pszPath);
        goto cleanup;
    }

    if (H5Dread(hDset, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpace, H5P_DEFAULT, vec.data()) < 0) {
        CPLError(CE_Warning, CPLE_FileIO, "Failed to read data slice from %s", pszPath);
        vec.clear();
        goto cleanup;
    }

    bSuccess = true;

cleanup:
    if (hMemSpace >= 0) H5Sclose(hMemSpace);
    if (hFileSpace >= 0) H5Sclose(hFileSpace);
    if (hDset >= 0) H5Dclose(hDset);
    return bSuccess;
}

// Helper to read a string attribute from an HDF5 object
static std::string ReadH5StringAttribute(hid_t hObjectID, const char* pszAttrName)
{
    if( H5Aexists(hObjectID, pszAttrName) <= 0 ) return "";
    hid_t hAttr = H5Aopen_name(hObjectID, pszAttrName);
    if( hAttr < 0 ) return "";

    std::string strVal;
    hid_t hAttrType = H5Aget_type(hAttr);
    hid_t hAttrSpace = H5Aget_space(hAttr);
    if( H5Sget_simple_extent_npoints(hAttrSpace) == 1 )
    {
        size_t nSize = H5Aget_storage_size(hAttr);
        char* pszVal = new char[nSize + 1];
        if (H5Aread(hAttr, hAttrType, pszVal) >= 0)
        {
            pszVal[nSize] = '\0';
            strVal = pszVal;
        }
        delete[] pszVal;
    }
    
    H5Sclose(hAttrSpace);
    H5Tclose(hAttrType);
    H5Aclose(hAttr);
    return strVal;
}

CPLErr NisarDataset::GenerateGCPsFromGeolocationGrid(const char* pszProductGroup) {

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
    double startingRange = 0.0, rangePixelSpacing = 0.0, prf = 0.0, scene_start_time = 0.0;
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
    
    // Declare all path strings here, before any potential 'goto' calls
    const char* pszStartTimePath = "/science/LSAR/identification/zeroDopplerStartTime";
    std::string sGridPath = CPLSPrintf("/science/LSAR/%s/metadata/geolocationGrid", pszProductGroup);
    std::string sStartingRangePath = CPLSPrintf("/science/LSAR/%s/swaths/frequencyA/startingRange", pszProductGroup);
    std::string sSlantRangeSpacingPath = CPLSPrintf("/science/LSAR/%s/swaths/frequencyA/slantRangeSpacing", pszProductGroup);
    std::string sPulseRepetitionFrequencyPath = CPLSPrintf("/science/LSAR/%s/swaths/frequencyA/nominalAcquisitionPRF", pszProductGroup);
    std::string sSlantRangePath = CPLSPrintf("/science/LSAR/%s/swaths/frequencyA/slantRange", pszProductGroup);


    // Open the geolocationGrid group
    hGridGroup = H5Gopen2(hHDF5, sGridPath.c_str(), H5P_DEFAULT);
    if (hGridGroup < 0) {
        CPLError(CE_Failure, CPLE_FileIO, "Could not open geolocationGrid group at %s", sGridPath.c_str());
        goto cleanup;
    }

    // Read the EPSG code from the scalar DATASET
    hEpsgDset = H5Dopen2(hGridGroup, "epsg", H5P_DEFAULT);
    if (hEpsgDset < 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to open 'epsg' dataset in geolocationGrid.");
        goto cleanup;
    }

    // Use H5T_NATIVE_INT to match the Int32 spec. Reading into a long long is safe.
    if (H5Dread(hEpsgDset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, &epsg_code) < 0 || epsg_code <= 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to read a valid EPSG code from 'epsg' dataset.");
        //H5Dclose(hEpsgDset); // Close handle even on read failure
        goto cleanup;
    }

    //H5Dclose(hEpsgDset); // Close handle on success
    CPLDebug("NISAR_DRIVER", "Read EPSG code %lld from dataset.", epsg_code);


    // Read the grid vectors into memory
    if (!Read2DSliceAsVec(hGridGroup, "coordinateX", x_coords) ||
        !Read2DSliceAsVec(hGridGroup, "coordinateY", y_coords) ||
        !Read1DDoubleVec(hGridGroup, "slantRange", slant_ranges) ||
        !Read1DDoubleVec(hGridGroup, "zeroDopplerTime", azimuth_times)) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to read one or more geolocation grid datasets.");
        goto cleanup;
    }
    CPLDebug("NISAR_DRIVER", "Read geolocation grid arrays. Azimuth points: %zu, Range points: %zu",
             azimuth_times.size(), slant_ranges.size());

    // Get the time epoch for the azimuth grid
    time_epoch = 0.0;
    hAzimuthTimeDset = H5Dopen2(hGridGroup, "zeroDopplerTime", H5P_DEFAULT);
    if( hAzimuthTimeDset < 0 ) {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to open geolocationGrid/zeroDopplerTime dataset.");
        goto cleanup;
    }
    time_units = ReadH5StringAttribute(hAzimuthTimeDset, "units");
    //H5Dclose(hAzimuthTimeDset);

    if( sscanf(time_units.c_str(), "seconds since %d-%d-%dT%d:%d:%d",
               &nEpochYear, &nEpochMonth, &nEpochDay, &nEpochHour, &nEpochMin, &nEpochSec) == 6)
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
    } else {
        CPLError(CE_Failure, CPLE_AppDefined, "Could not parse time epoch from units: %s", time_units.c_str());
        goto cleanup;
    }

    // Read scalar parameters for pixel/line conversion

    // Read the first value from the "slantRange" array dataset
    hSlantRangeDset = H5Dopen2(hHDF5, sSlantRangePath.c_str(), H5P_DEFAULT);
    if (hSlantRangeDset < 0) {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to open 'slantRange' dataset.");
        goto cleanup;
    }

    // Create a memory space for a single double value
    mem_dims[0] = 1;
    hMemSpace = H5Screate_simple(1, mem_dims, nullptr);
    if (hMemSpace < 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to create memory space for 'slantRange' read.");
        goto cleanup;
    }

    // Select the first element from the file's dataspace
    hFileSpace = H5Dget_space(hSlantRangeDset);
    if (hFileSpace < 0) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to get filespace for 'slantRange' dataset.");
        goto cleanup;
    }
    offset[0] = 0;
    count[0] = 1;
    H5Sselect_hyperslab(hFileSpace, H5S_SELECT_SET, offset, nullptr, count, nullptr);

    // Read the single value
    if (H5Dread(hSlantRangeDset, H5T_NATIVE_DOUBLE, hMemSpace, hFileSpace, H5P_DEFAULT, &startingRange) < 0) {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read first element from 'slantRange' dataset.");
        goto cleanup;
    }

    CPLDebug("NISAR_DRIVER", "Read startingRange: %.10g", startingRange);

    // Read slantRangeSpacing
    hScalarDset = H5Dopen2(hHDF5, sSlantRangeSpacingPath.c_str(), H5P_DEFAULT);
    if(hScalarDset < 0) { CPLError(CE_Failure, CPLE_FileIO, "Failed to open slantRangeSpacing."); goto cleanup; }
    if(H5Dread(hScalarDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &rangePixelSpacing) < 0) {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read slantRangeSpacing."); goto cleanup;
    }
    //H5Dclose(hScalarDset); 
    //hScalarDset = -1;

    // Read processedPulseRepetitionFrequency
    hScalarDset = H5Dopen2(hHDF5, sPulseRepetitionFrequencyPath.c_str(), H5P_DEFAULT);
    if(hScalarDset < 0) { CPLError(CE_Failure, CPLE_FileIO, "Failed to open processedPulseRepetitionFrequency."); goto cleanup; }
    if(H5Dread(hScalarDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &prf) < 0) {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read processedPulseRepetitionFrequency."); goto cleanup;
    }
    //H5Dclose(hScalarDset); 
    //hScalarDset = -1;

    // Read and parse the Scene Start Time STRING
    hScalarDset = H5Dopen2(hHDF5, pszStartTimePath, H5P_DEFAULT);
    if (hScalarDset >= 0) {
        hStrType = H5Dget_type(hScalarDset);
        if (hStrType >= 0) {
            if (H5Tis_variable_str(hStrType) > 0) { // Handle variable-length
                // NOTE: This H5Dread allocates memory that must be freed with H5free_memory
                H5Dread(hScalarDset, hStrType, H5S_ALL, H5S_ALL, H5P_DEFAULT, &pszStartTimeStr);
                if(pszStartTimeStr != nullptr) {
                    bStartTimeAllocatedByHDF5 = true;
                }
            } else { //Handle fixed-length
                size_t nSize = H5Tget_size(hStrType);
                if (nSize > 0) {
                    // Allocate with CPLMalloc, free with CPLFree in cleanup block
                    pszStartTimeStr = (char *)CPLMalloc(nSize + 1);
                    if (H5Dread(hScalarDset, hStrType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszStartTimeStr) >= 0) {
                        pszStartTimeStr[nSize] = '\0'; // Ensure null termination
                    } else {
                        CPLFree(pszStartTimeStr);
                        pszStartTimeStr = nullptr; // Reset on failure
                    }
                }
            }
            //H5Tclose(hStrType);
        }
        //H5Dclose(hScalarDset);
        //hScalarDset = -1;
    }

    if (pszStartTimeStr == nullptr || pszStartTimeStr[0] == '\0') {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read a valid zeroDopplerStartTime string.");
        goto cleanup;
    }

    int nYear, nMonth, nDay, nHour, nMin;
    double dfSec;
    if (sscanf(pszStartTimeStr, "%d-%d-%dT%d:%d:%lf",
               &nYear, &nMonth, &nDay, &nHour, &nMin, &dfSec) == 6) {
        struct tm brokendown_time;
        memset(&brokendown_time, 0, sizeof(brokendown_time)); // Important: zero out the struct
        brokendown_time.tm_year = nYear - 1900;
        brokendown_time.tm_mon = nMonth - 1;
        brokendown_time.tm_mday = nDay;
        brokendown_time.tm_hour = nHour;
        brokendown_time.tm_min = nMin;
        scene_start_time = static_cast<double>(CPLYMDHMSToUnixTime(&brokendown_time)) + dfSec;
        CPLDebug("NISAR_DRIVER", "Parsed start time %s to %f seconds since epoch.", pszStartTimeStr, scene_start_time);
    } else {
        CPLError(CE_Failure, CPLE_AppDefined, "Could not parse zeroDopplerStartTime string: %s", pszStartTimeStr);
        goto cleanup;
    }

    // Create the CRS from the EPSG code
    poCRS = new OGRSpatialReference();
    if (poCRS->importFromEPSG(static_cast<int>(epsg_code)) != OGRERR_NONE) {
        CPLError(CE_Failure, CPLE_AppDefined, "Failed to import EPSG:%lld.", epsg_code);
        goto cleanup;
    }
    poCRS->SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);

    // Build the GCP list
    try {
        gcp_list.reserve(azimuth_times.size() * slant_ranges.size());
        for (size_t i = 0; i < azimuth_times.size(); ++i) {
            for (size_t j = 0; j < slant_ranges.size(); ++j) {
                GDAL_GCP gcp;
                size_t grid_index = i * slant_ranges.size() + j;

                gcp.dfGCPX = x_coords.at(grid_index);
                gcp.dfGCPY = y_coords.at(grid_index);
                gcp.dfGCPZ = 0.0;

                gcp.dfGCPPixel = ((slant_ranges.at(j) - startingRange) / rangePixelSpacing) + 0.5;
                // Convert grid azimuth time to Unix time before subtracting
                gcp_unix_time = time_epoch + azimuth_times.at(i);
                gcp.dfGCPLine = ((gcp_unix_time - scene_start_time) * prf) + 0.5;

                gcp.pszId = CPLStrdup(CPLSPrintf("%zu", gcp_list.size() + 1));
                gcp.pszInfo = CPLStrdup("");

                gcp_list.push_back(gcp);
            }
        }
    } catch (const std::exception& e) {
        CPLError(CE_Failure, CPLE_AppDefined, "Exception while building GCP list: %s", e.what());
        goto cleanup;
    }

    // Set the GCPs on the dataset
    this->SetGCPs(gcp_list.size(), gcp_list.data(), poCRS);
    CPLDebug("NISAR_DRIVER", "Successfully set %zu GCPs on the dataset.", gcp_list.size());
    eErr = CE_None; // Success!

cleanup:
    // Clean up all resources
    if (pszStartTimeStr) {
        if (bStartTimeAllocatedByHDF5) {
            H5free_memory(pszStartTimeStr);
        } else {
            CPLFree(pszStartTimeStr);
        }
    }
    if (hScalarDset >= 0) H5Dclose(hScalarDset);
    if (hGridGroup >= 0) H5Gclose(hGridGroup);
    if (hEpsgDset >= 0) H5Dclose(hEpsgDset);
    if (hSlantRangeDset >= 0) H5Dclose(hSlantRangeDset);
    if (hMemSpace >= 0) H5Sclose(hMemSpace);
    if (hFileSpace >= 0) H5Sclose(hFileSpace);
    if (hStrType >= 0) H5Tclose(hStrType);
    if (poCRS) poCRS->Release();

    // The GCP list is now owned by the dataset, so we do NOT free it here.

    return eErr;
}
