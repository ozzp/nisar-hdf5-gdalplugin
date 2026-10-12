/*
 * Copyright 2025, California Institute of Technology.
 * All rights reserved. U.S. Government sponsorship acknowledged.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
// nisardataset.cpp

#include "nisardataset.h"
#include "nisarrasterband.h"
#include "nisarinterpolated.h"

#include <sstream>  // For std::ostringstream
#include <iomanip>  // For std::setprecision
#include <vector>
#include <string>
#include <algorithm>
#include <limits>
#include <thread>

#include "cpl_port.h"
#include "cpl_conv.h"
#include "cpl_string.h"  // For CPLStrintf, EQUAL
#include "cpl_time.h"
#include "nisar_priv.h"
#include "hdf5.h"
#include "H5FDros3.h"  // For H5FD_ros3_fapl_t and H5FD_CURR_ROS3_FAPL_T_VERSION
#include "hdf5vfl.h"

// Helper to handle API change between 3.10 and 3.12
static std::string NisarGetExtension(const char *pszFilename)
{
#if GDAL_VERSION_MAJOR > 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
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
    hid_t hDset = -1;
    hid_t hType = -1;
    hid_t hSpace = -1;
    hid_t hMemType = -1;
    char *pszBuffer = nullptr;
    hsize_t nStrings = 0;
    size_t nStringSize = 0;     // Size of string in *file*
    size_t nMemStringSize = 0;  // Size of string in *memory*
    int ndims = 0;

    std::ostringstream oss;

    // Suppress HDF5 errors for this read attempt
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open dataset
    hDset = H5Dopen2(hParentGroup, pszDatasetName, H5P_DEFAULT);
    if (hDset < 0)
    {
        goto cleanup_and_restore;  // Failed to open dataset
    }

    // Get type and verify it's a string
    hType = H5Dget_type(hDset);
    if (hType < 0 || H5Tget_class(hType) != H5T_STRING)
    {
        goto cleanup_and_restore;  // Not a string
    }

    // Size of one string element IN THE FILE
    nStringSize = H5Tget_size(hType);
    if (nStringSize == 0)
        nStringSize = 1;

    // Get space and verify it's 1D
    hSpace = H5Dget_space(hDset);
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
        goto cleanup_and_restore;  // Empty array
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
    pszBuffer =
        static_cast<char *>(VSI_MALLOC2_VERBOSE(nStrings, nMemStringSize));
    if (pszBuffer == nullptr)
        goto cleanup_and_restore;
    if (H5Dread(hDset, hMemType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszBuffer) < 0)
    {
        goto cleanup_and_restore;  // Read failed
    }

    // Iterate, and append
    for (hsize_t i = 0; i < nStrings; ++i)
    {
        // Point to the beginning of the i-th *null-terminated* string
        const char *pszSourceString = pszBuffer + (i * nMemStringSize);

        oss << pszSourceString;
        if (i < nStrings - 1)
        {
            oss << ",";  // Add comma separator
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
    if (hDset >= 0)
        H5Dclose(hDset);

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
    hid_t hDset = -1;
    hid_t hType = -1;
    hid_t hSpace = -1;
    hid_t hMemType = -1;
    char *pszBuffer = nullptr;
    size_t nSize = 0;  // Declared at top

    // Suppress HDF5 errors for this read attempt
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    // Open dataset
    hDset = H5Dopen2(hParentGroup, pszDatasetName, H5P_DEFAULT);
    if (hDset < 0)
    {
        goto cleanup_and_restore;  // Failed to open dataset
    }

    // Get type and verify it's a string
    hType = H5Dget_type(hDset);
    if (hType < 0 || H5Tget_class(hType) != H5T_STRING)
    {
        goto cleanup_and_restore;  // Not a string
    }

    // Get string size
    nSize = H5Tget_size(hType);
    if (nSize == 0)
    {
        nSize = 1;  // Safety
    }

    // Get space and verify it's scalar
    hSpace = H5Dget_space(hDset);
    if (hSpace < 0 || H5Sget_simple_extent_ndims(hSpace) != 0)
    {
        // Note: This helper only supports scalar strings, not arrays of strings
        goto cleanup_and_restore;
    }

    // Create memory type for reading
    hMemType = H5Tcopy(H5T_C_S1);
    H5Tset_size(hMemType, nSize + 1);           // +1 for null terminator
    H5Tset_strpad(hMemType, H5T_STR_NULLTERM);  // Explicitly ask for Null Term

    // Read the string
    pszBuffer = static_cast<char *>(CPLMalloc(nSize + 1));
    if (H5Dread(hDset, hMemType, H5S_ALL, H5S_ALL, H5P_DEFAULT, pszBuffer) >= 0)
    {
        pszBuffer[nSize] = '\0';  // Ensure null termination
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
    if (hDset >= 0)
        H5Dclose(hDset);

    return sResult;
}

/************************************************************************/
/*                              Identify()                              */
/************************************************************************/

int NisarDataset::Identify(GDALOpenInfo *poOpenInfo)
{
    const char *pszPrefix = "NISAR:";
    size_t nPrefixLen = strlen(pszPrefix);

    // Explicit Driver Prefix Check
    // Always the fastest path.
    // The connection string is ours whatever the file extension (NISAR
    // granules are .h5, but do not depend on it).
    if (EQUALN(poOpenInfo->pszFilename, pszPrefix, nPrefixLen))
    {
        return TRUE;
    }

    // Check the Header Buffer
    // GDAL pre-loads the first 1024 bytes into poOpenInfo->pHeader.
    // The HDF5 signature is: \211 H D F \r \n \032 \n
    if (poOpenInfo->nHeaderBytes < 8 ||
        memcmp(poOpenInfo->pabyHeader, "\211HDF\r\n\032\n", 8) != 0)
    {
        return FALSE;
    }

    // Remote/Virtual Path Heuristics
    // We avoid H5Fopen on remote files during Identify to prevent S3 Latency Tax.
    bool bIsRemote = STARTS_WITH_CI(poOpenInfo->pszFilename, "s3://") ||
                     STARTS_WITH_CI(poOpenInfo->pszFilename, "/vsis3/") ||
                     STARTS_WITH_CI(poOpenInfo->pszFilename, "http");

    if (bIsRemote)
    {
        // For remote files, we rely on the filename containing "NISAR"
        // OR the extension being .h5 (which we verified in step 2 via header).
        CPLString osFilename = poOpenInfo->pszFilename;
        return (osFilename.ifind("NISAR") != std::string::npos);
    }

    // Local File Deep Check
    // Only perform this if we are certain it's HDF5 (Step 2) but need to
    // verify it's NISAR-flavored.
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    bool bIsNisar = false;
    hid_t hFile = H5Fopen(poOpenInfo->pszFilename, H5F_ACC_RDONLY, H5P_DEFAULT);
    if (hFile >= 0)
    {
        if (H5Lexists(hFile, "/science/LSAR/identification", H5P_DEFAULT) > 0 ||
            H5Lexists(hFile, "/science/SSAR/identification", H5P_DEFAULT) > 0)
        {
            bIsNisar = true;
        }
        H5Fclose(hFile);
    }

    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
    return bIsNisar;
}

/************************************************************************/
/*                     ReadIdentificationMetadata()                     */
/************************************************************************/
/**
 * \brief Reads essential identification metadata.
 *
 * This function is called from NisarDataset::Open() immediately after
 * the HDF5 file is opened. It finds the /science/[LSAR|SSAR]/identification
 * group and reads the productType and productLevel datasets to populate
 * the corresponding member variables (m_sProductType, m_sInst, etc.).
 * This is necessary for the Open() function to determine which
 * HDF5 dataset to open (e.g., L1 swaths vs L2/L3 grids).
 */
void NisarDataset::ReadIdentificationMetadata()
{
    // Reset flags
    m_sInst.clear();
    m_sProductType.clear();
    m_bIsLevel1 = false;
    m_bIsLevel2 = false;
    m_bIsLevel3 = false;

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
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore
        return;
    }

    // Open the identified group
    hIdentGroup = H5Gopen2(hHDF5, sIdentPath.c_str(), H5P_DEFAULT);
    if (hIdentGroup < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Found identification path '%s' but failed to open it.",
                 sIdentPath.c_str());
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore
        return;
    }

    // Read productType
    m_sProductType = ReadHDF5StringDataset(hIdentGroup, "productType");

    // Read productLevel
    std::string sProductLevel =
        ReadHDF5StringDataset(hIdentGroup, "productLevel");

    // Set flags based on productLevel
    if (EQUAL(sProductLevel.c_str(), "L1"))
    {
        m_bIsLevel1 = true;
    }
    else if (EQUAL(sProductLevel.c_str(), "L2"))
    {
        m_bIsLevel2 = true;
    }
    else if (EQUAL(sProductLevel.c_str(), "L3"))
    {
        m_bIsLevel3 = true;
    }

    // If productLevel was missing or weird, we force L2 for known grid types.
    if (EQUAL(m_sProductType.c_str(), "GUNW") ||
        EQUAL(m_sProductType.c_str(), "GCOV") ||
        EQUAL(m_sProductType.c_str(), "GOFF") ||
        EQUAL(m_sProductType.c_str(), "GSLC"))
    {
        m_bIsLevel2 = true;
        m_bIsLevel3 = false;
        m_bIsLevel1 = false;
    }
    else if (EQUAL(m_sProductType.c_str(),
                   "SME2"))  // Add other L3 products here if known!
    {
        m_bIsLevel3 = true;
        m_bIsLevel1 = false;
        m_bIsLevel2 = false;
    }
    else if (EQUAL(m_sProductType.c_str(), "RSLC") ||
             EQUAL(m_sProductType.c_str(), "RIFG") ||
             EQUAL(m_sProductType.c_str(), "RUNW"))
    {
        m_bIsLevel1 = true;
        m_bIsLevel2 = false;
        m_bIsLevel3 = false;
    }

    // Restore HDF5 error handling
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

    // Cleanup
    H5Gclose(hIdentGroup);

    // Log results
    CPLDebug(
        "NISAR_DRIVER",
        "Identified Product: INST=%s, Type=%s, Level=%s (L1=%d, L2=%d, L3=%d)",
        m_sInst.c_str(), m_sProductType.c_str(), sProductLevel.c_str(),
        m_bIsLevel1, m_bIsLevel2, m_bIsLevel3);

    if (m_sProductType.empty() || sProductLevel.empty())
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Failed to read 'productType' or 'productLevel' from %s",
                 sIdentPath.c_str());
    }
}

// Helper to read an HDF5 attribute and add it to a CSL list
static herr_t NISAR_AttributeCallback(hid_t hLocation, const char *attr_name,
                                      const H5A_info_t * /*pAinfo*/,
                                      void *op_data)
{
    NISAR_AttrCallbackData *data =
        static_cast<NISAR_AttrCallbackData *>(op_data);

    // Skip HDF5 internal attributes that are not useful for metadata
    if (EQUAL(attr_name, "DIMENSION_LIST") ||
        EQUAL(attr_name, "REFERENCE_LIST") || EQUAL(attr_name, "CLASS") ||
        EQUAL(attr_name, "NAME"))
    {
        return 0;
    }

    hid_t attr_id = -1;
    hid_t attr_type = -1;
    hid_t native_type = -1;
    hid_t attr_space = -1;
    std::string value_str;

    attr_id =
        H5Aopen_by_name(hLocation, ".", attr_name, H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0)
        return 0;

    attr_type = H5Aget_type(attr_id);
    attr_space = H5Aget_space(attr_id);

    if (attr_type < 0 || attr_space < 0)
    {
        if (attr_type >= 0)
            H5Tclose(attr_type);
        if (attr_space >= 0)
            H5Sclose(attr_space);
        H5Aclose(attr_id);
        return 0;
    }

    native_type = H5Tget_native_type(attr_type, H5T_DIR_ASCEND);
    if (native_type < 0)
    {
        H5Tclose(attr_type);
        H5Sclose(attr_space);
        H5Aclose(attr_id);
        return 0;
    }

    H5T_class_t type_class = H5Tget_class(native_type);
    hssize_t n_points = H5Sget_simple_extent_npoints(attr_space);

    // Process scalar attributes
    if (n_points == 1)
    {
        if (type_class == H5T_STRING)
        {
            if (H5Tis_variable_str(native_type))
            {
                char *pszReadVL = nullptr;
                if (H5Aread(attr_id, native_type, &pszReadVL) >= 0 && pszReadVL)
                {
                    value_str = pszReadVL;
                    H5free_memory(pszReadVL);
                }
                else
                    value_str = "(read error VL string)";
            }
            else
            {  // Fixed length string
                size_t type_size = H5Tget_size(native_type);
                if (type_size > 0)
                {
                    char *pszReadFixed =
                        static_cast<char *>(VSI_MALLOC_VERBOSE(type_size + 1));
                    if (pszReadFixed)
                    {
                        if (H5Aread(attr_id, native_type, pszReadFixed) >= 0)
                        {
                            pszReadFixed[type_size] = '\0';
                            value_str = pszReadFixed;
                        }
                        else
                            value_str = "(read error fixed string)";

                        VSIFree(pszReadFixed);
                    }
                    else
                        value_str = "(memory alloc error)";
                }
                else
                    value_str = "(zero size fixed string)";
            }
        }
        else if (type_class == H5T_INTEGER)
        {
            long long llVal = 0;
            if (H5Aread(attr_id, H5T_NATIVE_LLONG, &llVal) >= 0)
                value_str = CPLSPrintf("%lld", llVal);
            else
                value_str = "(read error integer)";
        }
        else if (type_class == H5T_FLOAT)
        {
            // --- HDF5 2.0 BFLOAT16 SUPPORT ---
            // HDF5's native type conversion implicitly handles bfloat16 datatypes here.
            // By requesting H5T_NATIVE_DOUBLE as the destination memory type, the library
            // automatically converts the 16-bit ml-float into standard double precision.
            // Custom bitwise conversion is no longer required.
            double dfVal = 0.0;
            if (H5Aread(attr_id, H5T_NATIVE_DOUBLE, &dfVal) >= 0)
                value_str = CPLSPrintf("%.18g", dfVal);  // Full precision
            else
                value_str = "(read error float)";
        }
#ifdef H5T_COMPLEX
        else if (type_class == H5T_COMPLEX)
        {
            // HDF5 2.0 FIRST-CLASS COMPLEX SUPPORT
            // Direct memory mapping without marshaling compounds.
            hid_t base_type = H5Tget_super(native_type);

            if (base_type >= 0)
            {
                if (H5Tequal(base_type, H5T_NATIVE_FLOAT) > 0)
                {
                    std::complex<float> cfVal;
                    if (H5Aread(attr_id, H5T_NATIVE_FLOAT_COMPLEX, &cfVal) >= 0)
                    {
                        if (std::isnan(cfVal.real()) ||
                            std::isnan(cfVal.imag()))
                            value_str = "nan";
                        else
                            value_str = CPLSPrintf("%.10g + %.10gj",
                                                   cfVal.real(), cfVal.imag());
                    }
                    else
                        value_str = "(read error native complex float)";
                }
                else if (H5Tequal(base_type, H5T_NATIVE_DOUBLE) > 0)
                {
                    std::complex<double> cdVal;
                    if (H5Aread(attr_id, H5T_NATIVE_DOUBLE_COMPLEX, &cdVal) >=
                        0)
                    {
                        if (std::isnan(cdVal.real()) ||
                            std::isnan(cdVal.imag()))
                            value_str = "nan";
                        else
                            value_str = CPLSPrintf("%.18g + %.18gj",
                                                   cdVal.real(), cdVal.imag());
                    }
                    else
                        value_str = "(read error native complex double)";
                }
                else
                {
                    value_str = "(unhandled native complex base type)";
                }
                H5Tclose(base_type);
            }
        }
#endif
        // Handle Compound Type (Fallback for pre-HDF5 2.0 NISAR datasets)
        else if (type_class == H5T_COMPOUND)
        {
            hid_t hRealType = -1;
            hid_t hImagType = -1;
            char *name1 = nullptr;
            char *name2 = nullptr;
            bool bIsComplex = false;
            GDALDataType eBaseType = GDT_Unknown;

            if (H5Tget_nmembers(native_type) == 2)
            {
                hRealType = H5Tget_member_type(native_type, 0);
                hImagType = H5Tget_member_type(native_type, 1);

                if (hRealType >= 0 && hImagType >= 0)
                {
                    if (H5Tequal(hRealType, hImagType) > 0)
                    {
                        name1 = H5Tget_member_name(native_type, 0);
                        name2 = H5Tget_member_name(native_type, 1);

                        // Check conventional naming ('r'/'i' or 'R'/'I')
                        bool isReal =
                            (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                        bool isImaginary =
                            (name2 && (name2[0] == 'i' || name2[0] == 'I'));

                        if (isReal && isImaginary)
                        {
                            bIsComplex = true;
                            if (H5Tequal(hRealType, H5T_NATIVE_FLOAT) > 0)
                                eBaseType = GDT_Float32;
                            else if (H5Tequal(hRealType, H5T_NATIVE_DOUBLE) > 0)
                                eBaseType = GDT_Float64;
                            else if (H5Tequal(hRealType, H5T_NATIVE_SHORT) > 0)
                                eBaseType = GDT_Int16;
                            else if (H5Tequal(hRealType, H5T_NATIVE_INT) > 0)
                                eBaseType = GDT_Int32;
                        }
                    }
                    H5Tclose(hRealType);
                    hRealType = -1;
                    H5Tclose(hImagType);
                    hImagType = -1;
                }
                else
                {
                    if (hRealType >= 0)
                        H5Tclose(hRealType);
                    if (hImagType >= 0)
                        H5Tclose(hImagType);
                }

                if (name1)
                    H5free_memory(name1);
                if (name2)
                    H5free_memory(name2);
            }

            if (bIsComplex && eBaseType != GDT_Unknown)
            {
                if (eBaseType == GDT_Float32)
                {
                    std::complex<float> cfVal;
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(std::complex<float>));
                    H5Tinsert(mem_type, "r", 0, H5T_NATIVE_FLOAT);
                    H5Tinsert(mem_type, "i", sizeof(float), H5T_NATIVE_FLOAT);
                    if (H5Aread(attr_id, mem_type, &cfVal) >= 0)
                    {
                        if (std::isnan(cfVal.real()) ||
                            std::isnan(cfVal.imag()))
                            value_str = "nan";
                        else
                            value_str = CPLSPrintf("%.10g + %.10gj",
                                                   cfVal.real(), cfVal.imag());
                    }
                    else
                        value_str = "(read error compound complex float)";

                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Float64)
                {
                    std::complex<double> cdVal;
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(std::complex<double>));
                    H5Tinsert(mem_type, "r", 0, H5T_NATIVE_DOUBLE);
                    H5Tinsert(mem_type, "i", sizeof(double), H5T_NATIVE_DOUBLE);
                    if (H5Aread(attr_id, mem_type, &cdVal) >= 0)
                    {
                        if (std::isnan(cdVal.real()) ||
                            std::isnan(cdVal.imag()))
                            value_str = "nan";
                        else
                            value_str = CPLSPrintf("%.18g + %.18gj",
                                                   cdVal.real(), cdVal.imag());
                    }
                    else
                        value_str = "(read error compound complex double)";

                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Int16)
                {
                    ComplexInt16Attr ciVal;
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(ComplexInt16Attr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexInt16Attr, r),
                              H5T_NATIVE_SHORT);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexInt16Attr, i),
                              H5T_NATIVE_SHORT);
                    if (H5Aread(attr_id, mem_type, &ciVal) >= 0)
                        value_str = CPLSPrintf("%d + %dj", ciVal.r, ciVal.i);
                    else
                        value_str = "(read error complex int16)";

                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Int32)
                {
                    ComplexInt32Attr ciVal;
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(ComplexInt32Attr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexInt32Attr, r),
                              H5T_NATIVE_INT);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexInt32Attr, i),
                              H5T_NATIVE_INT);
                    if (H5Aread(attr_id, mem_type, &ciVal) >= 0)
                        value_str = CPLSPrintf("%d + %dj", ciVal.r, ciVal.i);
                    else
                        value_str = "(read error complex int32)";

                    H5Tclose(mem_type);
                }
                else
                {
                    value_str = "(unhandled complex base type)";
                }
            }
            else
            {
                value_str = "(compound data)";
            }
        }
        else if (type_class == H5T_VLEN)
        {
            value_str = "(variable-length data)";
        }
    }

    if (value_str.empty())
    {
        const char *class_name = "Unknown";
        switch (type_class)
        {
            case H5T_INTEGER:
                class_name = "Integer";
                break;
            case H5T_FLOAT:
                class_name = "Float";
                break;
            case H5T_STRING:
                class_name = "String";
                break;
#ifdef H5T_COMPLEX
            case H5T_COMPLEX:
                class_name = "Complex";
                break;
#endif
            case H5T_COMPOUND:
                class_name = "Compound";
                break;
            case H5T_VLEN:
                class_name = "VLEN";
                break;
            default:
                break;
        }
        value_str = CPLSPrintf("(unhandled attr: class=%s, points=%lld)",
                               class_name, static_cast<long long>(n_points));
    }

    std::string finalKey;
    if (data->pszPrefix && data->pszPrefix[0] != '\0')
        finalKey = std::string(data->pszPrefix) + "#" + attr_name;
    else
        finalKey = attr_name;

    *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), finalKey.c_str(),
                                          value_str.c_str());

    // Cleanup HDF5 handles
    H5Tclose(native_type);
    H5Tclose(attr_type);
    H5Sclose(attr_space);
    H5Aclose(attr_id);
    return 0;  // Success
}

// Helper to read a string attribute from an HDF5 object
static std::string ReadH5StringAttribute(hid_t hObjectID,
                                         const char *pszAttrName)
{
    // Suppress errors for this check
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    if (H5Aexists(hObjectID, pszAttrName) <= 0)
    {
        H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
        return "";
    }

    // Use H5Aopen (modern API)
    hid_t hAttr = H5Aopen(hObjectID, pszAttrName, H5P_DEFAULT);
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore errors

    if (hAttr < 0)
        return "";

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

                char *pszVal = static_cast<char *>(CPLMalloc(nSize + 1));

                // Read using hMemType (Memory), not hAttrType (File)
                if (H5Aread(hAttr, hMemType, pszVal) >= 0)
                {
                    pszVal[nSize] = '\0';  // Safety null termination
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
    std::vector<std::string> *pFoundPaths = nullptr;  // list in Open()
    hid_t hStartingGroupID = -1;  // group/file ID for opening datasets
    std::string sBasePath{};      // path prefix for GDAL subdatasets
};

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

    std::string full_path = data->sBasePath + name;

    // Log entry for every object visited
    CPLDebug("NISAR_VISITOR_DETAIL", "Visiting object: Path='%s', Type=%d",
             name, static_cast<int>(oinfo->type));

    // Filter 1: Only consider actual Datasets
    if (oinfo->type != H5O_TYPE_DATASET)
    {
        CPLDebug("NISAR_VISITOR_DETAIL", "--> Skipping '%s' (Not a dataset)",
                 name);
        return H5_ITER_CONT;  // Skip groups, named types, etc.
    }

    // Filter 2: Check Rank (>= 2 dimensions usually desired for rasters)
    // Try to get dataspace directly from the object ID provided by H5Ovisit
    hid_t dspace_id = -1;
    int rank = -1;

    // NOTE: H5Dget_space requires a *dataset* ID, not a generic object ID.
    // We still need to open it as a dataset first. Let's revert to H5Dopen2
    // but ensure the starting group ID passed in op_data is correct.
    // The error might be elsewhere. Let's try H5Dopen2 again.

    hid_t dset_id = -1;

    // Use the hStartingGroupID from the visitor data, which should be the file handle
    dset_id = H5Dopen2(data->hStartingGroupID, name, H5P_DEFAULT);
    if (dset_id < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "Could not open dataset '%s' using H5Dopen2 during subdataset "
                 "discovery.",
                 name);
        return H5_ITER_CONT;  // Skip if cannot open
    }

    // Check if it's actually a dataset type after opening
    if (H5Iget_type(dset_id) != H5I_DATASET)
    {
        CPLDebug("NISAR_VISITOR_DETAIL",
                 "--> Skipping '%s' (Opened object is not H5I_DATASET)", name);
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

// Parses "seconds since YYYY-MM-DDTHH:MM:SS[.fff]" into a Unix epoch.
static bool ParseSecondsSinceEpoch(const std::string &sUnits, double &dfEpoch)
{
    int nYear, nMonth, nDay, nHour, nMin;
    double dfSec;
    if (sscanf(sUnits.c_str(), "seconds since %d-%d-%dT%d:%d:%lf", &nYear,
               &nMonth, &nDay, &nHour, &nMin, &dfSec) != 6)
        return false;
    struct tm epoch_tm;
    memset(&epoch_tm, 0, sizeof(epoch_tm));
    epoch_tm.tm_year = nYear - 1900;
    epoch_tm.tm_mon = nMonth - 1;
    epoch_tm.tm_mday = nDay;
    epoch_tm.tm_hour = nHour;
    epoch_tm.tm_min = nMin;
    dfEpoch = static_cast<double>(CPLYMDHMSToUnixTime(&epoch_tm)) + dfSec;
    return true;
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

#ifdef H5T_COMPLEX
    //  HDF5 2.0 NATIVE COMPLEX SUPPORT
    if (eHDF5Class == H5T_COMPLEX)
    {
        CPLDebug("NISAR_GetGDALDataType",
                 "Checking HDF5 2.0 Native Complex type for GDAL mapping.");

        hid_t hBaseType = H5Tget_super(hH5Type);
        GDALDataType eComplexType = GDT_Unknown;

        if (hBaseType >= 0)
        {
            if (H5Tequal(hBaseType, H5T_NATIVE_FLOAT) > 0)
                eComplexType = GDT_CFloat32;
            else if (H5Tequal(hBaseType, H5T_NATIVE_DOUBLE) > 0)
                eComplexType = GDT_CFloat64;
            else if (H5Tequal(hBaseType, H5T_NATIVE_SHORT) > 0)
                eComplexType = GDT_CInt16;
            else if (H5Tequal(hBaseType, H5T_NATIVE_INT) > 0)
                eComplexType = GDT_CInt32;

            H5Tclose(hBaseType);
        }

        if (eComplexType != GDT_Unknown)
        {
            return eComplexType;
        }
        CPLDebug("NISAR_GetGDALDataType",
                 "Native complex type has unhandled base type.");
    }
#endif

    // Check if it's a Compound Type (Legacy Complex)
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
            {  // Handle error getting member types
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
             static_cast<int>(eHDF5Class));
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
                     pszAttrName, static_cast<long long>(hObjectID));
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
                 pszAttrName, static_cast<long long>(nPoints));
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
/*                            NisarDataset()                            */
/************************************************************************/

NisarDataset::NisarDataset()
{
}

/************************************************************************/
/*                           ~NisarDataset()                            */
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
    CSLDestroy(m_papszDumpMetadata);
    m_papszDumpMetadata = nullptr;
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
                    char **pszVals = static_cast<char **>(
                        VSI_MALLOC2_VERBOSE(nTotalSize, sizeof(char *)));
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
                    char *pszBuffer = static_cast<char *>(
                        VSI_MALLOC2_VERBOSE(nTotalSize, nFixedSize));
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
                    char **pszValsHead = static_cast<char **>(
                        VSI_MALLOC2_VERBOSE(nHeadTail, sizeof(char *)));
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
                    char *pszBufferHead = static_cast<char *>(
                        VSI_MALLOC2_VERBOSE(nHeadTail, nFixedSize));
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
                    char **pszValsTail = static_cast<char **>(
                        VSI_MALLOC2_VERBOSE(nHeadTail, sizeof(char *)));
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
                    char *pszBufferTail = static_cast<char *>(
                        VSI_MALLOC2_VERBOSE(nHeadTail, nFixedSize));
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
/*                       GetMetadataDomainList()                        */
/************************************************************************/

char **NisarDataset::GetMetadataDomainList()
{
    CPLDebug("NISAR_DRIVER", "GetMetadataDomainList called");
    char **papszDomains = GDALPamDataset::GetMetadataDomainList();
    // The base class may already list some of these (e.g. SUBDATASETS when
    // set through SetMetadata()), so only add what is missing.
    const auto AddDomain = [&papszDomains](const char *pszDomain)
    {
        if (CSLFindString(papszDomains, pszDomain) < 0)
            papszDomains = CSLAddString(papszDomains, pszDomain);
    };
    AddDomain("NISAR_GLOBAL");
    if (papszSubDatasets != nullptr)
        AddDomain("SUBDATASETS");
    if (m_bDumpEnabled)
        AddDomain("NISAR_DUMP");

    // Add DERIVED_SUBDATASETS if this is a raster dataset (not container)
    // and the type is numeric.
    // hDataset is > 0 for a specific raster, eDataType is set in Open()
    if (hDataset >= 0 && (eDataType > GDT_Unknown && eDataType < GDT_CInt16))
    {
        AddDomain("DERIVED_SUBDATASETS");
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
                        char *pszVal =
                            static_cast<char *>(CPLMalloc(nSize + 1));
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

NisarMetadataList NisarDataset::GetMetadata(const char *pszDomain)

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
            return m_papszGlobalMetadata;
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
        return m_papszGlobalMetadata;
    }

    // Handle SUBDATASETS Domain
    if (pszDomain != nullptr && EQUAL(pszDomain, "SUBDATASETS"))
    {
        return papszSubDatasets;
    }

    // Handle NISAR_DUMP Domain (h5dump-style listing, lazily built)
    if (pszDomain != nullptr && EQUAL(pszDomain, "NISAR_DUMP"))
    {
        if (hHDF5 < 0)
            return nullptr;
        std::lock_guard<std::mutex> lock(m_DumpMetadataMutex);
        if (!m_bGotDumpMetadata)
        {
            m_bGotDumpMetadata = LoadDumpMetadata();
        }
        return m_papszDumpMetadata;
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
                CPLError(
                    CE_Warning, CPLE_AppDefined,
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
        // Case 2: L2/L3 Product (no GCPs, has a raster dataset open)
        else if (this->hDataset >= 0)
        {
            // Hoist 3D Dataset Attributes to GDAL Dataset
            NISAR_AttrCallbackData dset_attr_cb_data;
            dset_attr_cb_data.ppapszList = &papszHDFMetadata;
            dset_attr_cb_data.pszPrefix =
                "";  // No prefix, attach directly to root
            hsize_t attr_idx = 0;

            // Read attributes like 'description', 'units', '_FillValue' from the dataset
            H5Aiterate2(this->hDataset, H5_INDEX_NAME, H5_ITER_NATIVE,
                        &attr_idx, NISAR_AttributeCallback, &dset_attr_cb_data);

            std::string datasetPath = get_hdf5_object_name(this->hDataset);

            // Determine the parent group path by anchoring to the frequency group
            // rather than the immediate parent directory, as GUNW products have deeper nesting
            // (e.g., /science/LSAR/GUNW/grids/frequencyA/pixelOffsets/HH/...)
            std::string gridsBasePath;
            size_t freqPosA = datasetPath.find("/frequencyA/");
            size_t freqPosB = datasetPath.find("/frequencyB/");

            if (freqPosA != std::string::npos)
            {
                // +12 captures "/frequencyA/" perfectly including the trailing slash
                gridsBasePath = datasetPath.substr(0, freqPosA + 12);
            }
            else if (freqPosB != std::string::npos)
            {
                gridsBasePath = datasetPath.substr(0, freqPosB + 12);
            }
            else
            {
                // Fallback for paths that don't contain frequency groups
                size_t lastSlashPos = datasetPath.find_last_of('/');
                if (lastSlashPos != std::string::npos)
                {
                    gridsBasePath = datasetPath.substr(0, lastSlashPos + 1);
                }
                else
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "Could not determine parent group path for L2/L3 "
                             "metadata from %s.",
                             datasetPath.c_str());
                    gridsBasePath = "";
                }
            }

            if (!gridsBasePath.empty())
            {
                CPLDebug("NISAR_DRIVER",
                         "L2/L3 dataset detected. Reading extended metadata "
                         "relative to: %s",
                         gridsBasePath.c_str());

                // Read Scalar Datasets
                const std::vector<std::string> scalar_datasets_to_read = {
                    //"xCoordinateSpacing",
                    //"yCoordinateSpacing",
                    "numberOfSubSwaths"  // May not exist for all L2/L3
                    // Add others if needed
                };

                // Read 1D Array Datasets
                const std::vector<std::string> array_datasets_to_summarize = {
                    "listOfCovarianceTerms",
                    "listOfPolarizations"  // May not exist for all L2/L3
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

            // Suppress errors for these existence checks
            H5E_auto2_t old_func;
            void *old_client_data;
            H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

            if (H5Lexists(this->hHDF5, "/science/LSAR/identification",
                          H5P_DEFAULT) > 0)
            {
                sIdentPath = "/science/LSAR/identification";
            }
            else if (H5Lexists(this->hHDF5, "/science/SSAR/identification",
                               H5P_DEFAULT) > 0)
            {
                sIdentPath = "/science/SSAR/identification";
            }

            // Restore errors
            H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);

            if (!sIdentPath.empty())
            {
                CPLDebug("NISAR_DRIVER",
                         "Reading default metadata from root group ('%s') for "
                         "container dataset.",
                         sIdentPath.c_str());
                hid_t hIdentGroup =
                    H5Gopen2(this->hHDF5, sIdentPath.c_str(), H5P_DEFAULT);
                if (hIdentGroup >= 0)
                {
                    callback_data.pszPrefix = sIdentPath.c_str();
                    idx = 0;  //Reset the iterator index
                    H5Literate(hIdentGroup, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                               NISAR_DatasetMetadataCallback, &callback_data);

                    // Clean up the handle
                    H5Gclose(hIdentGroup);
                }
            }
            else
            {
                CPLDebug(
                    "NISAR_DRIVER",
                    "No identification group found for container dataset.");
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
    NisarDataset *poDS = nullptr;
    char **papszMetadata = nullptr;  // CSL list being built
    std::string sPrefix{};           // Optional prefix if needed
};

// Callback for H5Ovisit to recursively read metadata
herr_t NisarDataset::MetadataVisitCallback(hid_t hGroup, const char *name,
                                           const H5O_info2_t *info,
                                           void *op_data)
{
    NISAR_MetadataVisitData *data =
        static_cast<NISAR_MetadataVisitData *>(op_data);

    if (EQUAL(name, "."))
        return 0;
    if (info->type != H5O_TYPE_DATASET)
        return 0;

    // Identify Key Name early to skip expensive reads
    std::string sKey = name;
    // Normalize path separators to underscores for the key
    std::replace(sKey.begin(), sKey.end(), '/', '_');

    // FILTER: Skip massive configuration dumps
    if (sKey.find("runConfigurationContents") != std::string::npos)
    {
        return 0;  // Skip entirely
    }

    hid_t hDset = H5Dopen2(hGroup, name, H5P_DEFAULT);
    if (hDset < 0)
        return 0;

    hid_t hSpace = H5Dget_space(hDset);
    int nRank = H5Sget_simple_extent_ndims(hSpace);
    hid_t hType = H5Dget_type(hDset);
    H5T_class_t type_class = H5Tget_class(hType);

    bool bShouldRead = false;

    // Logic: Read Scalars and 1-element Strings
    if (nRank == 0)
    {
        bShouldRead = true;
    }

    else if (nRank == 1)
    {
        hsize_t dims;
        H5Sget_simple_extent_dims(hSpace, &dims, nullptr);

        if (type_class == H5T_STRING)
        {
            bShouldRead = true;
        }
        else if (dims == 1)
        {
            bShouldRead = true;
        }
    }

    if (bShouldRead)
    {
        std::string sValue;

        if (type_class == H5T_STRING && nRank == 1)
        {
            sValue = data->poDS->ReadHDF5StringArrayAsList(hGroup, name);
        }
        else
        {
            sValue = data->poDS->ReadHDF5StringDataset(hGroup, name);
        }

        if (sValue.empty() && type_class != H5T_STRING)
        {
            double dfVal;
            if (H5Dread(hDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                        &dfVal) >= 0)
            {
                sValue = CPLSPrintf("%.15g", dfVal);
            }
        }

        if (!sValue.empty())
        {
            // SANITIZE: Remove Newlines
            if (sValue.find('\n') != std::string::npos ||
                sValue.find('\r') != std::string::npos)
            {
                std::replace(sValue.begin(), sValue.end(), '\n', ' ');
                std::replace(sValue.begin(), sValue.end(), '\r', ' ');
            }

            // START UNIT READING BLOCK
            std::string sUnits = ReadH5StringAttribute(hDset, "units");
            if (!sUnits.empty() && !EQUAL(sUnits.c_str(), "unitless") &&
                !EQUAL(sUnits.c_str(), "1"))
            {
                sKey += " (" + sUnits + ")";
            }
            // END UNIT READING BLOCK

            data->papszMetadata = CSLSetNameValue(data->papszMetadata,
                                                  sKey.c_str(), sValue.c_str());
        }
    }

    if (hType >= 0)
        H5Tclose(hType);
    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hDset >= 0)
        H5Dclose(hDset);

    return 0;
}

void NisarDataset::InitializeMetadataMap()
{
    // Only map if product type is known
    if (!m_sInst.empty() && !m_sProductType.empty())
    {
        m_oMetadataMap["ATTITUDE"] = {"/science/" + m_sInst + "/" +
                                          m_sProductType + "/metadata/attitude",
                                      "NISAR_ATTITUDE"};
        m_oMetadataMap["CALIBRATIONINFORMATION"] = {
            "/science/" + m_sInst + "/" + m_sProductType +
                "/metadata/calibrationInformation",
            "NISAR_CALIBRATIONINFORMATION"};
        m_oMetadataMap["CEOSANALYSISREADYDATA"] = {
            "/science/" + m_sInst + "/" + m_sProductType +
                "/metadata/ceosAnalysisReadyData",
            "NISAR_CEOSANALYSISREADYDATA"};
        m_oMetadataMap["ORBIT"] = {"/science/" + m_sInst + "/" +
                                       m_sProductType + "/metadata/orbit",
                                   "NISAR_ORBIT"};
        m_oMetadataMap["PROCESSINGINFORMATION"] = {
            "/science/" + m_sInst + "/" + m_sProductType +
                "/metadata/processingInformation",
            "NISAR_PROCESSINGINFORMATION"};
        m_oMetadataMap["RADARGRID"] = {"/science/" + m_sInst + "/" +
                                           m_sProductType +
                                           "/metadata/radarGrid",
                                       "NISAR_RADARGRID"};
        m_oMetadataMap["SOURCEDATA"] = {"/science/" + m_sInst + "/" +
                                            m_sProductType +
                                            "/metadata/sourceData",
                                        "NISAR_SOURCEDATA"};
    }
}

void NisarDataset::LoadMetadataDomain(const std::string &sKeyword)
{
    auto it = m_oMetadataMap.find(sKeyword);
    if (it == m_oMetadataMap.end())
    {
        CPLDebug("NISAR_DRIVER",
                 "Requested metadata keyword '%s' not recognized.",
                 sKeyword.c_str());
        return;
    }

    const std::string &sH5Path = it->second.sHDF5Path;
    const std::string &sDomain = it->second.sGDALDomain;

    // Suppress errors temporarily in case the group doesn't exist
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hid_t hGroup = H5Gopen2(hHDF5, sH5Path.c_str(), H5P_DEFAULT);

    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);  // Restore

    if (hGroup < 0)
    {
        CPLDebug("NISAR_DRIVER", "Metadata group not found: %s",
                 sH5Path.c_str());
        return;
    }

    CPLDebug("NISAR_DRIVER", "Loading metadata domain: %s from %s",
             sDomain.c_str(), sH5Path.c_str());

    NISAR_MetadataVisitData visitData;
    visitData.poDS = this;
    visitData.papszMetadata = nullptr;

    // Recursively visit all objects in this group
    H5Ovisit(hGroup, H5_INDEX_NAME, H5_ITER_NATIVE, MetadataVisitCallback,
             &visitData, H5O_INFO_BASIC);

    // Apply the gathered list to the dataset
    if (visitData.papszMetadata)
    {
        SetMetadata(visitData.papszMetadata, sDomain.c_str());
        CSLDestroy(visitData.papszMetadata);
    }

    H5Gclose(hGroup);
}

// ====================================================================
// NISAR_DUMP (h5dump-style listing, DUMP=YES)
// ====================================================================

struct NISAR_DumpVisitData
{
    NisarDataset *poDS = nullptr;
    std::string sRoot{};          // absolute path of the traversal root
    char **papszLines = nullptr;  // CSL list of output lines
    size_t nMaxElements = 0;      // element cap for printed DATA blocks
    size_t nMaxStringChars = 0;   // per-string cap for printed values
    std::vector<std::string> aosOpenGroups{};  // groups whose '}' is pending
    std::map<std::string, std::string> oVisitedObjects{};  // token -> path
};

static std::string NisarDumpObjectKind(H5O_type_t eType)
{
    switch (eType)
    {
        case H5O_TYPE_GROUP:
            return "GROUP";
        case H5O_TYPE_DATASET:
            return "DATASET";
        case H5O_TYPE_NAMED_DATATYPE:
            return "DATATYPE";
        default:
            return "OBJECT";
    }
}

static bool NisarDumpIsUnder(const std::string &sParent,
                             const std::string &sPath)
{
    if (sPath.size() <= sParent.size() ||
        sPath.compare(0, sParent.size(), sParent) != 0)
        return false;
    return sParent == "/" || sPath[sParent.size()] == '/';
}

// Emits the closing brace of every open group that does not contain sPath
// (H5Lvisit is pre-order, so leaving a subtree means moving to a sibling or
// ancestor). An empty sPath closes everything.
static void NisarDumpCloseGroups(NISAR_DumpVisitData *data,
                                 const std::string &sPath)
{
    while (
        !data->aosOpenGroups.empty() &&
        (sPath.empty() || !NisarDumpIsUnder(data->aosOpenGroups.back(), sPath)))
    {
        data->papszLines = CSLAddString(data->papszLines, "}");
        data->aosOpenGroups.pop_back();
    }
}

// Byte length of the UTF-8 sequence introduced by lead byte c (1 for ASCII
// and for bytes that cannot start a sequence, which are passed through).
static size_t NisarDumpUtf8SeqLen(unsigned char c)
{
    if (c >= 0xF0 && c <= 0xF7)
        return 4;
    if (c >= 0xE0)
        return 3;
    if (c >= 0xC0)
        return 2;
    return 1;
}

static size_t NisarDumpUtf8Length(const std::string &s)
{
    size_t nChars = 0;
    for (size_t i = 0; i < s.size();
         i += NisarDumpUtf8SeqLen(static_cast<unsigned char>(s[i])))
        ++nChars;
    return nChars;
}

// Quotes at most nMax characters of sIn, cutting only on UTF-8 code-point
// boundaries. nStorageSize > 0 means sIn is a bounded read of a fixed-length
// string of that declared size (so it may end in a split code point), and the
// truncation marker reports the STRSIZE instead of the (unknown) length.
static std::string NisarDumpQuote(const std::string &sIn, size_t nMax,
                                  size_t nStorageSize = 0)
{
    std::string sOut = "\"";
    size_t nUsed = 0;
    for (size_t i = 0; i < sIn.size();)
    {
        const size_t nLen =
            NisarDumpUtf8SeqLen(static_cast<unsigned char>(sIn[i]));
        if (nUsed++ >= nMax || i + nLen > sIn.size())
        {
            if (nStorageSize > 0)
                sOut +=
                    CPLSPrintf("...\" (STRSIZE %llu)",
                               static_cast<unsigned long long>(nStorageSize));
            else
                sOut += CPLSPrintf(
                    "...\" (%llu chars)",
                    static_cast<unsigned long long>(NisarDumpUtf8Length(sIn)));
            return sOut;
        }
        if (nLen > 1)
        {
            sOut.append(sIn, i, nLen);
            i += nLen;
            continue;
        }
        const char c = sIn[i++];
        switch (c)
        {
            case '\n':
                sOut += "\\n";
                break;
            case '\r':
                sOut += "\\r";
                break;
            case '\t':
                sOut += "\\t";
                break;
            case '"':
                sOut += "\\\"";
                break;
            case '\\':
                sOut += "\\\\";
                break;
            default:
                sOut += c;
                break;
        }
    }
    return sOut + "\"";
}

// h5dump-style datatype description, e.g. H5T_IEEE_F32LE, H5T_STD_U8LE,
// H5T_STRING { STRSIZE 4; ... }, H5T_COMPOUND { ... }.
static std::string NisarDumpTypeString(hid_t hType)
{
    const H5T_class_t eClass = H5Tget_class(hType);
    const size_t nSize = H5Tget_size(hType);
    const H5T_order_t eOrder = H5Tget_order(hType);
    const char *pszOrder = (eOrder == H5T_ORDER_BE) ? "BE" : "LE";

    switch (eClass)
    {
        case H5T_INTEGER:
            return CPLSPrintf("H5T_STD_%c%u%s",
                              (H5Tget_sign(hType) == H5T_SGN_NONE) ? 'U' : 'I',
                              static_cast<unsigned>(nSize * 8), pszOrder);
        case H5T_FLOAT:
            return CPLSPrintf("H5T_IEEE_F%u%s",
                              static_cast<unsigned>(nSize * 8), pszOrder);
        case H5T_STRING:
        {
            std::string s = "H5T_STRING { STRSIZE ";
            if (H5Tis_variable_str(hType) > 0)
                s += "H5T_VARIABLE";
            else
                s += CPLSPrintf("%llu", static_cast<unsigned long long>(nSize));
            s += "; STRPAD ";
            switch (H5Tget_strpad(hType))
            {
                case H5T_STR_NULLPAD:
                    s += "H5T_STR_NULLPAD";
                    break;
                case H5T_STR_SPACEPAD:
                    s += "H5T_STR_SPACEPAD";
                    break;
                default:
                    s += "H5T_STR_NULLTERM";
                    break;
            }
            s += "; CSET ";
            s += (H5Tget_cset(hType) == H5T_CSET_UTF8) ? "H5T_CSET_UTF8"
                                                       : "H5T_CSET_ASCII";
            s += "; CTYPE H5T_C_S1; }";
            return s;
        }
        case H5T_COMPOUND:
        {
            std::string s = "H5T_COMPOUND { ";
            const int nMembers = H5Tget_nmembers(hType);
            for (int i = 0; i < nMembers; ++i)
            {
                hid_t hMember = H5Tget_member_type(hType, i);
                char *pszName = H5Tget_member_name(hType, i);
                if (hMember >= 0)
                {
                    s += NisarDumpTypeString(hMember);
                    H5Tclose(hMember);
                }
                s += CPLSPrintf(" \"%s\"; ", pszName ? pszName : "?");
                if (pszName)
                    H5free_memory(pszName);
            }
            return s + "}";
        }
        case H5T_ENUM:
        {
            hid_t hSuper = H5Tget_super(hType);
            std::string s = "H5T_ENUM { ";
            if (hSuper >= 0)
            {
                s += NisarDumpTypeString(hSuper) + "; ";
                H5Tclose(hSuper);
            }
            return s + CPLSPrintf("%d members }", H5Tget_nmembers(hType));
        }
        case H5T_VLEN:
        {
            hid_t hSuper = H5Tget_super(hType);
            std::string s = "H5T_VLEN { ";
            if (hSuper >= 0)
            {
                s += NisarDumpTypeString(hSuper);
                H5Tclose(hSuper);
            }
            return s + " }";
        }
        case H5T_ARRAY:
        {
            hid_t hSuper = H5Tget_super(hType);
            std::string s = "H5T_ARRAY { ";
            const int nRank = H5Tget_array_ndims(hType);
            if (nRank > 0 && nRank <= H5S_MAX_RANK)
            {
                hsize_t adims[H5S_MAX_RANK];
                H5Tget_array_dims2(hType, adims);
                s += "[ ";
                for (int i = 0; i < nRank; ++i)
                    s += CPLSPrintf("%llu%s",
                                    static_cast<unsigned long long>(adims[i]),
                                    (i < nRank - 1) ? ", " : " ");
                s += "] ";
            }
            if (hSuper >= 0)
            {
                s += NisarDumpTypeString(hSuper);
                H5Tclose(hSuper);
            }
            return s + " }";
        }
        case H5T_REFERENCE:
            return "H5T_REFERENCE";
        case H5T_OPAQUE:
            return CPLSPrintf("H5T_OPAQUE { %llu bytes }",
                              static_cast<unsigned long long>(nSize));
        case H5T_BITFIELD:
            return CPLSPrintf("H5T_STD_B%u%s", static_cast<unsigned>(nSize * 8),
                              pszOrder);
#ifdef H5T_COMPLEX
        case H5T_COMPLEX:
        {
            hid_t hSuper = H5Tget_super(hType);
            std::string s = "H5T_COMPLEX { ";
            if (hSuper >= 0)
            {
                s += NisarDumpTypeString(hSuper);
                H5Tclose(hSuper);
            }
            return s + " }";
        }
#endif
        default:
            return CPLSPrintf("H5T_CLASS_%d", static_cast<int>(eClass));
    }
}

static std::string NisarDumpSpaceString(hid_t hSpace)
{
    const H5S_class_t eClass = H5Sget_simple_extent_type(hSpace);
    if (eClass == H5S_SCALAR)
        return "SCALAR";
    if (eClass == H5S_NULL)
        return "NULL";
    const int nRank = H5Sget_simple_extent_ndims(hSpace);
    if (nRank <= 0 || nRank > H5S_MAX_RANK)
        return "SIMPLE { ? }";
    hsize_t adims[H5S_MAX_RANK], amax[H5S_MAX_RANK];
    H5Sget_simple_extent_dims(hSpace, adims, amax);
    std::string sCur = "( ", sMax = "( ";
    for (int i = 0; i < nRank; ++i)
    {
        const char *pszSep = (i < nRank - 1) ? ", " : " ";
        sCur += CPLSPrintf("%llu%s", static_cast<unsigned long long>(adims[i]),
                           pszSep);
        if (amax[i] == H5S_UNLIMITED)
            sMax += CPLSPrintf("H5S_UNLIMITED%s", pszSep);
        else
            sMax += CPLSPrintf(
                "%llu%s", static_cast<unsigned long long>(amax[i]), pszSep);
    }
    return "SIMPLE { " + sCur + ") / " + sMax + ") }";
}

// Reads every element of an attribute (bIsAttr) or dataset and renders them
// into sOut as a comma-separated list. Only INTEGER, FLOAT, STRING and
// two-member complex COMPOUND / native COMPLEX types are printed; anything
// else leaves sOut empty. Returns false only when an HDF5 read failed.
static bool NisarDumpReadValues(hid_t hObj, bool bIsAttr, hid_t hType,
                                hid_t hSpace, size_t nCount,
                                size_t nMaxStringChars, std::string &sOut)
{
    const H5T_class_t eClass = H5Tget_class(hType);
    sOut.clear();
    bool bReadFailed = false;

    // A missing file->memory conversion path means the type is unsupported,
    // not that the read failed; only an actual H5Aread/H5Dread error is fatal.
    auto Read = [&](hid_t hMemType, void *pBuf) -> bool
    {
        H5T_cdata_t *psCData = nullptr;
        if (H5Tfind(hType, hMemType, &psCData) == nullptr)
            return false;
        const herr_t eErr = bIsAttr ? H5Aread(hObj, hMemType, pBuf)
                                    : H5Dread(hObj, hMemType, H5S_ALL, H5S_ALL,
                                              H5P_DEFAULT, pBuf);
        if (eErr < 0)
            bReadFailed = true;
        return eErr >= 0;
    };
    auto Join = [&](size_t i)
    {
        if (i > 0)
            sOut += ", ";
    };

    if (eClass == H5T_STRING)
    {
        if (H5Tis_variable_str(hType) > 0)
        {
            std::vector<char *> apsz(nCount, nullptr);
            hid_t hMem = H5Tcopy(H5T_C_S1);
            H5Tset_size(hMem, H5T_VARIABLE);
            H5Tset_cset(hMem, H5Tget_cset(hType));
            // Variable-length strings are materialized whole by H5Dread, so
            // ask HDF5 for the total payload first (this reads the sequence
            // lengths, not the heap data) and refuse to read past the byte
            // budget: up to 4 UTF-8 bytes per printable character plus a
            // terminator per element (NisarDumpQuote enforces the character
            // cap itself). H5Dvlen_get_buf_size crashes on scalar dataspaces
            // (HDF5 2.2), so a scalar VL string is read whole like an attribute.
            hsize_t nVlenBytes = 0;
            constexpr hsize_t nMaxH = std::numeric_limits<hsize_t>::max();
            const hsize_t nBytesPerElement =
                nMaxStringChars >= (nMaxH - 1) / 4
                    ? nMaxH
                    : static_cast<hsize_t>(nMaxStringChars) * 4 + 1;
            const hsize_t nVlenCap =
                nCount > nMaxH / nBytesPerElement
                    ? nMaxH
                    : static_cast<hsize_t>(nCount) * nBytesPerElement;
            if (!bIsAttr && H5Sget_simple_extent_type(hSpace) != H5S_SCALAR &&
                H5Dvlen_get_buf_size(hObj, hMem, hSpace, &nVlenBytes) >= 0 &&
                nVlenBytes > nVlenCap)
            {
                sOut = CPLSPrintf(
                    "(variable-length string, %llu bytes, not printed)",
                    static_cast<unsigned long long>(nVlenBytes));
            }
            else if (Read(hMem, apsz.data()))
            {
                for (size_t i = 0; i < nCount; ++i)
                {
                    Join(i);
                    sOut +=
                        NisarDumpQuote(apsz[i] ? apsz[i] : "", nMaxStringChars);
                }
                H5Treclaim(hMem, hSpace, H5P_DEFAULT, apsz.data());
            }
            H5Tclose(hMem);
        }
        else
        {
            // Read through a memory type no larger than the print cap so
            // huge fixed-length strings never get allocated in full; HDF5
            // truncates on the fixed->fixed string conversion. UTF-8 code
            // points take up to 4 bytes, so size the read accordingly.
            const size_t nSize = H5Tget_size(hType);
            const size_t nBytesPerChar =
                H5Tget_cset(hType) == H5T_CSET_UTF8 ? 4 : 1;
            const size_t nMemSize =
                std::min(nSize, nMaxStringChars * nBytesPerChar + 1);
            hid_t hMem = H5Tcopy(hType);
            std::vector<char> abyBuf(nCount * nMemSize + 1, '\0');
            if (nMemSize > 0 && hMem >= 0 && H5Tset_size(hMem, nMemSize) >= 0 &&
                Read(hMem, abyBuf.data()))
            {
                for (size_t i = 0; i < nCount; ++i)
                {
                    Join(i);
                    std::string s(
                        abyBuf.data() + i * nMemSize,
                        strnlen(abyBuf.data() + i * nMemSize, nMemSize));
                    sOut += NisarDumpQuote(s, nMaxStringChars,
                                           nMemSize < nSize ? nSize : 0);
                }
            }
            if (hMem >= 0)
                H5Tclose(hMem);
        }
    }
    else if (eClass == H5T_INTEGER)
    {
        if (H5Tget_sign(hType) == H5T_SGN_NONE)
        {
            std::vector<unsigned long long> a(nCount);
            if (Read(H5T_NATIVE_ULLONG, a.data()))
                for (size_t i = 0; i < nCount; ++i)
                {
                    Join(i);
                    sOut += CPLSPrintf("%llu", a[i]);
                }
        }
        else
        {
            std::vector<long long> a(nCount);
            if (Read(H5T_NATIVE_LLONG, a.data()))
                for (size_t i = 0; i < nCount; ++i)
                {
                    Join(i);
                    sOut += CPLSPrintf("%lld", a[i]);
                }
        }
    }
    else if (eClass == H5T_FLOAT)
    {
        std::vector<double> a(nCount);
        if (Read(H5T_NATIVE_DOUBLE, a.data()))
            for (size_t i = 0; i < nCount; ++i)
            {
                Join(i);
                sOut += CPLSPrintf("%.17g", a[i]);
            }
    }
    else
    {
        CPLPushErrorHandler(CPLQuietErrorHandler);
        const GDALDataType eGDT = NisarDataset::GetGDALDataType(hType);
        CPLPopErrorHandler();
        if (eGDT == GDT_CFloat32 || eGDT == GDT_CFloat64 ||
            eGDT == GDT_CInt16 || eGDT == GDT_CInt32)
        {
            std::vector<double> a(nCount * 2);
            hid_t hMem = H5Tcreate(H5T_COMPOUND, 2 * sizeof(double));
            if (eClass == H5T_COMPOUND && H5Tget_nmembers(hType) == 2)
            {
                for (unsigned i = 0; i < 2; ++i)
                {
                    char *pszMember = H5Tget_member_name(hType, i);
                    H5Tinsert(hMem,
                              pszMember ? pszMember : (i == 0 ? "r" : "i"),
                              i * sizeof(double), H5T_NATIVE_DOUBLE);
                    if (pszMember)
                        H5free_memory(pszMember);
                }
            }
            else
            {
                H5Tinsert(hMem, "r", 0, H5T_NATIVE_DOUBLE);
                H5Tinsert(hMem, "i", sizeof(double), H5T_NATIVE_DOUBLE);
            }
#ifdef H5T_COMPLEX
            const bool bOk = (eClass == H5T_COMPLEX)
                                 ? Read(H5T_NATIVE_DOUBLE_COMPLEX, a.data())
                                 : Read(hMem, a.data());
#else
            const bool bOk = Read(hMem, a.data());
#endif
            H5Tclose(hMem);
            if (bOk)
                for (size_t i = 0; i < nCount; ++i)
                {
                    Join(i);
                    sOut += CPLSPrintf("%.17g%+.17gj", a[2 * i], a[2 * i + 1]);
                }
        }
    }
    return !bReadFailed;
}

static herr_t NisarDumpAttributeCallback(hid_t hLocation,
                                         const char *pszAttrName,
                                         const H5A_info_t * /*pAinfo*/,
                                         void *pOpData)
{
    NISAR_DumpVisitData *data = static_cast<NISAR_DumpVisitData *>(pOpData);

    hid_t hAttr = H5Aopen(hLocation, pszAttrName, H5P_DEFAULT);
    if (hAttr < 0)
    {
        CPLDebug("NISAR_DRIVER",
                 "NISAR_DUMP: H5Aopen('%s') failed; aborting traversal.",
                 pszAttrName);
        return H5_ITER_ERROR;
    }
    hid_t hType = H5Aget_type(hAttr);
    hid_t hSpace = H5Aget_space(hAttr);
    bool bReadOk = true;

    data->papszLines = CSLAddString(
        data->papszLines, CPLSPrintf("   ATTRIBUTE \"%s\" {", pszAttrName));
    if (hType >= 0)
        data->papszLines = CSLAddString(
            data->papszLines,
            ("      DATATYPE  " + NisarDumpTypeString(hType)).c_str());
    if (hSpace >= 0)
    {
        data->papszLines = CSLAddString(
            data->papszLines,
            ("      DATASPACE  " + NisarDumpSpaceString(hSpace)).c_str());
        const hssize_t nPoints = H5Sget_simple_extent_npoints(hSpace);
        if (hType >= 0 && nPoints > 0)
        {
            if (static_cast<size_t>(nPoints) > data->nMaxElements)
            {
                data->papszLines = CSLAddString(
                    data->papszLines,
                    CPLSPrintf("      DATA { (%lld elements, not printed) }",
                               static_cast<long long>(nPoints)));
            }
            else
            {
                std::string sValues;
                bReadOk = NisarDumpReadValues(hAttr, true, hType, hSpace,
                                              static_cast<size_t>(nPoints),
                                              data->nMaxStringChars, sValues);
                if (!sValues.empty())
                    data->papszLines = CSLAddString(
                        data->papszLines,
                        ("      DATA { " + sValues + " }").c_str());
            }
        }
    }
    data->papszLines = CSLAddString(data->papszLines, "   }");

    if (hSpace >= 0)
        H5Sclose(hSpace);
    if (hType >= 0)
        H5Tclose(hType);
    H5Aclose(hAttr);
    if (!bReadOk)
    {
        CPLDebug("NISAR_DRIVER",
                 "NISAR_DUMP: H5Aread('%s') failed; aborting traversal.",
                 pszAttrName);
        return H5_ITER_ERROR;
    }
    return 0;
}

static std::string NisarDumpJoinPath(const std::string &sRoot, const char *name)
{
    std::string sPath = sRoot;
    if (!EQUAL(name, "."))
    {
        if (sPath.empty() || sPath.back() != '/')
            sPath += "/";
        sPath += name;
    }
    return sPath;
}

// H5Lvisit visits links, not objects: a soft or external link is reported
// as such (and not followed, as in h5dump), and a second hard link to an
// already dumped object is reported as a HARDLINK alias instead of being
// dumped again.
herr_t NisarDataset::DumpLinkCallback(hid_t hRoot, const char *name,
                                      const H5L_info2_t *info, void *op_data)
{
    NISAR_DumpVisitData *data = static_cast<NISAR_DumpVisitData *>(op_data);
    const std::string sPath = NisarDumpJoinPath(data->sRoot, name);

    if (info->type == H5L_TYPE_HARD)
    {
        H5O_info2_t oInfo;
        if (H5Oget_info_by_name3(hRoot, name, &oInfo, H5O_INFO_BASIC,
                                 H5P_DEFAULT) < 0)
        {
            CPLDebug(
                "NISAR_DRIVER",
                "NISAR_DUMP: H5Oget_info('%s') failed; aborting traversal.",
                sPath.c_str());
            return H5_ITER_ERROR;
        }
        char *pszToken = nullptr;
        if (H5Otoken_to_str(hRoot, &oInfo.token, &pszToken) < 0 ||
            pszToken == nullptr)
        {
            CPLDebug(
                "NISAR_DRIVER",
                "NISAR_DUMP: H5Otoken_to_str('%s') failed; aborting traversal.",
                sPath.c_str());
            return H5_ITER_ERROR;
        }
        const std::string sToken(pszToken);
        H5free_memory(pszToken);

        const auto oSeen = data->oVisitedObjects.find(sToken);
        if (oSeen == data->oVisitedObjects.end())
        {
            data->oVisitedObjects[sToken] = sPath;
            return DumpObject(hRoot, name, &oInfo, op_data);
        }
        NisarDumpCloseGroups(data, sPath);
        data->papszLines = CSLAddString(
            data->papszLines,
            CPLSPrintf("%s \"%s\" {", NisarDumpObjectKind(oInfo.type).c_str(),
                       sPath.c_str()));
        data->papszLines =
            CSLAddString(data->papszLines, CPLSPrintf("   HARDLINK \"%s\"",
                                                      oSeen->second.c_str()));
        data->papszLines = CSLAddString(data->papszLines, "}");
        return H5_ITER_CONT;
    }

    NisarDumpCloseGroups(data, sPath);
    if (info->type == H5L_TYPE_SOFT || info->type == H5L_TYPE_EXTERNAL)
    {
        std::vector<char> abyVal(info->u.val_size + 1, '\0');
        if (info->u.val_size > 0 && H5Lget_val(hRoot, name, abyVal.data(),
                                               abyVal.size(), H5P_DEFAULT) < 0)
        {
            CPLDebug("NISAR_DRIVER",
                     "NISAR_DUMP: H5Lget_val('%s') failed; aborting traversal.",
                     sPath.c_str());
            return H5_ITER_ERROR;
        }
        if (info->type == H5L_TYPE_SOFT)
        {
            data->papszLines =
                CSLAddString(data->papszLines,
                             CPLSPrintf("SOFTLINK \"%s\" {", sPath.c_str()));
            data->papszLines =
                CSLAddString(data->papszLines,
                             CPLSPrintf("   LINKTARGET \"%s\"", abyVal.data()));
        }
        else
        {
            const char *pszFile = "";
            const char *pszObj = "";
            unsigned nFlags = 0;
            if (info->u.val_size > 0 &&
                H5Lunpack_elink_val(abyVal.data(), info->u.val_size, &nFlags,
                                    &pszFile, &pszObj) < 0)
            {
                pszFile = "";
                pszObj = "";
            }
            data->papszLines = CSLAddString(
                data->papszLines,
                CPLSPrintf("EXTERNAL_LINK \"%s\" {", sPath.c_str()));
            data->papszLines = CSLAddString(
                data->papszLines, CPLSPrintf("   TARGETFILE \"%s\"", pszFile));
            data->papszLines = CSLAddString(
                data->papszLines, CPLSPrintf("   TARGETPATH \"%s\"", pszObj));
        }
    }
    else
    {
        data->papszLines = CSLAddString(
            data->papszLines,
            CPLSPrintf("USERDEFINED_LINK \"%s\" {", sPath.c_str()));
        data->papszLines = CSLAddString(
            data->papszLines,
            CPLSPrintf("   LINKCLASS %d", static_cast<int>(info->type)));
    }
    data->papszLines = CSLAddString(data->papszLines, "}");
    return H5_ITER_CONT;
}

herr_t NisarDataset::DumpObject(hid_t hRoot, const char *name,
                                const H5O_info2_t *info, void *op_data)
{
    NISAR_DumpVisitData *data = static_cast<NISAR_DumpVisitData *>(op_data);
    const std::string sPath = NisarDumpJoinPath(data->sRoot, name);

    hid_t hObj = H5Oopen(hRoot, name, H5P_DEFAULT);
    if (hObj < 0)
    {
        CPLDebug("NISAR_DRIVER",
                 "NISAR_DUMP: H5Oopen('%s') failed; aborting traversal.",
                 sPath.c_str());
        return H5_ITER_ERROR;
    }

    NisarDumpCloseGroups(data, sPath);
    const bool bIsGroup = (info->type == H5O_TYPE_GROUP);
    bool bReadOk = true;

    switch (info->type)
    {
        case H5O_TYPE_GROUP:
            data->papszLines = CSLAddString(
                data->papszLines, CPLSPrintf("GROUP \"%s\" {", sPath.c_str()));
            break;

        case H5O_TYPE_DATASET:
        {
            data->papszLines =
                CSLAddString(data->papszLines,
                             CPLSPrintf("DATASET \"%s\" {", sPath.c_str()));
            hid_t hType = H5Dget_type(hObj);
            hid_t hSpace = H5Dget_space(hObj);
            if (hType >= 0)
            {
                std::string sType =
                    "   DATATYPE  " + NisarDumpTypeString(hType);
                const H5T_class_t eCls = H5Tget_class(hType);
                const GDALDataType eGDT =
                    (eCls == H5T_INTEGER || eCls == H5T_FLOAT ||
                     eCls == H5T_COMPOUND
#ifdef H5T_COMPLEX
                     || eCls == H5T_COMPLEX
#endif
                     )
                        ? GetGDALDataType(hType)
                        : GDT_Unknown;
                if (eGDT != GDT_Unknown)
                    sType +=
                        CPLSPrintf("  (GDAL %s)", GDALGetDataTypeName(eGDT));
                data->papszLines =
                    CSLAddString(data->papszLines, sType.c_str());
            }
            if (hSpace >= 0)
            {
                data->papszLines = CSLAddString(
                    data->papszLines,
                    ("   DATASPACE  " + NisarDumpSpaceString(hSpace)).c_str());

                const int nRank = H5Sget_simple_extent_ndims(hSpace);
                const hssize_t nPoints = H5Sget_simple_extent_npoints(hSpace);
                if (data->poDS->m_bDumpFull && hType >= 0 && nRank <= 1 &&
                    nPoints > 0 &&
                    static_cast<size_t>(nPoints) <= data->nMaxElements)
                {
                    std::string sValues;
                    bReadOk =
                        NisarDumpReadValues(hObj, false, hType, hSpace,
                                            static_cast<size_t>(nPoints),
                                            data->nMaxStringChars, sValues);
                    if (!sValues.empty())
                        data->papszLines = CSLAddString(
                            data->papszLines,
                            ("   DATA { " + sValues + " }").c_str());
                }
                else if (data->poDS->m_bDumpFull && nPoints > 0)
                {
                    data->papszLines = CSLAddString(
                        data->papszLines,
                        CPLSPrintf("   DATA { (%lld elements, not printed) }",
                                   static_cast<long long>(nPoints)));
                }
            }
            if (hSpace >= 0)
                H5Sclose(hSpace);
            if (hType >= 0)
                H5Tclose(hType);
            break;
        }

        case H5O_TYPE_NAMED_DATATYPE:
            data->papszLines =
                CSLAddString(data->papszLines,
                             CPLSPrintf("DATATYPE \"%s\" { %s", sPath.c_str(),
                                        NisarDumpTypeString(hObj).c_str()));
            break;

        default:
            data->papszLines = CSLAddString(
                data->papszLines, CPLSPrintf("OBJECT \"%s\" {", sPath.c_str()));
            break;
    }

    if (!bReadOk)
    {
        data->papszLines = CSLAddString(data->papszLines, "}");
        H5Oclose(hObj);
        CPLDebug("NISAR_DRIVER",
                 "NISAR_DUMP: H5Dread('%s') failed; aborting traversal.",
                 sPath.c_str());
        return H5_ITER_ERROR;
    }

    hsize_t idx = 0;
    const herr_t eAttr = H5Aiterate2(hObj, H5_INDEX_NAME, H5_ITER_NATIVE, &idx,
                                     NisarDumpAttributeCallback, data);

    // A group's '}' is deferred until the visit leaves its subtree so that
    // children are nested inside it, as in h5dump.
    if (bIsGroup)
        data->aosOpenGroups.push_back(sPath);
    else
        data->papszLines = CSLAddString(data->papszLines, "}");
    H5Oclose(hObj);
    if (eAttr < 0)
    {
        CPLDebug("NISAR_DRIVER",
                 "NISAR_DUMP: attribute iteration on '%s' failed; aborting "
                 "traversal.",
                 sPath.c_str());
        return H5_ITER_ERROR;
    }
    return H5_ITER_CONT;
}

bool NisarDataset::LoadDumpMetadata()
{
    CSLDestroy(m_papszDumpMetadata);
    m_papszDumpMetadata = nullptr;
    if (hHDF5 < 0)
        return false;
    bool bOk = false;

    NISAR_DumpVisitData data;
    data.poDS = this;
    data.sRoot = m_sDumpRoot.empty() ? "/" : m_sDumpRoot;
    data.papszLines = nullptr;
    data.nMaxElements = static_cast<size_t>(
        std::max(0, atoi(CPLGetConfigOption("NISAR_DUMP_MAX_ELEMENTS", "64"))));
    data.nMaxStringChars = static_cast<size_t>(std::max(
        0, atoi(CPLGetConfigOption("NISAR_DUMP_MAX_STRING_CHARS", "2048"))));

    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

    hid_t hRoot = H5Oopen(hHDF5, data.sRoot.c_str(), H5P_DEFAULT);
    if (hRoot >= 0)
    {
        data.papszLines = CSLAddString(
            data.papszLines,
            CPLSPrintf("HDF5 \"%s\" %s {", pszFilename ? pszFilename : "",
                       m_bDumpFull ? "FULL" : "HEADER"));
        // H5Lvisit does not report the root itself, so dump it first (its
        // header and attributes), then every link below it.
        H5O_info2_t oRootInfo;
        herr_t eVisit = H5Oget_info3(hRoot, &oRootInfo, H5O_INFO_BASIC);
        if (eVisit >= 0)
        {
            char *pszToken = nullptr;
            if (H5Otoken_to_str(hRoot, &oRootInfo.token, &pszToken) >= 0 &&
                pszToken)
            {
                data.oVisitedObjects[pszToken] = data.sRoot;
                H5free_memory(pszToken);
            }
            eVisit = DumpObject(hRoot, ".", &oRootInfo, &data);
        }
        if (eVisit >= 0)
            eVisit = H5Lvisit2(hRoot, H5_INDEX_NAME, H5_ITER_NATIVE,
                               DumpLinkCallback, &data);
        H5Oclose(hRoot);
        if (eVisit < 0)
        {
            CSLDestroy(data.papszLines);
            data.papszLines = nullptr;
            CPLError(CE_Failure, CPLE_AppDefined,
                     "NISAR_DUMP: traversal of '%s' failed (I/O error?); "
                     "no listing produced.",
                     data.sRoot.c_str());
        }
        else
        {
            NisarDumpCloseGroups(&data, std::string());
            data.papszLines = CSLAddString(data.papszLines, "}");
            bOk = true;
        }
    }
    else
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "NISAR_DUMP: cannot open DUMP_ROOT '%s'.", data.sRoot.c_str());
    }

    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
    m_papszDumpMetadata = data.papszLines;
    CPLDebug("NISAR_DRIVER", "NISAR_DUMP: %d lines from '%s'.",
             CSLCount(m_papszDumpMetadata), data.sRoot.c_str());
    return bOk;
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
GDALDataset *NisarDataset::Open(GDALOpenInfo *poOpenInfo)
{
    // ====================================================================
    // 3D INTERPOLATION ROUTING HOOK
    // Catch the QUANTITY option before any HDF5 or string parsing begins.
    // ====================================================================
    if (poOpenInfo->papszOpenOptions != nullptr &&
        CSLFetchNameValue(poOpenInfo->papszOpenOptions, "QUANTITY") != nullptr)
    {
        if (CSLFetchNameValue(poOpenInfo->papszOpenOptions, "DEM_FILE") ==
            nullptr)
        {
            CPLError(CE_Failure, CPLE_OpenFailed,
                     "NISAR Driver: DEM_FILE open option is REQUIRED when "
                     "QUANTITY is specified.");
            return nullptr;
        }

        CPLDebug(
            "NISAR_DRIVER",
            "QUANTITY option detected. Routing to NisarInterpolatedDataset.");
        return NisarInterpolatedDataset::Open(poOpenInfo);
    }

    // ====================================================================
    // PATH PARSING
    // ====================================================================
    const char *pszFullInput = poOpenInfo->pszFilename;
    const char *pszDataIdentifier = nullptr;
    char *pszActualFilename = nullptr;
    const char *pszSubdatasetPath = nullptr;
    const char *pszPrefix = "NISAR:";
    size_t nPrefixLen = strlen(pszPrefix);

    const bool bHasPrefix = EQUALN(pszFullInput, pszPrefix, nPrefixLen);
    if (bHasPrefix)
    {
        pszDataIdentifier = pszFullInput + nPrefixLen;
    }
    else
    {
        pszDataIdentifier = pszFullInput;
    }

    if (pszDataIdentifier == nullptr || pszDataIdentifier[0] == '\0')
    {
        CPLError(CE_Failure, CPLE_OpenFailed, "Empty filename provided.");
        return nullptr;
    }

    const char *pszLastColon = strrchr(pszDataIdentifier, ':');
    if (pszLastColon != nullptr && pszLastColon[1] == '/' &&
        pszLastColon[2] == '/')
    {
        pszActualFilename = CPLStrdup(pszDataIdentifier);
        pszSubdatasetPath = nullptr;
    }
    else if (pszLastColon != nullptr)
    {
        pszSubdatasetPath = pszLastColon + 1;
        size_t nFilenameLen = pszLastColon - pszDataIdentifier;
        pszActualFilename = static_cast<char *>(CPLMalloc(nFilenameLen + 1));
        strncpy(pszActualFilename, pszDataIdentifier, nFilenameLen);
        pszActualFilename[nFilenameLen] = '\0';
    }
    else
    {
        pszActualFilename = CPLStrdup(pszDataIdentifier);
        pszSubdatasetPath = nullptr;
    }

    size_t nActualLen = strlen(pszActualFilename);
    if (nActualLen >= 2 && pszActualFilename[0] == '"' &&
        pszActualFilename[nActualLen - 1] == '"')
    {
        memmove(pszActualFilename, pszActualFilename + 1, nActualLen - 2);
        pszActualFilename[nActualLen - 2] = '\0';
    }

    // Mirror Identify(): an explicit NISAR: prefix claims the file whatever
    // its extension; only unprefixed names are gated on .h5.
    if (!bHasPrefix &&
        !EQUAL(NisarGetExtension(pszActualFilename).c_str(), "h5"))
    {
        CPLFree(pszActualFilename);
        return nullptr;
    }

    if (poOpenInfo->eAccess == GA_Update)
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "The NISAR driver only supports read access.");
        CPLFree(pszActualFilename);
        return nullptr;
    }

    // ====================================================================
    // URI REWRITING (Identify if file is remote)
    // ====================================================================
    CPLString osNormalizedPath;
    bool bIsVSIL = false;

    if (STARTS_WITH_CI(pszActualFilename, "s3://"))
    {
        osNormalizedPath = "/vsis3/";
        osNormalizedPath += (pszActualFilename + 5);
        bIsVSIL = true;
    }
    else if (STARTS_WITH_CI(pszActualFilename, "https://") ||
             STARTS_WITH_CI(pszActualFilename, "http://"))
    {
        osNormalizedPath = "/vsicurl/";
        osNormalizedPath += pszActualFilename;
        bIsVSIL = true;
    }
    else if (STARTS_WITH_CI(pszActualFilename, "/vsis3/") ||
             STARTS_WITH_CI(pszActualFilename, "/vsicurl/"))
    {
        osNormalizedPath = pszActualFilename;
        bIsVSIL = true;
    }
    else
    {
        osNormalizedPath = pszActualFilename;
    }

    const char *filenameForH5Fopen = osNormalizedPath.c_str();

    // ====================================================================
    // VIRTUAL FILE LAYER (VFL) ROUTING
    // ====================================================================
    hid_t fapl_id_base = H5Pcreate(H5P_FILE_ACCESS);
    bool bNeedToCloseFaplBase = true;

    if (bIsVSIL)
    {
        CPLDebug("NISAR_DRIVER", "Remote cloud file detected. Routing HDF5 "
                                 "through GDAL VSIL VFL...");

        // Route HDF5 I/O directly into GDAL's /vsicurl/ layer!
        // This gives HDF5 the illusion of the full 4.8 GB file, while GDAL handles the HTTP Range GETs.
        if (H5Pset_driver(fapl_id_base, NisarVFL::HDF5VFLGetFileDriver(),
                          NULL) < 0)
        {
            CPLError(CE_Failure, CPLE_AppDefined,
                     "Failed to configure GDAL VSIL VFD.");
            H5Pclose(fapl_id_base);
            CPLFree(pszActualFilename);
            return nullptr;
        }
    }

    // ====================================================================
    // PAGE BUFFERING (Hardcoded 4 MiB)
    // ====================================================================
    size_t nPageBufferSize = 4 * 1024 * 1024;  // 4 MiB
    if (H5Pset_page_buffer_size(fapl_id_base, nPageBufferSize, 0, 0) < 0)
    {
        CPLDebug("NISAR_DRIVER",
                 "Warning: Failed to set HDF5 Page Buffer Size.");
    }
    else
    {
        CPLDebug("NISAR_DRIVER", "Set HDF5 FAPL page buffer to 4 MiB.");
    }

    // ====================================================================
    // FINAL FILE OPEN
    // ====================================================================
    CPLDebug("NISAR_DRIVER", "Attempting H5Fopen with optimized FAPL.");
    hid_t hHDF5 = H5Fopen(filenameForH5Fopen, H5F_ACC_RDONLY, fapl_id_base);

    if (bNeedToCloseFaplBase)
        H5Pclose(fapl_id_base);

    if (hHDF5 < 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "H5Fopen failed for '%s'. Ensure metadata fits in 8MiB.",
                 pszActualFilename);
        CPLFree(pszActualFilename);
        return nullptr;
    }

    // ====================================================================
    // CREATE DATASET OBJECT
    // ====================================================================
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

    const char *pszMaskOpt =
        CSLFetchNameValue(poOpenInfo->papszOpenOptions, "MASK");
    if (pszMaskOpt && CPLTestBool(pszMaskOpt))
    {
        poDS->m_bMaskEnabled = true;
    }

    poDS->hHDF5 = hHDF5;
    poDS->pszFilename = pszActualFilename;
    pszActualFilename = nullptr;  // Ownership transferred

    poDS->SetDescription(poOpenInfo->pszFilename);
    poDS->ReadIdentificationMetadata();

    // ====================================================================
    // DUMP / DUMP_ROOT / DUMP_MODE (NISAR_DUMP metadata domain)
    // ====================================================================
    // The NISAR_DUMP domain is always available on request (gdalinfo -mdd NISAR_DUMP);
    // DUMP=YES additionally advertises it in the domain list (gdalinfo -mdd all).
    poDS->m_bDumpEnabled =
        CPLFetchBool(poOpenInfo->papszOpenOptions, "DUMP", false);
    {
        const char *pszDumpMode = CSLFetchNameValueDef(
            poOpenInfo->papszOpenOptions, "DUMP_MODE", "HEADER");
        if (EQUAL(pszDumpMode, "FULL"))
        {
            poDS->m_bDumpFull = true;
        }
        else if (!EQUAL(pszDumpMode, "HEADER"))
        {
            CPLError(CE_Failure, CPLE_OpenFailed,
                     "Invalid DUMP_MODE '%s' (expected HEADER or FULL).",
                     pszDumpMode);
            delete poDS;
            return nullptr;
        }

        const char *pszDumpRoot =
            CSLFetchNameValue(poOpenInfo->papszOpenOptions, "DUMP_ROOT");
        const bool bExplicitRoot = pszDumpRoot != nullptr;
        if (bExplicitRoot && pszDumpRoot[0] == '\0')
        {
            CPLError(CE_Failure, CPLE_OpenFailed,
                     "DUMP_ROOT must name an HDF5 group (use / for the whole "
                     "file).");
            delete poDS;
            return nullptr;
        }
        if (bExplicitRoot)
        {
            poDS->m_sDumpRoot = pszDumpRoot;
        }
        else
        {
            // Instrument precedence: explicit raster path (/science/<INST>/...),
            // then INST, then the instrument detected from identification.
            const char *pszInstOpt =
                CSLFetchNameValue(poOpenInfo->papszOpenOptions, "INST");
            std::string sInst = pszInstOpt ? pszInstOpt : poDS->m_sInst;
            if (pszSubdatasetPath != nullptr &&
                STARTS_WITH_CI(pszSubdatasetPath, "/science/"))
            {
                const char *pszInstStart =
                    pszSubdatasetPath + strlen("/science/");
                const char *pszInstEnd = strchr(pszInstStart, '/');
                const std::string sPathInst(
                    pszInstStart, pszInstEnd ? pszInstEnd - pszInstStart
                                             : strlen(pszInstStart));
                if (!sPathInst.empty())
                    sInst = sPathInst;
            }
            for (auto &c : sInst)
                c = toupper(c);
            poDS->m_sDumpRoot = sInst.empty() ? "/" : "/science/" + sInst;
        }
        if (poDS->m_sDumpRoot[0] != '/')
            poDS->m_sDumpRoot.insert(0, "/");
        while (poDS->m_sDumpRoot.size() > 1 && poDS->m_sDumpRoot.back() == '/')
            poDS->m_sDumpRoot.pop_back();

        if (poDS->m_sDumpRoot != "/" && (bExplicitRoot || poDS->m_bDumpEnabled))
        {
            H5E_auto2_t old_func_dump;
            void *old_client_data_dump;
            H5Eget_auto2(H5E_DEFAULT, &old_func_dump, &old_client_data_dump);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
            const hid_t hDumpRoot =
                H5Gopen2(poDS->hHDF5, poDS->m_sDumpRoot.c_str(), H5P_DEFAULT);
            if (hDumpRoot >= 0)
                H5Gclose(hDumpRoot);
            H5Eset_auto2(H5E_DEFAULT, old_func_dump, old_client_data_dump);
            if (hDumpRoot < 0)
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "DUMP_ROOT '%s' is not an existing HDF5 group.",
                         poDS->m_sDumpRoot.c_str());
                delete poDS;
                return nullptr;
            }
        }
        CPLDebug("NISAR_DRIVER", "NISAR_DUMP: root='%s' mode=%s advertised=%d",
                 poDS->m_sDumpRoot.c_str(),
                 poDS->m_bDumpFull ? "FULL" : "HEADER",
                 poDS->m_bDumpEnabled ? 1 : 0);
    }

    const char *pathToOpen = nullptr;
    std::string sConstructedPath;

    if (pszSubdatasetPath != nullptr)
    {
        pathToOpen = pszSubdatasetPath;
    }
    else
    {
        const char *pszInst =
            CSLFetchNameValue(poOpenInfo->papszOpenOptions, "INST");
        const char *pszFreq =
            CSLFetchNameValue(poOpenInfo->papszOpenOptions, "FREQ");
        const char *pszPol =
            CSLFetchNameValue(poOpenInfo->papszOpenOptions, "POL");

        if (pszInst || pszFreq || pszPol)
        {
            if (pszInst == nullptr)
                pszInst = "LSAR";
            if (pszFreq == nullptr)
                pszFreq = "A";

            if (!EQUAL(pszInst, "LSAR") && !EQUAL(pszInst, "SSAR"))
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid INST open option.");
                delete poDS;
                return nullptr;
            }
            if (!EQUAL(pszFreq, "A") && !EQUAL(pszFreq, "B"))
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid FREQ open option.");
                delete poDS;
                return nullptr;
            }
            if (poDS->m_sProductType.empty())
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Could not determine NISAR product type.");
                delete poDS;
                return nullptr;
            }

            std::string sInstUpper = pszInst;
            for (auto &c : sInstUpper)
                c = toupper(c);
            std::string sFreqUpper = pszFreq;
            for (auto &c : sFreqUpper)
                c = toupper(c);

            std::string sMetadataGroupPath =
                "/science/" + sInstUpper + "/" + poDS->m_sProductType;

            if (poDS->m_bIsLevel1)
                sMetadataGroupPath += "/swaths/frequency";
            else if (poDS->m_bIsLevel2)
                sMetadataGroupPath += "/grids/frequency";
            else if (poDS->m_bIsLevel3)
                sMetadataGroupPath += "/grids/radarData/frequency";

            sMetadataGroupPath += sFreqUpper;

            std::string sMetadataDatasetName;
            const bool bIsGCOV = EQUAL(poDS->m_sProductType.c_str(), "GCOV");

            if (bIsGCOV)
            {
                sMetadataDatasetName = "listOfCovarianceTerms";
                if (pszPol == nullptr)
                    pszPol = "HHHH";
            }
            else
            {
                sMetadataDatasetName = "listOfPolarizations";
                if (pszPol == nullptr)
                    pszPol = "HH";
            }

            std::string sPolUpper = pszPol;
            for (auto &c : sPolUpper)
                c = toupper(c);
            std::string sPolList;

            H5E_auto2_t old_func_md;
            void *old_client_data_md;
            H5Eget_auto2(H5E_DEFAULT, &old_func_md, &old_client_data_md);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

            hid_t hMetadataGroup =
                H5Gopen2(poDS->hHDF5, sMetadataGroupPath.c_str(), H5P_DEFAULT);
            if (hMetadataGroup >= 0)
            {
                sPolList = poDS->ReadHDF5StringDataset(
                    hMetadataGroup, sMetadataDatasetName.c_str());
                if (sPolList.empty())
                {
                    sPolList = poDS->ReadHDF5StringArrayAsList(
                        hMetadataGroup, sMetadataDatasetName.c_str());
                }
                H5Gclose(hMetadataGroup);
            }
            H5Eset_auto2(H5E_DEFAULT, old_func_md, old_client_data_md);

            if (sPolList.empty())
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Failed to read polarization list.");
                delete poDS;
                return nullptr;
            }

            char **papszPolList =
                CSLTokenizeString2(sPolList.c_str(), ",",
                                   CSLT_STRIPLEADSPACES | CSLT_STRIPENDSPACES);
            bool bPolFound = false;
            for (int i = 0; papszPolList && papszPolList[i] != nullptr; ++i)
            {
                if (EQUAL(papszPolList[i], pszPol))
                {
                    bPolFound = true;
                    break;
                }
            }
            CSLDestroy(papszPolList);

            if (!bPolFound)
            {
                CPLError(CE_Failure, CPLE_OpenFailed,
                         "Invalid POL open option: '%s'.", pszPol);
                delete poDS;
                return nullptr;
            }

            poDS->m_sInst = pszInst;
            poDS->m_sFreq = pszFreq;
            poDS->m_sPol = pszPol;

            sConstructedPath = sMetadataGroupPath + "/" + sPolUpper;
            pathToOpen = sConstructedPath.c_str();
        }
        else
        {
            // Container dataset discovery
            poDS->hDataset = -1;
            NISARVisitorData visitor_data;
            std::vector<std::string> found_paths_vector;
            visitor_data.pFoundPaths = &found_paths_vector;

            // Anchor the search to the specific instrument group
            std::string sScienceRoot = "/science/" + poDS->m_sInst;
            hid_t hScienceGroup =
                H5Gopen2(poDS->hHDF5, sScienceRoot.c_str(), H5P_DEFAULT);

            if (hScienceGroup >= 0)
            {
                visitor_data.hStartingGroupID =
                    hScienceGroup;  // Use the anchored group handle
                visitor_data.sBasePath =
                    sScienceRoot + "/";  // Store the absolute prefix

                H5Ovisit(hScienceGroup, H5_INDEX_NAME, H5_ITER_NATIVE,
                         NISAR_FindDatasetsVisitor, &visitor_data,
                         H5O_INFO_BASIC);

                H5Gclose(hScienceGroup);
            }

            if (!found_paths_vector.empty())
            {
                char **papszMetadataList = nullptr;
                int nSubdatasets = 0;

                for (const std::string &hdf5_path : found_paths_vector)
                {
                    hid_t hSubDataset =
                        H5Dopen2(poDS->hHDF5, hdf5_path.c_str(), H5P_DEFAULT);
                    if (hSubDataset < 0)
                        continue;

                    hid_t hSubType = H5Dget_type(hSubDataset);
                    if (hSubType >= 0 && H5Tget_class(hSubType) == H5T_STRING)
                    {
                        H5Tclose(hSubType);
                        H5Dclose(hSubDataset);
                        continue;
                    }

                    nSubdatasets++;
                    std::string name_key =
                        CPLSPrintf("SUBDATASET_%d_NAME", nSubdatasets);
                    std::string name_val = std::string(pszPrefix) + "\"" +
                                           poDS->pszFilename +
                                           "\":" + hdf5_path;
                    papszMetadataList = CSLSetNameValue(
                        papszMetadataList, name_key.c_str(), name_val.c_str());

                    std::string desc_key =
                        CPLSPrintf("SUBDATASET_%d_DESC", nSubdatasets);
                    std::string desc_val = "[";
                    hid_t hSubSpace = H5Dget_space(hSubDataset);
                    int nSubDim = (hSubSpace >= 0)
                                      ? H5Sget_simple_extent_ndims(hSubSpace)
                                      : -1;

                    if (nSubDim > 0)
                    {
                        hsize_t adimsSub[H5S_MAX_RANK];
                        H5Sget_simple_extent_dims(hSubSpace, adimsSub, nullptr);
                        for (int i = 0; i < nSubDim; ++i)
                        {
                            desc_val += CPLSPrintf(
                                "%llu%s",
                                static_cast<unsigned long long>(adimsSub[i]),
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
                    papszMetadataList = CSLSetNameValue(
                        papszMetadataList, desc_key.c_str(), desc_val.c_str());

                    if (hSubType >= 0)
                        H5Tclose(hSubType);
                    if (hSubSpace >= 0)
                        H5Sclose(hSubSpace);
                    H5Dclose(hSubDataset);
                }

                CSLDestroy(poDS->papszSubDatasets);
                poDS->papszSubDatasets = papszMetadataList;
                poDS->SetMetadata(poDS->papszSubDatasets, "SUBDATASETS");
                poDS->nRasterXSize = 0;
                poDS->nRasterYSize = 0;
                poDS->nBands = 0;
                poDS->TryLoadXML();
                poDS->SetDescription(poOpenInfo->pszFilename);
                return poDS;
            }
            else
            {
                delete poDS;
                return nullptr;
            }
        }
    }

    // ====================================================================
    // DAPL & CHUNK CACHE (Targeting 512x512)
    // ====================================================================
    hid_t dapl_id = H5Pcreate(H5P_DATASET_ACCESS);
    bool bNeedToCloseDapl = (dapl_id >= 0);

    if (dapl_id >= 0)
    {
        // Since IReadBlock bypasses HDF5 pixel reads entirely, this cache is
        // essentially a safety net for any incidental chunk scans.
        size_t nCacheSlots = 521;  // Prime number of slots for 512x512 tiles
        size_t nCacheSizeBytes = 8 * 1024 * 1024;  // 8 MiB dataset cache
        H5Pset_chunk_cache(dapl_id, nCacheSlots, nCacheSizeBytes, 0.75);
    }

    H5E_auto2_t old_func_exists;
    void *old_client_data_exists;
    H5Eget_auto2(H5E_DEFAULT, &old_func_exists, &old_client_data_exists);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    htri_t bExists = H5Lexists(poDS->hHDF5, pathToOpen, H5P_DEFAULT);
    H5Eset_auto2(H5E_DEFAULT, old_func_exists, old_client_data_exists);

    if (bExists <= 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "The HDF5 dataset '%s' does not exist.", pathToOpen);
        if (bNeedToCloseDapl)
            H5Pclose(dapl_id);
        delete poDS;
        return nullptr;
    }

    poDS->hDataset = H5Dopen2(poDS->hHDF5, pathToOpen, dapl_id);
    if (bNeedToCloseDapl)
        H5Pclose(dapl_id);

    if (poDS->hDataset < 0)
    {
        CPLError(CE_Failure, CPLE_OpenFailed,
                 "H5Dopen2 failed for dataset '%s'.", pathToOpen);
        delete poDS;
        return nullptr;
    }

    // Map Data Types & Dimensions
    hid_t hH5DataType = H5Dget_type(poDS->hDataset);
    if (hH5DataType < 0)
    {
        delete poDS;
        return nullptr;
    }
    poDS->eDataType = NisarDataset::GetGDALDataType(hH5DataType);
    H5Tclose(hH5DataType);

    if (poDS->eDataType == GDT_Unknown)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Unsupported HDF5 data type.");
        delete poDS;
        return nullptr;
    }

    hid_t hDataspace = H5Dget_space(poDS->hDataset);
    if (hDataspace < 0)
    {
        delete poDS;
        return nullptr;
    }
    const int nDims = H5Sget_simple_extent_ndims(hDataspace);

    if (nDims < 2)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "Dataset requires rank >= 2.");
        H5Sclose(hDataspace);
        delete poDS;
        return nullptr;
    }

    hsize_t adims[H5S_MAX_RANK];
    H5Sget_simple_extent_dims(hDataspace, adims, nullptr);

    int nBandsToCreate = 1;
    if (nDims == 3)
    {
        nBandsToCreate = static_cast<int>(adims[0]);
        poDS->nRasterYSize = static_cast<int>(adims[1]);
        poDS->nRasterXSize = static_cast<int>(adims[2]);
    }
    else if (nDims == 2)
    {
        poDS->nRasterYSize = static_cast<int>(adims[0]);
        poDS->nRasterXSize = static_cast<int>(adims[1]);
    }
    else
    {
        poDS->nRasterYSize = static_cast<int>(adims[nDims - 2]);
        poDS->nRasterXSize = static_cast<int>(adims[nDims - 1]);
    }
    H5Sclose(hDataspace);

    if (poDS->nRasterXSize <= 0 || poDS->nRasterYSize <= 0)
    {
        delete poDS;
        return nullptr;
    }

    double dfNoData = 0.0;
    bool bHasNoData = false;
    H5E_auto2_t old_func_fill;
    void *old_client_data_fill;
    H5Eget_auto2(H5E_DEFAULT, &old_func_fill, &old_client_data_fill);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    hid_t hFillAttr = H5Aopen(poDS->hDataset, "_FillValue", H5P_DEFAULT);
    if (hFillAttr >= 0)
    {
        if (H5Aread(hFillAttr, H5T_NATIVE_DOUBLE, &dfNoData) >= 0)
            bHasNoData = true;
        H5Aclose(hFillAttr);
    }
    H5Eset_auto2(H5E_DEFAULT, old_func_fill, old_client_data_fill);

    for (int i = 0; i < nBandsToCreate; i++)
    {
        NisarRasterBand *poBand = new NisarRasterBand(poDS, i + 1);
        if (!poBand->IsValid())
        {
            delete poBand;
            delete poDS;
            return nullptr;
        }
        poDS->SetBand(i + 1, poBand);
        if (bHasNoData)
            poBand->SetNoDataValue(dfNoData);
    }

    if (nDims == 3 && poDS->hDataset >= 0)
    {
        std::string sCurrentPath = pathToOpen;
        size_t nLastSlash = sCurrentPath.find_last_of('/');
        if (nLastSlash != std::string::npos)
        {
            std::string sZAxisPath =
                sCurrentPath.substr(0, nLastSlash) + "/heightAboveEllipsoid";
            H5Eget_auto2(H5E_DEFAULT, &old_func_fill, &old_client_data_fill);
            H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
            htri_t bHasHeight =
                H5Lexists(poDS->hHDF5, sZAxisPath.c_str(), H5P_DEFAULT);
            H5Eset_auto2(H5E_DEFAULT, old_func_fill, old_client_data_fill);

            if (bHasHeight > 0)
            {
                std::vector<double> adfHeights;
                if (Read1DDoubleVec(poDS->hHDF5, sZAxisPath.c_str(),
                                    adfHeights) &&
                    adfHeights.size() == static_cast<size_t>(nBandsToCreate))
                {
                    for (int i = 0; i < nBandsToCreate; i++)
                    {
                        GDALRasterBand *poBand = poDS->GetRasterBand(i + 1);
                        if (poBand)
                        {
                            poBand->SetDescription(
                                CPLSPrintf("Height: %.1f m", adfHeights[i]));
                            poBand->SetMetadataItem(
                                "HEIGHT_METERS",
                                CPLSPrintf("%.1f", adfHeights[i]));
                            poBand->SetMetadataItem(
                                "Z_VALUE", CPLSPrintf("%.1f", adfHeights[i]));
                        }
                    }
                }
            }
        }
    }

    if (poDS->m_bIsLevel1)
    {
        poDS->GenerateGCPsFromGeolocationGrid(poDS->m_sProductType.c_str(),
                                              pathToOpen);
    }

    if (poDS->nBands == 0)
    {
        poDS->nBands = 1;
        NisarRasterBand *poBand = new NisarRasterBand(poDS, 1);
        if (!poBand->IsValid())
        {
            delete poBand;
            delete poDS;
            return nullptr;
        }
        poDS->SetBand(1, poBand);
    }

    //poDS->SetDescription(poOpenInfo->pszFilename);
    if (pathToOpen)
        poDS->SetMetadataItem("HDF5_PATH", pathToOpen);

    if (poDS->hDataset >= 0)
    {
        std::string sTargetString =
            std::string("NISAR:") + poDS->pszFilename + ":" + pathToOpen;
        bool bIsComplex = GDALDataTypeIsComplex(poDS->eDataType);
        bool bIsNumeric =
            (poDS->eDataType > GDT_Unknown && poDS->eDataType < GDT_CInt16);

        if (bIsComplex)
        {
            poDS->SetMetadataItem(
                "AMPLITUDE",
                CPLSPrintf("DERIVED_SUBDATASET:AMPLITUDE:\"%s\"",
                           sTargetString.c_str()),
                "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("PHASE",
                                  CPLSPrintf("DERIVED_SUBDATASET:PHASE:\"%s\"",
                                             sTargetString.c_str()),
                                  "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("REAL",
                                  CPLSPrintf("DERIVED_SUBDATASET:REAL:\"%s\"",
                                             sTargetString.c_str()),
                                  "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("IMAG",
                                  CPLSPrintf("DERIVED_SUBDATASET:IMAG:\"%s\"",
                                             sTargetString.c_str()),
                                  "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem(
                "INTENSITY",
                CPLSPrintf("DERIVED_SUBDATASET:INTENSITY:\"%s\"",
                           sTargetString.c_str()),
                "DERIVED_SUBDATASETS");
            poDS->SetMetadataItem("CONJ",
                                  CPLSPrintf("DERIVED_SUBDATASET:CONJ:\"%s\"",
                                             sTargetString.c_str()),
                                  "DERIVED_SUBDATASETS");
        }
        if (bIsNumeric)
        {
            poDS->SetMetadataItem(
                "LOGAMPLITUDE",
                CPLSPrintf("DERIVED_SUBDATASET:LOGAMPLITUDE:\"%s\"",
                           sTargetString.c_str()),
                "DERIVED_SUBDATASETS");
        }
    }

    bool bIsMask = false;
    if (pathToOpen &&
        std::string(pathToOpen).find("/mask") != std::string::npos)
        bIsMask = true;
    if (!bIsMask)
    {
        const char *pszH5Path = poDS->GetMetadataItem("HDF5_PATH");
        if (pszH5Path &&
            std::string(pszH5Path).find("/mask") != std::string::npos)
            bIsMask = true;
    }

    if (bIsMask)
    {
        GDALRasterBand *poBand = poDS->GetRasterBand(1);
        if (poBand)
        {
            poBand->SetNoDataValue(255.0);
            char **papszCategories = nullptr;
            papszCategories =
                CSLAddString(papszCategories, "Invalid or partially focused");
            papszCategories =
                CSLAddString(papszCategories, "Valid (Sub-swath 1)");
            papszCategories =
                CSLAddString(papszCategories, "Valid (Sub-swath 2)");
            papszCategories =
                CSLAddString(papszCategories, "Valid (Sub-swath 3)");
            papszCategories =
                CSLAddString(papszCategories, "Valid (Sub-swath 4)");
            papszCategories =
                CSLAddString(papszCategories, "Valid (Sub-swath 5)");
            poBand->SetCategoryNames(papszCategories);
            CSLDestroy(papszCategories);
            poBand->SetColorInterpretation(GCI_PaletteIndex);
        }
    }

    poDS->TryLoadXML();

    if (poDS->m_sInst.empty() || poDS->m_sProductType.empty())
    {
        std::string sInst = "LSAR";
        std::string sType = poDS->ReadHDF5StringDataset(
            poDS->hHDF5, "/science/LSAR/identification/productType");
        if (sType.empty())
        {
            sInst = "SSAR";
            sType = poDS->ReadHDF5StringDataset(
                poDS->hHDF5, "/science/SSAR/identification/productType");
        }
        if (!sType.empty())
        {
            poDS->m_sInst = sInst;
            poDS->m_sProductType = sType;
        }
    }

    if (!poDS->m_sInst.empty() && !poDS->m_sProductType.empty())
    {
        poDS->InitializeMetadataMap();
        const char *pszMetaOption =
            poOpenInfo->papszOpenOptions
                ? CSLFetchNameValue(poOpenInfo->papszOpenOptions, "METADATA")
                : nullptr;
        if (pszMetaOption)
        {
            if (EQUAL(pszMetaOption, "ALL"))
            {
                for (auto const &[key, val] : poDS->m_oMetadataMap)
                    poDS->LoadMetadataDomain(key);
            }
            else
            {
                char **papszTokens = CSLTokenizeString2(
                    pszMetaOption, ",",
                    CSLT_STRIPLEADSPACES | CSLT_STRIPENDSPACES);
                for (int i = 0; papszTokens && papszTokens[i] != nullptr; i++)
                {
                    std::string sToken = papszTokens[i];
                    for (auto &c : sToken)
                        c = toupper(c);
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
#if GDAL_VERSION_MAJOR > 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
CPLErr NisarDataset::GetGeoTransform(GDALGeoTransform &oGT) const
{
#else
CPLErr NisarDataset::GetGeoTransform(double *padfTransform)
{
    // Legacy Signature: create an temporary object to hold the result
    GDALGeoTransform oGT;
    // Initialize identity
    oGT[0] = 0.0;
    oGT[1] = 1.0;
    oGT[2] = 0.0;
    oGT[3] = 0.0;
    oGT[4] = 0.0;
    oGT[5] = 1.0;
#endif

    // Check PAM (Persistent Aux Metadata) - Highest Priority
    // We must handle the parent call differently because the parent signature
    // also changes between versions.
#if GDAL_VERSION_MAJOR > 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR >= 12)
    if (GDALPamDataset::GetGeoTransform(oGT) == CE_None)
        return CE_None;
#else
    if (GDALPamDataset::GetGeoTransform(padfTransform) == CE_None)
        return CE_None;
#endif

    // Check Internal Cache
    {
        std::lock_guard<std::mutex> lock(m_GeoTransformMutex);
        if (m_bGotGeoTransform)
        {
            memcpy(oGT.data(), m_adfGeoTransform, sizeof(double) * 6);
// LEGACY ADAPTER WRITE-BACK
#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
            if (padfTransform)
                memcpy(padfTransform, oGT.data(), sizeof(double) * 6);
#endif
            return CE_None;
        }
    }

    // Early Exit for Non-Grid Data
    // If we have GCPs (Level 1 / Swaths), we should not return a GeoTransform.
    if (const_cast<NisarDataset *>(this)->GetGCPCount() > 0)
        return CE_Failure;
    if (hDataset < 0)
        return CE_Failure;

    CPLDebug("NISAR_DRIVER", "GetGeoTransform: Cache miss. Calculating...");

    // Try reading explicit 'GeoTransform' attribute (Generic Fallback)
    if (ReadGeoTransformAttribute(this->hDataset, "GeoTransform", oGT) ==
        CE_None)
    {
        std::lock_guard<std::mutex> lock(m_GeoTransformMutex);
        m_bGotGeoTransform = true;
        memcpy(m_adfGeoTransform, oGT.data(), sizeof(double) * 6);
// LEGACY ADAPTER WRITE-BACK
#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
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
    // or Level 3 products (SME2)
    if (m_bIsLevel2 || m_bIsLevel3)
    {
        std::string sCurrentPath = get_hdf5_object_name(this->hDataset);

        // Safety Check: Only scan for coordinates if we are in a known grid-like hierarchy
        bool bIsStandardGrid =
            (sCurrentPath.find("/grids/") != std::string::npos);
        bool bIsCalibrationGrid =
            (sCurrentPath.find("/calibrationInformation/") !=
             std::string::npos);
        bool bIsRadarGrid =
            (sCurrentPath.find("/radarGrid/") != std::string::npos);

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
                if (nLastSlash == std::string::npos || nLastSlash == 0)
                    break;

                sSearchPath = sSearchPath.substr(0, nLastSlash);

                // Check for sibling coordinates
                std::string sTestX = sSearchPath + "/xCoordinates";

                // Fast existence check
                H5E_auto2_t old_func;
                void *old_client_data;
                H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
                H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);  // Suppress errors
                htri_t exists =
                    H5Lexists(this->hHDF5, sTestX.c_str(), H5P_DEFAULT);
                H5Eset_auto2(H5E_DEFAULT, old_func,
                             old_client_data);  // Restore errors

                if (exists > 0)
                {
                    sCoordsRoot = sSearchPath;
                    CPLDebug("NISAR_DRIVER",
                             "GetGeoTransform: Found coordinate arrays at: %s",
                             sCoordsRoot.c_str());
                    break;
                }
            }

            if (!sCoordsRoot.empty())
            {
                // Open and Read Coordinates
                std::string sXCoordsPath = sCoordsRoot + "/xCoordinates";
                std::string sYCoordsPath = sCoordsRoot + "/yCoordinates";

                hid_t hX =
                    H5Dopen2(this->hHDF5, sXCoordsPath.c_str(), H5P_DEFAULT);
                hid_t hY =
                    H5Dopen2(this->hHDF5, sYCoordsPath.c_str(), H5P_DEFAULT);

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
                        auto ReadVal = [](hid_t hD, hsize_t idx) -> double
                        {
                            double val = 0.0;
                            hsize_t count[1] = {1};
                            hsize_t start[1] = {idx};
                            hid_t hMem = H5Screate_simple(1, count, nullptr);
                            hid_t hFile = H5Dget_space(hD);
                            H5Sselect_hyperslab(hFile, H5S_SELECT_SET, start,
                                                nullptr, count, nullptr);
                            if (H5Dread(hD, H5T_NATIVE_DOUBLE, hMem, hFile,
                                        H5P_DEFAULT, &val) < 0)
                                val = 0.0;
                            H5Sclose(hFile);
                            H5Sclose(hMem);
                            return val;
                        };

                        double dfXStart = ReadVal(hX, 0);
                        double dfXEnd = ReadVal(hX, dimsX[0] - 1);
                        double dfYStart = ReadVal(hY, 0);
                        double dfYEnd = ReadVal(hY, dimsY[0] - 1);

                        // Calculate GeoTransform
                        // Calculate Pixel Size (Resolution)
                        // Note: We use the array size (dims - 1) for the span.
                        double resX = (dfXEnd - dfXStart) /
                                      static_cast<double>(dimsX[0] - 1);
                        double resY = (dfYEnd - dfYStart) /
                                      static_cast<double>(dimsY[0] - 1);

                        // Calculate Top-Left Corner (Origin)
                        // NISAR coordinates represent Pixel Centers.
                        // GDAL GeoTransform[0],[3] represents Top-Left Corner.
                        // We must shift by half a pixel.
                        double dfULX = dfXStart - (0.5 * resX);
                        double dfULY = dfYStart - (0.5 * resY);

                        double adfCalcGT[6];
                        adfCalcGT[0] = dfULX;  // Top-Left X
                        adfCalcGT[1] = resX;   // W-E Resolution
                        adfCalcGT[2] = 0.0;    // Rotation (0)
                        adfCalcGT[3] = dfULY;  // Top-Left Y
                        adfCalcGT[4] = 0.0;    // Rotation (0)
                        adfCalcGT[5] = resY;   // N-S Resolution

                        // Clean up HDF5 handles
                        H5Dclose(hX);
                        H5Dclose(hY);

                        // Cache and Return
                        {
                            std::lock_guard<std::mutex> lock(
                                m_GeoTransformMutex);
                            m_bGotGeoTransform = true;
                            memcpy(m_adfGeoTransform, adfCalcGT,
                                   sizeof(double) * 6);
                            memcpy(oGT.data(), m_adfGeoTransform,
                                   sizeof(double) * 6);
                        }

                        CPLDebug("NISAR_DRIVER",
                                 "GetGeoTransform: Derived from coordinates. "
                                 "Origin=(%.2f, %.2f) Res=(%.6f, %.6f)",
                                 adfCalcGT[0], adfCalcGT[3], adfCalcGT[1],
                                 adfCalcGT[5]);

// LEGACY ADAPTER WRITE-BACK
#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
                        if (padfTransform)
                            memcpy(padfTransform, oGT.data(),
                                   sizeof(double) * 6);
#endif

                        return CE_None;
                    }
                }

                // Cleanup if we failed inside the block
                if (hX >= 0)
                    H5Dclose(hX);
                if (hY >= 0)
                    H5Dclose(hY);
            }
        }
        else
        {
            CPLDebug("NISAR_DRIVER",
                     "GetGeoTransform: Path '%s' is not recognized as a grid "
                     "or calibration layer. Skipping coordinate discovery.",
                     sCurrentPath.c_str());
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
                             static_cast<int>(epsg_code));
                    goto cleanup;  //Jump to cleanup AFTER successful assignment
                }
                else
                {
                    CPLError(CE_Warning, CPLE_AppDefined,
                             "Failed to import EPSG code %lld into "
                             "OGRSpatialReference.",
                             epsg_code);
                    delete poTmpSRS;
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
                        pszWKT_Alloc = static_cast<char *>(VSIMalloc(
                            nWktLen + 1));  // Use VSI for GDAL consistency
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

std::string NisarFindGroupUpward(hid_t hFile, const std::string &sPath,
                                 const std::vector<const char *> &apszNames)
{
    H5E_auto2_t old_func;
    void *old_client_data;
    H5Eget_auto2(H5E_DEFAULT, &old_func, &old_client_data);
    H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
    std::string sGroup = sPath;
    for (;;)
    {
        const size_t nSlash = sGroup.rfind('/');
        if (nSlash == std::string::npos || nSlash == 0)
        {
            sGroup.clear();
            break;
        }
        sGroup.resize(nSlash);
        bool bAll = true;
        for (const char *pszName : apszNames)
        {
            if (H5Lexists(hFile, (sGroup + "/" + pszName).c_str(),
                          H5P_DEFAULT) <= 0)
            {
                bAll = false;
                break;
            }
        }
        if (bAll)
            break;
    }
    H5Eset_auto2(H5E_DEFAULT, old_func, old_client_data);
    return sGroup;
}

CPLErr
NisarDataset::GenerateGCPsFromGeolocationGrid(const char *pszProductGroup,
                                              const char *pszRasterPath)
{
    // DECLARE ALL VARIABLES AT THE TOP
    CPLErr eErr = CE_Failure;
    hid_t hGridGroup = -1;
    hid_t hScalarDset = -1;
    OGRSpatialReference *poCRS = nullptr;
    std::vector<GDAL_GCP> gcp_list;

    hid_t hEpsgDset = -1;
    long long epsg_code = 0;
    std::vector<double> x_coords, y_coords, slant_ranges, azimuth_times;
    std::vector<double> swath_times;
    double startingRange = 0.0, rangePixelSpacing = 0.0,
           azimuthTimeSpacing = 0.0, swath_start_time = 0.0;

    double time_epoch = 0.0, swath_time_epoch = 0.0;
    hid_t hAzimuthTimeDset = -1;
    hid_t hSwathTimeDset = -1;
    std::string time_units, swath_time_units;

    hid_t hSlantRangeDset = -1;
    hid_t hMemSpace = -1;
    hid_t hFileSpace = -1;

    hsize_t mem_dims[1];
    hsize_t offset[1];
    hsize_t count[1];

    // Declare all path strings here, using member variables
    // m_sInst ("LSAR" or "SSAR") was set in NisarDataset::Open()
    std::string sGridPath = "/science/" + m_sInst + "/" + pszProductGroup +
                            "/metadata/geolocationGrid";

    // Radar axes from the nearest ancestor groups holding slantRange(+Spacing) / zeroDopplerTime
    // (+Spacing): RSLC frequency<F> + swaths, RIFG/RUNW the layer group. None (e.g. cubes) -> no GCPs.
    const std::string sRasterPath = pszRasterPath ? pszRasterPath : "";
    const std::string sRangeGroup = NisarFindGroupUpward(
        hHDF5, sRasterPath, {"slantRange", "slantRangeSpacing"});
    const std::string sTimeGroup = NisarFindGroupUpward(
        hHDF5, sRasterPath, {"zeroDopplerTime", "zeroDopplerTimeSpacing"});
    if (sRangeGroup.empty() || sTimeGroup.empty())
    {
        CPLDebug("NISAR_DRIVER", "No radar axes above '%s'; skipping GCPs.",
                 sRasterPath.c_str());
        return CE_None;
    }

    std::string sSlantRangeSpacingPath = sRangeGroup + "/slantRangeSpacing";
    std::string sSlantRangePath = sRangeGroup + "/slantRange";
    // Line axis per spec: zeroDopplerTime[0] and zeroDopplerTimeSpacing
    std::string sSwathTimePath = sTimeGroup + "/zeroDopplerTime";
    std::string sSwathTimeSpacingPath = sTimeGroup + "/zeroDopplerTimeSpacing";

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
    if (!ParseSecondsSinceEpoch(time_units, time_epoch))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Could not parse time epoch from units: %s",
                 time_units.c_str());
        goto cleanup;
    }

    // Swath azimuth axis: first zero-Doppler time and its epoch
    hSwathTimeDset = H5Dopen2(hHDF5, sSwathTimePath.c_str(), H5P_DEFAULT);
    if (hSwathTimeDset < 0 ||
        !Read1DDoubleVec(hHDF5, sSwathTimePath.c_str(), swath_times))
    {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read %s dataset.",
                 sSwathTimePath.c_str());
        goto cleanup;
    }
    swath_time_units = ReadH5StringAttribute(hSwathTimeDset, "units");
    if (!ParseSecondsSinceEpoch(swath_time_units, swath_time_epoch))
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "Could not parse swath time epoch from units: %s",
                 swath_time_units.c_str());
        goto cleanup;
    }
    // Swath start expressed in the geolocationGrid epoch (keeps sub-µs precision)
    swath_start_time = swath_times[0] + (swath_time_epoch - time_epoch);

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
    H5Dclose(hScalarDset);
    hScalarDset = -1;

    // Read zeroDopplerTimeSpacing (line spacing in seconds)
    hScalarDset = H5Dopen2(hHDF5, sSwathTimeSpacingPath.c_str(), H5P_DEFAULT);
    if (hScalarDset < 0)
    {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to open %s.",
                 sSwathTimeSpacingPath.c_str());
        goto cleanup;
    }
    if (H5Dread(hScalarDset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &azimuthTimeSpacing) < 0 ||
        !(azimuthTimeSpacing > 0.0))
    {
        CPLError(CE_Failure, CPLE_FileIO, "Failed to read a positive %s.",
                 sSwathTimeSpacingPath.c_str());
        goto cleanup;
    }
    CPLDebug("NISAR_DRIVER",
             "GCP line axis: t0=%.6f s (grid epoch), spacing=%.9g s",
             swath_start_time, azimuthTimeSpacing);

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
                gcp.dfGCPLine = ((azimuth_times.at(i) - swath_start_time) /
                                 azimuthTimeSpacing) +
                                0.5;

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
    if (hScalarDset >= 0)
        H5Dclose(hScalarDset);
    if (hGridGroup >= 0)
        H5Gclose(hGridGroup);
    if (hEpsgDset >= 0)
        H5Dclose(hEpsgDset);
    if (hAzimuthTimeDset >= 0)
        H5Dclose(hAzimuthTimeDset);
    if (hSwathTimeDset >= 0)
        H5Dclose(hSwathTimeDset);
    if (hSlantRangeDset >= 0)
        H5Dclose(hSlantRangeDset);
    if (hMemSpace >= 0)
        H5Sclose(hMemSpace);
    if (hFileSpace >= 0)
        H5Sclose(hFileSpace);
    if (poCRS)
        poCRS->Release();

    // The GCP list is now owned by the dataset, so we do NOT free it here.

    return eErr;
}
