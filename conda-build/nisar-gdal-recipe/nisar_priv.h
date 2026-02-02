#ifndef NISAR_PRIV_H
#define NISAR_PRIV_H

#include "hdf5.h"
#include "cpl_string.h"  // For CSLSetNameValue, CPLDebug, CPLError
#include "cpl_conv.h"    // For CPLStrdup, CPLFree
#include "cpl_error.h"
#include "gdal_priv.h"
#include "gdal.h"  // For CE_Failure etc.

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <limits>

// Define the logic strategy for the mask
enum class NisarMaskType {
    GCOV, // Logic: 1-5 Valid; 0, 255 Invalid
    GUNW  // Logic: Digit parsing (Ref != 0 && Sec != 0)
};

class NisarHDF5MaskBand final: public GDALRasterBand
{
    hid_t m_hMaskDS; 
    NisarMaskType m_eType; // Store the logic type

public:
    NisarHDF5MaskBand(NisarDataset* poDS, hid_t hMaskDS, NisarMaskType eType);
    virtual ~NisarHDF5MaskBand();
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage) override;
};

/**
 * Gets the full HDF5 path of an object from its handle (hid_t).
 *
 * This function is defined as 'static inline' to allow its implementation
 * to live in a header file without causing linker errors.
 *
 * @param hObjectID The hid_t of the open HDF5 object.
 * @return A std::string containing the full path, or an empty string on failure.
 */
static inline std::string get_hdf5_object_name(hid_t hObjectID)
{
    if (hObjectID < 0)
    {
        return "";  // Return empty for invalid handle
    }

    // Call 1: Get the length of the name.
    ssize_t nNameLen = H5Iget_name(hObjectID, nullptr, 0);
    if (nNameLen <= 0)
    {
        // This can happen for the root group ("/") which has a name but length 0,
        // or an actual error. H5Iget_name is ambiguous here, but returning ""
        // is a safe fallback.
        return "";
    }

    // Allocate a string buffer of the correct size.
    std::string sName;
    sName.resize(nNameLen);

    // Call 2: Get the actual name.
    if (H5Iget_name(hObjectID, &sName[0], nNameLen + 1) < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "H5Iget_name failed to retrieve object name.");
        return "";  // Return empty on failure
    }

    return sName;
}

// Struct to pass data to the callback
struct NISAR_AttrCallbackData
{
    char ***ppapszList;
    const char *pszPrefix;
};

// Helper structs for reading complex compound attributes
typedef struct
{
    float r; /*real part*/
    float i; /*imaginary part*/
} ComplexFloatAttr;

typedef struct
{
    double r; /*real part*/
    double i; /*imaginary part*/
} ComplexDoubleAttr;

typedef struct
{
    short r; /*real part*/
    short i; /*imaginary part*/
} ComplexInt16Attr;

typedef struct
{
    int r; /*real part*/
    int i; /*imaginary part*/
} ComplexInt32Attr;

// Static helper function to read an HDF5 attribute and add it to a CSL list
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
        return 0;  // Skip this attribute
    }

    hid_t attr_id = -1;
    hid_t attr_type = -1;
    hid_t native_type = -1;  // Use native types for reading
    hid_t attr_space = -1;
    std::string value_str;

    // Open the attribute by name
    attr_id =
        H5Aopen_by_name(hLocation, ".", attr_name, H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0)
        return 0;  // Attribute likely doesn't exist, not an error

    attr_type = H5Aget_type(attr_id);
    attr_space = H5Aget_space(attr_id);

    if (attr_type < 0 || attr_space < 0)
    {
        if (attr_type >= 0)
            H5Tclose(attr_type);
        if (attr_space >= 0)
            H5Sclose(attr_space);
        H5Aclose(attr_id);
        return 0;  // Error getting type or space
    }

    // Convert attribute type to native type for reading
    native_type = H5Tget_native_type(attr_type, H5T_DIR_ASCEND);
    if (native_type < 0)
    {
        H5Tclose(attr_type);
        H5Sclose(attr_space);
        H5Aclose(attr_id);
        return 0;  // Error getting native type
    }

    H5T_class_t type_class = H5Tget_class(native_type);
    hssize_t n_points = H5Sget_simple_extent_npoints(attr_space);

    // Only process scalar attributes (n_points == 1) for most types
    if (n_points == 1)
    {
        if (type_class == H5T_STRING)
        {
            // String handling code
            if (H5Tis_variable_str(native_type))
            {
                char *pszReadVL = nullptr;
                if (H5Aread(attr_id, native_type, &pszReadVL) >= 0 && pszReadVL)
                {
                    value_str = pszReadVL;
                    H5free_memory(pszReadVL);
                }
                else
                {
                    value_str = "(read error VL string)";
                }
            }
            else
            {  // Fixed length string
                size_t type_size = H5Tget_size(native_type);
                if (type_size > 0)
                {
                    char *pszReadFixed =
                        (char *)VSI_MALLOC_VERBOSE(type_size + 1);
                    if (pszReadFixed)
                    {
                        if (H5Aread(attr_id, native_type, pszReadFixed) >= 0)
                        {
                            pszReadFixed[type_size] = '\0';
                            value_str = pszReadFixed;
                        }
                        else
                        {
                            value_str = "(read error fixed string)";
                        }
                        VSIFree(pszReadFixed);
                    }
                    else
                    {
                        value_str = "(memory alloc error)";
                    }
                }
                else
                {
                    value_str = "(zero size fixed string)";
                }
            }
        }
        else if (type_class == H5T_INTEGER)
        {
            // Integer handling code
            long long llVal = 0;
            if (H5Aread(attr_id, H5T_NATIVE_LLONG, &llVal) >= 0)
                value_str = CPLSPrintf("%lld", llVal);
            else
            {
                value_str = "(read error integer)";
            }
        }
        else if (type_class == H5T_FLOAT)
        {
            // Float handling code
            double dfVal = 0.0;
            if (H5Aread(attr_id, H5T_NATIVE_DOUBLE, &dfVal) >= 0)
                value_str = CPLSPrintf("%.18g", dfVal);  // Full precision
            else
            {
                value_str = "(read error float)";
            }
        }
        // Handle Compound Type like GetGDALDataType
        else if (type_class == H5T_COMPOUND)
        {
            hid_t hRealType = -1;
            hid_t hImagType = -1;
            char *name1 = nullptr;
            char *name2 = nullptr;
            bool bIsComplex = false;
            GDALDataType eBaseType =
                GDT_Unknown;  // To store the base type if complex

            if (H5Tget_nmembers(native_type) == 2)
            {
                hRealType = H5Tget_member_type(native_type, 0);
                hImagType = H5Tget_member_type(native_type, 1);

                if (hRealType >= 0 && hImagType >= 0)
                {
                    if (H5Tequal(hRealType, hImagType) > 0)
                    {  // Members must be same type
                        name1 = H5Tget_member_name(native_type, 0);
                        name2 = H5Tget_member_name(native_type, 1);

                        // Check conventional naming ('r'/'i' or 'R'/'I')
                        bool isReal =
                            (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                        bool isImaginary =
                            (name2 && (name2[0] == 'i' || name2[0] == 'I'));

                        if (isReal && isImaginary)
                        {
                            bIsComplex =
                                true;  // Flag that it matches complex structure
                            // Determine the base GDAL type
                            if (H5Tequal(hRealType, H5T_NATIVE_FLOAT) > 0)
                                eBaseType = GDT_Float32;
                            else if (H5Tequal(hRealType, H5T_NATIVE_DOUBLE) > 0)
                                eBaseType = GDT_Float64;
                            else if (H5Tequal(hRealType, H5T_NATIVE_SHORT) > 0)
                                eBaseType = GDT_Int16;
                            else if (H5Tequal(hRealType, H5T_NATIVE_INT) > 0)
                                eBaseType = GDT_Int32;
                            // Add Int8, UInts etc. if needed
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

            // If it matched the complex structure, read and format it
            if (bIsComplex && eBaseType != GDT_Unknown)
            {
                if (eBaseType == GDT_Float32)
                {
                    ComplexFloatAttr cfVal;
                    // Need to create the memory type matching the struct
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(ComplexFloatAttr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexFloatAttr, r),
                              H5T_NATIVE_FLOAT);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexFloatAttr, i),
                              H5T_NATIVE_FLOAT);
                    if (H5Aread(attr_id, mem_type, &cfVal) >= 0)
                    {
                        if (std::isnan(cfVal.r) || std::isnan(cfVal.i))
                        {
                            value_str = "nan";
                        }
                        else
                        {
                            value_str =
                                CPLSPrintf("%.10g + %.10gj", cfVal.r, cfVal.i);
                        }
                    }
                    else
                    {
                        value_str = "(read error complex float)";
                    }
                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Float64)
                {
                    ComplexDoubleAttr cdVal;
                    hid_t mem_type =
                        H5Tcreate(H5T_COMPOUND, sizeof(ComplexDoubleAttr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexDoubleAttr, r),
                              H5T_NATIVE_DOUBLE);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexDoubleAttr, i),
                              H5T_NATIVE_DOUBLE);
                    if (H5Aread(attr_id, mem_type, &cdVal) >= 0)
                    {
                        if (std::isnan(cdVal.r) || std::isnan(cdVal.i))
                        {
                            value_str = "nan";
                        }
                        else
                        {
                            value_str =
                                CPLSPrintf("%.18g + %.18gj", cdVal.r, cdVal.i);
                        }
                    }
                    else
                    {
                        value_str = "(read error complex double)";
                    }
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
                    {
                        value_str = CPLSPrintf("%d + %dj", ciVal.r, ciVal.i);
                    }
                    else
                    {
                        value_str = "(read error complex int16)";
                    }
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
                    {
                        value_str = CPLSPrintf("%d + %dj", ciVal.r, ciVal.i);
                    }
                    else
                    {
                        value_str = "(read error complex int32)";
                    }
                    H5Tclose(mem_type);
                }
                // Add else if for other base types if needed
                else
                {
                    value_str =
                        "(unhandled complex base type)";  // Should not happen if eBaseType != GDT_Unknown
                }
            }
            else
            {
                // It's a compound type, but not one we recognize as complex
                value_str = "(compound data)";
            }
        }
        // END COMPOUND
        else if (type_class == H5T_VLEN)
        {
            // VLEN handling (unchanged placeholder)
            value_str = "(variable-length data)";
        }
    }
    // Handle non-scalar attributes if needed (e.g., small arrays?)
    // else { value_str = "(non-scalar attribute)"; }

    // If we couldn't read the value or it's an unhandled type
    if (value_str.empty())
    {
        // Construct a more informative message if possible
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
            case H5T_COMPOUND:
                class_name = "Compound";
                break;
            case H5T_VLEN:
                class_name = "VLEN";
                break;
            // Add others...
            default:
                break;
        }
        value_str = CPLSPrintf("(unhandled attr: class=%s, points=%lld)",
                               class_name, static_cast<long long>(n_points));
    }

    std::string finalKey;
    if (data->pszPrefix && data->pszPrefix[0] != '\0')
    {
        finalKey = std::string(data->pszPrefix) + "#" + attr_name;
    }
    else
    {
        finalKey = attr_name;
    }

    *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), finalKey.c_str(),
                                          value_str.c_str());

    // Cleanup HDF5 handles
    H5Tclose(native_type);
    H5Tclose(attr_type);
    H5Sclose(attr_space);
    H5Aclose(attr_id);
    return 0;  // Success
}
#endif  // NISAR_PRIV_H
