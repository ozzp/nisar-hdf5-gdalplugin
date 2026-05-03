#ifndef NISAR_PRIV_H
#define NISAR_PRIV_H

#include "hdf5.h"
#include "cpl_string.h"  // For CSLSetNameValue, CPLDebug, CPLError
#include "cpl_conv.h"    // For CPLStrdup, CPLFree
#include "cpl_error.h"
#include "gdal_priv.h"
#include "gdal_version.h"
#include "gdal.h"  // For CE_Failure etc.

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <limits>
#include <complex> // Added for native complex number mapping
#include <cmath>

// Define the logic strategy for the mask
enum class NisarMaskType {
    GCOV, // Logic: 1-5 Valid; 0, 255 Invalid
    GUNW  // Logic: Digit parsing (Ref != 0 && Sec != 0)
};

class NisarHDF5MaskBand final: public GDALRasterBand
{
    hid_t m_hMaskDS; 
    hid_t m_hMaskFileSpaceID; //Cached dataspace handle
    NisarMaskType m_eType; // Store the logic type

public:
    NisarHDF5MaskBand(NisarDataset* poDS, hid_t hMaskDS, NisarMaskType eType);
    virtual ~NisarHDF5MaskBand();
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff, void *pImage) override;
};

/**
 * Gets the full HDF5 path of an object from its handle (hid_t).
 */
static inline std::string get_hdf5_object_name(hid_t hObjectID)
{
    if (hObjectID < 0)
    {
        return "";  // Return empty for invalid handle
    }

    ssize_t nNameLen = H5Iget_name(hObjectID, nullptr, 0);
    if (nNameLen <= 0)
    {
        return "";
    }

    std::string sName;
    sName.resize(nNameLen);

    if (H5Iget_name(hObjectID, &sName, nNameLen + 1) < 0)
    {
        CPLError(CE_Warning, CPLE_AppDefined,
                 "H5Iget_name failed to retrieve object name.");
        return "";  
    }

    return sName;
}

// Struct to pass data to the callback
struct NISAR_AttrCallbackData
{
    char ***ppapszList;
    const char *pszPrefix;
};

// Legacy fallback structs for integer complexes (std::complex<int> isn't standardized for layout)
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
        return 0; 
    }

    hid_t attr_id = -1;
    hid_t attr_type = -1;
    hid_t native_type = -1; 
    hid_t attr_space = -1;
    std::string value_str;

    attr_id = H5Aopen_by_name(hLocation, ".", attr_name, H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0) return 0;

    attr_type = H5Aget_type(attr_id);
    attr_space = H5Aget_space(attr_id);

    if (attr_type < 0 || attr_space < 0)
    {
        if (attr_type >= 0) H5Tclose(attr_type);
        if (attr_space >= 0) H5Sclose(attr_space);
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
                else value_str = "(read error VL string)";
            }
            else
            {  // Fixed length string
                size_t type_size = H5Tget_size(native_type);
                if (type_size > 0)
                {
                    char *pszReadFixed = (char *)VSI_MALLOC_VERBOSE(type_size + 1);
                    if (pszReadFixed)
                    {
                        if (H5Aread(attr_id, native_type, pszReadFixed) >= 0)
                        {
                            pszReadFixed[type_size] = '\0';
                            value_str = pszReadFixed;
                        }
                        else value_str = "(read error fixed string)";
                        
                        VSIFree(pszReadFixed);
                    }
                    else value_str = "(memory alloc error)";
                }
                else value_str = "(zero size fixed string)";
            }
        }
        else if (type_class == H5T_INTEGER)
        {
            long long llVal = 0;
            if (H5Aread(attr_id, H5T_NATIVE_LLONG, &llVal) >= 0)
                value_str = CPLSPrintf("%lld", llVal);
            else value_str = "(read error integer)";
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
                        if (std::isnan(cfVal.real()) || std::isnan(cfVal.imag())) value_str = "nan";
                        else value_str = CPLSPrintf("%.10g + %.10gj", cfVal.real(), cfVal.imag());
                    }
                    else value_str = "(read error native complex float)";
                }
                else if (H5Tequal(base_type, H5T_NATIVE_DOUBLE) > 0)
                {
                    std::complex<double> cdVal;
                    if (H5Aread(attr_id, H5T_NATIVE_DOUBLE_COMPLEX, &cdVal) >= 0)
                    {
                        if (std::isnan(cdVal.real()) || std::isnan(cdVal.imag())) value_str = "nan";
                        else value_str = CPLSPrintf("%.18g + %.18gj", cdVal.real(), cdVal.imag());
                    }
                    else value_str = "(read error native complex double)";
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
                        bool isReal = (name1 && (name1[0] == 'r' || name1[0] == 'R'));
                        bool isImaginary = (name2 && (name2[0] == 'i' || name2[0] == 'I'));

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
                    if (hRealType >= 0) H5Tclose(hRealType);
                    if (hImagType >= 0) H5Tclose(hImagType);
                }
                
                if (name1) H5free_memory(name1);
                if (name2) H5free_memory(name2);
            }

            if (bIsComplex && eBaseType != GDT_Unknown)
            {
                if (eBaseType == GDT_Float32)
                {
                    std::complex<float> cfVal;
                    hid_t mem_type = H5Tcreate(H5T_COMPOUND, sizeof(std::complex<float>));
                    H5Tinsert(mem_type, "r", 0, H5T_NATIVE_FLOAT);
                    H5Tinsert(mem_type, "i", sizeof(float), H5T_NATIVE_FLOAT);
                    if (H5Aread(attr_id, mem_type, &cfVal) >= 0)
                    {
                        if (std::isnan(cfVal.real()) || std::isnan(cfVal.imag())) value_str = "nan";
                        else value_str = CPLSPrintf("%.10g + %.10gj", cfVal.real(), cfVal.imag());
                    }
                    else value_str = "(read error compound complex float)";
                    
                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Float64)
                {
                    std::complex<double> cdVal;
                    hid_t mem_type = H5Tcreate(H5T_COMPOUND, sizeof(std::complex<double>));
                    H5Tinsert(mem_type, "r", 0, H5T_NATIVE_DOUBLE);
                    H5Tinsert(mem_type, "i", sizeof(double), H5T_NATIVE_DOUBLE);
                    if (H5Aread(attr_id, mem_type, &cdVal) >= 0)
                    {
                        if (std::isnan(cdVal.real()) || std::isnan(cdVal.imag())) value_str = "nan";
                        else value_str = CPLSPrintf("%.18g + %.18gj", cdVal.real(), cdVal.imag());
                    }
                    else value_str = "(read error compound complex double)";
                    
                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Int16)
                {
                    ComplexInt16Attr ciVal;
                    hid_t mem_type = H5Tcreate(H5T_COMPOUND, sizeof(ComplexInt16Attr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexInt16Attr, r), H5T_NATIVE_SHORT);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexInt16Attr, i), H5T_NATIVE_SHORT);
                    if (H5Aread(attr_id, mem_type, &ciVal) >= 0)
                        value_str = CPLSPrintf("%d + %dj", ciVal.r, ciVal.i);
                    else
                        value_str = "(read error complex int16)";
                    
                    H5Tclose(mem_type);
                }
                else if (eBaseType == GDT_Int32)
                {
                    ComplexInt32Attr ciVal;
                    hid_t mem_type = H5Tcreate(H5T_COMPOUND, sizeof(ComplexInt32Attr));
                    H5Tinsert(mem_type, "r", HOFFSET(ComplexInt32Attr, r), H5T_NATIVE_INT);
                    H5Tinsert(mem_type, "i", HOFFSET(ComplexInt32Attr, i), H5T_NATIVE_INT);
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
            case H5T_INTEGER: class_name = "Integer"; break;
            case H5T_FLOAT: class_name = "Float"; break;
            case H5T_STRING: class_name = "String"; break;
#ifdef H5T_COMPLEX
            case H5T_COMPLEX: class_name = "Complex"; break;
#endif
            case H5T_COMPOUND: class_name = "Compound"; break;
            case H5T_VLEN: class_name = "VLEN"; break;
            default: break;
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
#endif  // NISAR_PRIV_H
