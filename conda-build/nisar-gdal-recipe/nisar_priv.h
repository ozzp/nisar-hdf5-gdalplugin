#ifndef NISAR_PRIV_H
#define NISAR_PRIV_H

#include "hdf5.h"
#include "cpl_string.h"  // For CSLSetNameValue, CPLDebug, CPLError
#include "cpl_conv.h"    // For CPLStrdup, CPLFree
#include "gdal.h"        // For CE_Failure etc.

#include <string>
#include <sstream>
#include <iomanip>
#include <limits>

// Struct to pass data to the callback
struct NISAR_AttrCallbackData {
    char ***ppapszList;
};

// Static helper function to read an HDF5 attribute and add it to a CSL list
static herr_t NISAR_AttributeCallback(hid_t loc_id, const char *attr_name,
                                      const H5A_info_t * /*ainfo*/, void *op_data)
{
    NISAR_AttrCallbackData *data = static_cast<NISAR_AttrCallbackData*>(op_data);
    hid_t attr_id = -1;
    hid_t attr_type = -1;
    hid_t attr_space = -1;
    std::string value_str;
    herr_t status = -1;
    bool bValueSet = false;

    attr_id = H5Aopen_by_name(loc_id, ".", attr_name, H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0) return 0;

    attr_type = H5Aget_type(attr_id);
    if (attr_type < 0) { H5Aclose(attr_id); return 0; }

    attr_space = H5Aget_space(attr_id);
    if (attr_space < 0) { H5Tclose(attr_type); H5Aclose(attr_id); return 0; }

    H5T_class_t type_class = H5Tget_class(attr_type);
    hssize_t n_points = H5Sget_simple_extent_npoints(attr_space);
    size_t type_size = H5Tget_size(attr_type);

    if (n_points <= 0) {
        value_str = "";
        bValueSet = true;
    } else if (type_class == H5T_STRING) {
        char *pszReadVL = nullptr;
        char *pszReadFixed = nullptr;
        bool bIsVariable = H5Tis_variable_str(attr_type);

        if(bIsVariable) {
            status = H5Aread(attr_id, attr_type, &pszReadVL);
            if(status >= 0 && pszReadVL != nullptr) {
                 value_str = pszReadVL;
                 bValueSet = true;
                 H5free_memory(pszReadVL); // H5free_memory is fine for single VLEN reads
            }
        } else if (type_size > 0 && n_points == 1) {
            pszReadFixed = (char *)VSIMalloc(type_size + 1);
            if (pszReadFixed) {
                 status = H5Aread(attr_id, attr_type, pszReadFixed);
                 if (status >= 0) {
                      pszReadFixed[type_size] = '\0';
                      value_str = pszReadFixed;
                      bValueSet = true;
                 }
                 VSIFree(pszReadFixed);
            }
        }
    } else if (type_class == H5T_INTEGER && n_points == 1) {
        long long llVal = 0;
        status = H5Aread(attr_id, H5T_NATIVE_LLONG, &llVal);
        if (status >= 0) { value_str = CPLSPrintf("%lld", llVal); bValueSet = true; }
    } else if (type_class == H5T_FLOAT && n_points == 1) {
         double dfVal = 0.0;
         status = H5Aread(attr_id, H5T_NATIVE_DOUBLE, &dfVal);
         if (status >= 0) {
            std::ostringstream oss;
            oss << std::scientific << std::setprecision(std::numeric_limits<double>::max_digits10);
            oss << dfVal;
            value_str = oss.str();
            bValueSet = true;
         }
    } else {
        value_str = CPLSPrintf("(unhandled attribute type: class %d, %lld points)",
                                type_class, static_cast<long long>(n_points));
        bValueSet = true;
    }

    if (bValueSet) {
        *(data->ppapszList) = CSLSetNameValue(*(data->ppapszList), attr_name, value_str.c_str());
    }

    H5Tclose(attr_type);
    H5Sclose(attr_space);
    H5Aclose(attr_id);
    return 0;
}

#endif // NISAR_PRIV_H
