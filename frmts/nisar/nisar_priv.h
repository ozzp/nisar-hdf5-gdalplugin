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

#ifndef NISAR_PRIV_H
#define NISAR_PRIV_H

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <limits>
#include <complex>  // Added for native complex number mapping
#include <cmath>

#include "hdf5.h"
#include "cpl_string.h"  // For CSLSetNameValue, CPLDebug, CPLError
#include "cpl_conv.h"    // For CPLStrdup, CPLFree
#include "cpl_error.h"
#include "gdal_priv.h"
#include "gdal_version.h"
#include "gdal.h"  // For CE_Failure etc.

// Define the logic strategy for the mask
enum class NisarMaskType
{
    GCOV,  // Logic: 1-5 Valid; 0, 255 Invalid
    GUNW   // Logic: Digit parsing (Ref != 0 && Sec != 0)
};

class NisarHDF5MaskBand final : public GDALRasterBand
{
    hid_t m_hMaskDS;
    hid_t m_hMaskFileSpaceID;  //Cached dataspace handle
    NisarMaskType m_eType;     // Store the logic type

  public:
    NisarHDF5MaskBand(NisarDataset *poDS, hid_t hMaskDS, NisarMaskType eType);
    virtual ~NisarHDF5MaskBand();
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff,
                              void *pImage) override;
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

    if (H5Iget_name(hObjectID, &sName[0], nNameLen + 1) < 0)
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

#endif  // NISAR_PRIV_H
