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
// nisardataset.h

#ifndef NISAR_DATASET_H
#define NISAR_DATASET_H

// S3 / HDF5 Page Buffer Constants
#ifndef NISAR_DEFAULT_PAGE_SIZE
#define NISAR_DEFAULT_PAGE_SIZE 4194304  // 4 MiB (Standard NISAR Page)
#endif

// We want enough pages to hold a "spatial working set."
// 32 pages * 4 MiB = 128 MiB total buffer.
#define NISAR_DEFAULT_PAGE_COUNT 32

// HDF5 Chunk Cache (RDCC) Constants
// This is for uncompressed pixels. 512 MB is a baseline for L2/L3.
#define NISAR_DEFAULT_CHUNK_CACHE_MB 512

// RDCC Slots should be a prime number roughly 10-100x the number of chunks in a cache.
// 524287 is a large prime that should work well for NISAR's massive grids.
#define NISAR_DEFAULT_CHUNK_CACHE_SLOTS 524287

#include <string>
#include <vector>
#include <array>
#include <map>
#include <mutex>
#include <cstring>  // For memcpy

#include "cpl_string.h"
#include "cpl_error.h"
#include "cpl_list.h"
#include "cpl_conv.h"  // For CPLsetenv, CPLMalloc, etc.
#include "ogr_spatialref.h"
#include "hdf5.h"
#include "gdal_pam.h"
#include "gdal_priv.h"
#include "gdal.h"  // Include GDAL header for CPLErr and error codes
#include "gdal_version.h"

// GDAL 3.13 changed GDALMajorObject::GetMetadata() to return CSLConstList.
#if GDAL_VERSION_NUM >= GDAL_COMPUTE_VERSION(3, 13, 0)
using NisarMetadataList = CSLConstList;
#else
using NisarMetadataList = char **;
#endif

class NisarRasterBand;

// Nearest ancestor group of sPath (its parent first) holding every named member; "" if none.
std::string NisarFindGroupUpward(hid_t hFile, const std::string &sPath,
                                 const std::vector<const char *> &apszNames);

// DEBUGGING: PRINT GDAL VERSION VALUES
// This uses a helper macro to convert numbers to strings for printing
//#define STR_HELPER(x) #x
//#define STR(x) STR_HELPER(x)

//#ifdef GDAL_VERSION_NUM
//    #pragma message("-------------------------------------------------------")
//    #pragma message("DEBUG: GDAL_VERSION_NUM   = " STR(GDAL_VERSION_NUM))
//    #pragma message("DEBUG: GDAL_VERSION_MAJOR = " STR(GDAL_VERSION_MAJOR))
//    #pragma message("DEBUG: GDAL_VERSION_MINOR = " STR(GDAL_VERSION_MINOR))
//    #pragma message("-------------------------------------------------------")
//#else
//    #pragma message("-------------------------------------------------------")
//    #pragma message("ERROR: GDAL_VERSION_NUM is NOT defined!")
//    #pragma message("-------------------------------------------------------")
//#endif

// Compatibility shim for GDAL < 3.12
// GDALGeoTransform class was introduced in 3.12. Before that, it was just raw double[6].
// We typedef it to std::array so .data() and operator[] work in this implementation.
// We check Major/Minor directly to avoid macro expansion issues with GDAL_VERSION_NUM
#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
#pragma message("-> ACTIVATING LEGACY TYPEDEF for GDALGeoTransform")
using GDALGeoTransform = std::array<double, 6>;
#else
#pragma message("-> SKIPPING LEGACY TYPEDEF (Assuming GDAL 3.12+ Native Class)")
#endif

/**************************************************************************/
/* ====================================================================   */
/*                              NisarDataset                             */
/* ====================================================================   */
/* This class inherits from GDALPamDataset and represents a NISAR HDF5    */
/* dataset. It stores handles to the HDF5 file and the main data dataset. */
/* It also includes methods for opening the dataset, retrieving metadata, */
/* and managing subdatasets.                                              */
/**************************************************************************/

class NisarDataset final : public GDALPamDataset
{
    friend class NisarRasterBand;
    CPL_DISALLOW_COPY_ASSIGN(NisarDataset)

    // Core HDF5 Handles & Info (Declare first)
    hid_t hHDF5 = -1;
    hid_t hDataset = -1;
    char *pszFilename = nullptr;
    GDALDataType eDataType = GDT_Unknown;  // Has default initializer
    char **papszSubDatasets = nullptr;     // Has default initializer

    // Caching Flags (Declare together)
    mutable bool m_bGotSRS = false;  //Flag indicating if SRS was fetched
    mutable bool m_bGotGlobalMetadata = false;
    mutable bool m_bGotMetadata = false;  // Flag for default domain HDF5 read
    // GeoTransform Caching
    mutable bool m_bGotGeoTransform = false;
    mutable double m_adfGeoTransform[6] = {0.0, 1.0, 0.0, 0.0, 0.0, 1.0};
    mutable std::mutex m_GeoTransformMutex{};
    //
    // Cached Objects / Data (Declare together)
    mutable OGRSpatialReference *m_poSRS = nullptr;  // Cached SRS object
    // Cached list for global attrs
    mutable char **m_papszGlobalMetadata = nullptr;
    // Cached h5dump-style listing (NISAR_DUMP domain)
    mutable bool m_bGotDumpMetadata = false;
    mutable char **m_papszDumpMetadata = nullptr;

    // Mutexes (Declare together, last among cached members)
    mutable std::mutex m_SRSMutex{};
    mutable std::mutex m_GlobalMetadataMutex{};
    mutable std::mutex m_MetadataMutex{};
    mutable std::mutex m_DumpMetadataMutex{};

    // Product identification
    std::string m_sProductType{};  // e.g., "GSLC", "RSLC"
    bool m_bIsLevel1 = false;
    bool m_bIsLevel2 = false;
    bool m_bIsLevel3 = false;

    // Open options used
    std::string m_sInst{};        // LSAR or SSAR
    std::string m_sFreq{};        // A or B
    std::string m_sPol{};         // HH, HV, etc.
    bool m_bMaskEnabled = false;  //Default to NO
    bool m_bDumpEnabled = false;  // DUMP=YES
    std::string m_sDumpRoot{};    // DUMP_ROOT (HDF5 group path)
    bool m_bDumpFull = false;     // DUMP_MODE=FULL

  private:  // Keep static helpers private if only used internally
    struct MetadataCategory
    {
        std::string sHDF5Path{};
        std::string sGDALDomain{};
    };

    std::map<std::string, MetadataCategory> m_oMetadataMap{};

    void InitializeMetadataMap();
    void LoadMetadataDomain(const std::string &sKeyword);

    // Static callback for H5Ovisit
    static herr_t MetadataVisitCallback(hid_t hObject, const char *name,
                                        const H5O_info2_t *info, void *op_data);

    bool LoadDumpMetadata();
    // H5Lvisit callback: emits a record per link (object, soft/external link
    // or hard-link alias) and dispatches objects to DumpObject.
    static herr_t DumpLinkCallback(hid_t hGroup, const char *name,
                                   const H5L_info2_t *info, void *op_data);
    static herr_t DumpObject(hid_t hLoc, const char *name,
                             const H5O_info2_t *info, void *op_data);

    void ReadIdentificationMetadata();
    std::string ReadHDF5StringArrayAsList(hid_t hParentGroup,
                                          const char *pszDatasetName);
    std::string ReadHDF5StringDataset(hid_t hParentGroup,
                                      const char *pszDatasetName);

    CPLErr ReadGeoTransformAttribute(hid_t hObjectID, const char *pszAttrName,
                                     GDALGeoTransform &gt) const;

  public:
    NisarDataset();
    ~NisarDataset() override;

    static int Identify(GDALOpenInfo *poOpenInfo);
    static GDALDataType GetGDALDataType(hid_t hH5Type);

    static GDALDataset *Open(GDALOpenInfo *);

#if GDAL_VERSION_MAJOR < 3 ||                                                  \
    (GDAL_VERSION_MAJOR == 3 && GDAL_VERSION_MINOR < 12)
    // Legacy Signature
    CPLErr GetGeoTransform(double *padfTransform) override;
#else
    CPLErr GetGeoTransform(GDALGeoTransform &gt) const override;
#endif

    // Public Getters needed by NisarRasterBand (or using friend)
    hid_t GetHDF5Handle() const
    {
        return hHDF5;
    }

    hid_t GetDatasetHandle() const
    {
        return hDataset;
    }

    // Product identification (from /science/<INST>/identification)
    const std::string &GetInstrument() const
    {
        return m_sInst;
    }

    const std::string &GetProductType() const
    {
        return m_sProductType;
    }

    bool IsLevel1() const
    {
        return m_bIsLevel1;
    }

    bool IsLevel2() const
    {
        return m_bIsLevel2;
    }

    bool IsLevel3() const
    {
        return m_bIsLevel3;
    }

    //virtual CPLErr GetRasterBand( int nBand, GDALRasterBand ** ppBand );
    char **GetMetadataDomainList() override;
    NisarMetadataList GetMetadata(const char *pszDomain = "") override;
    const OGRSpatialReference *GetSpatialRef() const override;

    //const OGRSpatialReference *GetSpatialRef() const override;
    CPLErr GenerateGCPsFromGeolocationGrid(const char *pszProductGroup,
                                           const char *pszRasterPath);
    char **GetFileList() override;
};
#endif  //NISAR_DATASET_H
