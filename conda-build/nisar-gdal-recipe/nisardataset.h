// nisardataset.h
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

#ifndef NISAR_DATASET_H
#define NISAR_DATASET_H

#include <string>
#include <vector>
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

/**************************************************************************/
/* ====================================================================   */
/*                              NisarDataset                             */
/* ====================================================================   */
/* This class inherits from GDALPamDataset and represents a NISAR HDF5    */
/* dataset. It stores handles to the HDF5 file and the main data dataset. */
/* It also includes methods for opening the dataset, retrieving metadata, */
/* and managing subdatasets.                                              */
/**************************************************************************/

class NisarRasterBand;

class NisarDataset : public GDALPamDataset
{
    friend class NisarRasterBand;

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
    mutable double m_adfGeoTransform[6]; 
    mutable std::mutex m_GeoTransformMutex;
    //
    // Cached Objects / Data (Declare together)
    mutable OGRSpatialReference *m_poSRS = nullptr;  // Cached SRS object
    // Cached list for global attrs
    mutable char **m_papszGlobalMetadata = nullptr;

    // Mutexes (Declare together, last among cached members)
    mutable std::mutex m_SRSMutex;
    mutable std::mutex m_GlobalMetadataMutex;
    mutable std::mutex m_MetadataMutex;

    // Product identification
    std::string m_sProductType; // e.g., "GSLC", "RSLC"
    bool m_bIsLevel1 = false;
    bool m_bIsLevel2 = false;

    // Open options used
    std::string m_sInst; // LSAR or SSAR
    std::string m_sFreq; // A or B
    std::string m_sPol;  // HH, HV, etc.
    bool m_bMaskEnabled = true; //Default to YES

  private:  // Keep static helpers private if only used internally
    struct MetadataCategory {
        std::string sHDF5Path;      
        std::string sGDALDomain;    
    };
    std::map<std::string, MetadataCategory> m_oMetadataMap;

    void InitializeMetadataMap();
    void LoadMetadataDomain(const std::string& sKeyword);

    // Static callback for H5Ovisit
    static herr_t MetadataVisitCallback(hid_t hObject, const char *name, const H5O_info2_t *info, void *op_data);

    void ReadIdentificationMetadata();
    std::string ReadHDF5StringArrayAsList(hid_t hParentGroup, const char *pszDatasetName);
    std::string ReadHDF5StringDataset(hid_t hParentGroup, const char *pszDatasetName);
    static GDALDataType GetGDALDataType(hid_t hH5Type);
    CPLErr GetGeoTransform_Logic(GDALGeoTransform &gt);
    CPLErr ReadGeoTransformAttribute(hid_t hObjectID, const char *pszAttrName,
                                     GDALGeoTransform &gt) const;

  public:
    NisarDataset();
    ~NisarDataset() override;

    static int Identify(GDALOpenInfo *poOpenInfo);

    static GDALDataset *Open(GDALOpenInfo *);

    // Public Getters needed by NisarRasterBand (or using friend)
    hid_t GetHDF5Handle() const
    {
        return hHDF5;
    }

    hid_t GetDatasetHandle() const
    {
        return hDataset;
    }

    //virtual CPLErr GetRasterBand( int nBand, GDALRasterBand ** ppBand );
    char **GetMetadataDomainList() override;
    char **GetMetadata(const char *pszDomain = "") override;
    //CPLErr GetGeoTransform( double * padfTransform ) override;
    CPLErr GetGeoTransform(GDALGeoTransform &gt) const override;
    const OGRSpatialReference *GetSpatialRef() const override;

    //const OGRSpatialReference *GetSpatialRef() const override;
    CPLErr GenerateGCPsFromGeolocationGrid(const char *pszProductGroup);
    char **GetFileList() override;
};

#endif  //NISAR_DATASET_H
