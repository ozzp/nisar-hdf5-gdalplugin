// nisarrasterband.h
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

#ifndef NISAR_RASTER_BAND_H
#define NISAR_RASTER_BAND_H

#include <mutex>
#include "gdal_pam.h"
#include "gdal_priv.h"

#include "hdf5.h"

class NisarDataset;

/************************************************************************/
/* ==================================================================== */
/*                            NisarRasterBand                           */
/* ==================================================================== */
/* This class inherits from GDALPamRasterBand and represents a single band */
/* within the NISAR dataset. It includes methods for reading           */
/* data blocks (IReadBlock) and retrieving NoData values (GetNoDataValue). */
/************************************************************************/

class NisarRasterBand final : public GDALPamRasterBand
{
    friend class NisarDataset;

  private:
    hid_t hH5Type = -1;  // Store copy of HDF5 native data type
    // Cached HDF5 handles
    hid_t m_hFileSpaceID = -1;  // Cached filespace for the HDF5 dataset
    hid_t m_hMemSpaceID = -1;   // Cached memory space for a full block

    // Metadata caching members for this band
    mutable char **m_papszMetadata =
        nullptr;  // Cached merged metadata (includes HDF5 attrs)
    mutable bool m_bMetadataRead =
        false;  // Flag: Have we read HDF5 attrs for default domain?
    mutable std::mutex m_MetadataMutex;  // Mutex for metadata access

  public:
    NisarRasterBand(NisarDataset *poDSIn, int nBandIn, hid_t hDatasetID,
                    hid_t hH5DatasetType);
    NisarRasterBand(NisarDataset *poDS, int nBand);
    virtual ~NisarRasterBand() override;
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff,
                              void *pImage) override;
    double GetNoDataValue(int *pbSuccess = nullptr) override;
    // Add Metadata override
    char **GetMetadata(const char *pszDomain = "") override;

    // Add getter if needed outside
    // hid_t GetH5Type() const { return hH5Type; }

    //virtual GDALRasterBand *GetOverview( int nOverviewIndex ) override;
    //virtual int GetOverviewCount() override;
    //GDALColorInterp GetColorInterpretation() override;
    // virtual CPLErr SetColorInterpretation( GDALColorInterp eColorInterp ) override;

    // virtual CPLErr SetNoDataValue( double dfNoData ) override;

    //virtual GDALDataType GetRasterDataType(void) const override;
};

#endif  // NISAR_RASTER_BAND_H
