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
#include <cmath> // for std::isnan

#include <zlib.h> //Deflate decompression

#include "gdal_pam.h"
#include "gdal_priv.h"
#include "gdal_version.h"

#include "hdf5.h"
#include "cpl_json.h"
#include "cpl_vsi.h"

class NisarDataset;
class NisarOverviewBand;
class NisarHDF5MaskBand;


/***************************************************************************/
/* ======================================================================  */
/*                            NisarRasterBand                              */
/* ======================================================================  */
/* This class inherits from GDALPamRasterBand and represents a single band */
/* within the NISAR dataset. It includes methods for reading               */
/* data blocks (IReadBlock) and retrieving NoData values (GetNoDataValue). */
/***************************************************************************/

class NisarRasterBand final : public GDALPamRasterBand
{
    friend class NisarDataset;

    private:
      std::mutex m_oMutex; // Protects shared VSI file pointers
      std::mutex m_oMegaFetchMutex;
      VSILFILE* m_fp = nullptr; // shared file pointer opened in the Dataset
      hid_t hH5Type = -1;  // Store copy of HDF5 native data type
      // Cached HDF5 handles
      hid_t m_hFileSpaceID = -1;  // Cached filespace for the HDF5 dataset
      hid_t m_hMemSpaceID = -1;   // Cached memory space for a full block

      bool m_bIsDeflated = false;
      int m_nDeflateLevel = 1;
      bool m_bIsShuffled = false;
      bool m_bNeedsEndianSwap = false;

      bool m_bHasMinMax = false;

      std::vector<std::unique_ptr<NisarOverviewBand>> m_apoOverviews;
      NisarHDF5MaskBand* m_poMaskBand = nullptr; // Cache the mask band
      bool m_bMaskBandOwned = false;

      struct NisarChunkInfo {
          int nBlockX;
          int nBlockY;
          vsi_l_offset nOffset;
          size_t nLength;
          bool bIsMissing;
      };
      
      // The class-level cache for our B-Tree layout
      std::vector<NisarChunkInfo> m_aoAllChunks;
      
      bool ProcessAndCopyChunk(const GByte* pSrcData, size_t nSrcSize, void* pDstData);
      std::string GetRawVSIPath() const;
      std::string GetStandardDatasetURI() const;

  public:
    NisarRasterBand(NisarDataset *poDSIn, int nBandIn, hid_t hDatasetID,
                    hid_t hH5DatasetType);
    NisarRasterBand(NisarDataset *poDS, int nBand);
    virtual ~NisarRasterBand() override;
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff,
                              void *pImage) override;

    virtual GDALRasterBand* GetMaskBand() override;
    virtual int GetMaskFlags() override;

    virtual double GetMinimum(int* pbSuccess = nullptr) override;
    virtual double GetMaximum(int* pbSuccess = nullptr) override;
    virtual CPLErr GetStatistics(int bApproxOK, int bForce,
                                 double *pdfMin, double *pdfMax,
                                 double *pdfMean, double *pdfStdDev) override;

    static thread_local bool bDisableOverviewRouting;
    virtual int GetOverviewCount() override;
    virtual GDALRasterBand* GetOverview(int i) override;

    bool WriteVirtualZarrSidecar(const std::string& osS3Url, 
                                 const std::string& osZarrGroupPath, // e.g., "science/LSAR/GCOV/grids/frequencyA/HHHH"
                                 const std::vector<NisarChunkInfo>& aoChunks,
                                 const std::string& osOutJsonPath);

};

#endif  // NISAR_RASTER_BAND_H
