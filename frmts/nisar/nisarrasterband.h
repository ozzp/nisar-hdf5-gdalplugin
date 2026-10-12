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
// nisarrasterband.h

#ifndef NISAR_RASTER_BAND_H
#define NISAR_RASTER_BAND_H

#include <mutex>
#include <cmath>  // for std::isnan

#include <zlib.h>  //Deflate decompression

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
    CPL_DISALLOW_COPY_ASSIGN(NisarRasterBand)

  private:
    bool m_bChunksMapped = false;
    bool m_bValid = true;
    std::mutex m_oChunkMapMutex{};
    void MapChunks();
    std::mutex m_oMutex{};  // Protects shared VSI file pointers
    std::mutex m_oMegaFetchMutex{};
    VSILFILE *m_fp = nullptr;  // shared file pointer opened in the Dataset
    hid_t hH5Type = -1;        // Store copy of HDF5 native data type
    // Cached HDF5 handles
    hid_t m_hFileSpaceID = -1;  // Cached filespace for the HDF5 dataset
    hid_t m_hMemSpaceID = -1;   // Cached memory space for a full block

    bool m_bIsDeflated = false;
    int m_nDeflateLevel = 1;
    bool m_bIsShuffled = false;
    bool m_bNeedsEndianSwap = false;

    bool m_bHasMinMax = false;

    std::vector<std::unique_ptr<NisarOverviewBand>> m_apoOverviews{};
    NisarHDF5MaskBand *m_poMaskBand = nullptr;  // Cache the mask band
    bool m_bMaskBandOwned = false;

    struct NisarChunkInfo
    {
        int nBlockX = 0;
        int nBlockY = 0;
        vsi_l_offset nOffset = 0;
        size_t nLength = 0;
        bool bIsMissing = true;
    };

    // The class-level cache for our B-Tree layout
    std::vector<NisarChunkInfo> m_aoAllChunks{};

    bool ProcessAndCopyChunk(const GByte *pSrcData, size_t nSrcSize,
                             void *pDstData);
    std::string GetRawVSIPath() const;
    std::string GetStandardDatasetURI() const;

  public:
    /** False when the constructor could not initialise the band (bad rank,
     *  chunk-map allocation failure); Open() must not install such a band. */
    bool IsValid() const
    {
        return m_bValid;
    }

    NisarRasterBand(NisarDataset *poDSIn, int nBandIn, hid_t hDatasetID,
                    hid_t hH5DatasetType);
    NisarRasterBand(NisarDataset *poDS, int nBand);
    virtual ~NisarRasterBand() override;
    virtual CPLErr IReadBlock(int nBlockXOff, int nBlockYOff,
                              void *pImage) override;

    virtual GDALRasterBand *GetMaskBand() override;
    virtual int GetMaskFlags() override;

    virtual double GetMinimum(int *pbSuccess = nullptr) override;
    virtual double GetMaximum(int *pbSuccess = nullptr) override;
    virtual CPLErr GetStatistics(int bApproxOK, int bForce, double *pdfMin,
                                 double *pdfMax, double *pdfMean,
                                 double *pdfStdDev) override;

    static thread_local bool bDisableOverviewRouting;
    virtual int GetOverviewCount() override;
    virtual GDALRasterBand *GetOverview(int i) override;

    bool WriteVirtualZarrSidecar(
        const std::string &osS3Url,
        const std::string &
            osZarrGroupPath,  // e.g., "science/LSAR/GCOV/grids/frequencyA/HHHH"
        const std::vector<NisarChunkInfo> &aoChunks,
        const std::string &osOutJsonPath);
};

#endif  // NISAR_RASTER_BAND_H
