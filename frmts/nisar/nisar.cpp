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
// nisar.cpp

#include "gdal_priv.h"
#include "nisardataset.h"
#include "gdal_version.h"
#include "hdf5vfl.h"

CPL_C_START
void CPL_DLL GDALRegister_NISAR();
#ifdef NISAR_DRIVER_VERSION
int CPL_DLL GDALGetPluginVersion();
#endif
CPL_C_END

#ifdef NISAR_DRIVER_VERSION
int GDALGetPluginVersion()
{
    return GDAL_VERSION_NUM;
}
#endif

/************************************************************************/
/*                          GDALRegister_NISAR()                        */
/* Registers the NISAR driver with GDAL, making it available for use.   */
/************************************************************************/

void GDALRegister_NISAR()

{
    if (GDALGetDriverByName("NISAR") != nullptr)
        return;

    GDALDriver *poDriver = new GDALDriver();

    poDriver->SetDescription("NISAR");
    poDriver->SetMetadataItem(GDAL_DCAP_VIRTUALIO, "YES");
    // This allows GDAL's internal block cache to be more aggressive
    // with this driver in multi-threaded scenarios.
    poDriver->SetMetadataItem("GDAL_THREAD_SAFE", "YES");
    poDriver->SetMetadataItem("GDAL_RAW_BINARY_LAYOUT", "YES");
#ifdef NISAR_DRIVER_VERSION
    // Only defined by the standalone plugin build (frmts/nisar/CMakeLists.txt).
    poDriver->SetMetadataItem("DRIVER_VERSION",
                              "v" NISAR_DRIVER_VERSION " (Build Date: " __DATE__
                              " " __TIME__ ")");
#endif
    poDriver->SetMetadataItem(GDAL_DCAP_RASTER, "YES");
    poDriver->SetMetadataItem(GDAL_DMD_LONGNAME, "NISAR HDF5");
    poDriver->SetMetadataItem(GDAL_DMD_HELPTOPIC, "drivers/raster/nisar.html");
    poDriver->SetMetadataItem(GDAL_DMD_EXTENSION, "h5");
    poDriver->SetMetadataItem(GDAL_DMD_SUBDATASETS, "YES");
    poDriver->SetMetadataItem(GDAL_DMD_OPENOPTIONLIST,
                              R"(<OpenOptionList>
                                  <Option name='ENABLE_PAGE_BUFFERING' type='boolean' description='Perform discovery pass to align HDF5 page buffering. (Note: Driver defaults to 4MB speculative alignment if NO)' default='NO'/>
                                  <Option name='INST' type='string' description='Instrument to open' default='LSAR'/>
                                  <Option name='FREQ' type='string' description='Frequency band to open' default='A'/>
                                  <Option name='POL' type='string' description='Polarization to open (e.g., HHHH, HH)'/>
                                  <Option name='METADATA' type='string' description='Filter specific metadata domains to load'/>
                                  <Option name='DEM_FILE' type='string' description='Path to DEM for 3D cube interpolation'/>
                                  <Option name='DEM_RESAMPLING' type='string-select' description='DEM interpolation method' default='CUBICSPLINE'>
                                  <Value>NEAREST</Value>
                                  <Value>BILINEAR</Value>
                                  <Value>CUBIC</Value>
                                  <Value>CUBICSPLINE</Value>
                                  </Option>
                                  <Option name='DEM_NODATA_HEIGHT' type='float' description='Height (m) assumed where the DEM is nodata or absent (L1 radar-grid interpolation)' default='0'/>
                                  <Option name='QUANTITY' type='string' description='Metadata-cube quantity to interpolate onto the imaging grid (e.g. incidenceAngle). Resolved under metadata/radarGrid (L2) or metadata/geolocationGrid (L1) when no HDF5 path is given; reference grid follows INST/FREQ/POL'/>
                                  <Option name='MASK' type='boolean' description='Apply valid data mask (default NO)'/>
                                  <Option name='DUMP' type='boolean' description='Advertise the NISAR_DUMP metadata domain in the domain list (gdalinfo -mdd all). Not required for gdalinfo -mdd NISAR_DUMP' default='NO'/>
                                  <Option name='DUMP_ROOT' type='string' description='HDF5 group where the NISAR_DUMP traversal starts (default /science/&lt;INST&gt;; use / for the whole file)'/>
                                  <Option name='DUMP_MODE' type='string-select' description='HEADER lists objects, datatypes, dataspaces and attributes; FULL also prints values of scalars and small 1-D datasets' default='HEADER'>
                                  <Value>HEADER</Value>
                                  <Value>FULL</Value>
                                  </Option>
                                  </OpenOptionList>)");
    poDriver->pfnOpen = NisarDataset::Open;

    poDriver->pfnIdentify = NisarDataset::Identify;

    GetGDALDriverManager()->RegisterDriver(poDriver);
}
