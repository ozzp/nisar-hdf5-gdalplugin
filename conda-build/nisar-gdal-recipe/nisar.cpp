// nisar.cpp
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

#include "gdal_priv.h"
#include "nisardataset.h"
#include "gdal_version.h"

CPL_C_START
void CPL_DLL GDALRegister_NISAR();
int CPL_DLL GDALGetPluginVersion();
CPL_C_END

// Required version-checking function
int GDALGetPluginVersion()
{
    return GDAL_VERSION_NUM;
}

/************************************************************************/
/*                          GDALRegister_NISAR()                        */
/* Registers the NISAR driver with GDAL, making it available for use.   */
/************************************************************************/

void GDALRegister_NISAR()

{
    if( GDALGetDriverByName( "NISAR" ) != nullptr )
        return;

    GDALDriver *poDriver = new GDALDriver();

    poDriver->SetDescription( "NISAR" );
    if (CPLGetConfigOption("GDAL_HTTP_MAX_RETRY", nullptr) == nullptr) {
        CPLSetConfigOption("GDAL_HTTP_MAX_RETRY", "5");
    }
    poDriver->SetMetadataItem(GDAL_DCAP_VIRTUALIO, "YES");
    // This allows GDAL's internal block cache to be more aggressive
    // with this driver in multi-threaded scenarios.
    poDriver->SetMetadataItem("GDAL_THREAD_SAFE", "YES");
    poDriver->SetMetadataItem("GDAL_RAW_BINARY_LAYOUT", "YES");
    poDriver->SetMetadataItem( "DRIVER_VERSION", "v0.3.1 (Build Date: " __DATE__ " " __TIME__ ")" );
    poDriver->SetMetadataItem( GDAL_DCAP_RASTER, "YES" );
    poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                               "NISAR HDF5" );
    poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                               "drivers/raster/nisar.html" );
    poDriver->SetMetadataItem( GDAL_DMD_EXTENSION, "h5" );
    poDriver->SetMetadataItem( GDAL_DMD_SUBDATASETS, "YES" );
    poDriver->SetMetadataItem(
                               GDAL_DMD_OPENOPTIONLIST,
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
                                  <Option name='QUANTITY' type='string' description='Quantity to interpolate'/>
                                  <Option name='MASK' type='boolean' description='Apply valid data mask (default NO)'/>
                                  </OpenOptionList>)");
    poDriver->pfnOpen = NisarDataset::Open;

    poDriver->pfnIdentify = NisarDataset::Identify;

    GetGDALDriverManager()->RegisterDriver( poDriver );
}
