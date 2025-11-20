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
    poDriver->SetMetadataItem( GDAL_DCAP_RASTER, "YES" );
    poDriver->SetMetadataItem( GDAL_DMD_LONGNAME,
                               "NISAR HDF5" );
    poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC,
                               "drivers/raster/nisar.html" );
    poDriver->SetMetadataItem( GDAL_DMD_EXTENSION, "h5" );

    poDriver->pfnOpen = NisarDataset::Open;

    poDriver->pfnIdentify = NisarDataset::Identify;

    GetGDALDriverManager()->RegisterDriver( poDriver );
}
