# NISAR (NASA-ISRO SAR) HDF5 products.
# In-tree GDAL build: copy this file to <gdal>/frmts/nisar/CMakeLists.txt.
# The standalone plugin build uses CMakeLists.txt in this directory.
add_gdal_driver(TARGET gdal_NISAR
                SOURCES nisar.cpp
                        nisardataset.cpp
                        nisardataset.h
                        nisarrasterband.cpp
                        nisarrasterband.h
                        nisaroverviewband.h
                        nisarinterpolated.cpp
                        nisarinterpolated.h
                        nisarinterpolatedrasterband.cpp
                        nisarinterpolatedrasterband.h
                        nisar_priv.h
                        hdf5vfl.cpp
                        hdf5vfl.h
                PLUGIN_CAPABLE_IF "NOT GDAL_USE_ZLIB_INTERNAL"
                SKIP_GDAL_PRIV_HEADER  # Avoid issues with -D_POSIX_C_SOURCE redef with HDF5
)

if(NOT TARGET gdal_NISAR)
    return()
endif()

gdal_standard_includes(gdal_NISAR)
# nisarinterpolated.cpp includes vrtdataset.h
target_include_directories(gdal_NISAR PRIVATE ${GDAL_RASTER_FORMAT_SOURCE_DIR}/vrt)

if (DEFINED HDF5_DEFINITIONS)
  target_compile_definitions(gdal_NISAR PRIVATE ${HDF5_DEFINITIONS})
endif ()
if (WIN32)
  target_compile_definitions(gdal_NISAR PRIVATE -DWIN32)
endif ()
target_include_directories(gdal_NISAR SYSTEM PRIVATE ${HDF5_INCLUDE_DIRS})
gdal_target_link_libraries(gdal_NISAR PRIVATE ${HDF5_C_LIBRARIES})

if (HDF5_BUILD_SHARED_LIBS)
  target_compile_definitions(gdal_NISAR PRIVATE -DH5_BUILT_AS_DYNAMIC_LIB)
else ()
  target_compile_definitions(gdal_NISAR PRIVATE -DH5_BUILT_AS_STATIC_LIB)
endif ()

if (GDAL_USE_ZLIB_INTERNAL)
  gdal_add_vendored_lib(gdal_NISAR libz)
elseif (GDAL_USE_ZLIB)
  gdal_target_link_libraries(gdal_NISAR PRIVATE ZLIB::ZLIB)
endif ()
