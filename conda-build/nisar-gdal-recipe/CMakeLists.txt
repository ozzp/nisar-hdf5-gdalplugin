# Set minimum CMake version and define the project.
cmake_minimum_required(VERSION 3.16)
project(gdal-driver-nisar CXX)

find_package(ZLIB REQUIRED)

# GDAL (3.9+) requires C++17.
# Without this, including gdal_priv.h will fail.
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the necessary dependencies.
find_package(GDAL REQUIRED)
find_package(HDF5 REQUIRED)

# Use the standard command to create a plugin module.
add_library(gdal_NISAR MODULE
    nisar.cpp
    nisardataset.cpp
    nisarrasterband.cpp
    nisarinterpolated.cpp
    nisarinterpolatedrasterband.cpp
    hdf5vfl.cpp
)

# On macOS, GDAL plugins MUST have a .dylib extension.
if(APPLE)
    set_target_properties(gdal_NISAR PROPERTIES SUFFIX ".dylib")
endif()

# Tell CMake NOT to prepend "lib" to the filename.
# GDAL expects "gdal_NISAR.dylib", not "libgdal_NISAR.dylib".
set_target_properties(gdal_NISAR PROPERTIES PREFIX "")

# Ensure that the @rpath inside our .dylib points correctly to the Conda
# $PREFIX/lib where libgdal and libhdf5 live
set_target_properties(gdal_NISAR PROPERTIES Build_With_Install_RPATH TRUE)

# ---------------------------------------------------------
# ZLIB-NG SIMD INTEGRATION (With Graceful Fallback)
# ---------------------------------------------------------
find_path(ZLIB_NG_INCLUDE_DIR NAMES zlib-ng.h)
find_library(ZLIB_NG_LIBRARY NAMES z-ng zlib-ng)

if(ZLIB_NG_INCLUDE_DIR AND ZLIB_NG_LIBRARY)
    message(STATUS "SUCCESS: Found zlib-ng at ${ZLIB_NG_LIBRARY}. Enabling SIMD acceleration.")
    
    target_include_directories(gdal_NISAR PRIVATE ${ZLIB_NG_INCLUDE_DIR})
    target_link_libraries(gdal_NISAR PUBLIC GDAL::GDAL HDF5::HDF5 ${ZLIB_NG_LIBRARY})
    target_compile_definitions(gdal_NISAR PRIVATE GDAL_COMPILATION USE_ZLIB_NG)
else()
    message(STATUS "WARNING: zlib-ng not found! Falling back to standard zlib.")
    
    find_package(ZLIB REQUIRED)
    target_link_libraries(gdal_NISAR PUBLIC GDAL::GDAL HDF5::HDF5 ZLIB::ZLIB)
    target_compile_definitions(gdal_NISAR PRIVATE GDAL_COMPILATION)
endif()
# ---------------------------------------------------------

# Install the compiled plugin to the correct GDAL plugin directory.
install(TARGETS gdal_NISAR
        LIBRARY DESTINATION lib/gdalplugins)
