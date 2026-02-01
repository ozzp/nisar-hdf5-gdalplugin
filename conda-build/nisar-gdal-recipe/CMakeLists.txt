# Set minimum CMake version and define the project.
cmake_minimum_required(VERSION 3.16)
project(gdal-driver-nisar CXX)

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
)

# On macOS, GDAL plugins MUST have a .dylib extension.
# We need to explicitly set this for our module target.
if(APPLE)
    set_target_properties(gdal_NISAR PROPERTIES SUFFIX ".dylib")
endif()

# Tell CMake NOT to prepend "lib" to the filename.
# GDAL expects "gdal_NISAR.dylib", not "libgdal_NISAR.dylib".
set_target_properties(gdal_NISAR PROPERTIES PREFIX "")

# Link your plugin against the GDAL and HDF5 libraries.
target_link_libraries(gdal_NISAR PUBLIC GDAL::GDAL HDF5::HDF5)

# Add compile definitions that were previously handled by add_gdal_driver().
target_compile_definitions(gdal_NISAR PRIVATE GDAL_COMPILATION)

# Install the compiled plugin to the correct GDAL plugin directory.
install(TARGETS gdal_NISAR
        LIBRARY DESTINATION lib/gdalplugins)
