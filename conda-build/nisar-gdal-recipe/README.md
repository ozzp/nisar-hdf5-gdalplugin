# nisar-hdf5-gdalplugin
Development of NISAR HDF5 GDAL plugin
# GDAL Plugin for NISAR HDF5 with Cloud-Optimized Access

Development of a read-only GDAL plugin for NISAR HDF5 data, with a focus on supporting efficient cloud-optimized access.

## GDAL Driver

* **Inheritance:** Inherit from the `GDALDriver` class to handle interaction between GDAL and HDF5 files.
* **Implementation:** Implement as a loadable module for dynamic loading of shared libraries.
* **File Identification:**  Implement logic to identify NISAR HDF5 files. Consider building a dependence on the Standard Product File Naming Scheme.
* **HDF5 Parsing:** Parse HDF5 structure and metadata to identify relevant raster datasets.
* **Partial Reads:** Enable efficient partial reads of data in chunks aligned with defined chunk sizes for optimized cloud access.
* **Metadata Handling:**
    * Extract georeferencing, projections, and other metadata from the HDF5 file.
    * Expose this metadata through the GDAL API.
    * Enable the creation of new metadata and writing to the output format.
* **HTTP Range Requests:** Leverage HTTP GET Range Requests to read and download specific chunks from remote object stores.
* **Overviews:** Support the reading of resampled datasets (overviews).
* **Parallelization:** Explore potential parallelization for reading chunks/pages.
* **GDAL Registration:** Register the new driver with GDAL to enable its use with GDAL utilities (`gdalinfo`, `gdal_translate`, `gdalwarp`, `gdaladdo`, etc.).

## HDF5 Library

* Utilize the HDF5 library (`libhdf5`) for low-level access and manipulation of HDF5 files.
* Refer to the HDF5 documentation for details on reading datasets: [https://support.hdfgroup.org/archive/support/HDF5/doc/RM/RM_H5D.html#Dataset-Read](https://support.hdfgroup.org/archive/support/HDF5/doc/RM/RM_H5D.html#Dataset-Read)
* Make sure to install openssl-devel, curl
* ./configure --prefix=/usr/local/hdf5 --enable-cxx --enable-build-mode=debug --enable-internal-debug=all --enable-trace --enable-profiling --enable-ros3-vfd
* Make sure (Read-Only) S3 VFD: yes
* h5cc -showconfig (To check Configuration post install)
* g++ -o test test.cpp -I/usr/local/hdf5/include -L/usr/local/hdf5/lib -lhdf5 -lhdf5_cpp  (Make sure to set LD_LIBRARY_PATH)

## Driver Implementation Details

* **GDALDataset Subclass(NisarDataset):** A subclass of `GDALDataset` that represents the NISAR HDF5 dataset. It supports opening files from both local storage and AWS S3 (using the HDF5 ROS3 VFD with authentication via environment variables and a two-pass open strategy to optimize page buffering for S3 access). The driver can parse connection strings to open specific HDF5 datasets within the file; if no specific path is given, it performs subdataset discovery by iterating through the HDF5 structure to find and list relevant raster datasets (under /science/LSAR/). When a specific or default dataset is opened, the driver determines its raster properties (dimensions, data type, band count), creates corresponding raster bands, and attempts to set an optimized HDF5 chunk cache. It provides georeferencing information by reading the epsg_code or WKT from a projection dataset and calculating the GeoTransform from coordinate and spacing datasets. Metadata is handled for different domains: the default domain includes attributes from the opened dataset and related coordinate/projection datasets, and a custom "NISAR_GLOBAL" domain reads attributes from the root group. The driver also implements caching for spatial reference and metadata to improve performance on repeated calls.
* **GDALRasterBand Subclass(NisarRasterBand):** A subclass of `GDALRasterBand` that represents raster bands within the dataset.  The NisarRasterBand class, inheriting from GDALPamRasterBand, represents a single band of a NISAR HDF5 raster dataset. Its constructor initializes band-specific properties like the GDAL data type (derived from the parent dataset), the block size (typically set to match the HDF5 dataset's chunk dimensions for efficient I/O), and stores a copy of the HDF5 native data type for use in read operations. The core functionality lies in the overridden IReadBlock method, which calculates the appropriate HDF5 hyperslab (offset and count) corresponding to GDAL's block request, reads the actual pixel data from the associated HDF5 dataset using H5Dread, and correctly handles partial blocks at the raster edges by padding the GDAL-provided buffer with zeros. It also includes a GetMetadata method to read attributes directly attached to its HDF5 dataset and populate the band's default metadata domain, integrating with the PAM system.
* **Driver Registration:** The NISAR GDAL driver is registered through a dedicated function, GDALRegister_NISAR(), which is automatically invoked when GDAL loads the driver plugin. Inside this function, a new GDALDriver object is created and configured with descriptive metadata, its short name ("NISAR"), a longer descriptive name, the common file extension ("h5"), and a help topic path. The static NisarDataset::Open method is assigned to the pfnOpen function pointer of the GDALDriver object, enabling GDAL to call this method when a user attempts to open a file using the "NISAR:" prefix or when GDAL identifies a file as a NISAR product. Finally, this configured GDALDriver object is registered with the global GDAL driver manager, making the NISAR driver available for use within the GDAL.
* **Georeferencing:** Implemented for L2 datasets that support it.
* **Overviews:** TBD: Implement overview support.

## AWS Authentication
* ** AWS Credentials specified as environmental variables: AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN

## GDAL Command Line Interface

* **Metadata** Determine what metadata from input source to have in output's Label.  Determine what metadata to create for output's Label.

## GDAL Sample Commands
* **(Get info for all datasets)** gdalinfo NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5
* **(Get info for specific subdataset)** gdalinfo NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5 -co OPTIONS="BAND=S DATASET=HHHH MASK=ON"
* **(Convert specific subdataset to GTIFF)** gdal_translate -of GTiff NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5 -co OPTIONS="FREQ=A DATASET=HH MASK=ON" output_name.tif
* **(Reproject single Subdataset)** gdalwarp -of GTiff -t_srs EPSG:4326 -co OPTIONS="BAND=S DATASET=HHHH MASK=ON" NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5 output_name.tif
* **(Reproject multiple/all H5 Datasets)** gdalwarp -of GTiff -t_srs EPSG:4326 -co OPTIONS="BAND=ALL DATASET=ALL MASK=ANY" NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5
* **(Clip to a Geographic Extent)** gdalwarp -of GTiff -te xmin ymin xmax ymax -co OPTIONS="BAND=S DATASET=HHHH MASK=ON" NISAR:NISAR_L2_PR_GSLC_001_030_A_019_2000_SHNA_A_20081012T060910_20081012T060926_D00402_N_F_J_001.h5 output.tif
* **(Multiband GeoTIFF)**
* **(Overviews)** gdaladdo
  
## Product Implementation Priority

Prioritize implementing support for the following products:

* L2 GCOV: Example: create Georeferenced longitute-latitude color contour plot using rtcGammaToSigmaFactor raster.  
* L2 GSLC: Example: convert single-band Complex32 dataset into two-band Float32 GTIFF of Amplitude and Phase.
* L2 GOFF:

## Visualization and Validation
* QGIS

## Existing Code Examples

* Mike Smyth's code for the VICAR GDAL plugin:
    *  [https://github.jpl.nasa.gov/Cartography/vicar-gdalplugin](https://github.jpl.nasa.gov/Cartography/vicar-gdalplugin)
    *  [https://github.com/Cartography-jpl/vicar-gdalplugin](https://github.com/Cartography-jpl/vicar-gdalplugin)
* Michael's code for NISAR data readers:
    *  [https://github.com/aivazis/qed/tree/main/pkg/readers/nisar](https://github.com/aivazis/qed/tree/main/pkg/readers/nisar)
    *  [https://github.com/aivazis/qed](https://github.com/aivazis/qed)
    *  [https://github.com/pyre/pyre](https://github.com/pyre/pyre)
 
    *  https://www.hdfeos.org/software/gdal.php
