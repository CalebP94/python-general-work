from osgeo import gdal
from osgeo.gdalconst import GA_ReadOnly

"""
Sieve - remove unwanted values from a raster band. 
"""


def sieve(input_path, output_path, threshold):
    # Open the input raster dataset
    input_ds = gdal.Open(input_path, GA_ReadOnly)
    if not input_ds:
        print("Error: Unable to open input raster dataset.")
        return

    # Create an output raster dataset
    driver = gdal.GetDriverByName("GTiff")
    output_ds = driver.CreateCopy(output_path, input_ds)
    if not output_ds:
        print("Error: Unable to create output raster dataset.")
        return

    # Apply the gdal_sieve algorithm
    gdal.SieveFilter(input_ds.GetRasterBand(1), None, output_ds.GetRasterBand(1), threshold)

    # Close the datasets
    input_ds = None
    output_ds = None

# Example usage
input_raster = "path/to/your/input_raster.tif"
output_raster = "path/to/your/output_raster.tif"
sieve_threshold = 100  # Adjust the threshold based on your requirements

sieve(input_raster, output_raster, sieve_threshold)