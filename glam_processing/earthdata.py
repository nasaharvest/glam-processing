import os
import logging

import h5py

import rasterio
from rasterio.io import MemoryFile

from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

import numpy as np

from .spectral import calc_ndvi, calc_ndwi
from . import exceptions

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

SUPPORTED_DATASETS = [
    # MODIS
    "MOD09Q1",
    "MYD09Q1",
    "MOD13Q1",
    "MYD13Q1",
    "MOD09A1",
    "MYD09A1",
    "MYD09GA",
    "MOD09Q1N",
    "MOD13Q4N",
    "MOD09CMG",
    "MCD12Q1",
    # VIIRS/NPP
    "VNP09H1",
    "VNP09A1",
    "VNP09GA",
    "VNP09CMG",
    "VNP21A2",
    # MERRA-2
    "M2SDNXSLV",
]


# WKT of default Sinusoidal Projection
SINUS_WKT = 'PROJCS["unnamed",GEOGCS["Unknown datum based upon the custom spheroid",DATUM["Not_specified_based_on_custom_spheroid",SPHEROID["Custom spheroid",6371007.181,0]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]]],PROJECTION["Sinusoidal"],PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'


def authenticate(strategy="interactive", persist=True):
    """Authenticate with earthaccess"""

    from earthaccess import Auth

    auth = Auth()
    auth.login(strategy=strategy, persist=persist)
    return auth.authenticated


def get_granules(product, start_date, end_date):
    # get list of granules for a product
    # start_date and end_date should be in YYYY-MM-DD format
    return None


def get_dtype_from_sds_name(sds_name):
    # given name of sds return string representation of dtype
    if "sur_refl_b" in sds_name:
        return "int16"
    elif sds_name in ["sur_refl_qc_500m"]:
        return "uint32"
    elif sds_name in [
        "sur_refl_szen",
        "sur_refl_vzen",
        "sur_refl_raz",
        "RelativeAzimuth",
        "SolarZenith",
        "SensorZenith",
        "SurfReflect_I1",
        "SurfReflect_I2",
        "SurfReflect_I3",
        "SurfReflect_I1_1",
        "SurfReflect_I2_1",
    ]:
        return "int16"
    elif sds_name in [
        "sur_refl_state_500m",
        "sur_refl_day_of_year",
        "sur_refl_state_250m",
        "sur_refl_qc_250m",
        "SurfReflect_Day_Of_Year",
        "SurfReflect_State_500m",
        "SurfReflect_QC_500m",
    ]:
        return "uint16"
    elif sds_name in ["LC_Type1"]:
        return "uint8"
    elif sds_name in ["HOUR_NO_RAIN", "T2MMAX", "T2MMIN", "T2MMEAN", "TPRECMAX"]:
        return "float32"


def get_h5_geo_info(file):
    # Get info from the StructMetadata Object
    metadata = file["HDFEOS INFORMATION"]["StructMetadata.0"][()].split()
    # Info returned is of type Byte, must convert to string before using it
    metadata_byte2str = [s.decode("utf-8") for s in metadata]
    # Get upper left points
    ulc = [i for i in metadata_byte2str if "UpperLeftPointMtrs" in i]
    ulc_lon = float(
        ulc[0].replace("=", ",").replace("(", "").replace(")", "").split(",")[1]
    )
    ulc_lat = float(
        ulc[0].replace("=", ",").replace("(", "").replace(")", "").split(",")[2]
    )
    return (ulc_lon, 0.0, ulc_lat, 0.0)


def is_nrt(product):
    if product[-1] == "N":
        nrt = True
    else:
        nrt = False
    return nrt


def get_sds(dataset, name):
    for sds in dataset.subdatasets:
        sds_parts = sds.split(":")
        if sds_parts[0] == "HDF4_EOS":
            if sds_parts[-1] == name:
                band = rasterio.open(sds)
                return (band.read(), band.nodata)
        elif sds_parts[0] == "HDF5":
            if sds_parts[-1].split("/")[-1] == name:
                band = rasterio.open(sds)
                return (band.read(), band.nodata)
        elif sds_parts[0] == "netcdf":
            if sds_parts[-1] == name:
                band = rasterio.open(sds)
                return (band.read(), band.nodata)


def get_sds_path(dataset, name):
    for sds in dataset.subdatasets:
        sds_parts = sds.split(":")
        if sds_parts[0] == "HDF4_EOS":
            if sds_parts[-1] == name:
                return sds
        elif sds_parts[0] == "HDF5":
            if sds_parts[-1].split("/")[-1] == name:
                return sds


def apply_mask(in_array, source_dataset, nodata):
    """
    This function removes non-clear pixels from an input array,
    including clouds, cloud shadow, and water.

    For M*D CMG files, removes pixels ranked below "8" in
    MOD13Q1 compositing method, as well as water.

    Returns a cleaned array.

    ...

    Parameters
    ----------

    in_array: numpy.array
        The array to be cleaned. This must have the same dimensions
        as source_dataset, and preferably have been extracted from the
        stack.
    source_dataset: str
        Path to a hierarchical data file containing QA layers with
        which to perform the masking. Currently valid formats include
        MOD09Q1 hdf and VNP09H1 files.
    """

    # get file extension and product suffix
    file_path = source_dataset.name
    file_name, ext = os.path.splitext(os.path.basename(file_path))
    suffix = file_name.split(".")[0]

    # product-conditional behavior

    # MODIS pre-generated VI masking
    if suffix in ["MOD13Q1", "MOD13Q4", "MOD13Q4N"]:
        if suffix[-1] == "1":
            pr_arr, pr_nodata = get_sds(
                source_dataset, "250m 16 days pixel reliability"
            )
            qa_arr, qa_nodata = get_sds(source_dataset, "250m 16 days VI Quality")
        else:
            pr_arr, pr_nodata = get_sds(source_dataset, "250m 8 days pixel reliability")
            qa_arr, qa_nodata = get_sds(source_dataset, "250m 8 days VI Quality")

        # in_array[(pr_arr != 0) & (pr_arr != 1)] = nodata

        # mask clouds
        in_array[(qa_arr & 0b11) > 1] = nodata  # bits 0-1 > 01 = Cloudy

        # mask Aerosol
        in_array[(qa_arr & 0b11000000) == 0] = nodata  # climatology
        in_array[(qa_arr & 0b11000000) == 192] = nodata  # high

        # mask water
        in_array[
            ((qa_arr & 0b11100000000000) != 2048)
            & ((qa_arr & 0b11100000000000) != 4096)
            & ((qa_arr & 0b11100000000000) != 8192)
        ] = nodata
        # 001 = land, 010 = coastline, 100 = ephemeral water

        # mask snow/ice
        in_array[(qa_arr & 0b100000000000000) != 0] = nodata  # bit 14

        # mask cloud shadow
        in_array[(qa_arr & 0b1000000000000000) != 0] = nodata  # bit 15

        # mask cloud adjacent pixels
        in_array[(qa_arr & 0b100000000) != 0] = nodata  # bit 8

    # MODIS and VIIRS surface reflectance masking
    # CMG
    elif "CMG" in suffix:
        if ext == ".hdf":  # MOD09CMG
            qa_arr, qa_nodata = get_sds(source_dataset, "Coarse Resolution QA")
            state_arr, state_nodata = get_sds(
                source_dataset, "Coarse Resolution State QA"
            )
            vang_arr, vang_nodata = get_sds(
                source_dataset, "Coarse Resolution View Zenith Angle"
            )
            vang_arr[vang_arr <= 0] = 9999
            sang_arr, sang_nodata = get_sds(
                source_dataset, "Coarse Resolution Solar Zenith Angle"
            )
            rank_arr = np.full(qa_arr.shape, 10)  # empty rank array

            # perform the ranking!
            logging.debug("--rank 9: SNOW")
            SNOW = (state_arr & 0b1000000000000) | (
                state_arr & 0b1000000000000000
            )  # state bit 12 OR 15
            rank_arr[SNOW > 0] = 9  # snow
            del SNOW
            logging.debug("--rank 8: HIGHAEROSOL")
            HIGHAEROSOL = state_arr & 0b11000000  # state bits 6 AND 7
            rank_arr[HIGHAEROSOL == 192] = 8
            del HIGHAEROSOL
            logging.debug("--rank 7: CLIMAEROSOL")
            CLIMAEROSOL = state_arr & 0b11000000  # state bits 6 & 7
            # CLIMAEROSOL=(cloudMask & 0b100000000000000) # cloudMask bit 14
            rank_arr[CLIMAEROSOL == 0] = 7  # default aerosol level
            del CLIMAEROSOL
            logging.debug("--rank 6: UNCORRECTED")
            UNCORRECTED = qa_arr & 0b11  # qa bits 0 AND 1
            rank_arr[UNCORRECTED == 3] = 6  # flagged uncorrected
            del UNCORRECTED
            logging.debug("--rank 5: SHADOW")
            SHADOW = state_arr & 0b100  # state bit 2
            rank_arr[SHADOW == 4] = 5  # cloud shadow
            del SHADOW
            logging.debug("--rank 4: CLOUDY")
            # set adj to 11 and internal to 12 to verify in qa output
            # state bit 0 OR bit 1 OR bit 10 OR bit 13
            CLOUDY = state_arr & 0b11
            # rank_arr[CLOUDY!=0]=4 # cloud pixel
            del CLOUDY
            CLOUDADJ = state_arr & 0b10000000000000
            # rank_arr[CLOUDADJ>0]=4 # adjacent to cloud
            del CLOUDADJ
            CLOUDINT = state_arr & 0b10000000000
            rank_arr[CLOUDINT > 0] = 4
            del CLOUDINT
            logging.debug("--rank 3: HIGHVIEW")
            rank_arr[sang_arr > (85 / 0.01)] = 3  # HIGHVIEW
            logging.debug("--rank 2: LOWSUN")
            rank_arr[vang_arr > (60 / 0.01)] = 2  # LOWSUN
            # BAD pixels
            # qa bits (2-5 OR 6-9 == 1110)
            logging.debug("--rank 1: BAD pixels")
            BAD = (qa_arr & 0b111100) | (qa_arr & 0b1110000000)
            rank_arr[BAD == 112] = 1
            rank_arr[BAD == 896] = 1
            rank_arr[BAD == 952] = 1
            del BAD

            logging.debug("-building water mask")
            water = state_arr & 0b111000  # check bits
            water[water == 56] = 1  # deep ocean
            water[water == 48] = 1  # continental/moderate ocean
            water[water == 24] = 1  # shallow inland water
            water[water == 40] = 1  # deep inland water
            water[water == 0] = 1  # shallow ocean
            rank_arr[water == 1] = 0
            vang_arr[water == 32] = 9999  # ephemeral water???
            water[state_arr == 0] = 0
            water[water != 1] = 0  # set non-water to zero
            in_array[rank_arr <= 7] = nodata
        elif ext == ".h5":  # VNP09CMG
            qf2, qf2_nodata = get_sds(source_dataset, "SurfReflect_QF2")
            qf4, qf4_nodata = get_sds(source_dataset, "SurfReflect_QF4")
            state_arr, state_nodata = get_sds(source_dataset, "State_QA")
            vang_arr, vang_nodata = get_sds(source_dataset, "SensorZenith")
            vang_arr[vang_arr <= 0] = 9999
            sang_arr, sang_nodata = get_sds(source_dataset, "SolarZenith")
            rank_arr = np.full(state_arr.shape, 10)  # empty rank array

            # perform the ranking!
            logging.debug("--rank 9: SNOW")
            SNOW = state_arr & 0b1000000000000000  # state bit 15
            rank_arr[SNOW > 0] = 9  # snow
            del SNOW
            logging.debug("--rank 8: HIGHAEROSOL")
            HIGHAEROSOL = qf2 & 0b10000  # qf2 bit 4
            rank_arr[HIGHAEROSOL != 0] = 8
            del HIGHAEROSOL
            logging.debug("--rank 7: AEROSOL")
            CLIMAEROSOL = state_arr & 0b1000000  # state bit 6
            # CLIMAEROSOL=(cloudMask & 0b100000000000000) # cloudMask bit 14
            # rank_arr[CLIMAEROSOL==0]=7 # "No"
            del CLIMAEROSOL
            # logging.debug("--rank 6: UNCORRECTED")
            # UNCORRECTED = (qa_arr & 0b11) # qa bits 0 AND 1
            # rank_arr[UNCORRECTED==3]=6 # flagged uncorrected
            # del UNCORRECTED
            logging.debug("--rank 5: SHADOW")
            SHADOW = state_arr & 0b100  # state bit 2
            rank_arr[SHADOW != 0] = 5  # cloud shadow
            del SHADOW
            logging.debug("--rank 4: CLOUDY")
            # set adj to 11 and internal to 12 to verify in qa output
            # CLOUDY = ((state_arr & 0b11)) # state bit 0 OR bit 1 OR bit 10 OR bit 13
            # rank_arr[CLOUDY!=0]=4 # cloud pixel
            # del CLOUDY
            # CLOUDADJ = (state_arr & 0b10000000000) # nonexistent for viirs
            # #rank_arr[CLOUDADJ>0]=4 # adjacent to cloud
            # del CLOUDADJ
            CLOUDINT = state_arr & 0b10000000000  # state bit 10
            rank_arr[CLOUDINT > 0] = 4
            del CLOUDINT
            logging.debug("--rank 3: HIGHVIEW")
            rank_arr[sang_arr > (85 / 0.01)] = 3  # HIGHVIEW
            logging.debug("--rank 2: LOWSUN")
            rank_arr[vang_arr > (60 / 0.01)] = 2  # LOWSUN
            # BAD pixels
            # qa bits (2-5 OR 6-9 == 1110)
            logging.debug("--rank 1: BAD pixels")
            BAD = qf4 & 0b110
            rank_arr[BAD != 0] = 1
            del BAD

            logging.debug("-building water mask")
            water = state_arr & 0b111000  # check bits 3-5
            water[water == 40] = 0  # "coastal" = 101
            water[water > 8] = 1  # sea water = 011; inland water = 010
            # water[water==16]=1 # inland water = 010
            # water[state_arr==0]=0
            water[water != 1] = 0  # set non-water to zero
            water[water != 0] = 1
            rank_arr[water == 1] = 0
            in_array[rank_arr <= 7] = nodata
        else:
            raise exceptions.FileTypeError("File must be of format .hdf or .h5")
    elif "MERRA2" in suffix:
        pass
    # standard
    else:
        # viirs
        if ext == ".h5":
            qa_arr, qa_nodata = get_sds(source_dataset, "SurfReflect_QC_500m")
            state_arr, state_nodata = get_sds(source_dataset, "SurfReflect_State_500m")
        # MOD09A1
        elif suffix == "MOD09A1":
            qa_arr, qa_nodata = get_sds(source_dataset, "sur_refl_qc_500m")
            state_arr, state_nodata = get_sds(source_dataset, "sur_refl_state_500m")
        elif suffix == "MOD09GA":
            qa_arr, qa_nodata = get_sds(source_dataset, "QC_500m_1")
            state_arr, state_nodata = get_sds(source_dataset, "state_1km_1")
        # all other MODIS products
        elif ext == ".hdf":
            qa_arr, qa_nodata = get_sds(source_dataset, "sur_refl_qc_250m")
            state_arr, state_nodata = get_sds(source_dataset, "sur_refl_state_250m")
        else:
            raise exceptions.FileTypeError("File must be of format .hdf or .h5")

        # mask clouds
        in_array[(state_arr & 0b11) != 0] = nodata
        in_array[(state_arr & 0b10000000000) != 0] = -3000  # internal cloud mask

        # mask cloud shadow
        in_array[(state_arr & 0b100) != 0] = nodata

        # mask cloud adjacent pixels
        in_array[(state_arr & 0b10000000000000) != 0] = nodata

        # mask aerosols
        in_array[(state_arr & 0b11000000) == 0] = nodata  # climatology
        # high; known to be an unreliable flag in MODIS collection 6
        in_array[(state_arr & 0b11000000) == 192] = nodata

        # mask snow/ice
        in_array[(state_arr & 0b1000000000000) != 0] = nodata

        # mask water
        # checks against three 'allowed' land/water classes and excludes pixels that don't match
        in_array[
            ((state_arr & 0b111000) != 8)
            & ((state_arr & 0b111000) != 16)
            & ((state_arr & 0b111000) != 32)
        ] = nodata

        # mask bad solar zenith
        # in_array[(qa_arr & 0b11100000) != 0] = nodata

    # return output
    return in_array


def get_ndvi_array(dataset):
    file_path = dataset.name
    file_name = os.path.basename(file_path)
    suffix = file_name.split(".")[0][3:]
    f, ext = os.path.splitext(file_name)

    if suffix in ["09Q4", "13Q4", "13Q4N"]:
        band_name = "250m 8 days NDVI"
        ndvi_array, ndvi_nodata = get_sds(dataset, band_name)
        return (ndvi_array, ndvi_nodata)
    elif suffix == "13Q1":
        band_name = "250m 16 days NDVI"
        ndvi_array, ndvi_nodata = get_sds(dataset, band_name)
        return (ndvi_array, ndvi_nodata)
    elif suffix == "09CM":
        if ext == ".hdf":
            red_name = "Coarse Resolution Surface Reflectance Band 1"
            nir_name = "Coarse Resolution Surface Reflectance Band 2"
        elif ext == ".h5":
            red_name = "SurfReflect_I1"
            nir_name = "SurfReflect_I2"

        red_band, red_nodata = get_sds(dataset, red_name)
        nir_band, nir_nodata = get_sds(dataset, nir_name)

        ndvi_array = calc_ndvi(red_band, nir_band)
        return (ndvi_array, red_nodata)
    elif suffix == "09GA":
        if ext == ".hdf":
            red_name = "sur_refl_b01_1"
            nir_name = "sur_refl_b02_1"
        elif ext == ".h5":
            red_name = "SurfReflect_I1_1"
            nir_name = "SurfReflect_I2_1"
        else:
            raise exceptions.FileTypeError("File must be of type .hdf or .h5")

        # Discovered negative surface reflectance values in MOD09 & MYD09
        # that threw off NDVI calculations
        # clip values to (0,10000)

        # get numpy array from red band dataset
        red_band, red_nodata = get_sds(dataset, red_name)
        # dont clip nodata values
        red_band[red_band != red_nodata] = np.clip(
            red_band[red_band != red_nodata], 0, 10000
        )

        # get numpy array from nir band dataset
        nir_band, nir_nodata = get_sds(dataset, nir_name)
        nir_band[nir_band != nir_nodata] = np.clip(
            nir_band[nir_band != nir_nodata], 0, 10000
        )

        ndvi_array = calc_ndvi(red_band, nir_band)

        return (ndvi_array, red_nodata)

    else:
        if ext == ".hdf":
            red_name = "sur_refl_b01"
            nir_name = "sur_refl_b02"
        elif ext == ".h5":
            red_name = "SurfReflect_I1"
            nir_name = "SurfReflect_I2"
        else:
            raise exceptions.FileTypeError("File must be of type .hdf or .h5")

        # Discovered negative surface reflectance values in MOD09 & MYD09
        # that threw off NDVI calculations
        # clip values to (0,10000)

        # get numpy array from red band dataset
        red_band, red_nodata = get_sds(dataset, red_name)
        # dont clip nodata values
        red_band[red_band != red_nodata] = np.clip(
            red_band[red_band != red_nodata], 0, 10000
        )

        # get numpy array from nir band dataset
        nir_band, nir_nodata = get_sds(dataset, nir_name)
        nir_band[nir_band != nir_nodata] = np.clip(
            nir_band[nir_band != nir_nodata], 0, 10000
        )

        ndvi_array = calc_ndvi(red_band, nir_band)

        return (ndvi_array, red_nodata)


def get_ndwi_array(dataset):
    file_path = dataset.name
    file_name = os.path.basename(file_path)
    suffix = file_name.split(".")[0][3:]
    f, ext = os.path.splitext(file_name)

    if suffix == "09A1":
        nir_name = "sur_refl_b02"
        swir_name = "sur_refl_b06"

        # Discovered negative surface reflectance values in MOD09 & MYD09
        # that threw off NDVI calculations
        # clip values to (0,10000)

        nir_band, nir_nodata = get_sds(dataset, nir_name)
        nir_band[nir_band != nir_nodata] = np.clip(
            nir_band[nir_band != nir_nodata], 0, 10000
        )

        swir_band, swir_nodata = get_sds(dataset, swir_name)
        swir_band[swir_band != swir_nodata] = np.clip(
            swir_band[swir_band != swir_nodata], 0, 10000
        )

        ndwi_array = calc_ndwi(nir_band, swir_band)
    else:
        raise exceptions.UnsupportedError(
            "Only MOD09A1 imagery is supported for NDWI generation"
        )

    return (ndwi_array, nir_nodata)


def create_ndvi_geotiff(file, out_dir):
    dataset = rasterio.open(file)

    file_path = dataset.name
    file_name = os.path.basename(file_path)
    f, ext = os.path.splitext(file_name)

    # calculate ndvi and export to geotiff
    ndvi_array, ndvi_nodata = get_ndvi_array(dataset)

    # apply mask
    ndvi_array = apply_mask(ndvi_array, dataset, ndvi_nodata)

    out_name = file_name.replace(ext, ".ndvi.tif")
    output = os.path.join(out_dir, out_name)

    # coerce dtype to int16
    dtype = "int16"

    ndvi_array = ndvi_array.astype(dtype)

    profile = rasterio.open(dataset.subdatasets[0]).profile.copy()
    profile.update({"driver": "GTiff", "dtype": dtype, "nodata": ndvi_nodata})

    if "VNP" in file_name:
        f = h5py.File(file_path, "r")
        geo_info = get_h5_geo_info(f)
        if profile["height"] == 1200:  # VIIRS VNP09A1, VNP09GA - 1km
            yRes = -926.6254330555555
            xRes = 926.6254330555555
        elif profile["height"] == 2400:  # VIIRS VNP09H1, VNP09GA - 500m
            yRes = -463.31271652777775
            xRes = 463.31271652777775
        new_transform = rasterio.Affine(
            xRes, geo_info[1], geo_info[0], geo_info[3], yRes, geo_info[2]
        )
        profile.update({"transform": new_transform})

    # assign CRS if None
    if profile["crs"] == None:
        profile.update({"crs": SINUS_WKT})

    # create cog
    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            mem.write(ndvi_array)
            dst_profile = cog_profiles.get("deflate")
            cog_translate(
                mem,
                output,
                dst_profile,
                in_memory=True,
                quiet=True,
            )

    return output


def create_ndwi_geotiff(file, out_dir):
    dataset = rasterio.open(file)

    file_path = dataset.name
    file_name = os.path.basename(file_path)
    f, ext = os.path.splitext(file_name)

    # calculate ndvi and export to geotiff
    ndwi_array, ndwi_nodata = get_ndwi_array(dataset)

    # apply mask
    ndwi_array = apply_mask(ndwi_array, dataset, ndwi_nodata)

    out_name = file_name.replace(ext, ".ndwi.tif")
    output = os.path.join(out_dir, out_name)

    # coerce dtype to int16
    dtype = "int16"

    ndwi_array = ndwi_array.astype(dtype)

    profile = rasterio.open(dataset.subdatasets[0]).profile.copy()
    profile.update({"driver": "GTiff", "dtype": dtype, "nodata": ndwi_nodata})

    if "VNP" in file_name:
        f = h5py.File(file_path, "r")
        geo_info = get_h5_geo_info(f)
        if profile["height"] == 1200:  # VIIRS VNP09A1, VNP09GA - 1km
            yRes = -926.6254330555555
            xRes = 926.6254330555555
        elif profile["height"] == 2400:  # VIIRS VNP09H1, VNP09GA - 500m
            yRes = -463.31271652777775
            xRes = 463.31271652777775
        new_transform = rasterio.Affine(
            xRes, geo_info[1], geo_info[0], geo_info[3], yRes, geo_info[2]
        )
        profile.update({"transform": new_transform})

    # assign CRS if None
    if profile["crs"] == None:
        profile.update({"crs": SINUS_WKT})

    # create cog
    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            mem.write(ndwi_array)
            dst_profile = cog_profiles.get("deflate")
            cog_translate(
                mem,
                output,
                dst_profile,
                in_memory=True,
                quiet=True,
            )

    return output


def create_sds_geotiff(file, product, sds_name, out_dir, mask=True):
    filename, ext = os.path.splitext(file)
    if ext in [".nc4", ".nc"]:
        dataset = rasterio.open(f"netcdf:{file}")
    else:
        dataset = rasterio.open(file)

    file_path = dataset.name
    file_name = os.path.basename(file_path)

    # get sds array and nodata value
    sds_array, sds_nodata = get_sds(dataset, sds_name)

    # apply mask
    if mask:
        sds_array = apply_mask(sds_array, dataset, sds_nodata)

    # if band is surface reflectance then clip values (exclude nodata)
    if sds_name.lower().find("refl") > -1 and product != "VNP09A1":
        sds_array[sds_array != sds_nodata] = np.clip(
            sds_array[sds_array != sds_nodata], 0, 10000
        )

    out_name = file_name.replace(ext, f".{sds_name}.tif")
    output = os.path.join(out_dir, out_name)

    dtype = get_dtype_from_sds_name(sds_name)

    sds_array = sds_array.astype(dtype)

    profile = rasterio.open(dataset.subdatasets[0]).profile.copy()
    profile.update({"driver": "GTiff", "dtype": dtype, "nodata": sds_nodata})

    if "VNP" in product:
        f = h5py.File(file_path, "r")
        geo_info = get_h5_geo_info(f)
        out_name = file_name.replace(".h5", f".{sds_name}.tif")
        output = os.path.join(out_dir, out_name)
        if profile["height"] == 1200:  # VIIRS VNP09A1, VNP09GA - 1km
            yRes = -926.6254330555555
            xRes = 926.6254330555555
        elif profile["height"] == 2400:  # VIIRS VNP09H1, VNP09GA - 500m
            yRes = -463.31271652777775
            xRes = 463.31271652777775
        new_transform = rasterio.Affine(
            xRes, geo_info[1], geo_info[0], geo_info[3], yRes, geo_info[2]
        )
        profile.update({"transform": new_transform})
        tags = dataset.tags()
        for tag in tags:
            if sds_name + "__FillValue" in tag:
                sds_nodata = tags[tag]
        profile.update({"nodata": int(sds_nodata)})

    # assign CRS if None
    if profile["crs"] == None:
        profile.update({"crs": SINUS_WKT})

    #  create cog
    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            mem.write(sds_array)
            dst_profile = cog_profiles.get("deflate")
            cog_translate(
                mem,
                output,
                dst_profile,
                in_memory=True,
                quiet=True,
            )

    return output
