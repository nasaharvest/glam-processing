import os
import logging

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


def cloud_optimize(dataset, out_file, nodata=False):
    import rasterio
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.cogeo import cog_validate
    from rio_cogeo.profiles import cog_profiles

    raster = rasterio.open(dataset)
    meta = raster.meta.copy()

    if nodata:
        meta.update({"nodata": nodata})

    out_meta = meta
    cog_options = cog_profiles.get("deflate")
    out_meta.update(cog_options)
    out_meta.update({"BIGTIFF": "IF_SAFER"})
    cog_translate(
        raster,
        out_file,
        out_meta,
        allow_intermediate_compression=True,
        quiet=False,
    )

    return True


def calc_mean_raster(file_list: list, output: str):
    """
    A function to calculate the mean of a list of rasters
    of the same shape.

    Parameters
    ----------

    file_list: list
            List of raster filepaths

    """
    import rasterio
    from rasterio.io import MemoryFile
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    import rioxarray as riox

    import numpy as np

    from tqdm import tqdm

    # open raster files
    raster_data = [rasterio.open(f) for f in file_list]

    profile = raster_data[0].profile
    profile.update({"dtype": "float32"})

    # calculate mean raster

    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            for i, window in tqdm(
                raster_data[0].block_windows(1),
                total=len(list(raster_data[0].block_windows(1))),
                desc=f"Reading Windows",
            ):
                window_data = np.array([r.read(window=window) for r in raster_data])
                avg_value = np.ma.average(
                    window_data, axis=0, weights=(window_data != raster_data[0].nodata)
                )
                # avg_value[avg_value.mask == True] = raster_data[0].nodata
                avg_array = np.ma.masked_array(
                    avg_value,
                    mask=(avg_value == raster_data[0].nodata),
                    fill_value=raster_data[0].nodata,
                )

                mem.write(avg_array.astype("float32"), window=window)

            out_profile = cog_profiles.get("deflate")

            out_profile.update({"BIGTIFF": "IF_SAFER"})

            profile.update(out_profile)

            cog_translate(
                mem,
                output,
                profile,
                in_memory=True,
                allow_intermediate_compression=True,
                quiet=False,
            )


def calc_median_raster(file_list: list, output: str):
    """
    A function to calculate the median of a list of rasters
    of the same shape.

    Parameters
    ----------

    file_list: list
            List of raster filepaths

    """
    import rasterio
    from rasterio.io import MemoryFile
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    import rioxarray as riox

    import numpy as np

    from tqdm import tqdm

    # open raster files
    raster_data = [rasterio.open(f) for f in file_list]

    profile = raster_data[0].profile
    profile.update({"dtype": "float32"})

    # calculate median raster

    with MemoryFile() as memfile:
        with memfile.open(**profile) as mem:
            for i, window in tqdm(
                raster_data[0].block_windows(1),
                total=len(list(raster_data[0].block_windows(1))),
                desc=f"Reading Windows",
            ):
                window_data = np.array([r.read(window=window) for r in raster_data])
                median_value = np.ma.median(window_data, axis=0)
                # median_value[median_value.mask == True] = raster_data[0].nodata
                median_array = np.ma.masked_array(
                    median_value,
                    mask=(median_value == raster_data[0].nodata),
                    fill_value=raster_data[0].nodata,
                )
                mem.write(median_array.astype("float32"), window=window)

            out_profile = cog_profiles.get("deflate")

            out_profile.update({"BIGTIFF": "IF_SAFER"})

            profile.update(out_profile)

            cog_translate(
                mem,
                output,
                profile,
                in_memory=True,
                allow_intermediate_compression=True,
                quiet=False,
            )
