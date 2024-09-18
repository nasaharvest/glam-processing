import os
import gzip
import shutil
import logging

from datetime import datetime, timedelta
import requests
import subprocess
from multiprocessing import Pool

from bs4 import BeautifulSoup

from tqdm import tqdm

import rasterio
from rasterio.io import MemoryFile
from rasterio.crs import CRS
from rasterio.merge import merge
from rasterio.rio.overview import get_maximum_overview_level
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.cogeo import cog_validate
from rio_cogeo.profiles import cog_profiles

import rioxarray

import earthaccess
from earthaccess import Auth, DataCollections, DataGranules


from .earthdata import (
    create_ndvi_geotiff,
    create_ndwi_geotiff,
    create_sds_geotiff,
    SUPPORTED_DATASETS as EARTHDATA_DATASETS,
)

from .utils import cloud_optimize

from . import exceptions


logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H:%M:%S",
)
log = logging.getLogger(__name__)

# add more CLMS dataset id's as needed
CLMS_DATASETS = ["swi_12.5km_v3_10daily"]

UCSB_DATASETS = ["CHIRPS-2.0"]

SERVIR_DATASETS = ["esi/4WK", "esi/12WK"]

SUPPORTED_DATASETS = (
    EARTHDATA_DATASETS + CLMS_DATASETS + UCSB_DATASETS + SERVIR_DATASETS
)

SUPPORTED_INDICIES = ["NDVI", "NDWI"]


class GlamDownloader:
    def __init__(self, dataset):
        self.dataset = dataset

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, value):
        if value not in SUPPORTED_DATASETS:
            raise exceptions.UnsupportedError(
                f"Dataset '{value}' not recognized or not supported."
            )
        self._dataset = value

    @staticmethod
    def supported_datasets():
        return SUPPORTED_DATASETS

    @staticmethod
    def supported_indicies():
        return SUPPORTED_INDICIES

    def _cloud_optimize(self, dataset, out_file, nodata=False):
        optimized = cloud_optimize(dataset, out_file, nodata)

        return optimized

    def _create_mosaic_cog_from_vrt(self, vrt_path):
        temp_path = vrt_path.replace("vrt", "temp.tif")
        out_path = vrt_path.replace("vrt", "tif")
        log.info(temp_path)
        log.info(out_path)

        log.info("Creating global mosaic tiff.")
        mosaic_command = [
            "gdal_translate",
            "-of",
            "GTiff",
            "-co",
            "COMPRESS=DEFLATE",
            "-co",
            "BIGTIFF=IF_SAFER",
            vrt_path,
            temp_path,
        ]
        subprocess.call(mosaic_command)
        os.remove(vrt_path)

        log.info("Creating COG.")

        optimized = self._cloud_optimize(temp_path, out_path)
        if optimized:
            os.remove(temp_path)

        return out_path

    def _create_mosaic_cog_from_tifs(self, date_string, files, out_dir):
        date = datetime.strptime(date_string, "%Y-%m-%d")
        year = date.year
        doy = date.strftime("%j")

        # get index or sds name
        sample_file = files[0]
        variable = sample_file.split(".")[-2]

        file_name = f"{self.dataset}.{variable}.{year}.{doy}.tif"
        out_path = os.path.join(out_dir, file_name)
        vrt_path = out_path.replace("tif", "vrt")

        log.info("Creating mosaic VRT.")

        vrt_command = ["gdalbuildvrt", vrt_path]
        vrt_command += files
        subprocess.call(vrt_command)

        out = self._create_mosaic_cog_from_vrt(vrt_path)

        return out


class EarthDataDownloader(GlamDownloader):
    def __init__(self, dataset):
        super().__init__(dataset)
        self.auth = Auth()

    @property
    def auth(self):
        return self._auth

    @auth.setter
    def auth(self, value):
        if not value.authenticated:
            try:
                value.login()
            except:
                value.login(strategy="interactive", persist=True)
        self._auth = value

    @property
    def authenticated(self):
        return self.auth.authenticated

    @property
    def collection(self):
        return DataCollections().short_name(self.dataset).cloud_hosted(True).get(1)[0]

    def info(self):
        return self.collection.summary()

    def query_granules(self, start_date, end_date):
        log.info("Querying available granules")
        concept_id = self.collection.concept_id()
        query = DataGranules().concept_id(concept_id).temporal(start_date, end_date)
        granules = query.get_all()

        return granules

    def query_composites(self, start_date, end_date):
        granules = self.query_granules(start_date, end_date)
        composites = []
        for granule in tqdm(granules, desc="Getting available composite dates"):
            composite_obj = {}
            composite_obj["id"] = (
                granule["meta"]["native-id"].split(".")[0]
                + "."
                + granule["meta"]["native-id"].split(".")[1]
            )
            composite_obj["start_date"] = granule["umm"]["TemporalExtent"][
                "RangeDateTime"
            ]["BeginningDateTime"][:10]
            composite_obj["end_date"] = granule["umm"]["TemporalExtent"][
                "RangeDateTime"
            ]["EndingDateTime"][:10]
            if composite_obj not in composites:
                composites.append(composite_obj)

        return composites

    def download_granules(self, start_date, end_date, out_dir):
        local_path = os.path.abspath(out_dir)
        granules = self.query_granules(start_date, end_date)
        granule_count = len(granules)

        download_complete = False
        while not download_complete:
            files = earthaccess.download(granules, local_path=local_path)
            try:
                for file in files:
                    assert os.path.isfile(file)
                if len(files) == granule_count:
                    download_complete = True
            except TypeError:
                download_complete = False
                log.info(
                    f"{len(files)} of {granule_count} files downloaded. Retrying..."
                )

            log.info(f"Successfilly downloaded {len(files)} of {granule_count} files.")
        return files

    def download_vi_granules(self, start_date, end_date, out_dir, vi="NDVI"):
        out = os.path.abspath(out_dir)

        vi_functions = {
            "NDVI": create_ndvi_geotiff,
            "NDWI": create_ndwi_geotiff,
        }

        if vi not in SUPPORTED_INDICIES:
            raise exceptions.UnsupportedError(
                f"Vegetation index '{vi}' not recognized or not supported."
            )

        granule_files = self.download_granules(start_date, end_date, out)

        vi_files = []
        for file in tqdm(granule_files, desc=f"Creating {vi} files"):
            vi_files.append(vi_functions[vi](file, out))

        # Remove granule files after tiffs are created.
        for file in granule_files:
            os.remove(file)

        return vi_files

    def download_sds_granules(self, sds_name, start_date, end_date, out_dir):
        out = os.path.abspath(out_dir)

        granule_files = self.download_granules(start_date, end_date, out)

        sds_files = []
        for file in tqdm(granule_files, desc=f"Creating {sds_name} files"):
            sds_files.append(create_sds_geotiff(file, self.dataset, sds_name, out))

        # Remove granule files after tiffs are created.
        for file in granule_files:
            os.remove(file)

        return sds_files

    def download_vi_composites(self, start_date, end_date, out_dir, vi="NDVI"):
        out = os.path.abspath(out_dir)

        composites = self.query_composites(start_date, end_date)

        output = []
        for composite in tqdm(composites, desc=f"Creating {vi} composites"):
            vi_files = self.download_vi_granules(
                composite["start_date"], composite["end_date"], out, vi=vi
            )

            vi_mosaic = self._create_mosaic_cog_from_tifs(
                composite["start_date"], vi_files, out
            )
            # Remove tiffs after mosaic creation.
            for file in vi_files:
                os.remove(file)

            output.append(vi_mosaic)

        return output

    def download_sds_composites(self, sds_name, start_date, end_date, out_dir):
        out = os.path.abspath(out_dir)

        composites = self.query_composites(start_date, end_date)

        output = []
        for composite in tqdm(composites, desc=f"Creating {sds_name} composites"):
            sds_files = self.download_sds_granules(
                sds_name, composite["start_date"], composite["end_date"], out
            )

            sds_mosaic = self._create_mosaic_cog_from_tifs(
                composite["start_date"], sds_files, out
            )
            # Remove tiffs after mosaic creation.
            for file in sds_files:
                os.remove(file)

            output.append(sds_mosaic)

        return output


class CLMSDownloader(GlamDownloader):
    def __init__(self, dataset):
        super().__init__(dataset)

        self.manifest = f"https://globalland.vito.be/download/manifest/{self.dataset}_netcdf/manifest_clms_global_{self.dataset}_netcdf_latest.txt"

    def query_composites(self, start_date, end_date):

        r = requests.get(self.manifest)
        download_list = r.text.split("\n")

        composites = []
        for url in tqdm(download_list, desc=f"Querying available {self.dataset} files"):
            if url.endswith(".nc"):
                datestring = url.split("/")[-2]

                year = datestring[:4]
                month = datestring[4:6]
                day = datestring[6:8]
                date = datetime(int(year), int(month), int(day))
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")

                if start <= date <= end:
                    composites.append({"date": date.strftime("%Y-%m-%d"), "url": url})

        return composites

    def download_composites(self, start_date, end_date, out_dir):

        composites = self.query_composites(start_date, end_date)

        completed = []

        for composite in tqdm(
            composites, desc=f"Downloading {self.dataset} composites"
        ):
            date = composite.get("date")
            url = composite.get("url")

            r = requests.get(url)

            out = os.path.join(out_dir, f"{self.dataset}.{date}.tif")

            # Temporary NetCDF file; later to be converted to tiff
            file_nc = out.replace("tif", "nc")

            # write output .nc file
            with open(file_nc, "wb") as fd:  # write data in chunks
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)

            # checksum
            # size of downloaded file (bytes)
            observed_size = int(os.stat(file_nc).st_size)
            # size anticipated from header (bytes)
            expected_size = int(r.headers["Content-Length"])

            # if checksum is failed, log and return empty
            if int(observed_size) != int(expected_size):
                w = f"\nExpected file size:\t{expected_size} bytes\nObserved file size:\t{observed_size} bytes"
                log.warning(w)
                os.remove(file_nc)
                return ()

            # Use rioxarray to remove time dimension and create intermediate geotiff
            xds = rioxarray.open_rasterio(os.path.abspath(file_nc), decode_times=False)
            new_ds = xds.squeeze()

            # Select SWI layer for T-Value of 10
            temp = os.path.join(out_dir, f"{self.dataset}.{date}.temp.tif")
            new_ds["SWI_010"].rio.to_raster(temp)

            optimized = self._cloud_optimize(temp, out, nodata=False)

            if optimized:
                os.remove(file_nc)
                os.remove(temp)
                completed.append(out)

        return completed


class UCSBDownloader(GlamDownloader):
    def __init__(self, dataset):
        super().__init__(dataset)
        self.index = f"https://data.chc.ucsb.edu/products/{dataset}/global_dekad/tifs/"
        self.prelim_index = (
            f"https://data.chc.ucsb.edu/products/{dataset}/prelim/global_dekad/tifs/"
        )

    def query_prelim_composites(self, start_date, end_date):
        r = requests.get(self.prelim_index)
        index_links = BeautifulSoup(r.text, "html.parser").find_all("a")

        file_names = []

        for link in tqdm(
            index_links, desc=f"Querying available preliminary {self.dataset} files"
        ):
            if link.get("href").endswith(".tif"):
                file_parts = link.get("href").split(".")

                day = file_parts[-2]
                if int(day) == 2:
                    day = 11
                elif int(day) == 3:
                    day = 21

                month = file_parts[-3]
                year = file_parts[-4]

                composite_start = datetime(int(year), int(month), int(day))
                composite_end = composite_start + timedelta(days=9)
                date_range_start = datetime.strptime(start_date, "%Y-%m-%d")
                date_range_end = datetime.strptime(end_date, "%Y-%m-%d")

                if (
                    composite_start >= date_range_start
                    and composite_start <= date_range_end
                ) or (
                    composite_end >= date_range_start
                    and composite_end <= date_range_end
                ):
                    file_name = link.get("href")
                    file_names.append(file_name)

        return file_names

    def query_composites(self, start_date, end_date):
        r = requests.get(self.index)
        index_links = BeautifulSoup(r.text, "html.parser").find_all("a")

        file_names = []

        for link in tqdm(index_links, desc=f"Querying available {self.dataset} files"):
            if link.get("href").endswith(".tif.gz"):
                file_parts = link.get("href").split(".")

                day = file_parts[-3]
                if int(day) == 2:
                    day = 11
                elif int(day) == 3:
                    day = 21

                month = file_parts[-4]
                year = file_parts[-5]

                composite_start = datetime(int(year), int(month), int(day))
                composite_end = composite_start + timedelta(days=9)
                date_range_start = datetime.strptime(start_date, "%Y-%m-%d")
                date_range_end = datetime.strptime(end_date, "%Y-%m-%d")

                if (
                    composite_start >= date_range_start
                    and composite_start <= date_range_end
                ) or (
                    composite_end >= date_range_start
                    and composite_end <= date_range_end
                ):
                    file_name = link.get("href")
                    file_names.append(file_name)

        return file_names

    def download_composites(self, start_date, end_date, out_dir, prelim=True):

        composites = self.query_composites(start_date, end_date)

        completed = []

        for composite in tqdm(
            composites, desc=f"Downloading {self.dataset} composites"
        ):
            url = self.index + composite
            r = requests.get(url)

            zipped_out = os.path.join(out_dir, composite)
            unzipped_out = zipped_out.strip(".gz")

            with open(zipped_out, "wb") as fd:  # write data in chunks
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)

            # CHECKSUM
            # size of downloaded file in bytes
            observed_size = int(os.stat(zipped_out).st_size)
            # size of promised file in bytes, extracted from server-delivered headers
            expected_size = int(r.headers["Content-Length"])

            # checksum failure; return empty tuple
            if observed_size != expected_size:  # checksum failure
                w = f"WARNING:\nExpected file size:\t{expected_size} bytes\nObserved file size:\t{observed_size} bytes"
                log.warning(w)
                return ()  # no files for you today, but we'll try again tomorrow!

            # use gzip to unzip file to final location
            # tf = file_unzipped.replace(".tif", ".UNMASKED.tif")
            with gzip.open(zipped_out) as fz:
                with open(unzipped_out, "w+b") as fu:
                    shutil.copyfileobj(fz, fu)
            os.remove(zipped_out)  # delete zipped version

            optimized = self._cloud_optimize(unzipped_out, unzipped_out, -9999)

            if optimized:
                completed.append(unzipped_out)

        if prelim:
            prelim_composites = self.query_prelim_composites(start_date, end_date)
            for prelim_composite in tqdm(
                prelim_composites, desc=f"Downloading {self.dataset} prelim composites"
            ):
                if f"{prelim_composite}.gz" not in composites:
                    filename, ext = os.path.splitext(prelim_composite)

                    out = os.path.join(out_dir, f"{filename}.prelim{ext}")
                    url = self.prelim_index + prelim_composite
                    r = requests.get(url)

                    with open(out, "wb") as fd:  # write data in chunks
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            fd.write(chunk)

                    # CHECKSUM
                    # size of downloaded file in bytes
                    observed_size = int(os.stat(out).st_size)
                    # size of promised file in bytes, extracted from server-delivered headers
                    expected_size = int(r.headers["Content-Length"])

                    # checksum failure; return empty tuple
                    if observed_size != expected_size:  # checksum failure
                        w = f"WARNING:\nExpected file size:\t{expected_size} bytes\nObserved file size:\t{observed_size} bytes"
                        log.warning(w)
                        return ()  # no files for you today, but we'll try again tomorrow!

                    optimized = self._cloud_optimize(out, out, -9999)
                    if optimized:
                        completed.append(out)

        return completed


class SERVIRDownloader(GlamDownloader):
    def __init__(self, dataset):
        super().__init__(dataset)

        self.index = f"https://gis1.servirglobal.net/data/{dataset}/"

    def query_composites(self, start_date, end_date):
        y1 = int(start_date.split("-")[0])
        y2 = int(end_date.split("-")[0])

        file_names = []

        for year in tqdm(
            range(y1, y2 + 1), desc=f"Querying available {self.dataset} files"
        ):
            dataset_url = self.index + str(year)
            r = requests.get(dataset_url)

            soup = BeautifulSoup(r.text, "html.parser")
            links = soup.find_all("a")

            for link in links:
                if link.text.endswith(".tif"):
                    file_name, ext = os.path.splitext(link.text)
                    datestring = file_name.split("_")[-1]
                    date = datetime.strptime(datestring, "%Y%j")
                    if date >= datetime.strptime(
                        start_date, "%Y-%m-%d"
                    ) and date <= datetime.strptime(end_date, "%Y-%m-%d"):

                        file_names.append(str(year) + "/" + link.text)

        return file_names

    def download_composites(self, start_date, end_date, out_dir):
        composites = self.query_composites(start_date, end_date)
        completed = []

        for composite in tqdm(
            composites, desc=f"Downloading {self.dataset} composites"
        ):
            out = os.path.join(out_dir, composite.split("/")[-1])
            url = self.index + composite
            r = requests.get(url)

            with open(out, "wb") as fd:  # write data in chunks
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    fd.write(chunk)

            # CHECKSUM
            # size of downloaded file in bytes
            observed_size = int(os.stat(out).st_size)
            # size of promised file in bytes, extracted from server-delivered headers
            expected_size = int(r.headers["Content-Length"])

            # checksum failure; return empty tuple
            if observed_size != expected_size:  # checksum failure
                w = f"WARNING:\nExpected file size:\t{expected_size} bytes\nObserved file size:\t{observed_size} bytes"
                log.warning(w)
                return ()  # no files for you today, but we'll try again tomorrow!

            optimized = self._cloud_optimize(out, out, -9999)
            if optimized:
                completed.append(out)

        return completed


class Downloader:
    def __init__(self, dataset):
        # add more short names as needed
        self.short_names = {"chirps": "CHIRPS-2.0", "swi": "swi_12.5km_v3_10daily"}
        dataset = self.short_names.get(dataset, dataset)
        self.dataset = dataset

        if dataset in EARTHDATA_DATASETS:
            self.instance = EarthDataDownloader(dataset)
        elif dataset in UCSB_DATASETS:
            self.instance = UCSBDownloader(dataset)
        elif dataset in CLMS_DATASETS:
            self.instance = CLMSDownloader(dataset)
        elif dataset in SERVIR_DATASETS:
            self.instance = SERVIRDownloader(dataset)
        else:
            raise ValueError(f"Dataset {dataset} not supported")

    def __getattr__(self, name):
        # assume it is implemented by self.instance
        return self.instance.__getattribute__(name)
