import os
import gzip
import shutil
import logging
import re
import csv
from io import StringIO

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

try:
    import boto3
except ImportError:
    boto3 = None


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
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# CDSE datasets - Copernicus Data Space Ecosystem collections
CDSE_DATASETS = ["swi_global_12.5km_10daily_v3", "swi_global_12.5km_10daily_v4"]

UCSB_DATASETS = ["CHIRPS-2.0"]

SERVIR_DATASETS = ["esi/4WK", "esi/12WK"]

SUPPORTED_DATASETS = (
    EARTHDATA_DATASETS + CDSE_DATASETS + UCSB_DATASETS + SERVIR_DATASETS
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

    def _cloud_optimize(self, dataset, out_file, nodata=False, cog_driver=False):
        optimized = cloud_optimize(dataset, out_file, nodata, cog_driver)

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
                # try to retreive credentials from environment first
                value.login(strategy="environment")
            except:
                # otherwise prompt for credentials
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
        try:
            query = DataGranules().concept_id(concept_id).temporal(start_date, end_date)
            granules = query.get_all()
        except IndexError:
            granules = []

        return granules

    def query_composites(self, start_date, end_date):
        composites = []
        try:
            granules = self.query_granules(start_date, end_date)
            assert (
                len(granules) > 275
            )  # ensure we have enough granules to create a composite
            # todo: product specific granule check
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
        except AssertionError:
            log.info(
                f"Insufficient granules found to create a composite for {self.dataset}."
            )
            return composites
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

            log.debug(f"downloaded files: {len(vi_files)}")

            # filter files to ensure they belong in this composite
            composite_files = [file for file in vi_files if composite["id"] in file]
            log.debug(f"filtered files: {len(composite_files)}")

            vi_mosaic = self._create_mosaic_cog_from_tifs(
                composite["start_date"], composite_files, out
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


class CDSEDownloader(GlamDownloader):
    """
    Downloader for Copernicus Data Space Ecosystem (CDSE) datasets.
    Uses official CDSE S3 endpoint and boto3 resource API.
    
    For SWI (Soil Water Index) data:
    - swi_global_12.5km_10daily_v3: CLMS/bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v3
    - swi_global_12.5km_10daily_v4: CLMS/bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v4
    
    SWI T-values (depth levels in cm): 001, 005, 010, 015, 020, 040, 060, 100
    Default: T=010 (10cm depth)
    
    Requires CDSE S3 credentials to be set as environment variables:
    - CDSE_S3_ACCESS_KEY: S3 access key
    - CDSE_S3_SECRET_KEY: S3 secret key
    """
    
    def __init__(self, dataset, swi_t_value="010"):
        super().__init__(dataset)
        
        if boto3 is None:
            raise ImportError(
                "boto3 is required for CDSE S3 downloads. "
                "Install it with: pip install boto3"
            )
        
        # SWI T-value (depth level) to download
        valid_t_values = ["001", "005", "010", "015", "020", "040", "060", "100"]
        if swi_t_value not in valid_t_values:
            raise ValueError(
                f"Invalid SWI T-value '{swi_t_value}'. "
                f"Must be one of: {', '.join(valid_t_values)}"
            )
        self.swi_t_value = swi_t_value
        
        # S3 configuration
        self.s3_bucket = "eodata"  # CDSE bucket
        self.s3_resource = self._init_s3_resource()
        self.bucket = self.s3_resource.Bucket(self.s3_bucket)
        
        # Dataset-specific S3 path prefixes
        self.s3_prefixes = {
            "swi_global_12.5km_10daily_v3": "CLMS/bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v3",
            "swi_global_12.5km_10daily_v4": "CLMS/bio-geophysical/soil_water_index/swi_global_12.5km_10daily_v4"
        }
        
        if dataset not in self.s3_prefixes:
            raise ValueError(
                f"Dataset '{dataset}' not configured for CDSE access. "
                f"Available datasets: {list(self.s3_prefixes.keys())}"
            )
    
    def _init_s3_resource(self):
        """Initialize S3 client and resource with CDSE credentials using official endpoint."""
        access_key = os.environ.get("CDSE_S3_ACCESS_KEY")
        secret_key = os.environ.get("CDSE_S3_SECRET_KEY")
        
        if not access_key or not secret_key:
            log.error(
                "CDSE S3 credentials not found in environment variables. "
                "Please set CDSE_S3_ACCESS_KEY and CDSE_S3_SECRET_KEY."
            )
            raise ValueError(
                "CDSE S3 credentials required. "
                "Set environment variables: CDSE_S3_ACCESS_KEY and CDSE_S3_SECRET_KEY"
            )
        
        # Create session with explicit credentials
        session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        # Client for pagination
        self.s3_client = session.client(
            's3',
            endpoint_url='https://eodata.dataspace.copernicus.eu',
            region_name='default'
        )
        # Resource for bucket operations
        s3_resource = session.resource(
            's3',
            endpoint_url='https://eodata.dataspace.copernicus.eu',
            region_name='default'
        )
        return s3_resource
    
    def query_composites(self, start_date, end_date):
        """
        Query available composites in the date range by filtering S3 objects.
        Queries year/month prefixes to efficiently find files without listing all directories.
        
        Args:
            start_date (str): Start date in format 'YYYY-MM-DD'
            end_date (str): End date in format 'YYYY-MM-DD'
        
        Returns:
            list: List of composite metadata dictionaries with 'date' and 's3_key' keys
        """
        prefix = self.s3_prefixes.get(self.dataset)
        
        if not prefix:
            log.error(f"No S3 prefix configured for dataset {self.dataset}")
            return []
        
        log.info(f"Querying {self.dataset} for date range {start_date} to {end_date}")
        
        composites = []
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Generate year/month prefixes to query
        year_month_set = set()
        current = start_dt
        while current <= end_dt:
            year_month_set.add(current.strftime("%Y/%m"))
            # Move to next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        try:
            seen_dates = set()  # Track unique dates to avoid duplicates
            
            for year_month in sorted(year_month_set):
                # Query with year/month prefix to get actual files, not just directory markers
                query_prefix = f"{prefix}/{year_month}/"
                log.info(f"Listing S3 objects with prefix: {query_prefix}")
                
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=query_prefix)
                
                for page in pages:
                    if 'Contents' not in page:
                        continue
                    
                    for obj in page['Contents']:
                        try:
                            s3_key = obj['Key']
                            filename = os.path.basename(s3_key)
                            
                            if not filename or filename.endswith('/'):
                                # Skip directory entries
                                continue
                            
                            # Filter for SWI T-value (e.g., SWI010 for 10cm depth)
                            swi_pattern = f"SWI{self.swi_t_value}"
                            if swi_pattern not in filename:
                                continue
                            
                            # Extract date from filename (YYYYMMDD pattern)
                            date_match = re.search(r"(\d{8})", filename)
                            if date_match:
                                date_str = date_match.group(1)
                                try:
                                    file_date = datetime.strptime(date_str, "%Y%m%d")
                                    date_key = file_date.strftime("%Y-%m-%d")
                                    
                                    if start_dt <= file_date <= end_dt and date_key not in seen_dates:
                                        composites.append({
                                            "date": date_key,
                                            "s3_key": s3_key,
                                            "filename": filename
                                        })
                                        seen_dates.add(date_key)
                                except ValueError:
                                    continue
                        except Exception as e:
                            log.debug(f"Error processing S3 object: {e}")
                            continue
            
            # Sort by date
            composites.sort(key=lambda x: x["date"])
            log.info(f"Found {len(composites)} available composites in date range")
            return composites
        
        except Exception as e:
            log.error(f"Error querying S3 objects: {e}")
            return []
    
    def download_composites(self, start_date, end_date, out_dir):
        """
        Download cloud-optimized GeoTIFF composites from CDSE S3 and validate them.
        
        Args:
            start_date (str): Start date in format 'YYYY-MM-DD'
            end_date (str): End date in format 'YYYY-MM-DD'
            out_dir (str): Output directory path
        
        Returns:
            list: List of paths to downloaded and validated GeoTIFF files
        """
        out = os.path.abspath(out_dir)
        os.makedirs(out, exist_ok=True)
        
        composites = self.query_composites(start_date, end_date)
        
        if not composites:
            log.warning(
                f"No composites found for date range {start_date} to {end_date}. "
                "Please check your CDSE credentials and internet connectivity."
            )
            return []
        
        completed = []
        
        for composite in tqdm(
            composites, desc=f"Downloading {self.dataset} composites"
        ):
            date = composite.get("date")
            s3_key = composite.get("s3_key")
            filename = composite.get("filename")
            
            try:
                # Rename file using convention: {product_id}.{date}.tif
                new_filename = f"{self.dataset}.{date}.tif"
                file_path = os.path.join(out_dir, new_filename)
                
                log.info(f"Downloading {s3_key} to {file_path}")
                self.bucket.download_file(s3_key, file_path)
                
                # Verify file exists and has content
                if not os.path.isfile(file_path) or os.path.getsize(file_path) == 0:
                    log.error(f"Downloaded file is empty or missing: {file_path}")
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    continue
                
                # Validate COG
                try:
                    is_valid = cog_validate(file_path)
                    if is_valid:
                        completed.append(file_path)
                        log.info(f"Successfully validated COG for {date}: {new_filename}")
                    else:
                        log.error(f"COG validation failed for {date}: {new_filename}")
                        os.remove(file_path)
                
                except Exception as e:
                    log.error(f"Error validating COG for {date}: {e}")
                    if os.path.isfile(file_path):
                        os.remove(file_path)
            
            except Exception as e:
                log.error(f"Error downloading composite for {date}: {e}")
                continue
        
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
    def __init__(self, dataset, swi_t_value="010"):
        # add more short names as needed
        self.short_names = {
            "chirps": "CHIRPS-2.0",
            "swi": "swi_global_12.5km_10daily_v4"  # Default to v4, use explicit name for v3
        }
        dataset = self.short_names.get(dataset, dataset)
        self.dataset = dataset

        if dataset in EARTHDATA_DATASETS:
            self.instance = EarthDataDownloader(dataset)
        elif dataset in UCSB_DATASETS:
            self.instance = UCSBDownloader(dataset)
        elif dataset in CDSE_DATASETS:
            self.instance = CDSEDownloader(dataset, swi_t_value=swi_t_value)
        elif dataset in SERVIR_DATASETS:
            self.instance = SERVIRDownloader(dataset)
        else:
            raise ValueError(f"Dataset {dataset} not supported")

    def __getattr__(self, name):
        # assume it is implemented by self.instance
        return self.instance.__getattribute__(name)
