"""
This module provides functions to interact with the AEMET Open Data API. It includes
functions to fetch station inventory data, warning data, and observation data. The
module also includes functions to handle the extraction and decompression of data files.
"""

# Python standard library modules
import gzip  # For working with gzip-compressed files
import logging  # For logging and handling log messages
import os  # For interacting with the operating system (paths, files, etc.)
import re  # For working with regular expressions
import tarfile  # For handling tar-compressed files

# Third-party modules
import pandas as pd  # For data manipulation and analysis
import requests  # For making HTTP requests
from requests.exceptions import RequestException  # For handling requests-related exceptions
import tenacity  # For implementing automatic retries in operations

# Local or custom modules
import constants  # Global constants for the project
import commons  # Common functions from the extreme_event_analysis module

# Specific submodules from the standard library
from datetime import datetime, timedelta  # For working with time differences (days, hours, etc.)

# API key for accessing AEMET Open Data
__OPEN_DATA_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbHZhcm8uc2FudWRvQGFsdW1ub3MudWkxLmVzIiwianRpIjoiMzMzMWQ4YjgtMjc3OS00NzNmLWFjNDEtYTI0Zjg1NzczOTc4IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3MzExNzA2NzgsInVzZXJJZCI6IjMzMzFkOGI4LTI3NzktNDczZi1hYzQxLWEyNGY4NTc3Mzk3OCIsInJvbGUiOiIifQ.bNt0gjOKShj0PAf2XZ0IUMspaaKVlmdAxy4koTY7gjo"

# Base URL for AEMET Open Data
__OPENDATA_SERVER = "https://opendata.aemet.es/opendata"

# Request headers for AEMET Open Data API requests
__OPENDATA_REQUEST_HEADERS = {"cache-control": "no-cache"}

# Query string parameters for AEMET Open Data API requests
__OPENDATA_REQUEST_QUERYSTRING = {
    "api_key": f"{__OPEN_DATA_API_KEY}"
}

# Endpoints for different types of data from AEMET Open Data API
__OPENDATA_REQUEST_ENDPOINTS = {
    "stations": "/api/valores/climatologicos/inventarioestaciones/todasestaciones/",
    "warnings": "/api/avisos_cap/archivo/fechaini/{start_date}/fechafin/{end_date}",
    "observations": "/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones",
}

# Constants for file names
CAPS_TAR_FILENAME = "caps.tar"

# Retry decorator for API requests
def retry_on_request_exception(func):
    return tenacity.retry(
        wait=tenacity.wait_fixed(5),
        stop=tenacity.stop_after_attempt(3),
        retry=tenacity.retry_if_exception_type(RequestException),
    )(func)

def __build_request_url(endpoint: str) -> str:
    """
    Constructs the full request URL for a given AEMET Open Data API endpoint.

    Args:
        endpoint (str): The endpoint to build the URL for.

    Returns:
        str: The full request URL with the API key included.
    """
    return f"{__OPENDATA_SERVER}{__OPENDATA_REQUEST_ENDPOINTS[endpoint]}?api_key={__OPEN_DATA_API_KEY}"

def set_api_key(api_key: str):
    """
    Sets the API key for accessing the AEMET Open Data service.

    Args:
        api_key (str): The API key to be used for accessing the AEMET Open Data service.
    """
    global __OPEN_DATA_API_KEY
    __OPEN_DATA_API_KEY = api_key

@retry_on_request_exception
def __opendata_request_stations() -> pd.DataFrame:
    """
    Makes a request to the AEMET OpenData API to retrieve station data.

    Returns:
        pd.DataFrame: A DataFrame containing the station data retrieved from the API.

    Raises:
        RequestException: If there is an error with the request.
        ValueError: If there is an error parsing the JSON response.
    """
    try:
        response = requests.get(
            __build_request_url("stations"), headers=__OPENDATA_REQUEST_HEADERS
        )
        response.raise_for_status()
        logging.info("Station data retrieved successfully.")
        return pd.DataFrame(response.json()["datos"])
    except RequestException as e:
        logging.error(f"Error retrieving data: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error parsing JSON: {e}")
        raise

def fetch_stations():
    """
    Fetches the latest station inventory data from the AEMET Open Data API.

    Raises:
        Exception: If there is an error while saving the data to a CSV file.
    """
    logging.info("Downloading new station inventory data.")
    stations_df = __opendata_request_stations()
    path = constants.retrieve_filepath("stations")
    try:
        logging.info(f"Saving data to {path}.")
        stations_df.to_csv(path, index=False, sep="\t")
    except Exception as e:
        logging.error(f"Error saving station inventory: {e}")
        raise

@retry_on_request_exception
def __request_caps(event: str, date: datetime):
    """
    Requests and downloads warning data from the AEMET OpenData service for a specific event and date.

    Args:
        event (str): The event type for which warnings data is requested.
        date (datetime): The date for which warnings data is requested.

    Raises:
        RequestException: If there is an error while making the HTTP request.
        ValueError: If there is an error while parsing the JSON response.
    """
    try:
        response = requests.get(
            __build_request_url("warnings").format(
                start_date=date.strftime("%Y-%m-%dT%H:%M:%SUTC"),
                end_date=date.strftime("%Y-%m-%dT23:59:59UTC"),
            ),
            headers=__OPENDATA_REQUEST_HEADERS,
        )
        response.raise_for_status()
        download_url = response.json()["datos"]
    except RequestException as e:
        logging.error(f"Error retrieving data: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error parsing JSON: {e}")
        raise

    try:
        download_file = os.path.join(
            constants.retrieve_dirpath("warnings", event), date.strftime("%Y%m%d"), CAPS_TAR_FILENAME
        )
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()
            with open(download_file, "wb") as dl:
                dl.write(response.content)
        logging.info("Caps downloaded successfully.")
    except RequestException as e:
        logging.error(f"Error downloading data: {e}")
        raise

def fetch_caps(event: str, start: datetime, end: datetime):
    """
    Fetches and extracts warning data from the AEMET Open Data API for a specified date range.

    Args:
        event (str): The event identifier for which to fetch warnings.
        start (datetime): The start date of the range for which to fetch warnings.
        end (datetime): The end date of the range for which to fetch warnings.

    Raises:
        Exception: If there is an error while fetching or extracting warnings for any day in the range.
    """
    logging.info(f"Downloading warnings between {start:%Y-%m-%d} and {end:%Y-%m-%d}.")
    for n in range(int((end - start).days) + 1):
        try:
            logging.info(f"Downloading warnings for {(start + timedelta(n)):%Y-%m-%d}.")
            __request_caps(event, (start + timedelta(n)))
        except Exception as e:
            logging.error(f"Error fetching warnings: {e}")
            raise
        try:
            logging.info(f"Extracting warnings for {(start + timedelta(n)):%Y-%m-%d}.")
            __extract_tars(event, (start + timedelta(n)))
        except Exception as e:
            logging.error(f"Error extracting warnings: {e}")
            raise        

def __extract_tars(event: str, date: datetime):
    """
    Extracts warning data from tar files and decompresses any gzipped files within.

    Args:
        event (str): The event type for which warnings data is being extracted.
        date (datetime): The date for which warnings data is being extracted.

    Raises:
        Exception: If there is an error during the extraction or decompression process.
    """
    extraction_path = os.path.join(constants.retrieve_dirpath("warnings", event), date.strftime("%Y%m%d"))
    tar_path = os.path.join(extraction_path, CAPS_TAR_FILENAME)
    
    try:
        logging.info(f"Extracting warnings in {extraction_path}.")
        with tarfile.open(tar_path, "r") as t:
            members = t.getmembers()
            logging.info(f"Extracting {len(members)} files.")
            for member in members:
                t.extract(member, path=extraction_path)
        
        try:
            os.remove(tar_path)
        except Exception as e:
            logging.error(f"Error deleting tar: {e}")
            raise

        __extract_gzips(extraction_path)
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def __extract_gzips(path: str):
    """
    Extracts gzipped files in the specified directory and decompresses them.

    Args:
        path (str): The directory path where gzipped files are located.

    Raises:
        Exception: If there is an error during the decompression process.
    """
    logging.info(f"Decompressing gzips in {path}.")
    try:
        gz_files = [f for f in os.listdir(path) if f.endswith(".gz")]
        for gz in gz_files:
            file = os.path.join(path, re.search(r"\d+", gz).group() + ".tar")
            with gzip.open(os.path.join(path, gz), "rb") as gz_in, open(file, "wb") as gz_out:
                gz_out.write(gz_in.read())
            try:
                os.remove(os.path.join(path, gz))
            except Exception as e:
                logging.error(f"Error deleting gzip {gz}: {e}")
                raise

        __extract_caps(path)
    except Exception as e:
        logging.error(f"Error during gzip decompression: {e}")
        raise

def __extract_caps(path: str):
    """
    Extracts tar files containing CAP files in the specified directory.

    Args:
        path (str): The directory path where tar files are located.

    Raises:
        Exception: If there is an error during the extraction or deletion process.
    """
    logging.info(f"Extracting CAP files in {path}.")
    try:
        tar_files = [f for f in os.listdir(path) if f.endswith(".tar")]
        for tar in tar_files:
            with tarfile.open(os.path.join(path, tar), "r") as t:
                members = t.getmembers()
                for member in members:
                    t.extract(member, path=path)
            try:
                os.remove(os.path.join(path, tar))
            except Exception as e:
                logging.error(f"Error deleting {tar}: {e}")
                raise
    except Exception as e:
        logging.error(f"Error during CAP extraction: {e}")
        raise

@retry_on_request_exception
def __request_observations(date: datetime) -> pd.DataFrame:
    """
    Requests and retrieves observation data from the AEMET OpenData service for a specific date.

    Args:
        date (datetime): The date for which observation data is requested.

    Returns:
        pd.DataFrame: A DataFrame containing the observation data retrieved from the API.

    Raises:
        RequestException: If there is an error while making the HTTP request.
        ValueError: If there is an error while parsing the JSON response.
    """
    try:
        response = requests.get(
            __build_request_url("observations").format(
                start_date=date.strftime("%Y-%m-%dT%H:%M:%SUTC"),
                end_date=date.strftime("%Y-%m-%dT23:59:59UTC"),
            ),
            headers=__OPENDATA_REQUEST_HEADERS,
        )
        response.raise_for_status()
        data_link = response.json()["datos"]
        response = requests.get(data_link)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except RequestException as e:
        logging.error(f"Error retrieving data: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error parsing JSON: {e}")
        raise

def fetch_observations(event: str, start: datetime, end: datetime) -> None:
    """
    Fetches observation data from the AEMET Open Data API for a specified date range.

    Args:
        event (str): The event identifier for which to fetch observations.
        start (datetime): The start date of the range for which to fetch observations.
        end (datetime): The end date of the range for which to fetch observations.

    Raises:
        Exception: If there is an error while fetching or saving observations.
    """
    dfs = []
    for n in range((end - start).days):
        logging.info(f"Fetching observations for {(start + timedelta(n)):%Y-%m-%d}.")
        dfs.append(__request_observations(start + timedelta(n)))

    observations_df = pd.concat(dfs, ignore_index=True)
    observations_df.rename(columns=constants.mapping_observation_columns(), inplace=True)
    observations_df = observations_df[constants.columns_on_observations()]
    
    try:
        observations_df.to_csv(
            constants.retrieve_filepath("observations", event),
            index=False,
            sep="\t",
        )
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        raise