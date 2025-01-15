"""
This module provides functions for connecting to the AEMET OpenData API and processing meteorological data.

Imports:
    - gzip: For working with gzip compression.
    - logging: For logging and handling log messages.
    - os: For interacting with the operating system (paths, files, etc.).
    - re: For working with regular expressions.
    - tarfile: For working with tar archive files.
    - pandas as pd: For data manipulation and analysis.
    - requests: For making HTTP requests to access the AEMET API.
    - requests.exceptions.RequestException: For handling request-specific exceptions.
    - tenacity: For retrying operations.
    - constants: For accessing global constants used throughout the project.
    - datetime, timedelta: For working with dates and time differences.
"""

import gzip
import logging
import os
import re
import tarfile
from datetime import datetime, timedelta

import pandas as pd
import requests
from requests.exceptions import RequestException
import tenacity

import constants

__OPENDATA_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbHZhcm8uc2FudWRvQGFsdW1ub3MudWkxLmVzIiwianRpIjoiMzMzMWQ4YjgtMjc3OS00NzNmLWFjNDEtYTI0Zjg1NzczOTc4IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3MzExNzA2NzgsInVzZXJJZCI6IjMzMzFkOGI4LTI3NzktNDczZi1hYzQxLWEyNGY4NTc3Mzk3OCIsInJvbGUiOiIifQ.bNt0gjOKShj0PAf2XZ0IUMspaaKVlmdAxy4koTY7gjo"

__OPENDATA_SERVER = "https://opendata.aemet.es/opendata"

__OPENDATA_REQUEST_HEADERS = {"cache-control": "no-cache"}

__OPENDATA_REQUEST_QUERYSTRING = {"api_key": f"{__OPENDATA_API_KEY}"}

__OPENDATA_REQUEST_ENDPOINTS = {
    "warnings": "/api/avisos_cap/archivo/fechaini/{start_date}/fechafin/{end_date}",
    "observations": "/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones",
    # "stations": "/api/valores/climatologicos/inventarioestaciones/todasestaciones/",
    # "geocodes": ""
}

__CAPS_TAR_FILENAME = "caps.tar"


# Retry decorator for API requests
def retry_on_request_exception(func):
    """Decorate a function to retry it if a requests.exceptions.RequestException is raised. The function will be retried up to 3 times with a 5-second wait between attempts.

    Args:
        func: The function to decorate.

    Returns:
        The decorated function.
    """
    
    return tenacity.retry(
        wait=tenacity.wait_fixed(5),
        stop=tenacity.stop_after_attempt(3),
        retry=tenacity.retry_if_exception_type(RequestException),
    )(func)


def __build_request_url(endpoint: str) -> str:
    """Build a request URL for a given endpoint.

    Parameters
    ----------
    endpoint : str
        The endpoint for which to build the request URL.

    Returns
    -------
    str
        The request URL.
    """

    return f"{__OPENDATA_SERVER}{__OPENDATA_REQUEST_ENDPOINTS[endpoint]}?api_key={__OPENDATA_API_KEY}"


def set_api_key(api_key: str):
    """Set the AEMET OpenData API key.

    Parameters
    ----------
    api_key : str
        The API key to set.

    Returns
    -------
    None
    """

    global __OPENDATA_API_KEY
    __OPENDATA_API_KEY = api_key


@retry_on_request_exception
def __request_caps(event: str, date: datetime):
    """
    Download warnings for a given event and date.

    Parameters
    ----------
    event : str
        The event identifier for which to download warnings.
    date : datetime
        The date for which to download warnings.

    Returns
    -------
    None
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
            constants.get_path_to_dir("warnings", event),
            date.strftime("%Y%m%d"),
            __CAPS_TAR_FILENAME,
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
    Downloads and extracts warnings for a given event and date range.

    Parameters
    ----------
    event : str
        The event identifier for which to download warnings.
    start : datetime
        The start date of the event.
    end : datetime
        The end date of the event.

    Returns
    -------
    None
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
    Extracts tar files containing warnings for a specific event and date.

    This function extracts all files from a tar archive located in the 
    specified event and date directory. After extraction, the tar file is 
    removed. If any gzipped files are present in the extracted content, 
    they are further decompressed.

    Parameters
    ----------
    event : str
        The event identifier for which to extract warnings.
    date : datetime
        The date corresponding to the warnings being extracted.

    Raises
    ------
    Exception
        If an error occurs during extraction or file deletion.
    """

    extraction_path = os.path.join(
        constants.get_path_to_dir("warnings", event), date.strftime("%Y%m%d")
    )
    tar_path = os.path.join(extraction_path, __CAPS_TAR_FILENAME)

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
    Decompresses gzip files in the given path.

    This function is an auxiliary function used by :func:`fetch_caps` to extract
    gzipped files that are contained in the tar archives downloaded from AEMET's
    OpenData service.

    Parameters
    ----------
    path : str
        The path to the directory containing the gzip files to be decompressed.

    Raises
    ------
    Exception
        If an error occurs during decompression or file deletion.
    """

    logging.info(f"Decompressing gzips in {path}.")
    try:
        gz_files = [f for f in os.listdir(path) if f.endswith(".gz")]
        for gz in gz_files:
            file = os.path.join(path, re.search(r"\d+", gz).group() + ".tar")
            with gzip.open(os.path.join(path, gz), "rb") as gz_in, open(
                file, "wb"
            ) as gz_out:
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
    Extracts CAP files from tar archives in the specified directory.

    This function iterates over all tar files in the given directory, extracts
    their contents, and subsequently deletes the tar files.

    Parameters
    ----------
    path : str
        The path to the directory containing the tar files to be extracted.

    Raises
    ------
    Exception
        If an error occurs during extraction or file deletion.
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
    Downloads observations for a given date from AEMET's OpenData service.

    This function downloads observations for a given date using the OpenData
    service. The observations are downloaded for the given date and returned as a
    DataFrame.

    Parameters
    ----------
    date : datetime
        The date for which to download observations.

    Returns
    -------
    pd.DataFrame
        The downloaded observations as a DataFrame.

    Raises
    ------
    Exception
        If an error occurs during the request or JSON parsing.
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
    Downloads observations for a given event from AEMET's OpenData service.

    This function downloads observations for a given event using the OpenData
    service. The observations are downloaded for each day of the event and saved
    as a single file.

    Parameters
    ----------
    event : str
        The event identifier for which to download observations.
    start : datetime
        The start date of the event.
    end : datetime
        The end date of the event.

    Raises
    ------
    Exception
        If an error occurs during the request, JSON parsing or data saving.
    """

    dfs = []
    for n in range((end - start).days):
        logging.info(f"Fetching observations for {(start + timedelta(n)):%Y-%m-%d}.")
        dfs.append(__request_observations(start + timedelta(n)))

    observations_df = pd.concat(dfs, ignore_index=True)
    observations_df.rename(columns=constants.mapping_observations_fields, inplace=True)
    observations_df = observations_df[constants.columns_observations]

    try:
        observations_df.to_csv(
            constants.get_path_to_file("observations", event),
            index=False,
            sep="\t",
        )
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        raise
