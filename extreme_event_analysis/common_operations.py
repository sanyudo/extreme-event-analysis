"""
This module provides various utility functions for extreme event analysis, including
directory management, coordinate conversion, data cleaning, transformation, and extraction
for meteorological observations and warnings.
"""

# Python standard library modules
import logging  # For logging and handling log messages
import os  # For interacting with the operating system (paths, files, etc.)
import shutil # For deleting directories
from typing import Dict, List, Set
import re  # For working with regular expressions
import xml.etree.ElementTree as ET  # For parsing XML files
from datetime import datetime, timedelta  # For working with dates and times

# Third-party modules
import pandas as pd  # For data manipulation and analysis
import numpy as np # For operations
from shapely import Point, Polygon # Operation with coordinates


# Local or custom modules
import constants  # Global constants for the project

__SEVERE_PRECIPITATION_BY_TIMEFRAME = {1: 0.33, 12: 0.85}
__EXTREME_PRECIPITATION_BY_TIMEFRAME = {1: 0.50, 12: 1.00}

def ensure_directories(event: str, start: datetime, end: datetime) -> bool:
    """
    Ensure that directories specified in constants.DIR_PATHS exist for a given event.

    Args:
        event (str): The event string used to format the directory paths.

    Returns:
        bool: True if all directories exist or were successfully created, False otherwise.
    """
    for d in constants.path_to_dir.values():
        path = os.path.join(*[item.format(event=event) if isinstance(item, str) and "{event}" in item else item 
                 for item in d])
        try:
            if not os.path.exists(path):
                logging.info(f"Creating directory: {path}")
                os.makedirs(path, exist_ok=True)
            else:
                logging.info(f"Directory already exists: {path}")
        except Exception as e:
            logging.error(f"Error ensuring directory {path} exists for event {event}: {e}")
            return False
        
        path = constants.get_path_to_dir("warnings", event=event)
        for n in range((end - start).days + 1):
            n_path = os.path.join(path, (start + timedelta(n)).strftime("%Y%m%d"))
            try:
                if not os.path.exists(n_path):
                    logging.info(f"Creating directory: {n_path}")
                    os.makedirs(n_path, exist_ok=True)
                else:
                    logging.info(f"Directory already exists: {n_path}")
            except Exception as e:
                logging.error(f"Error ensuring directory {n_path} exists for event {event} and date {(start + timedelta(n)).strftime('%Y-%m-%d')}: {e}")
                return False    
    return True

def clean_files(event: str) -> bool:  
    """
    Cleans the directory associated with the given event by removing all subdirectories.    

    Args:
        event (str): The event string used to format the directory paths.

    Returns:
        bool: True if all directories exist or were successfully cleaned, False otherwise.
    """
    path = os.path.join(constants.get_path_to_dir("warnings", event=event))
    try:
        if os.path.exists(path):
            logging.info(f"Cleaning directory: {path}")
            for s in os.listdir(path):
                subpath = os.path.join(path, s)
                if os.path.isdir(subpath):
                    logging.info(f"Removing subdirectory: {subpath}")
                    shutil.rmtree(subpath)
    except Exception as e:
        logging.error(f"Error cleaning directory {path} for event {event}: {e}")
        return False
    return True

def dms_coordinates_to_degress(dms_coordinate: str, hemisphere: str) -> float:
    """
    Convert a coordinate in degrees, minutes, and seconds (DMS) format to decimal degrees.

    Args:
        dms_coordinate (str): The coordinate in DMS format as a string (e.g., "123456" for 12°34'56").
        hemisphere (str): The hemisphere indicator ('N', 'S', 'E', 'W').

    Returns:
        float: The coordinate in decimal degrees. Negative for 'S' and 'W' hemispheres.
    """
    logging.debug(f"Converting DMS coordinate: {dms_coordinate} {hemisphere}")
    dms_value = dms_coordinate.replace(hemisphere, "")
    logging.debug(f"Stripped DMS value: {dms_value}")
    degrees = int(dms_value[:2])
    minutes = int(dms_value[2:4])
    seconds = int(dms_value[4:])
    logging.debug(f"Parsed degrees: {degrees}, minutes: {minutes}, seconds: {seconds}")
    result = degrees + minutes / 60 + seconds / 3600
    logging.debug(f"Decimal degrees result: {result}")
    return -result if hemisphere in {"S", "W"} else result


"""
    Warnings
"""
def exist_caps(event: str) -> bool:
    """
    Checks if there are any XML files in the directory structure for a given event.

    Args:
        event (str): The name of the event to check for XML files.

    Returns:
        bool: True if there is at least one XML file in the directory structure, False otherwise.
    """
    path = constants.get_path_to_dir("warnings", event)
    logging.info(f"Checking for CAP XML files in {path}.")
    if os.path.exists(path):
        for dir_name in [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]:
            dir_path = os.path.join(path, dir_name)
            logging.info(f"Checking directory: {dir_path}")
            for subdir_name in [sd for sd in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, sd))]:
                subdir_path = os.path.join(dir_path, subdir_name)
                logging.info(f"Checking subdirectory: {subdir_path}")
                if any(cap.endswith(".xml") for cap in os.listdir(subdir_path)):
                    logging.info(f"Found CAP XML file in {subdir_path}")
                    return True
    logging.info(f"No CAP XML files found for event: {event}")
    return False

def data_extract_caps(event: str) -> pd.DataFrame:
    """
    Extract warning data from CAP XML files for a specific event.

    Args:
        event (str): The name of the event to extract data for.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted warning data.
    """
    path = constants.get_path_to_dir("warnings", event=event)
    logging.info(f"Consolidating CAP files in {path}.")

    df = pd.DataFrame(columns=constants.fields_cap)
    dirs = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    logging.debug(f"Directories found: {dirs}")
    for dir in dirs:
        caps = [f for f in os.listdir(os.path.join(path, dir)) if f.endswith(".xml")]
        logging.debug(f"CAP files in {dir}: {caps}")
        for cap in caps:
            try:
                logging.info(f"Reading file {os.path.join(path, dir, cap)}.")
                tree = ET.parse(os.path.join(path, dir, cap))
                root = tree.getroot()

                cap_identifier = root.find("cap:identifier", constants.namespace_cap).text
                cap_sent = root.find(".//cap:sent", constants.namespace_cap).text
                info_elements = root.findall(".//cap:info", constants.namespace_cap)
                selected_info = None
                for info in info_elements:
                    language = info.get("lang")
                    if language == "es-ES":
                        selected_info = info
                        break
                    elif language == "en-GB" and selected_info is None:
                        selected_info = info
                if selected_info is None:
                    selected_info = info_elements[0]

                cap_effective = selected_info.find(
                    ".//cap:effective", constants.namespace_cap
                ).text
                cap_expires = selected_info.find(".//cap:expires", constants.namespace_cap).text
                cap_event_code = selected_info.find(
                    ".//cap:eventCode/cap:value", constants.namespace_cap
                ).text
                if cap_event_code:
                    cap_event_code = cap_event_code.split(";")

                parameters = {}
                for param in selected_info.findall(".//cap:parameter", constants.namespace_cap):
                    param_name = param.find("cap:valueName", constants.namespace_cap).text
                    param_value = param.find("cap:value", constants.namespace_cap).text
                    parameters[param_name] = param_value

                    cap_severity = parameters.get("AEMET-Meteoalerta nivel", None)
                    cap_parameter = parameters.get("AEMET-Meteoalerta parametro", None)
                    if cap_parameter:
                        cap_parameter = cap_parameter.split(";")
                    else:
                        cap_parameter = ["", "", "0"]

                if cap_event_code[0] == "PR" and "una hora" in cap_parameter[1]:
                    cap_event_code[0] = "PR_1H"
                elif cap_event_code[0] == "PR" and "12 horas" in cap_parameter[1]:
                    cap_event_code[0] = "PR_12H"

                cap_polygon = []
                for area in selected_info.findall(".//cap:area", constants.namespace_cap):
                    geocode = area.find("cap:geocode", constants.namespace_cap)
                    cap_geocode = geocode.find("cap:value", constants.namespace_cap).text
                    cap_polygon = area.find("cap:polygon", constants.namespace_cap).text

                if cap_severity in constants.mapping_severity_values.keys():
                    if constants.mapping_severity_values.get(cap_severity) > 0:
                        df.loc[len(df)] = {
                            "id": cap_identifier,
                            "sent": cap_sent,
                            "effective": cap_effective,
                            "expires": cap_expires,
                            "severity": cap_severity,
                            "param_id": cap_event_code[0] if cap_event_code else None,
                            "param_name": cap_parameter[1],
                            "param_value": re.sub(r"[^\d]", "", cap_parameter[2]),
                            "geocode": cap_geocode,
                            "polygon": cap_polygon,
                        }
                        logging.debug(f"Row added to DataFrame: {df.iloc[-1].to_dict()}")
            except Exception as e:
                logging.error(f"Error reading {os.path.join(path, dir, cap)}: {e}")
                raise
    logging.info(f"Consolidation complete. Total rows: {len(df)}")
    return df.dropna()

def caps_to_warnings(event: str) -> pd.DataFrame:
    """
    Fetch warnings for a given event by extracting data from cap files.

    Args:
        event (str): The name or identifier of the event for which warnings are to be fetched.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted warning data.

    Logs:
        Logs an info message indicating the start of data extraction from cap files.
    """
    logging.info(f"Starting to fetch warnings for event: {event}")

    # Extract raw warning data from CAP XML files
    logging.info("Extracting data from CAP XML files...")
    warnings = data_extract_caps(event)
    logging.info(f"Extracted {len(warnings)} raw warnings")

    # Transform the raw warning data
    logging.info("Transforming raw warning data...")
    warnings = data_transform_warnings(warnings)
    logging.info(f"Transformed warnings into {len(warnings)} expanded rows")

    # Clean the transformed warning data
    logging.info("Cleaning transformed warning data...")
    warnings = data_clean_warnings(warnings)
    logging.info(f"Cleaned warnings, resulting in {len(warnings)} final rows")

    logging.info(f"Completed fetching warnings for event: {event}")
    try:
        logging.info(f"... storing data in {constants.get_path_to_file('warnings', event)}.")
        warnings.to_csv(
            constants.get_path_to_file('warnings', event),
            index=False,
            sep="\t",
        )
    except Exception as e:
        logging.error(f"Error storing warning data: {e}")
        raise

def data_transform_warnings(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw warning data by filtering, mapping severity, converting dates, and expanding rows.

    Args:
        warnings (pd.DataFrame): DataFrame containing raw warning data.

    Returns:
        pd.DataFrame: Transformed DataFrame with expanded rows for each day from 'effective' to 'expires'.
    """
    logging.info("Filtering warnings to include only allowed parameters...")
    warnings = warnings[warnings["param_id"].isin(constants.allowed_parameters_ids)]
    
    logging.info("Converting date columns to datetime objects...")
    warnings["sent"] = pd.to_datetime(
        warnings["sent"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    )
    warnings["effective"] = pd.to_datetime(
        warnings["effective"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    ).dt.date
    warnings["expires"] = pd.to_datetime(
        warnings["expires"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    ).dt.date

    logging.info("Converting 'param_value' and 'geocode' to numeric values...")
    warnings["param_value"] = pd.to_numeric(warnings["param_value"], errors="coerce")

    logging.info("Expanding rows for each day from 'effective' to 'expires'...")
    processed_rows = []
    for _, row in warnings.iterrows():
        row_dict = row.to_dict()
        effective = row_dict["effective"]
        expires = row_dict["expires"]

        while effective < expires:
            new_row = row_dict.copy()
            new_row["effective"] = effective
            new_row["expires"] = effective
            processed_rows.append(new_row)
            effective += timedelta(days=1)

        new_row = row_dict.copy()
        new_row["effective"] = effective
        new_row["expires"] = expires
        processed_rows.append(new_row)

    logging.info("Transformation complete. Returning processed DataFrame.")
    return pd.DataFrame(processed_rows)

def data_clean_warnings(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and processes raw warning data by sorting and grouping.

    This function sorts the input DataFrame of warnings based on multiple columns 
    and then groups the data by 'geocode', 'param_id', and 'effective' columns. 
    Within each group, it retains only the most severe and most recently sent warning.

    Parameters:
    warnings (pd.DataFrame): A DataFrame containing raw warning data with columns 
                             'geocode', 'param_id', 'effective', 'severity', and 'sent'.

    Returns:
    pd.DataFrame: A cleaned DataFrame with the most severe and recent warning for each group.
    """
    logging.info("Sorting warnings by geocode, param_id, effective, severity, and sent...")
    warnings.sort_values(
        by=[
            "geocode",
            "param_id",
            "effective",
            "severity",
            "sent",
        ],
        ascending=[True, True, True, False, False],
        inplace=True,
    )
    warnings.reset_index(drop=True, inplace=True)
    
    logging.info("Grouping warnings by geocode, param_id, and effective, and selecting the most severe and recent warning...")
    warnings = warnings.groupby(["geocode", "param_id", "effective"], as_index=False).apply(
        lambda x: x.sort_values(by=["severity", "sent"], ascending=[False, False]).head(1)
    )
    
    logging.info("Cleaning complete. Returning cleaned warnings DataFrame.")
    return warnings[constants.columns_warnings]

def exist_warnings(event: str) -> bool:
    """
    Check if warning data exist for a given event.

    Args:
        event (str): The name or identifier of the event.

    Returns:
        bool: True if warning files exist for the event, False otherwise.
    """
    return os.path.exists(constants.get_path_to_file("warnings", event))

def get_warnings(event: str) -> pd.DataFrame:
    """
    Retrieve warning data for a specific event from a CSV file.

    This function reads a tab-separated CSV file containing warning data for the specified event
    and returns it as a pandas DataFrame.

    Args:
        event (str): The name of the event for which to retrieve warning data.

    Returns:
        pd.DataFrame: A DataFrame containing the warning data for the specified event.

    Raises:
        FileNotFoundError: If the CSV file for the specified event does not exist.
        pd.errors.ParserError: If there is an error parsing the CSV file.
    """
    return prepare_warnings(pd.read_csv(constants.get_path_to_file("warnings", event=event), sep="\t"))

def prepare_warnings(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare and preprocess warning data.

    This function converts the 'effective' column to datetime objects,
    and 'param_value' and 'geocode' columns to numeric values.

    Args:
        warnings (pd.DataFrame): DataFrame containing warning data.

    Returns:
        pd.DataFrame: Preprocessed DataFrame with converted columns.

    Raises:
        Exception: If an error occurs during preprocessing, it is logged and the exception is raised.
    """
    logging.info("Preprocessing warning data...")
    try:
        warnings["effective"] = pd.to_datetime(warnings["effective"], format="%Y-%m-%d")
        warnings["param_value"] = pd.to_numeric(warnings["param_value"], errors="coerce")
        return warnings
    except Exception as e:
        logging.error(f"Error preprocessing warning data: {e}")
        raise

"""
    Stations
"""
def exist_stations() -> bool:
    """
    Checks if the file list for stations exists.

    Returns:
        bool: True if the file path for stations exists, False otherwise.
    """
    return os.path.exists(constants.get_path_to_file("stations"))

def get_stations() -> pd.DataFrame:
    """
    Retrieve and prepare data for stations.

    This function reads station data from a file specified by the constants module,
    processes it, and returns it as a pandas DataFrame. The data is expected to be
    in a tab-separated values (TSV) format.

    Returns:
        pd.DataFrame: A DataFrame containing the processed station data.

    Raises:
        Exception: If there is an error reading the file or processing the data,
                   an error message is logged and the exception is re-raised.
    """
    try:
        return prepare_stations(pd.read_csv(constants.get_path_to_file("stations"), sep="\t"))
    except Exception as e:
        logging.error(f"Error retrieving station data: {e}")
        raise

def prepare_stations(stations: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares and transforms the station data by renaming columns, formatting text fields,
    and converting coordinate formats.

    Args:
        stations (pd.DataFrame): DataFrame containing station data with columns that need to be transformed.

    Returns:
        pd.DataFrame: Transformed DataFrame with renamed columns, formatted text fields, and converted coordinates.
    """
    # Rename columns according to the mapping defined in constants
    stations.rename(columns=constants.mapping_observations_fields, inplace=True)
    
    # Format 'province' and 'name' columns to title case
    stations[["province", "name"]] = stations[["province", "name"]].applymap(str.title)
    
    # Convert 'latitude' and 'longitude' from DMS to decimal degrees
    if (not stations["latitude"].dtype == "float64") or any(re.search(r'[a-zA-Z]', str(x)) for x in stations["latitude"]):
        stations["latitude"] = stations["latitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    if (not stations["longitude"].dtype == "float64") or any(re.search(r'[a-zA-Z]', str(x)) for x in stations["longitude"]):
        stations["longitude"] = stations["longitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )
    
    # Select only the columns defined in constants
    stations = stations.loc[:, constants.columns_stations]
    
    # Convert 'latitude', 'longitude', and 'altitude' to numeric values
    stations["latitude"] = pd.to_numeric(stations["latitude"], errors="coerce")
    stations["longitude"] = pd.to_numeric(stations["longitude"], errors="coerce")
    stations["altitude"] = pd.to_numeric(stations["altitude"], errors="coerce")
    
    # Initialize 'geocode' column with None
    stations["geocode"] = None
    
    return stations

"""
    Events
"""
def get_events() -> pd.DataFrame:
    """
    Retrieve and prepare data events from a specified file.

    This function reads event data from a file specified by the constants module,
    processes it using the __prepare_data_events function, and returns the resulting
    DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the prepared event data.

    Raises:
        FileNotFoundError: If the file specified by constants.retrieve_filepath("events") does not exist.
        pd.errors.ParserError: If there is an error parsing the file.
    """
    return prepare_events(pd.read_csv(constants.get_path_to_file("events"), sep="\t"))

def prepare_events(events: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares and sorts event data by converting 'start' and 'end' columns to datetime objects.

    This function takes a DataFrame containing event data with 'start' and 'end' columns in string format,
    converts these columns to datetime objects, and sorts the DataFrame by the 'start' and 'end' dates.

    Args:
        events (pd.DataFrame): A DataFrame containing event data with 'start' and 'end' columns in string format.

    Returns:
        pd.DataFrame: A DataFrame with 'start' and 'end' columns converted to datetime objects and sorted by these columns.
    """
    events["start"] = pd.to_datetime(events["start"], format="%d/%m/%Y")
    events["end"] = pd.to_datetime(events["end"], format="%d/%m/%Y")
    return events.sort_values(by=["start", "end"])

"""
    Thresholds
"""
def get_thresholds() -> pd.DataFrame:
    """
    Retrieve and prepare data thresholds from a specified file.

    This function reads a tab-separated values (TSV) file containing threshold data 
    from a file path specified in the constants module. It then processes the data 
    using the __prepare_data_thresholds function and returns the resulting DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the processed threshold data.
    """
    return prepare_thresholds(pd.read_csv(constants.get_path_to_file("thresholds"), sep="\t"))

def prepare_thresholds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the data by converting specified columns to numeric values.

    This function takes a DataFrame and converts the columns specified in 
    `constants.columns_on_thresholds()` to numeric values, except for the 
    columns "geocode", "region", "area", and "province". If a conversion 
    fails, the value is set to NaN.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data to be processed.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns converted to numeric values.

    Raises:
        Exception: If an error occurs during the processing, it is logged and the exception is raised.
    """
    try:
        for c in constants.columns_thresholds:
            if c not in ["geocode", "region", "area", "province"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception as e:
        logging.error(f"Error preparing thresholds data: {e}")

"""
    Geocodes
"""
def get_geocodes() -> pd.DataFrame:
    """
    Retrieve and prepare data geocodes from a specified file.

    This function reads a tab-separated values (TSV) file containing threshold data 
    from a file path specified in the constants module. It then processes the data 
    using the __prepare_data_geocodes function and returns the resulting DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the processed threshold data.
    """
    return pd.read_csv(constants.get_path_to_file("geocodes"), sep="\t")

"""
    Observations
"""
def exist_observations(event: str) -> bool:
    """
    Check if observation data exists for a given event.

    Args:
        event (str): The name of the event to check for observation data.

    Returns:
        bool: True if the observation data file exists, False otherwise.
    """
    return os.path.exists((constants.get_path_to_file("observations", event)))

def get_observations(event: str, stations: list) -> pd.DataFrame:
    """
    Retrieve observational data for a given event from a CSV file.

    This function reads a tab-separated values (TSV) file containing observational data
    for the specified event and returns it as a pandas DataFrame.

    Args:
        event (str): The name of the event for which to retrieve observational data.

    Returns:
        pd.DataFrame: A DataFrame containing the observational data for the specified event.

    Raises:
        FileNotFoundError: If the file for the specified event does not exist.
        pd.errors.ParserError: If there is an error parsing the CSV file.
    """
    return prepare_observations(observations=(pd.read_csv(constants.get_path_to_file("observations", event=event), sep="\t")), stations=stations, event=event)

def prepare_observations(observations: pd.DataFrame, stations: list, event: str) -> pd.DataFrame:
    """
    Prepare and preprocess observational data.

    This function renames columns, filters observations by station IDs, converts date and numeric columns,
    and calculates additional precipitation metrics.

    Args:
        observations (pd.DataFrame): DataFrame containing raw observational data.
        stations (list): List of station IDs to filter the observations.

    Returns:
        pd.DataFrame: Preprocessed DataFrame with renamed columns, filtered rows, and additional metrics.
    """
    logging.info("Renaming observation columns...")
    observations.rename(columns=constants.mapping_observations_fields, inplace=True)
    
    logging.info("Selecting relevant columns...")
    observations = observations[constants.columns_observations]
    
    logging.info("Filtering observations by station IDs...")
    observations = observations[observations["idema"].isin(stations)]

    logging.info("Converting 'date' column to datetime...")
    observations["date"] = pd.to_datetime(observations["date"], format="%Y-%m-%d")
    
    logging.info("Converting numeric columns and handling missing values...")
    for column in ["minimum_temperature", "maximum_temperature", "precipitation", "wind_speed"]:
        observations[column] = pd.to_numeric(
            observations[column].str.replace(",", "."), errors="coerce"
        )
    
    logging.info("Dropping rows with NaN values...")
    observations.dropna(inplace=True)
    
    logging.info("Calculating additional precipitation metrics...")
    observations["uniform_precipitation_1h"] = np.round(observations["precipitation"] * 1 / 24, 1)
    observations["severe_precipitation_1h"] = np.round(observations["precipitation"] * float(__SEVERE_PRECIPITATION_BY_TIMEFRAME[1]), 1)
    observations["extreme_precipitation_1h"] = np.round(observations["precipitation"] * float(__EXTREME_PRECIPITATION_BY_TIMEFRAME[1]), 1)
    observations["uniform_precipitation_12h"] = np.round(observations["precipitation"] * 12 / 24, 1)
    observations["extreme_precipitation_12h"] = np.round(observations["precipitation"] * float(__EXTREME_PRECIPITATION_BY_TIMEFRAME[12]), 1)
    if "RAIN" in event:
        observations["snowfall_24h"] = observations.apply(lambda row: row["precipitation"] if row["maximum_temperature"] <= 5 else 0, axis=1)
    else:
        observations["snowfall_24h"] = observations.apply(lambda row: row["precipitation"] if row["minimum_temperature"] <= 5 else 0, axis=1)

    logging.info("Observational data preparation complete.")
    return observations

"""
    Geolocated stations
"""
def geolocate_stations() -> pd.DataFrame:
    geocodes = get_geocodes()
    stations = get_stations()
    geocodes["geocode"] = geocodes["geocode"].astype(str)        
    geocodes["area"] = geocodes["polygon"].apply(
        lambda coordinates: Polygon(
        [tuple(map(float, pair.split(","))) for pair in coordinates.split()]
        )
    )

    stations["geocode"] = None
    stations["point"] = stations.apply(
        lambda x: Point(x["latitude"], x["longitude"]), axis=1
    )
    stations["geocode"] = stations.apply(
        lambda station: next(
            (a["geocode"] for _, a in geocodes.iterrows() if a["area"].contains(station["point"])),
            None
        ),
        axis=1
    )

    stations.drop(columns=["point"], inplace=True)

    stations.to_csv(constants.get_path_to_file("geolocated"), sep="\t")

def exist_gelocated_stations() -> bool:
    """
    Checks if the file list for stations exists.

    Returns:
        bool: True if the file path for stations exists, False otherwise.
    """
    return os.path.exists(constants.get_path_to_file("geolocated"))

def get_geolocated_stations() -> pd.DataFrame:
    """
    Retrieve and prepare data for stations.

    This function reads station data from a file specified by the constants module,
    processes it, and returns it as a pandas DataFrame. The data is expected to be
    in a tab-separated values (TSV) format.

    Returns:
        pd.DataFrame: A DataFrame containing the processed station data.

    Raises:
        Exception: If there is an error reading the file or processing the data,
                   an error message is logged and the exception is re-raised.
    """
    try:
        return prepare_geolocated_stations(pd.read_csv(constants.get_path_to_file("geolocated"), sep="\t"))
    except Exception as e:
        logging.error(f"Error retrieving geolocated data: {e}")
        raise

def prepare_geolocated_stations(geolocated_sations: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares and transforms the station data by renaming columns, formatting text fields,
    and converting coordinate formats.

    Args:
        geolocated_sations (pd.DataFrame): DataFrame containing station data with columns that need to be transformed.

    Returns:
        pd.DataFrame: Transformed DataFrame with renamed columns, formatted text fields, and converted coordinates.
    """
    # Rename columns according to the mapping defined in constants
    geolocated_sations.rename(columns=constants.mapping_observations_fields, inplace=True)
    
    # Format 'province' and 'name' columns to title case
    geolocated_sations[["province", "name"]] = geolocated_sations[["province", "name"]].applymap(str.title)
    
    # Convert 'latitude' and 'longitude' from DMS to decimal degrees
    if (not geolocated_sations["latitude"].dtype == "float64") or any(re.search(r'[a-zA-Z]', str(x)) for x in geolocated_sations["latitude"]):
        geolocated_sations["latitude"] = geolocated_sations["latitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    if (not geolocated_sations["longitude"].dtype == "float64") or any(re.search(r'[a-zA-Z]', str(x)) for x in geolocated_sations["longitude"]):
        geolocated_sations["longitude"] = geolocated_sations["longitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )
    
    # Convert 'latitude', 'longitude', and 'altitude' to numeric values
    geolocated_sations["latitude"] = pd.to_numeric(geolocated_sations["latitude"], errors="coerce")
    geolocated_sations["longitude"] = pd.to_numeric(geolocated_sations["longitude"], errors="coerce")
    geolocated_sations["altitude"] = pd.to_numeric(geolocated_sations["altitude"], errors="coerce")
    
    return geolocated_sations
