"""
This module defines constants and utility functions for extreme event analysis.
"""

# Python standard library modules
import os  # For interacting with the operating system (paths, files, etc.)
from typing import Dict, List, Set

# File extension for data files
DATAFILE_EXTENSION = ".tsv"

# Directory paths for storing data
DIR_PATHS: Dict[str, List[str]] = {
    "root": [],
    "data": ["data"],
    "warnings": ["data", "warnings", "{event}"],
    "observations": ["data", "observations", "{event}"]  
}

# File paths for different types of data files
FILE_PATHS: Dict[str, List[str]] = {
    "stations": [*DIR_PATHS["root"], *DIR_PATHS["data"], f"weather_stations{DATAFILE_EXTENSION}"],
    "thresholds": [*DIR_PATHS["root"], *DIR_PATHS["data"], f"parameter_thresholds{DATAFILE_EXTENSION}"],
    "events": [*DIR_PATHS["root"], *DIR_PATHS["data"], f"events_list{DATAFILE_EXTENSION}"],
    "warnings": [*DIR_PATHS["root"], *DIR_PATHS["warnings"], f"warnings{DATAFILE_EXTENSION}"],
    "observations": [*DIR_PATHS["root"], *DIR_PATHS["observations"], f"observations{DATAFILE_EXTENSION}"],
}

# Namespace for CAP XML data
CAPXML_NAMESPACE: Dict[str, str] = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

# Parameters for observations
OBSERVATION_PARAMETERS: List[str] = ["PR", "VI", "BT", "TA", "NE"]

# Mapping of severity levels to numeric values
MAPVALUES_SEVERITY: Dict[str, int] = {"amarillo": 1, "naranja": 2, "rojo": 3}

# Mapping of station column names from source to target
MAPCOLUMNS_STATIONS: Dict[str, str] = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

# Mapping of observation column names from source to target
MAPCOLUMNS_OBSERVATIONS: Dict[str, str] = {
    "fecha": "date",
    "indicativo": "idema",
    "tmin": "min_temperature",
    "tmax": "max_temperature",
    "prec": "precipitation",
    "racha": "max_wind_speed",
}

# Columns for warning data
COLUMNS_WARNING: List[str] = [
    "id",
    "effective",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

# Columns for threshold data
COLUMNS_THRESHOLDS: List[str] = [
    "geocode",
    "region",
    "area",
    "province",
    "maximum_temperature_yellow_warning",
    "maximum_temperature_orange_warning",
    "maximum_temperature_red_warning",
    "minimum_temperature_yellow_warning",
    "minimum_temperature_orange_warning",
    "minimum_temperature_red_warning",
    "wind_speed_yellow_warning",
    "wind_speed_orange_warning",
    "wind_speed_red_warning",
    "precipitation_12h_yellow_warning",
    "precipitation_12h_orange_warning",
    "precipitation_12h_red_warning",
    "precipitation_1h_yellow_warning",
    "precipitation_1h_orange_warning",
    "precipitation_1h_red_warning",
    "snowfall_24h_yellow_warning",
    "snowfall_24h_orange_warning",
    "snowfall_24h_red_warning",
]

# Columns for CAP XML data
COLUMNS_CAPXML: List[str] = [
    "id",
    "sent",
    "effective",
    "expires",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

# Columns for event data
COLUMNS_EVENTS: Set[str] = {"id", "season", "category", "name", "start", "end"}

def set_root(root: str) -> None:
    """
    Set the root directory path in the DIR_PATHS dictionary.

    Args:
        root (str): The root directory path to be set.
    """
    DIR_PATHS["root"] = [root]

def parameters_allowed() -> List[str]:
    """
    Returns a list of observation parameters.

    Returns:
        List[str]: A list of allowed observation parameters.
    """
    return OBSERVATION_PARAMETERS


def namespace_on_capxml() -> Dict[str, str]:
    """
    Returns the namespace dictionary for CAP XML.

    Returns:
        Dict[str, str]: A dictionary containing the namespace mappings for CAP XML.
    """
    return CAPXML_NAMESPACE


def columns_on_capxml() -> List[str]:
    """
    Returns a list of column names used in CAPXML files.

    Returns:
        List[str]: A list of column names defined in the COLUMNS_CAPXML constant.
    """
    return COLUMNS_CAPXML


def columns_on_stations() -> List[str]:
    """
    Retrieve a list of column names for stations.

    Returns:
        List[str]: A list of column names for stations.
    """
    return list(MAPCOLUMNS_STATIONS.values())


def columns_on_observations() -> List[str]:
    """
    Returns a list of column names for observations.

    Returns:
        List[str]: A list of column names for observations.
    """
    return list(MAPCOLUMNS_OBSERVATIONS.values())


def columns_on_warnings() -> List[str]:
    """
    Returns a list of column names for warnings.

    Returns:
        List[str]: A list of column names for warnings.
    """
    return COLUMNS_WARNING


def columns_on_thresholds() -> List[str]:
    """
    Returns a list of column names for thresholds.

    Returns:
        List[str]: A list of column names for thresholds.
    """
    return COLUMNS_THRESHOLDS


def mapping_observation_columns() -> Dict[str, str]:
    """
    Returns a dictionary that maps observation columns.

    Returns:
        Dict[str, str]: A dictionary containing the mapping of observation columns.
    """
    return MAPCOLUMNS_OBSERVATIONS


def mapping_severity() -> Dict[str, int]:
    """
    Returns a dictionary mapping severity levels to numeric values.

    Returns:
        Dict[str, int]: A dictionary mapping severity levels to numeric values.
    """
    return MAPVALUES_SEVERITY


def retrieve_filepath(file: str, event: str = "") -> str:
    """
    Constructs a file path by formatting and joining path components.

    Args:
        file (str): The key to retrieve the file path template from FILE_PATHS.
        event (str, optional): The event string to format into the path template. Defaults to an empty string.

    Returns:
        str: The constructed file path.
    """
    return os.path.join(*[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in FILE_PATHS[file]])


def retrieve_dirpath(dir: str, event: str = "") -> str:
    """
    Constructs a directory path by formatting and joining components from a predefined dictionary.

    Args:
        dir (str): The key to retrieve the directory path components from the DIR_PATHS dictionary.
        event (str, optional): An optional event string to format into the path components. Defaults to an empty string.

    Returns:
        str: The constructed directory path.
    """
    return os.path.join(*DIR_PATHS["root"], *[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in DIR_PATHS[dir]])    