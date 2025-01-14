"""
This module defines constants and utility functions for extreme event analysis.
"""

# Python standard library modules
import os  # For interacting with the operating system (paths, files, etc.)
from typing import Dict, List, Set

# File extension for data files
__FILE_EXT= ".tsv"

# Directory paths for storing data
__DIR_PATHS: Dict[str, List[str]] = {
    "root": [],
    "data": ["data"],
    "warnings": ["data", "warnings", "{event}"],
    "observations": ["data", "observations", "{event}"]  
}

# File paths for different types of data files
__FILE_PATHS: Dict[str, List[str]] = {
    "stations": [*__DIR_PATHS["root"], *__DIR_PATHS["data"], f"weather_stations{__FILE_EXT}"],
    "thresholds": [*__DIR_PATHS["root"], *__DIR_PATHS["data"], f"parameter_thresholds{__FILE_EXT}"],
    "geocodes": [*__DIR_PATHS["root"], *__DIR_PATHS["data"], f"geocode_polygons{__FILE_EXT}"],
    "events": [*__DIR_PATHS["root"], *__DIR_PATHS["data"], f"events_list{__FILE_EXT}"],
    "warnings": [*__DIR_PATHS["root"], *__DIR_PATHS["warnings"], f"warnings{__FILE_EXT}"],
    "observations": [*__DIR_PATHS["root"], *__DIR_PATHS["observations"], f"observations{__FILE_EXT}"],
}

# Namespace for CAP XML data
__CAP_XML_NAMESPACE: Dict[str, str] = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

# Parameters for observations
__ALLOWED_OBSERVATIONS: List[str] = ["PR_1H", "PR_12H", "VI", "BT", "TA", "NE"]

# Mapping of parameters (ids) and observations
__MAPPING_PARAMETERS: Dict[str, str] = {
    "BT": "minimum_temperature",
    "AT": "maximum_temperature",
    "PR_1h": "precipitation_1h",
    "PR_12h": "precipitation_12h",
    "NE": "snowfall_24h",
    "VI": "wind_speed"
}

# Mapping of severity levels to numeric values
__MAPPING_SEVERITY: Dict[str, int] = {"verde": 0, "amarillo": 1, "naranja": 2, "rojo": 3}

# Mapping of station column names from source to target
__MAPPING_STATIONS_COLUMNS: Dict[str, str] = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

# Mapping of observation column names from source to target
__MAPPING_OBSERVATIONS_COLUMNS: Dict[str, str] = {
    "fecha": "date",
    "indicativo": "idema",
    "tmin": "minimum_temperature",
    "tmax": "maximum_temperature",
    "prec": "precipitation",
    "racha": "wind_speed",
}

# Columns for warning data
__COLUMNS_DATA_WARNINGS: List[str] = [
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
__COLUMNS_DATA_THRESHOLDS: List[str] = [
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
__COLUMNS_XML_CAP: List[str] = [
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
__COLUMNS_DATA_EVENTS: Set[str] = {"id", "season", "category", "name", "start", "end"}

# Columns used in the analysis of weather stations
__COLUMNS_ANALYSIS_STATIONS = [
    "idema",
    "name",
    "province",
    "latitude",
    "longitude",
    "altitude",
    "geocode",
]

# Columns used in the analysis of weather warnings
__COLUMNS_ANALYSIS_WARNINGS = [
        "date",
        "geocode",
        "region",
        "province",
        "polygon",
        "minimum_temperature",        
        "minimum_temperature_severity",
        "maximum_temperature",        
        "maximum_temperature_severity",
        "precipitation_1h",                    
        "precipitation_1h_severity",
        "precipitation_12h",              
        "precipitation_12h_severity",
        "snowfall_24h",                    
        "snowfall_24h_severity",
        "wind_speed",        
        "wind_speed_severity"
    ]

# Columns used in the analysis of weather observations
_COLUMNS_ANALYSIS_OBSERVATIONS = [
        "date",
        "idema",
        "name",
        "geocode",
        "province",
        "latitude",
        "longitude",
        "altitude",
        "minimum_temperature",        
        "minimum_temperature_severity",
        "maximum_temperature",        
        "maximum_temperature_severity",
        "uniform_precipitation_1h",                    
        "uniform_precipitation_1h_severity",
        "uniform_precipitation_12h",              
        "uniform_precipitation_12h_severity",
        "severe_precipitation_1h",            
        "severe_precipitation_1h_severity",
        "severe_precipitation_12h",                                
        "severe_precipitation_12h_severity",
        "snowfall_24h",                    
        "snowfall_24h_severity",
        "wind_speed",        
        "wind_speed_severity"
    ]        



def set_root(root: str) -> None:
    """
    Set the root directory path in the DIR_PATHS dictionary.

    Args:
        root (str): The root directory path to be set.
    """
    __DIR_PATHS["root"] = [root]

def parameters_allowed() -> List[str]:
    """
    Returns a list of observation parameters.

    Returns:
        List[str]: A list of allowed observation parameters.
    """
    return __ALLOWED_OBSERVATIONS

def mapping_parameters() -> Dict[str, str]:
    """
    Returns the mapping parameters.

    This function returns a dictionary containing the mapping parameters.

    Returns:
        Dict[str, str]: A dictionary where the keys and values are strings representing the mapping parameters.
    """
    return __MAPPING_PARAMETERS


def namespace_on_capxml() -> Dict[str, str]:
    """
    Returns the namespace dictionary for CAP XML.

    Returns:
        Dict[str, str]: A dictionary containing the namespace mappings for CAP XML.
    """
    return __CAP_XML_NAMESPACE

def columns_xml_cap() -> List[str]:
    """
    Returns a list of column names used in CAPXML files.

    Returns:
        List[str]: A list of column names defined in the COLUMNS_CAPXML constant.
    """
    return __COLUMNS_XML_CAP


def columns_data_stations() -> List[str]:
    """
    Retrieve a list of column names for stations.

    Returns:
        List[str]: A list of column names for stations.
    """
    return list(__MAPPING_STATIONS_COLUMNS.values())

def columns_analysis_stations() -> List[str]:
    """
    Retrieve a list of column names for stations.

    Returns:
        List[str]: A list of column names for stations.
    """
    return __COLUMNS_ANALYSIS_STATIONS

def columns_data_observations() -> List[str]:
    """
    Returns a list of column names for observations.

    Returns:
        List[str]: A list of column names for observations.
    """
    return list(__MAPPING_OBSERVATIONS_COLUMNS.values())

def columns_analysis_observations() -> List[str]:
    """
    Retrieve a list of column names for observations.

    Returns:
        List[str]: A list of column names for observations.
    """
    return _COLUMNS_ANALYSIS_OBSERVATIONS

def columns_data_warnings() -> List[str]:
    """
    Returns a list of column names for warnings.

    Returns:
        List[str]: A list of column names for warnings.
    """
    return __COLUMNS_DATA_WARNINGS

def columns_analysis_warnings() -> List[str]:
    """
    Retrieve a list of column names for warnings.

    Returns:
        List[str]: A list of column names for warnings.
    """
    return __COLUMNS_ANALYSIS_WARNINGS

def columns_data_thresholds() -> List[str]:
    """
    Returns a list of column names for thresholds.

    Returns:
        List[str]: A list of column names for thresholds.
    """
    return __COLUMNS_DATA_THRESHOLDS


def mapping_observation_columns() -> Dict[str, str]:
    """
    Returns a dictionary that maps observation columns.

    Returns:
        Dict[str, str]: A dictionary containing the mapping of observation columns.
    """
    return __MAPPING_OBSERVATIONS_COLUMNS


def mapping_severity() -> Dict[str, int]:
    """
    Returns a dictionary mapping severity levels to numeric values.

    Returns:
        Dict[str, int]: A dictionary mapping severity levels to numeric values.
    """
    return __MAPPING_SEVERITY


def retrieve_filepath(file: str, event: str = "") -> str:
    """
    Constructs a file path by formatting and joining path components.

    Args:
        file (str): The key to retrieve the file path template from FILE_PATHS.
        event (str, optional): The event string to format into the path template. Defaults to an empty string.

    Returns:
        str: The constructed file path.
    """
    return os.path.join(*[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in __FILE_PATHS[file]])


def retrieve_dirpath(dir: str, event: str = "") -> str:
    """
    Constructs a directory path by formatting and joining components from a predefined dictionary.

    Args:
        dir (str): The key to retrieve the directory path components from the DIR_PATHS dictionary.
        event (str, optional): An optional event string to format into the path components. Defaults to an empty string.

    Returns:
        str: The constructed directory path.
    """
    return os.path.join(*__DIR_PATHS["root"], *[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in __DIR_PATHS[dir]])    