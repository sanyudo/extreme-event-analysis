"""
This module defines constants and utility functions for extreme event analysis.
"""

# Python standard library modules
import os  # For interacting with the operating system (paths, files, etc.)
from typing import Dict, List, Set

# File extension for data files
file_extension = ".tsv"

# Directory paths for storing data
path_to_dir: Dict[str, List[str]] = {
    "root": [],
    "data": ["data"],
    "warnings": ["data", "warnings", "{event}"],
    "observations": ["data", "observations", "{event}"],
    "analysis": ["data", "analysis", "{event}"]
}

# File paths for different types of data files
path_to_file: Dict[str, List[str]] = {
    "stations": [*path_to_dir["root"], *path_to_dir["data"], f"weather_stations{file_extension}"],
    "thresholds": [*path_to_dir["root"], *path_to_dir["data"], f"parameter_thresholds{file_extension}"],
    "geocodes": [*path_to_dir["root"], *path_to_dir["data"], f"geocode_polygons{file_extension}"],
    "geolocated": [*path_to_dir["root"], *path_to_dir["data"], f"geolocated_stations{file_extension}"],
    "events": [*path_to_dir["root"], *path_to_dir["data"], f"events_list{file_extension}"],
    "warnings": [*path_to_dir["root"], *path_to_dir["warnings"], f"warnings{file_extension}"],
    "observations": [*path_to_dir["root"], *path_to_dir["observations"], f"observations{file_extension}"],
    "analysis": [*path_to_dir["root"], *path_to_dir["analysis"], f"analysis{file_extension}"],
    "results": [*path_to_dir["root"], *path_to_dir["analysis"], f"observed_data{file_extension}"],
    "situations": [*path_to_dir["root"], *path_to_dir["analysis"], f"observed_situation{file_extension}"],
    "predictions": [*path_to_dir["root"], *path_to_dir["analysis"], f"predicted_warnings{file_extension}"],
}

# Namespace for CAP XML data
namespace_cap: Dict[str, str] = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

# Mapping of parameters (ids) and observations
mapping_parameters_id: Dict[str, str] = {
    "BT": "minimum_temperature",
    "AT": "maximum_temperature",
    "PR": "precipitation",
    "PR_1H": "precipitation_1h",
    "PR_12H": "precipitation_12h",
    "NE": "snowfall_24h",
    "VI": "wind_speed"
}

# Parameters for observations
allowed_parameters_ids: List[str] = list(mapping_parameters_id.keys())
allowed_parameters: List[str] = list(mapping_parameters_id.values())

# Mapping of severity levels to numeric values
mapping_severity_values: Dict[str, int] = {"verde": 0, "amarillo": 1, "naranja": 2, "rojo": 3}

# Mapping of station column names from source to target
mapping_stations_fields: Dict[str, str] = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

columns_stations: List[str] = list(mapping_stations_fields.values())

# Mapping of observation column names from source to target
mapping_observations_fields: Dict[str, str] = {
    "fecha": "date",
    "indicativo": "idema",
    "tmin": "minimum_temperature",
    "tmax": "maximum_temperature",
    "prec": "precipitation",
    "racha": "wind_speed",
}

columns_observations : List[str] = list(mapping_observations_fields.values())

columns_geocodes = ["geocode", "polygon"]

# Columns for warning data
columns_warnings: List[str] = [
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
columns_thresholds: List[str] = [
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
fields_cap: List[str] = [
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
columns_events: Set[str] = {"id", "season", "category", "name", "start", "end"}

def set_path_to_root(root: str) -> None:
    """
    Set the root directory path in the DIR_PATHS dictionary.

    Args:
        root (str): The root directory path to be set.
    """
    path_to_dir["root"] = [root]

def get_path_to_file(file: str, event: str = "") -> str:
    """
    Constructs a file path by formatting and joining path components.

    Args:
        file (str): The key to retrieve the file path template from FILE_PATHS.
        event (str, optional): The event string to format into the path template. Defaults to an empty string.

    Returns:
        str: The constructed file path.
    """
    return os.path.join(*[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in path_to_file[file]])

def get_path_to_dir(dir: str, event: str = "") -> str:
    """
    Constructs a directory path by formatting and joining components from a predefined dictionary.

    Args:
        dir (str): The key to retrieve the directory path components from the DIR_PATHS dictionary.
        event (str, optional): An optional event string to format into the path components. Defaults to an empty string.

    Returns:
        str: The constructed directory path.
    """
    return os.path.join(*path_to_dir["root"], *[item.format(event=event) if isinstance(item, str) and "{event}" in item else item for item in path_to_dir[dir]])    
