"""Constants and utilities for the extreme event analysis project

This module provides constants and utilities used throughout the extreme event
analysis project.

Imports:
    - os: for interacting with the operating system (paths, files, etc.)
    - typing: for type hints and annotations
"""

import os
from typing import Dict, List, Set

file_extension = ".tsv"

path_to_dir: Dict[str, List[str]] = {
    "root": [],
    "data": ["data"],
    "warnings": ["data", "warnings", "{event}"],
    "observations": ["data", "observations", "{event}"],
    "analysis": ["data", "analysis", "{event}"],
    "maps": ["data", "analysis", "{event}", "maps"],
}

path_to_file: Dict[str, List[str]] = {
    "stations": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"weather_stations{file_extension}",
    ],
    "thresholds": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"parameter_thresholds{file_extension}",
    ],
    "geocodes": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"geocode_polygons{file_extension}",
    ],
    "snow_level": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"snow_level{file_extension}",
    ],    
    "geolocated": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"geolocated_stations{file_extension}",
    ],
    "events": [
        *path_to_dir["root"],
        *path_to_dir["data"],
        f"events_list{file_extension}",
    ],
    "warnings": [
        *path_to_dir["root"],
        *path_to_dir["warnings"],
        f"warnings{file_extension}",
    ],
    "observations": [
        *path_to_dir["root"],
        *path_to_dir["observations"],
        f"observations{file_extension}",
    ],
    "analysis": [
        *path_to_dir["root"],
        *path_to_dir["analysis"],
        f"Analysis{file_extension}",
    ],
    "results": [
        *path_to_dir["root"],
        *path_to_dir["analysis"],
        f"Observed_Data{file_extension}",
    ],
    "region": [
        *path_to_dir["root"],
        *path_to_dir["analysis"],
        f"Region_Observed{file_extension}",
    ],    
    "predictions": [
        *path_to_dir["root"],
        *path_to_dir["analysis"],
        f"Predicted_Warnings{file_extension}",
    ],
}

namespace_cap: Dict[str, str] = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

mapping_parameters_id: Dict[str, str] = {
    "BT": "minimum_temperature",
    "AT": "maximum_temperature",
    "PR": "precipitation",
    "PR_1H": "precipitation_1h",
    "PR_12H": "precipitation_12h",
    "PR_1H.UNIFORME": "uniform_precipitation_1h",
    "PR_12H.UNIFORME": "uniform_precipitation_12h",    
    "PR_1H.SEVERA": "severe_precipitation_1h",
    "PR_12H.SEVERA": "severe_precipitation_12h",       
    "PR_1H.EXTREMA": "extreme_precipitation_1h",
    "PR_12H.EXTREMA": "extreme_precipitation_12h",     
    "NE": "snowfall_24h",
    "VI": "wind_speed",
}

mapping_parameters_description: Dict[str, str] = {
    "BT": "Temperatura mínima",
    "AT": "Temperatura máxima",
    "PR": "Precipitación",
    "PR_1H": "Precipitación acumulada en una hora",
    "PR_12H": "Precipitación acumulada en 12 horas",
    "PR_1H.UNIFORME": "Precipitación acumulada en una hora (uniforme)",
    "PR_12H.UNIFORME": "Precipitación acumulada en 12 horas (uniforme)",    
    "PR_1H.SEVERA": "Precipitación acumulada en una hora (estimación severa del 33%)",
    "PR_12H.SEVERA": "Precipitación acumulada en 12 horas (estimación severa del 85%)",       
    "PR_1H.EXTREMA": "Precipitación acumulada en una hora (estimación extrema del 50%)",
    "PR_12H.EXTREMA": "Precipitación acumulada en 12 horas (estimación extrema del 100%)",       
    "NE": "Nieve acumulada en 24 horas",
    "VI": "Racha de viento máxima",
}

mapping_parameters_units: Dict[str, str] = {
    "BT": "ºC",
    "AT": "ºC",
    "PR": "mm",
    "PR_1H": "mm",
    "PR_12H": "mm",
    "PR_1H.UNIFORME": "mm",
    "PR_12H.UNIFORME": "mm",    
    "PR_1H.SEVERA": "mm",
    "PR_12H.SEVERA": "mm",       
    "PR_1H.EXTREMA": "mm",
    "PR_12H.EXTREMA": "mm",     
    "NE": "cm",
    "VI": "km/h",
}

mapping_parameters = {
    key: {
        "id": mapping_parameters_id[key],
        "description": mapping_parameters_description[key],
        "units": mapping_parameters_units[key]
    }
    for key in mapping_parameters_id  # Iterar sobre las claves comunes
}

allowed_parameters_ids: List[str] = list(mapping_parameters.keys())
allowed_parameters: List[str] = list(set(p["description"] for p in mapping_parameters.values()))

mapping_severity_values: Dict[str, int] = {
    "verde": 0,
    "amarillo": 1,
    "naranja": 2,
    "rojo": 3,
}

mapping_severity_text: list[str] = ["verde", "amarillo", "naranja", "rojo"]

mapping_stations_fields: Dict[str, str] = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

columns_stations: List[str] = list(mapping_stations_fields.values())

mapping_observations_fields: Dict[str, str] = {
    "fecha": "date",
    "indicativo": "idema",
    "altitud": "altitude",    
    "tmin": "minimum_temperature",
    "tmax": "maximum_temperature",
    "prec": "precipitation",
    "racha": "wind_speed",
}

columns_observations: List[str] = list(mapping_observations_fields.values())

columns_geocodes = ["geocode", "region", "area", "province", "polygon"]

columns_warnings: List[str] = [
    "id",
    "effective",
    "description",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

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

fields_cap: List[str] = [
    "id",
    "sent",
    "description",    
    "effective",
    "expires",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

columns_events: Set[str] = {"id", "season", "category", "name", "start", "end"}

def set_path_to_root(root: str) -> None:
    """
    Set the root path for the directory structure.

    Args:
        root (str): The root directory path to be set in the path_to_dir dictionary.
    """

    path_to_dir["root"] = [root]


def get_path_to_file(file: str, event: str = "") -> str:
    """
    Construct a path to a file based on the given file name and event ID (if applicable).

    Args:
        file (str): The name of the file as specified in the path_to_file dictionary.
        event (str, optional): The event ID to use in the path construction. Defaults to "".
    
    Returns:
        str: The fully constructed path to the file.
    """
    return os.path.join(
        *[
            (
                item.format(event=event)
                if isinstance(item, str) and "{event}" in item
                else item
            )
            for item in path_to_file[file]
        ]
    )


def get_path_to_dir(dir: str, event: str = "") -> str:
    """
    Construct a path to a directory based on the given directory name and event ID (if applicable).

    Args:
        dir (str): The name of the directory as specified in the path_to_dir dictionary.
        event (str, optional): The event ID to use in the path construction. Defaults to "".
    
    Returns:
        str: The fully constructed path to the directory.
    """
    return os.path.join(
        *path_to_dir["root"],
        *[
            (
                item.format(event=event)
                if isinstance(item, str) and "{event}" in item
                else item
            )
            for item in path_to_dir[dir]
        ],
    )
