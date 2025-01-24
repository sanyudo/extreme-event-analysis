"""
This module provides common operations and constants for the extreme event analysis project.

Imports:
    - logging: For logging and handling log messages.
    - os: For interacting with the operating system (paths, files, etc.).
    - shutil: For high-level file operations.
    - typing: for type hints and annotations
    - re: For working with regular expressions.
    - xml.etree.ElementTree as ET: For parsing and creating XML data.
    - datetime, timedelta: For working with dates and time differences.
    - pandas as pd: For data manipulation and analysis.
    - numpy as np: For numerical computations.
    - shapely: For geometric operations (Point, Polygon).
    - constants: For accessing global constants used throughout the project.
"""

import logging
import os
from typing import Dict, List, Set
import shutil
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from shapely import Point, Polygon

DATA_EXTENSION = ".tsv"
IMAGE_EXTENSION = ".png"
MAP_EXTENSION = ".html"

PATH_TO_DIR: Dict[str, List[str]] = {
    "root": [],
    "data": ["data"],
    "warnings": ["data", "avisos_emitidos", "{event}"],
    "observations": ["data", "datos_observados", "{event}"],
    "analysis": ["data", "analisis", "{event}"],
    "maps": ["data", "analisis", "{event}", "mapas"],
    "charts": ["data", "analisis", "{event}", "graficos"],
}

PATH_TO_FILE: Dict[str, List[str]] = {
    "shapefile": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        "shape",
        "ne_10m_admin_1_states_provinces.shp",
    ],
    "stations_list": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"inventario_estaciones{DATA_EXTENSION}",
    ],
    "thresholds_values": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"umbrales_aviso{DATA_EXTENSION}",
    ],
    "region_geocodes": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"poligonos_regiones{DATA_EXTENSION}",
    ],
    "snow_level": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"cota_nieve{DATA_EXTENSION}",
    ],
    "stations_geolocated": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"inventario_geolocalizado{DATA_EXTENSION}",
    ],
    "events_list": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["data"],
        f"listado_eventos{DATA_EXTENSION}",
    ],
    "warnings_list": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["warnings"],
        f"Avisos{DATA_EXTENSION}",
    ],
    "observations_list": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["observations"],
        f"Observaciones{DATA_EXTENSION}",
    ],
    "event_analysis": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["analysis"],
        f"Analisis{DATA_EXTENSION}",
    ],
    "event_prepared_data": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["analysis"],
        f"Datos_completos{DATA_EXTENSION}",
    ],
    "event_resulting_data": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["analysis"],
        f"Datos_observados_estaciones{DATA_EXTENSION}",
    ],
    "event_region_warnings": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["analysis"],
        f"Avisos_observados_regiones{DATA_EXTENSION}",
    ],
    "event_predicted_warnings": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["analysis"],
        f"Avisos_previstos_regiones{DATA_EXTENSION}",
    ],
    "confusion-matrix": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["charts"],
        f"MatrizConfusion{IMAGE_EXTENSION}",
    ],
    "distribution-chart": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["charts"],
        f"Barras{IMAGE_EXTENSION}",
    ],
    "error-map": [
        *PATH_TO_DIR["root"],
        *PATH_TO_DIR["charts"],
        f"MapaErrores{IMAGE_EXTENSION}",
    ],
}

CAP_XML_NAMESPACE: Dict[str, str] = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

MAPPING_PARAMETER_ID: Dict[str, str] = {
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

MAPPING_PARAMETER_DESCRIPTION: Dict[str, str] = {
    "BT": "Temperatura mínima",
    "AT": "Temperatura máxima",
    "PR": "Precipitación",
    "PR_1H": "Precipitación acumulada en una hora",
    "PR_12H": "Precipitación acumulada en 12 horas",
    "PR_1H.UNIFORME": "Precipitación acumulada en una hora (uniforme)",
    "PR_12H.UNIFORME": "Precipitación acumulada en 12 horas (uniforme)",
    "PR_1H.SEVERA": "Precipitación acumulada en una hora (severa)",
    "PR_12H.SEVERA": "Precipitación acumulada en 12 horas (severa)",
    "PR_1H.EXTREMA": "Precipitación acumulada en una hora (extrema)",
    "PR_12H.EXTREMA": "Precipitación acumulada en 12 horas (extrema)",
    "NE": "Nieve acumulada en 24 horas",
    "VI": "Racha de viento máxima",
}

MAPPING_PARAMETER_ABBREVIATIONS: Dict[str, str] = {
    "BT": "Temperatura mín.",
    "AT": "Temperatura máx.",
    "PR": "Precipitación",
    "PR_1H": "Precip. acumulada en 1h",
    "PR_12H": "Precip. acumulada en 12h",
    "PR_1H.UNIFORME": "Precip. acumulada en 1h (~uniforme)",
    "PR_12H.UNIFORME": "Precip. acumulada en 12h (~uniforme)",
    "PR_1H.SEVERA": "Precip. acumulada en 1h (~severa)",
    "PR_12H.SEVERA": "Precip. acumulada en 12h (~severa)",
    "PR_1H.EXTREMA": "Precip. acumulada en 1h (~extrema)",
    "PR_12H.EXTREMA": "Precip. acumulada en 12h (~extrema)",
    "NE": "Nieve acumulada en 24h",
    "VI": "Racha de viento máxima",
}

MAPPING_PARAMETER_UNIT: Dict[str, str] = {
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

MAPPING_PARAMETERS = {
    key: {
        "id": MAPPING_PARAMETER_ID[key],
        "description": MAPPING_PARAMETER_DESCRIPTION[key],
        "units": MAPPING_PARAMETER_UNIT[key],
    }
    for key in MAPPING_PARAMETER_ID  # Iterar sobre las claves comunes
}

ALLOWED_PARAMETER_ID: List[str] = list(MAPPING_PARAMETERS.keys())
ALLOWED_PARAMETER: List[str] = list(
    set(p["description"] for p in MAPPING_PARAMETERS.values())
)

MAPPING_SEVERITY_VALUE: Dict[str, int] = {
    "verde": 0,
    "amarillo": 1,
    "naranja": 2,
    "rojo": 3,
}

MAPPING_SEVERITY_TEXT: Dict[int, str] = {
    v: k for k, v in MAPPING_SEVERITY_VALUE.items()
}

MAPPING_STATION_FIELD: Dict[str, str] = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

FIELDS_STATION_DATA: List[str] = list(MAPPING_STATION_FIELD.values())

MAPPING_OBSERVATION_FIELD: Dict[str, str] = {
    "fecha": "date",
    "indicativo": "idema",
    "altitud": "altitude",
    "tmin": "minimum_temperature",
    "tmax": "maximum_temperature",
    "prec": "precipitation",
    "racha": "wind_speed",
}

FIELDS_OBSERVATION_DATA: List[str] = list(MAPPING_OBSERVATION_FIELD.values())

FIELDS_GEOCODE_DATA = ["geocode", "region", "area", "province", "polygon"]

FIELDS_WARNING_DATA: List[str] = [
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

FIELDS_THRESHOLD_DATA: List[str] = [
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

FIELDS_CAP_DATA: List[str] = [
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

FIELDS_EVENT_DATA: Set[str] = {"id", "season", "category", "name", "start", "end"}


def set_path_to_root(root: str) -> None:
    """
    Set the root path for the directory structure.

    Args:
        root (str): The root directory path to be set in the path_to_dir dictionary.
    """

    PATH_TO_DIR["root"] = [root]


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
            for item in PATH_TO_FILE[file]
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
        *PATH_TO_DIR["root"],
        *[
            (
                item.format(event=event)
                if isinstance(item, str) and "{event}" in item
                else item
            )
            for item in PATH_TO_DIR[dir]
        ],
    )


def ensure_directories(event: str, start: datetime, end: datetime) -> bool:
    """
    Ensures all directories for the given event exist, including the 'warnings' directory and its subdirectories for each day of the event.

    Parameters
    ----------
    event : str
        The event identifier for which to ensure directories exist.
    start : datetime
        The start date of the event.
    end : datetime
        The end date of the event.

    Returns
    -------
    bool
        True if all directories were successfully ensured, False otherwise.
    """
    for d in PATH_TO_DIR.values():
        path = os.path.join(
            *[
                (
                    item.format(event=event)
                    if isinstance(item, str) and "{event}" in item
                    else item
                )
                for item in d
            ]
        )
        try:
            if not os.path.exists(path):
                logging.info(f"Creating directory: {path}")
                os.makedirs(path, exist_ok=True)
            else:
                logging.info(f"Directory already exists: {path}")
        except Exception as e:
            logging.error(
                f"Error ensuring directory {path} exists for event {event}: {e}"
            )
            return False

        path = get_path_to_dir("warnings", event=event)
        for n in range((end - start).days + 1):
            n_path = os.path.join(path, (start + timedelta(n)).strftime("%Y%m%d"))
            try:
                if not os.path.exists(n_path):
                    logging.info(f"Creating directory: {n_path}")
                    os.makedirs(n_path, exist_ok=True)
                else:
                    logging.info(f"Directory already exists: {n_path}")
            except Exception as e:
                logging.error(
                    f"Error ensuring directory {n_path} exists for event {event} and date {(start + timedelta(n)).strftime('%Y-%m-%d')}: {e}"
                )
                return False
    return True


def clean_files(event: str) -> bool:
    """
    Removes all subdirectories in the warnings directory for the given event, effectively "cleaning" the directory of any previously downloaded warning files.

    Args:
        event (str): The identifier for the event to clean the warnings directory for.

    Returns:
        bool: True if the directory was successfully cleaned, False otherwise.
    """
    path = os.path.join(get_path_to_dir("warnings", event=event))
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


def __dms_coordinates_to_degress__(dms_coordinate: str, hemisphere: str) -> float:
    """
    Converts a DMS coordinate to decimal degrees.

    Args:
        dms_coordinate (str): The DMS coordinate to convert, e.g. "4321.123N".
        hemisphere (str): The hemisphere of the coordinate, either "N" or "S" for latitude, or "E" or "W" for longitude.

    Returns:
        float: The decimal degrees representation of the given DMS coordinate.

    Notes:
        The input DMS coordinate should be in the format "DDMM.SS[Hemisphere]", where DD is the degree, MM is the minute, SS is the seconds, and Hemisphere is one of "N", "S", "E", or "W".
    """
    logging.debug(f"Converting DMS coordinate: {dms_coordinate} {hemisphere}")
    dms_value = dms_coordinate.replace(hemisphere, "")
    if not hemisphere.isalpha() or len(hemisphere) != 1:
        logging.error(
            f"Hemisphere {hemisphere} should be a single letter, one of 'N', 'S', 'E', or 'W'"
        )
        return float(dms_coordinate)
    logging.debug(f"Stripped DMS value: {dms_value}")
    degrees = int(dms_value[:2])
    minutes = int(dms_value[2:4])
    seconds = int(dms_value[4:])
    logging.debug(f"Parsed degrees: {degrees}, minutes: {minutes}, seconds: {seconds}")
    result = degrees + minutes / 60 + seconds / 3600
    logging.debug(f"Decimal degrees result: {result}")
    return -result if hemisphere in {"S", "W"} else result


def exist_caps(event: str) -> bool:
    """
    Checks if there are any CAP XML files in the given event's directory.

    Args:
        event (str): The event identifier for which to check for CAP XML files.

    Returns:
        bool: True if any CAP XML files are found, False otherwise.
    """
    path = get_path_to_dir("warnings", event)
    logging.info(f"Checking for CAP XML files in {path}.")
    if os.path.exists(path):
        for dir_name in [
            d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))
        ]:
            dir_path = os.path.join(path, dir_name)
            logging.info(f"Checking directory: {dir_path}")
            for subdir_name in [
                sd
                for sd in os.listdir(dir_path)
                if os.path.isdir(os.path.join(dir_path, sd))
            ]:
                subdir_path = os.path.join(dir_path, subdir_name)
                logging.info(f"Checking subdirectory: {subdir_path}")
                if any(cap.endswith(".xml") for cap in os.listdir(subdir_path)):
                    logging.info(f"Found CAP XML file in {subdir_path}")
                    return True
    logging.info(f"No CAP XML files found for event: {event}")
    return False


def __extract_caps_data__(event: str) -> pd.DataFrame:
    """
    Consolidates all warning data from CAP XML files in the given event's directory.

    Args:
        event (str): The event identifier for which to consolidate warnings.

    Returns:
        pd.DataFrame: A DataFrame containing the consolidated warning data for the given event.
    """
    path = get_path_to_dir("warnings", event=event)
    logging.info(f"Consolidating CAP files in {path}.")

    df = pd.DataFrame(columns=FIELDS_CAP_DATA)
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

                cap_identifier = root.find("cap:identifier", CAP_XML_NAMESPACE).text
                cap_sent = root.find(".//cap:sent", CAP_XML_NAMESPACE).text
                info_elements = root.findall(".//cap:info", CAP_XML_NAMESPACE)
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
                    ".//cap:effective", CAP_XML_NAMESPACE
                ).text
                cap_expires = selected_info.find(
                    ".//cap:expires", CAP_XML_NAMESPACE
                ).text
                try:
                    cap_description = selected_info.find(
                        ".//cap:description", CAP_XML_NAMESPACE
                    ).text
                except:
                    cap_description = ""
                cap_event_code = selected_info.find(
                    ".//cap:eventCode/cap:value", CAP_XML_NAMESPACE
                ).text
                if cap_event_code:
                    cap_event_code = cap_event_code.split(";")

                parameters = {}
                for param in selected_info.findall(
                    ".//cap:parameter", CAP_XML_NAMESPACE
                ):
                    param_name = param.find("cap:valueName", CAP_XML_NAMESPACE).text
                    param_value = param.find("cap:value", CAP_XML_NAMESPACE).text
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
                for area in selected_info.findall(".//cap:area", CAP_XML_NAMESPACE):
                    geocode = area.find("cap:geocode", CAP_XML_NAMESPACE)
                    cap_geocode = geocode.find("cap:value", CAP_XML_NAMESPACE).text
                    cap_polygon = area.find("cap:polygon", CAP_XML_NAMESPACE).text

                if cap_severity in MAPPING_SEVERITY_VALUE.keys():
                    if MAPPING_SEVERITY_VALUE.get(cap_severity) > 0:
                        df.loc[len(df)] = {
                            "id": cap_identifier,
                            "sent": cap_sent,
                            "description": cap_description,
                            "effective": cap_effective,
                            "expires": cap_expires,
                            "severity": cap_severity,
                            "param_id": cap_event_code[0] if cap_event_code else None,
                            "param_name": cap_parameter[1],
                            "param_value": re.sub(r"[^\d]", "", cap_parameter[2]),
                            "geocode": cap_geocode,
                            "polygon": cap_polygon,
                        }
                        logging.debug(
                            f"Row added to DataFrame: {df.iloc[-1].to_dict()}"
                        )
            except Exception as e:
                logging.error(f"Error reading {os.path.join(path, dir, cap)}: {e}")
                raise
    logging.info(f"Consolidation complete. Total rows: {len(df)}")
    return df.dropna()


def caps_to_warnings(event: str) -> pd.DataFrame:
    """
    Fetches and processes warnings data for an event

    Parameters
    ----------
    event : str
        The event identifier for which to fetch warnings

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the processed warnings data

    Notes
    -----
    This function extracts data from CAP XML files, transforms it into a
    DataFrame, and cleans the data to remove any unnecessary rows or columns.

    The resulting DataFrame is then stored in a TSV file in the warnings
    directory with the same name as the event identifier.
    """
    logging.info(f"Starting to fetch warnings for event: {event}")

    logging.info("Extracting data from CAP XML files...")
    warnings = __extract_caps_data__(event)
    logging.info(f"Extracted {len(warnings)} raw warnings")

    logging.info("Transforming raw warning data...")
    warnings = __transform_caps_warnings__(warnings)
    logging.info(f"Transformed warnings into {len(warnings)} expanded rows")

    logging.info("Cleaning transformed warning data...")
    warnings = __clean_caps_files__(warnings)
    logging.info(f"Cleaned warnings, resulting in {len(warnings)} final rows")

    logging.info(f"Completed fetching warnings for event: {event}")
    try:
        logging.info(f"... storing data in {get_path_to_file('warnings_list', event)}.")
        warnings.to_csv(
            get_path_to_file("warnings_list", event),
            index=False,
            sep="\t",
        )
    except Exception as e:
        logging.error(f"Error storing warning data: {e}")
        raise


def __transform_caps_warnings__(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw warning data for analysis.

    This function performs the following transformations:
    1. Filters warnings to only include those with allowed parameter IDs.
    2. Converts 'sent', 'effective', and 'expires' columns to datetime objects.
    3. Converts 'param_value' and 'geocode' columns to numeric values.
    4. Expands rows by creating entries for each day between 'effective' and 'expires' dates.

    Parameters
    ----------
    warnings : pd.DataFrame
        Raw warning data to be transformed.

    Returns
    -------
    pd.DataFrame
        Transformed warning data with expanded rows for each day within the effective range.
    """

    logging.info("Filtering warnings to include only allowed parameters...")
    warnings = warnings[warnings["param_id"].isin(ALLOWED_PARAMETER_ID)]

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


def __clean_caps_files__(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and filters the warnings DataFrame for further analysis.

    This function performs the following operations on the warnings DataFrame:
    1. Sorts the warnings by geocode, param_id, effective date, severity (in descending order),
       and sent time (in descending order).
    2. Groups the warnings by geocode, param_id, and effective date, selecting the most severe
       and most recent warning for each group.
    3. Returns a DataFrame containing only the relevant columns for warnings.

    Parameters
    ----------
    warnings : pd.DataFrame
        The DataFrame containing warning data to be cleaned and filtered.

    Returns
    -------
    pd.DataFrame
        The cleaned and filtered DataFrame containing warnings.
    """

    logging.info(
        "Sorting warnings by geocode, param_id, effective, severity, and sent..."
    )
    warnings = warnings.sort_values(
        by=[
            "geocode",
            "param_id",
            "effective",
            "severity",
            "sent",
        ],
        ascending=[True, True, True, False, False],
    ).reset_index(drop=True)

    logging.info(
        "Grouping warnings by geocode, param_id, and effective, and selecting the most severe and recent warning..."
    )
    warnings = warnings.groupby(
        ["geocode", "param_id", "effective"], as_index=False
    ).apply(
        lambda x: x.sort_values(by=["severity", "sent"], ascending=[False, False]).head(
            1
        )
    )

    logging.info("Cleaning complete. Returning cleaned warnings DataFrame.")
    return warnings[FIELDS_WARNING_DATA]


def exist_warnings(event: str) -> bool:
    """
    Checks if a warnings file exists for a given event.

    Parameters
    ----------
    event : str
        The event identifier for which to check the warnings file.

    Returns
    -------
    bool
        True if the warnings file exists, False otherwise.
    """
    return os.path.exists(get_path_to_file("warnings_list", event))


def get_warnings(event: str) -> pd.DataFrame:
    """
    Retrieves and prepares the warnings DataFrame for a given event.

    This function reads the warnings data from a file corresponding to the
    specified event and applies preprocessing to the DataFrame.

    Parameters
    ----------
    event : str
        The event identifier for which to retrieve the warnings data.

    Returns
    -------
    pd.DataFrame
        The preprocessed warnings DataFrame.
    """

    return __prepare_raw_warnings__(
        pd.read_csv(get_path_to_file("warnings_list", event=event), sep="\t", dtype=str)
    )


def __prepare_raw_warnings__(warnings: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the warnings DataFrame by converting date and numeric columns.

    This function performs the following preprocessing steps:
    1. Converts the 'effective' column to datetime format.
    2. Converts the 'param_value' column to numeric, coercing errors to NaN.

    Parameters
    ----------
    warnings : pd.DataFrame
        The DataFrame containing warning data to be processed.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with the 'effective' and 'param_value' columns
        converted to appropriate types.

    Raises
    ------
    Exception
        If an error occurs during preprocessing, an error message is logged.
    """

    logging.info("Preprocessing warning data...")
    try:
        warnings["effective"] = pd.to_datetime(warnings["effective"], format="%Y-%m-%d")
        warnings["param_value"] = pd.to_numeric(
            warnings["param_value"], errors="coerce"
        )
        return warnings
    except Exception as e:
        logging.error(f"Error preprocessing warning data: {e}")
        raise


def exist_stations() -> bool:
    """
    Check if the stations file exists.

    Returns:
        bool: True if the stations file exists, False otherwise.
    """

    return os.path.exists(get_path_to_file("stations_list"))


def get_stations() -> pd.DataFrame:
    """
    Retrieves the stations configuration DataFrame.

    This function checks if the stations data file exists, reads it from the file,
    preprocesses the data using the prepare_stations function, and returns the
    preprocessed DataFrame.

    Returns:
        pd.DataFrame: Preprocessed stations configuration DataFrame
    """

    try:
        return __prepare_raw_stations__(
            pd.read_csv(get_path_to_file("stations_list"), sep="\t")
        )
    except Exception as e:
        logging.error(f"Error retrieving station data: {e}")
        raise


def __prepare_raw_stations__(stations: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the stations configuration DataFrame.

    This function performs the following preprocessing steps:

    1. Renames columns according to mapping_observations_fields.
    2. Title-cases the "province" and "name" columns.
    3. Converts "latitude" and "longitude" columns to numeric values if they are not
        already.
    4. Selects only the columns in columns_stations.
    5. Converts "latitude", "longitude", and "altitude" columns to numeric values and
        handles missing values.
    6. Adds a "geocode" column with None values.

    Returns
    -------
    pd.DataFrame
        The preprocessed stations DataFrame.
    """
    stations = stations.rename(columns=MAPPING_OBSERVATION_FIELD)

    stations[["province", "name"]] = stations[["province", "name"]].applymap(str.title)

    if (not stations["latitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in stations["latitude"]
    ):
        stations["latitude"] = stations["latitude"].apply(
            lambda x: __dms_coordinates_to_degress__(x, hemisphere=x[-1])
        )

    if (not stations["longitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in stations["longitude"]
    ):
        stations["longitude"] = stations["longitude"].apply(
            lambda x: __dms_coordinates_to_degress__(x, hemisphere=x[-1])
        )

    stations = stations.loc[:, FIELDS_STATION_DATA]

    stations["latitude"] = pd.to_numeric(stations["latitude"], errors="coerce")
    stations["longitude"] = pd.to_numeric(stations["longitude"], errors="coerce")
    stations["altitude"] = pd.to_numeric(stations["altitude"], errors="coerce")

    stations["geocode"] = None

    return stations


def get_events() -> pd.DataFrame:
    """
    Retrieves the events configuration DataFrame.

    This function reads the events configuration file and preprocesses it using
    `prepare_events`.

    Returns
    -------
    pd.DataFrame
        The events DataFrame with the following columns:
            - id: a unique identifier for each event
            - season: the season of the event (e.g. "2020-2021")
            - category: the category of the event (e.g. "snowfall")
            - name: the name of the event (e.g. "Snowfall event in the Pyrenees")
            - start: the start date of the event in the format "dd/mm/yyyy"
            - end: the end date of the event in the format "dd/mm/yyyy"

    Notes
    -----
    This function assumes that the events configuration file is a tab-separated
    values file with the columns specified above. The file should be stored in
    `path_to_dir["data"][0] + "/" + path_to_dir["events"][0]`
    """

    return __prepare_raw_events__(
        pd.read_csv(get_path_to_file("events_list"), sep="\t", dtype=str)
    )


def __prepare_raw_events__(events: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses an events DataFrame.

    The events DataFrame is a configuration file containing the start and end
    dates for each event. This function converts the "start" and "end" columns
    to datetime format and sorts the DataFrame by the start and end dates.

    Parameters
    ----------
    events : pd.DataFrame
        The events DataFrame with the following columns:
            - id: a unique identifier for each event
            - season: the season of the event (e.g. "2020-2021")
            - category: the category of the event (e.g. "snowfall")
            - name: the name of the event (e.g. "Snowfall event in the Pyrenees")
            - start: the start date of the event in the format "dd/mm/yyyy"
            - end: the end date of the event in the format "dd/mm/yyyy"

    Returns
    -------
    pd.DataFrame
        The preprocessed events DataFrame with the same columns as the input
        DataFrame, but with the "start" and "end" columns converted to datetime
        format and sorted by the start and end dates.
    """
    events["start"] = pd.to_datetime(events["start"], format="%d/%m/%Y")
    events["end"] = pd.to_datetime(events["end"], format="%d/%m/%Y")
    return events.sort_values(by=["start", "end"])


def get_thresholds() -> pd.DataFrame:
    """
    Retrieves the thresholds DataFrame from a CSV file.

    The thresholds DataFrame is a configuration file containing the warning
    thresholds for each parameter and geocode. The thresholds are used to
    determine the severity of a warning.

    Returns
    -------
    pd.DataFrame
        The thresholds DataFrame with the following columns:

        - geocode: str
            The unique identifier for the geocode.
        - region: str
            The region name.
        - area: str
            The area name.
        - province: str
            The province name.
        - param_id: str
            The parameter ID.
        - param_name: str
            The parameter name.
        - yellow_warning: float
            The yellow warning threshold.
        - orange_warning: float
            The orange warning threshold.
        - red_warning: float
            The red warning threshold.

    Raises
    ------
    Exception
        If an error occurs during retrieval, an error message is logged.
    """

    return __prepare_raw_thresholds__(
        pd.read_csv(get_path_to_file("thresholds_values"), sep="\t", dtype=str)
    )


def __prepare_raw_thresholds__(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the thresholds DataFrame for analysis.

    This function converts the columns of the thresholds DataFrame to numeric
    types, except for the columns specified as non-numeric. It handles any
    conversion errors by coercing them into NaN values.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing threshold data.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with numeric columns converted.

    Raises
    ------
    Exception
        If an error occurs during processing, an error message is logged.
    """

    try:
        for c in FIELDS_THRESHOLD_DATA:
            if c not in ["geocode", "region", "area", "province"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception as e:
        logging.error(f"Error preparing thresholds data: {e}")


def get_geocodes() -> pd.DataFrame:
    """
    Retrieve geocode data from a file.

    This function reads geocode data from a TSV file specified in the constants module
    and returns it as a pandas DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing geocode data with columns as specified in the file.
    """

    return pd.read_csv(get_path_to_file("region_geocodes"), sep="\t", dtype=str)


def exist_observations(event: str) -> bool:
    """
    Checks if observational data for a given event exists.

    Parameters
    ----------
    event : str
        The event identifier for which to check the existence of observations.

    Returns
    -------
    bool
        True if the observations exist, False otherwise.
    """

    return os.path.exists((get_path_to_file("observations_list", event)))


def get_observations(event: str, stations: list) -> pd.DataFrame:
    """
    Loads observational data from a file and prepares it for analysis.

    Parameters
    ----------
    event : str
        The event identifier for which to load observations.
    stations : list
        A list of station identifiers for which to load observations.

    Returns
    -------
    pd.DataFrame
        The prepared observational data.
    """

    return __prepare_raw_observations__(
        observations=(
            pd.read_csv(
                get_path_to_file("observations_list", event=event),
                sep="\t",
                dtype=str,
            )
        ),
        stations=stations,
    )


def __prepare_raw_observations__(
    observations: pd.DataFrame, stations: list
) -> pd.DataFrame:
    """
    Prepare observational data.

    Parameters
    ----------
    observations : pd.DataFrame
        Observational data to be processed.
    stations : list
        List of station IDs to filter observations by.

    Returns
    -------
    pd.DataFrame
        Processed observational data.

    Notes
    -----
    This function performs the following steps:

    1. Renames observation columns according to mapping_observations_fields.
    2. Selects relevant columns according to columns_observations.
    3. Filters observations by station IDs.
    4. Converts 'date' column to datetime.
    5. Converts numeric columns and handles missing values.
    """
    logging.info("Renaming observation columns...")
    observations = observations.rename(columns=MAPPING_OBSERVATION_FIELD)

    logging.info("Selecting relevant columns...")
    observations = observations[FIELDS_OBSERVATION_DATA]

    logging.info("Filtering observations by station IDs...")
    observations = observations[observations["idema"].isin(stations)]

    logging.info("Converting 'date' column to datetime...")
    observations["date"] = pd.to_datetime(observations["date"], format="%Y-%m-%d")

    logging.info("Converting numeric columns and handling missing values...")
    for column in [
        "minimum_temperature",
        "maximum_temperature",
        "precipitation",
        "wind_speed",
    ]:
        observations[column] = pd.to_numeric(
            observations[column].str.replace(",", "."), errors="coerce"
        )

    logging.info("Dropping rows with NaN values...")
    observations = observations.dropna()

    logging.info("Adding additional precipitation metrics...")
    observations["uniform_precipitation_1h"] = np.nan
    observations["severe_precipitation_1h"] = np.nan
    observations["extreme_precipitation_1h"] = np.nan
    observations["uniform_precipitation_12h"] = np.nan
    observations["severe_precipitation_12h"] = np.nan
    observations["extreme_precipitation_12h"] = np.nan
    observations["snowfall_24h"] = np.nan

    logging.info("Observational data preparation complete.")
    return observations


def geolocate_stations() -> pd.DataFrame:
    """
    Geolocate weather stations by assigning geocodes based on their latitude and longitude.

    This function retrieves geocodes and stations data, calculates geometric areas from geocode polygons,
    and assigns a geocode to each station based on whether the station's geographic point is contained
    within a geocode shape. It outputs the geolocated stations data to a TSV file.

    Returns:
        pd.DataFrame: The geolocated stations with assigned geocodes.
    """

    geocodes = get_geocodes()
    stations = get_stations()
    geocodes["geocode"] = geocodes["geocode"].astype(str)
    geocodes["geometry"] = geocodes["polygon"].apply(
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
            (
                a["geocode"]
                for _, a in geocodes.iterrows()
                if Polygon(a["geometry"]).contains(Point(station["point"]))
            ),
            None,
        ),
        axis=1,
    )

    geocodes["centroid"] = geocodes["geometry"].apply(
        lambda x: x.centroid if x else None
    )
    for _, stat in stations.iterrows():
        if stat["geocode"] is None:
            subset = geocodes[geocodes["province"] == stat["province"]]
            if subset.empty:
                subset = geocodes[geocodes["region"] == stat["province"]]
            if subset.empty:
                subset = geocodes.copy()
            subset["distance"] = subset["centroid"].apply(
                lambda x: x.distance(Point(stat["point"]))
            )
            stat["geocode"] = subset.sort_values("distance").iloc[0]["geocode"]

    stations = stations.drop(columns=["point"])
    stations.to_csv(get_path_to_file("stations_geolocated"), sep="\t")


def exist_gelocated_stations() -> bool:
    """
    Check if the geolocated stations file exists.

    Returns:
        bool: True if the file exists, False otherwise
    """
    return os.path.exists(get_path_to_file("stations_geolocated"))


def get_geolocated_stations() -> pd.DataFrame:
    """
    Retrieve the geolocated stations DataFrame, or raise an Exception if something fails.

    Returns:
        pd.DataFrame: the geolocated stations DataFrame
    """

    try:
        return __prepare_geolocated_stations__(
            pd.read_csv(get_path_to_file("stations_geolocated"), sep="\t", dtype=str)
        )
    except Exception as e:
        logging.error(f"Error retrieving geolocated data: {e}")
        raise


def __prepare_geolocated_stations__(geolocated_stations: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a geolocated stations DataFrame for analysis.

    This function renames columns according to the mapping defined in
    `mapping_observations_fields`, title-cases the "province" and "name"
    columns, and converts "latitude", "longitude", and "altitude" columns to
    numeric if they are not already.

    Parameters
    ----------
    geolocated_sations : pd.DataFrame
        The DataFrame to prepare.

    Returns
    -------
    pd.DataFrame
        The prepared DataFrame.
    """
    geolocate_stations = geolocated_stations.rename(columns=MAPPING_OBSERVATION_FIELD)

    geolocated_stations[["province", "name"]] = geolocated_stations[
        ["province", "name"]
    ].apply(lambda col: col.map(str.title))

    if (not geolocated_stations["latitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in geolocated_stations["latitude"]
    ):
        geolocated_stations["latitude"] = geolocated_stations["latitude"].apply(
            lambda x: __dms_coordinates_to_degress__(x, hemisphere=x[-1])
        )

    if (not geolocated_stations["longitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in geolocated_stations["longitude"]
    ):
        geolocated_stations["longitude"] = geolocated_stations["longitude"].apply(
            lambda x: __dms_coordinates_to_degress__(x, hemisphere=x[-1])
        )

    geolocated_stations["latitude"] = pd.to_numeric(
        geolocated_stations["latitude"], errors="coerce"
    )
    geolocated_stations["longitude"] = pd.to_numeric(
        geolocated_stations["longitude"], errors="coerce"
    )
    geolocated_stations["altitude"] = pd.to_numeric(
        geolocated_stations["altitude"], errors="coerce"
    )

    return geolocated_stations


def get_snow_level() -> pd.DataFrame:
    """
    Retrieve the snow level data conversion from a file.

    This function reads the snow level data from a TSV file specified in the constants
    module and returns it as a pandas DataFrame. The DataFrame contains columns
    representing temperature thresholds (in degrees Celsius).

    Returns
    -------
    pd.DataFrame
        A DataFrame containing snow level data with columns: "t", "-40", "-35",
        "-30", "-25", and "-20".
    """

    return pd.read_csv(
        get_path_to_file("snow_level"),
        sep="\t",
        names=[
            "t",
            "T-42",
            "T-41",
            "T-40",
            "T-39",
            "T-38",
            "T-37",
            "T-36",
            "T-35",
            "T-34",
            "T-33",
            "T-32",
            "T-31",
            "T-30",
            "T-29",
            "T-28",
            "T-27",
            "T-26",
            "T-25",
            "T-24",
            "T-23",
            "T-22",
            "T-21",
            "T-20",
            "T-19",
            "T-18",
            "T-17",
            "T-16",
        ],
    )
