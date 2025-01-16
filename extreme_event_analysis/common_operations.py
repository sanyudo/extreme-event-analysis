"""
This module provides common operations and utilities for the extreme event analysis project.

Imports:
    - logging: For logging and handling log messages.
    - os: For interacting with the operating system (paths, files, etc.).
    - shutil: For high-level file operations.
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
import shutil
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from shapely import Point, Polygon
import constants

__SEVERE_PRECIPITATION_BY_TIMEFRAME = {1: 0.33, 12: 0.85}
__EXTREME_PRECIPITATION_BY_TIMEFRAME = {1: 0.50, 12: 1.00}


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
    for d in constants.path_to_dir.values():
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
    path = constants.get_path_to_dir("warnings", event)
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


def data_extract_caps(event: str) -> pd.DataFrame:
    """
    Consolidates all warning data from CAP XML files in the given event's directory.

    Args:
        event (str): The event identifier for which to consolidate warnings.

    Returns:
        pd.DataFrame: A DataFrame containing the consolidated warning data for the given event.
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

                cap_identifier = root.find(
                    "cap:identifier", constants.namespace_cap
                ).text
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
                cap_expires = selected_info.find(
                    ".//cap:expires", constants.namespace_cap
                ).text
                cap_event_code = selected_info.find(
                    ".//cap:eventCode/cap:value", constants.namespace_cap
                ).text
                if cap_event_code:
                    cap_event_code = cap_event_code.split(";")

                parameters = {}
                for param in selected_info.findall(
                    ".//cap:parameter", constants.namespace_cap
                ):
                    param_name = param.find(
                        "cap:valueName", constants.namespace_cap
                    ).text
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
                for area in selected_info.findall(
                    ".//cap:area", constants.namespace_cap
                ):
                    geocode = area.find("cap:geocode", constants.namespace_cap)
                    cap_geocode = geocode.find(
                        "cap:value", constants.namespace_cap
                    ).text
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
    warnings = data_extract_caps(event)
    logging.info(f"Extracted {len(warnings)} raw warnings")

    logging.info("Transforming raw warning data...")
    warnings = data_transform_warnings(warnings)
    logging.info(f"Transformed warnings into {len(warnings)} expanded rows")

    logging.info("Cleaning transformed warning data...")
    warnings = data_clean_warnings(warnings)
    logging.info(f"Cleaned warnings, resulting in {len(warnings)} final rows")

    logging.info(f"Completed fetching warnings for event: {event}")
    try:
        logging.info(
            f"... storing data in {constants.get_path_to_file('warnings', event)}."
        )
        warnings.to_csv(
            constants.get_path_to_file("warnings", event),
            index=False,
            sep="\t",
        )
    except Exception as e:
        logging.error(f"Error storing warning data: {e}")
        raise


def data_transform_warnings(warnings: pd.DataFrame) -> pd.DataFrame:
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
    return warnings[constants.columns_warnings]


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
    return os.path.exists(constants.get_path_to_file("warnings", event))


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

    return prepare_warnings(
        pd.read_csv(constants.get_path_to_file("warnings", event=event), sep="\t")
    )


def prepare_warnings(warnings: pd.DataFrame) -> pd.DataFrame:
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

    return os.path.exists(constants.get_path_to_file("stations"))


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
        return prepare_stations(
            pd.read_csv(constants.get_path_to_file("stations"), sep="\t")
        )
    except Exception as e:
        logging.error(f"Error retrieving station data: {e}")
        raise


def prepare_stations(stations: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesses the stations configuration DataFrame.

    This function performs the following preprocessing steps:

    1. Renames columns according to constants.mapping_observations_fields.
    2. Title-cases the "province" and "name" columns.
    3. Converts "latitude" and "longitude" columns to numeric values if they are not
        already.
    4. Selects only the columns in constants.columns_stations.
    5. Converts "latitude", "longitude", and "altitude" columns to numeric values and
        handles missing values.
    6. Adds a "geocode" column with None values.

    Returns
    -------
    pd.DataFrame
        The preprocessed stations DataFrame.
    """
    stations.rename(columns=constants.mapping_observations_fields, inplace=True)

    stations[["province", "name"]] = stations[["province", "name"]].applymap(str.title)

    if (not stations["latitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in stations["latitude"]
    ):
        stations["latitude"] = stations["latitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    if (not stations["longitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in stations["longitude"]
    ):
        stations["longitude"] = stations["longitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    stations = stations.loc[:, constants.columns_stations]

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
    `constants.path_to_dir["data"][0] + "/" + constants.path_to_dir["events"][0]`
    """

    return prepare_events(pd.read_csv(constants.get_path_to_file("events"), sep="\t"))


def prepare_events(events: pd.DataFrame) -> pd.DataFrame:
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

    return prepare_thresholds(
        pd.read_csv(constants.get_path_to_file("thresholds"), sep="\t")
    )


def prepare_thresholds(df: pd.DataFrame) -> pd.DataFrame:
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
        for c in constants.columns_thresholds:
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

    return pd.read_csv(constants.get_path_to_file("geocodes"), sep="\t")


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

    return os.path.exists((constants.get_path_to_file("observations", event)))


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

    return prepare_observations(
        observations=(
            pd.read_csv(
                constants.get_path_to_file("observations", event=event), sep="\t"
            )
        ),
        stations=stations,
        event=event,
    )


def prepare_observations(
    observations: pd.DataFrame, stations: list, event: str
) -> pd.DataFrame:
    """
    Prepare observational data for analysis.

    Parameters
    ----------
    observations : pd.DataFrame
        Observational data to be processed.
    stations : list
        List of station IDs to filter observations by.
    event : str
        Event category to determine whether to use minimum or maximum temperature for snowfall calculation.

    Returns
    -------
    pd.DataFrame
        Processed observational data.

    Notes
    -----
    This function performs the following steps:

    1. Renames observation columns according to constants.mapping_observations_fields.
    2. Selects relevant columns according to constants.columns_observations.
    3. Filters observations by station IDs.
    4. Converts 'date' column to datetime.
    5. Converts numeric columns and handles missing values.
    6. Drops rows with NaN values.
    7. Calculates additional precipitation metrics.
    8. Calculates snowfall_24h column based on event category.
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
    observations.dropna(inplace=True)

    logging.info("Calculating additional precipitation metrics...")
    observations["uniform_precipitation_1h"] = np.round(
        observations["precipitation"] * 1 / 24, 1
    )
    observations["severe_precipitation_1h"] = np.round(
        observations["precipitation"] * float(__SEVERE_PRECIPITATION_BY_TIMEFRAME[1]), 1
    )
    observations["extreme_precipitation_1h"] = np.round(
        observations["precipitation"] * float(__EXTREME_PRECIPITATION_BY_TIMEFRAME[1]),
        1,
    )
    observations["uniform_precipitation_12h"] = np.round(
        observations["precipitation"] * 12 / 24, 1
    )
    observations["extreme_precipitation_12h"] = np.round(
        observations["precipitation"] * float(__EXTREME_PRECIPITATION_BY_TIMEFRAME[12]),
        1,
    )
    if not "RAIN" in event:
        observations["snowfall_24h"] = observations.apply(
            lambda row: row["precipitation"] if row["minimum_temperature"] <= 0 else 2,
            axis=1,
        )

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
    geocodes["shape"] = geocodes["polygon"].apply(
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
                if a["shape"].contains(station["point"])
            ),
            None,
        ),
        axis=1,
    )

    stations.drop(columns=["point"], inplace=True)

    stations.to_csv(constants.get_path_to_file("geolocated"), sep="\t")


def exist_gelocated_stations() -> bool:
    """
    Check if the geolocated stations file exists.

    Returns:
        bool: True if the file exists, False otherwise
    """
    return os.path.exists(constants.get_path_to_file("geolocated"))


def get_geolocated_stations() -> pd.DataFrame:
    """
    Retrieve the geolocated stations DataFrame, or raise an Exception if something fails.

    Returns:
        pd.DataFrame: the geolocated stations DataFrame
    """

    try:
        return prepare_geolocated_stations(
            pd.read_csv(constants.get_path_to_file("geolocated"), sep="\t")
        )
    except Exception as e:
        logging.error(f"Error retrieving geolocated data: {e}")
        raise


def prepare_geolocated_stations(geolocated_sations: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a geolocated stations DataFrame for analysis.

    This function renames columns according to the mapping defined in
    `constants.mapping_observations_fields`, title-cases the "province" and "name"
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
    geolocated_sations.rename(
        columns=constants.mapping_observations_fields, inplace=True
    )

    geolocated_sations[["province", "name"]] = geolocated_sations[
        ["province", "name"]
    ].applymap(str.title)

    if (not geolocated_sations["latitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in geolocated_sations["latitude"]
    ):
        geolocated_sations["latitude"] = geolocated_sations["latitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    if (not geolocated_sations["longitude"].dtype == "float64") or any(
        re.search(r"[a-zA-Z]", str(x)) for x in geolocated_sations["longitude"]
    ):
        geolocated_sations["longitude"] = geolocated_sations["longitude"].apply(
            lambda x: dms_coordinates_to_degress(x, hemisphere=x[-1])
        )

    geolocated_sations["latitude"] = pd.to_numeric(
        geolocated_sations["latitude"], errors="coerce"
    )
    geolocated_sations["longitude"] = pd.to_numeric(
        geolocated_sations["longitude"], errors="coerce"
    )
    geolocated_sations["altitude"] = pd.to_numeric(
        geolocated_sations["altitude"], errors="coerce"
    )

    return geolocated_sations
