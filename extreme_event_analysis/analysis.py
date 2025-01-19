"""
This module provides a class for performing an event analysis.

Imports:
- pandas as pd: For data manipulation and analysis.
- numpy as np: For numerical computations.
- os: For interacting with the operating system (paths, files, etc.).
- datetime, timedelta: For working with dates and time differences.
- aemet_opendata_connector: For connecting to the AEMET OpenData API.
- common_operations: For common operations and utilities.
- constants: For accessing global constants used throughout the project.
- logging: For logging and handling log messages.
"""

import pandas as pd
import numpy as np
import os

from datetime import datetime, timedelta

import aemet_opendata
import visuals
import common
import constants
import logging


class EventAnalysis:
    __event_id = ""
    __event_name = ""
    __event_start = datetime.now()
    __event_end = datetime.now()

    __columns_analysis = [
        "date",
        "geocode",
        "region",
        "area",
        "province",
        "polygon",
        "idema",
        "name",
        "latitude",
        "longitude",
        "altitude",
        "param_id",
        "param_name",
        "predicted_severity",
        "predicted_value",
        "region_severity",
        "region_value",
        "observed_severity",
        "observed_value",
    ]

    __columns_results = [
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
        "extreme_precipitation_1h",
        "extreme_precipitation_1h_severity",
        "extreme_precipitation_12h",
        "extreme_precipitation_12h_severity",
        "snowfall_24h",
        "snowfall_24h_severity",
        "wind_speed",
        "wind_speed_severity",
    ]

    __df_observations = pd.DataFrame(columns=constants.columns_observations)
    __df_warnings = pd.DataFrame(columns=constants.columns_warnings)
    __df_situations = pd.DataFrame(columns=constants.columns_warnings)
    __df_stations = pd.DataFrame(columns=constants.columns_stations)
    __df_thresholds = pd.DataFrame(columns=constants.columns_thresholds)
    __df_geocodes = pd.DataFrame(columns=constants.columns_geocodes)
    __df_analysis = pd.DataFrame(columns=__columns_analysis)

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        """
        Initializes an instance of the EventAnalysis class.

        Parameters
        ----------
        event_id : str
            The identifier for the event to be analyzed.
        event_name : str
            The name of the event to be analyzed.
        event_start : datetime
            The start date and time of the event to be analyzed.
        event_end : datetime
            The end date and time of the event to be analyzed.
        """
        self.__event_id = event_id
        self.__event_name = event_name
        self.__event_start = event_start
        self.__event_end = event_end

    def get_event(self) -> str:
        """
        Returns the event identifier, name, start date and end date of the analyzed event as a dictionary.

        Returns
        -------
        dict
            A dictionary containing the event identifier, name, start date and end date of the analyzed event.
        """
        return {
            "id": self.__event_id,
            "name": self.__event_name,
            "start": self.__event_start,
            "end": self.__event_end,
        }

    def get_warnings_start(self) -> datetime:
        """
        Returns the start date and time of the warnings for the analyzed event.

        This method first filters the warnings DataFrame to only include rows with a severity greater than or equal to 1.
        If this filtered DataFrame is not empty, it then returns the minimum effective date and time of the filtered warnings.
        Otherwise, it returns the start date and time of the analyzed event.

        Returns
        -------
        datetime
            The start date and time of the warnings for the analyzed event.
        """

        warnings = self.__df_warnings.copy()
        warnings["severity_mapped"] = warnings["severity"].map(
            constants.mapping_severity_values
        )
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].min()
        else:
            return self.__event_start

    def get_warnings_end(self) -> datetime:
        """
        Returns the end date and time of the warnings for the analyzed event.

        This method first filters the warnings DataFrame to only include rows with a severity greater than or equal to 1.
        If this filtered DataFrame is not empty, it then returns the maximum effective date and time of the filtered warnings.
        Otherwise, it returns the end date and time of the analyzed event.

        Returns
        -------
        datetime
            The end date and time of the warnings for the analyzed event.
        """

        warnings = self.__df_warnings.copy()
        warnings["severity_mapped"] = warnings["severity"].map(
            constants.mapping_severity_values
        )
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].max()
        else:
            return self.__event_end

    def fetch_warnings(self):
        """
        Downloads warnings data for the analyzed event.

        This method first ensures that the necessary directories exist for storing the warnings data.
        It then checks if the warnings file already exists for the event. If it does not exist, it first
        checks if the CAPS files exist for the event. If the CAP files do not exist, it downloads the CAP
        files using the fetch_caps function from the aemet_opendata_connector module. If the CAP files
        exist, it then transforms the CAP files into a warnings file using the caps_to_warnings function
        from the common_operations module. Finally, it checks if a geolocated stations file exists and
        creates one if it does not.

        Parameters
        ----------
        event : str
            The event identifier for which to download warnings data.

        Returns
        -------
        None
        """

        common.ensure_directories(
            event=(self.__event_id), start=(self.__event_start), end=(self.__event_end)
        )

        logging.info(f"Checking for warnings file ...")
        if not common.exist_warnings(event=(self.__event_id)):
            logging.info(f"... warnings file not found. Checking for caps ...")
            if not common.exist_caps(event=(self.__event_id)):
                logging.info(f"... caps not found. Downloading")
                aemet_opendata.fetch_caps(
                    event=(self.__event_id),
                    start=(self.__event_start),
                    end=(self.__event_end),
                )
            common.caps_to_warnings(event=(self.__event_id))

        logging.info(f"Checking for geolocated stations file ...")
        if not common.exist_gelocated_stations():
            logging.info(f"... file not found.")
            common.geolocate_stations()

    def load_data(self):
        """
        Loads the necessary data for the analysis.

        This method first loads the geolocated stations, thresholds and geocodes dataframes.
        It then loads the warnings dataframe for the analyzed event. Finally, it cleans up
        the files that are no longer needed.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info(f"Loading stations inventory ...")
        self.__df_stations = common.get_geolocated_stations()

        logging.info(f"Loading thresholds list ...")
        self.__df_thresholds = common.get_thresholds()

        logging.info(f"Loading thresholds list ...")
        self.__df_geocodes = common.get_geocodes()

        logging.info(f"Loading warnings data ...")
        self.__df_warnings = common.get_warnings(event=self.__event_id)

        common.clean_files(event=self.__event_id)

    def fetch_observations(self):
        """
        Downloads the observations data for the analyzed event and loads it to memory.

        If the observations file for the analyzed event does not exist, it is downloaded
        from AEMET's OpenData service. The start and end dates for the download are
        determined by the start and end dates of the warnings associated with the
        analyzed event. The downloaded data is then loaded into memory.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info(f"Checking for observations file ...")
        if not common.exist_observations(event=(self.__event_id)):
            logging.info(f"... observations file not found. Downloading")
            aemet_opendata.fetch_observations(
                event=(self.__event_id),
                start=(self.get_warnings_start()),
                end=(self.get_warnings_end()),
            )

        logging.info(f"Loading observations data ...")
        self.__df_observations = common.get_observations(
            event=self.__event_id, stations=self.__df_stations["idema"].tolist()
        )

    def prepare_analysis(self):
        """
        Prepares the data for analysis by performing several steps.

        This method involves geolocating the observations, composing the definitive
        observations data by merging with relevant datasets, and generating real
        situations for analysis.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info("Starting analysis preparation")
        logging.info("Geolocating stations")
        self.__geolocate_observations()
        logging.info("Composing definitive observations data")
        self.__evaluate_observations()
        logging.info("Generating real situations")
        self.generate_situations()
        logging.info("Analysis preparation completed")

    def __geolocate_observations(self):
        """
        Geolocates the observations by merging them with stations and geocodes datasets.

        This method merges the observations dataset with the stations and geocodes
        datasets. It then reorders the columns of the observations dataset to match
        the columns of the results dataset and initializes missing columns to NaN.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        logging.info("Preparing observations for comparison")

        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_stations,
            on="idema",
            suffixes=("", "stations_"),
        )

        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_geocodes,
            on="geocode",
            suffixes=("", "geocode_"),
        )

        logging.info("Reordering and initializing missing columns in observations")
        for col in self.__columns_results:
            if col not in self.__df_observations.columns:
                self.__df_observations[col] = np.nan
        self.__df_observations = self.__df_observations[self.__columns_results]

    def __evaluate_observations(self):
        """
        Composes the definitive observations data for analysis.

        This method performs several data preparation steps on the observations
        DataFrame. It begins by ensuring proper data types for the 'geocode' and
        'date' columns. Then, it merges the observations with the thresholds
        DataFrame on the 'geocode' column. After merging, it removes unnecessary
        threshold columns from the observations. The method proceeds to calculate
        severity levels for various meteorological parameters such as minimum and
        maximum temperatures, precipitation over different time periods, snowfall,
        and wind speed, based on predefined warning thresholds. Finally, it
        reorders the columns to match the expected structure for analysis.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info("Preparing observed data for comparison")
        logging.info("Converting geocodes")
        self.__df_observations["geocode"] = self.__df_observations["geocode"].astype(
            int
        )
        self.__df_observations["geocode"] = self.__df_observations["geocode"].astype(
            str
        )
        self.__df_thresholds["geocode"] = self.__df_thresholds["geocode"].astype(int)
        self.__df_thresholds["geocode"] = self.__df_thresholds["geocode"].astype(str)
        self.__df_observations["date"] = pd.to_datetime(self.__df_observations["date"])

        logging.info("Merging observed data with thresholds data")
        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_thresholds,
            on="geocode",
            suffixes=("", "thresholds_"),
        )

        logging.info("Dropping threshold columns from observations")
        self.__df_observations = self.__df_observations.loc[
            :, ~self.__df_observations.columns.str.startswith("thresholds_")
        ]

        logging.info("Calculating minimum temperature severity")
        self.__df_observations["minimum_temperature_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["minimum_temperature"]
                    <= row["minimum_temperature_yellow_warning"]
                    and row["minimum_temperature"]
                    > row["minimum_temperature_orange_warning"]
                    else (
                        2
                        if row["minimum_temperature"]
                        <= row["minimum_temperature_orange_warning"]
                        and row["minimum_temperature"]
                        > row["minimum_temperature_red_warning"]
                        else (
                            3
                            if row["minimum_temperature"]
                            <= row["minimum_temperature_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating maximum temperature severity")
        self.__df_observations["maximum_temperature_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["maximum_temperature"]
                    >= row["maximum_temperature_yellow_warning"]
                    and row["maximum_temperature"]
                    < row["maximum_temperature_orange_warning"]
                    else (
                        2
                        if row["maximum_temperature"]
                        >= row["maximum_temperature_orange_warning"]
                        and row["maximum_temperature"]
                        < row["maximum_temperature_red_warning"]
                        else (
                            3
                            if row["maximum_temperature"]
                            >= row["maximum_temperature_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating uniform precipitation 1h severity")
        self.__df_observations["uniform_precipitation_1h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["uniform_precipitation_1h"]
                    >= row["precipitation_1h_yellow_warning"]
                    and row["uniform_precipitation_1h"]
                    < row["precipitation_1h_orange_warning"]
                    else (
                        2
                        if row["uniform_precipitation_1h"]
                        >= row["precipitation_1h_orange_warning"]
                        and row["uniform_precipitation_1h"]
                        < row["precipitation_1h_red_warning"]
                        else (
                            3
                            if row["uniform_precipitation_1h"]
                            >= row["precipitation_1h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating severe precipitation 1h severity")
        self.__df_observations["severe_precipitation_1h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["severe_precipitation_1h"]
                    >= row["precipitation_1h_yellow_warning"]
                    and row["severe_precipitation_1h"]
                    < row["precipitation_1h_orange_warning"]
                    else (
                        2
                        if row["severe_precipitation_1h"]
                        >= row["precipitation_1h_orange_warning"]
                        and row["severe_precipitation_1h"]
                        < row["precipitation_1h_red_warning"]
                        else (
                            3
                            if row["severe_precipitation_1h"]
                            >= row["precipitation_1h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating extreme precipitation 1h severity")
        self.__df_observations["extreme_precipitation_1h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["extreme_precipitation_1h"]
                    >= row["precipitation_1h_yellow_warning"]
                    and row["extreme_precipitation_1h"]
                    < row["precipitation_1h_orange_warning"]
                    else (
                        2
                        if row["extreme_precipitation_1h"]
                        >= row["precipitation_1h_orange_warning"]
                        and row["extreme_precipitation_1h"]
                        < row["precipitation_1h_red_warning"]
                        else (
                            3
                            if row["extreme_precipitation_1h"]
                            >= row["precipitation_1h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating uniform precipitation 12h severity")
        self.__df_observations["uniform_precipitation_12h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["uniform_precipitation_12h"]
                    >= row["precipitation_12h_yellow_warning"]
                    and row["uniform_precipitation_12h"]
                    < row["precipitation_12h_orange_warning"]
                    else (
                        2
                        if row["uniform_precipitation_12h"]
                        >= row["precipitation_12h_orange_warning"]
                        and row["uniform_precipitation_12h"]
                        < row["precipitation_12h_red_warning"]
                        else (
                            3
                            if row["uniform_precipitation_12h"]
                            >= row["precipitation_12h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating severe precipitation 12h severity")
        self.__df_observations["severe_precipitation_12h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["severe_precipitation_12h"]
                    >= row["precipitation_12h_yellow_warning"]
                    and row["severe_precipitation_12h"]
                    < row["precipitation_12h_orange_warning"]
                    else (
                        2
                        if row["severe_precipitation_12h"]
                        >= row["precipitation_12h_orange_warning"]
                        and row["severe_precipitation_12h"]
                        < row["precipitation_12h_red_warning"]
                        else (
                            3
                            if row["severe_precipitation_12h"]
                            >= row["precipitation_12h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating extreme precipitation 12h severity")
        self.__df_observations["extreme_precipitation_12h_severity"] = (
            self.__df_observations.apply(
                lambda row: (
                    1
                    if row["extreme_precipitation_12h"]
                    >= row["precipitation_12h_yellow_warning"]
                    and row["extreme_precipitation_12h"]
                    < row["precipitation_12h_orange_warning"]
                    else (
                        2
                        if row["extreme_precipitation_12h"]
                        >= row["precipitation_12h_orange_warning"]
                        and row["extreme_precipitation_12h"]
                        < row["precipitation_12h_red_warning"]
                        else (
                            3
                            if row["extreme_precipitation_12h"]
                            >= row["precipitation_12h_red_warning"]
                            else 0
                        )
                    )
                ),
                axis=1,
            )
        )

        logging.info("Calculating snowfall 24h severity")
        self.__df_observations["snowfall_24h_severity"] = self.__df_observations.apply(
            lambda row: (
                1
                if row["snowfall_24h"] >= row["snowfall_24h_yellow_warning"]
                and row["snowfall_24h"] < row["snowfall_24h_orange_warning"]
                else (
                    2
                    if row["snowfall_24h"] >= row["snowfall_24h_orange_warning"]
                    and row["snowfall_24h"] < row["snowfall_24h_red_warning"]
                    else (
                        3
                        if row["snowfall_24h"] >= row["snowfall_24h_red_warning"]
                        else 0
                    )
                )
            ),
            axis=1,
        )

        logging.info("Calculating maximum wind speed severity")
        self.__df_observations["wind_speed_severity"] = self.__df_observations.apply(
            lambda row: (
                1
                if row["wind_speed"] >= row["wind_speed_yellow_warning"]
                and row["wind_speed"] < row["wind_speed_orange_warning"]
                else (
                    2
                    if row["wind_speed"] >= row["wind_speed_orange_warning"]
                    and row["wind_speed"] < row["wind_speed_red_warning"]
                    else 3 if row["wind_speed"] >= row["wind_speed_red_warning"] else 0
                )
            ),
            axis=1,
        )

        self.__df_observations = self.__df_observations[self.__columns_results]

    def generate_situations(self):
        """
        Generates a DataFrame of situations based on observed weather data.

        This method iterates over the observations DataFrame and creates new rows in the situations DataFrame
        for each parameter with a severity greater than zero. It constructs an identifier for each situation
        based on the event ID, date, geocode, and parameter ID, and assigns the effective date, severity,
        parameter ID, parameter name, parameter value, geocode, and polygon information to each new row.

        The situations DataFrame is composed of the following columns:
        - id: A unique identifier for each situation, constructed using the event ID, date, geocode, and parameter ID.
        - effective: The date of the observation.
        - severity: The severity of the parameter, mapped to a string value.
        - param_id: The identifier for the parameter being observed.
        - param_name: The name of the parameter being observed.
        - param_value: The value of the parameter being observed.
        - geocode: The geographical code associated with the observation.
        - polygon: The geographical polygon associated with the observation.

        The resulting DataFrame is stored in the self.__df_situations attribute.
        """

        self.__df_situations = pd.DataFrame(columns=constants.columns_warnings)
        mapping_values_to_severity = {
            v: k for k, v in constants.mapping_severity_values.items()
        }
        for _, obs in self.__df_observations.iterrows():
            if obs["minimum_temperature_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}BT",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["minimum_temperature_severity"]
                            ),
                            "param_id": "BT",
                            "param_name": "Temperaturas mínimas",
                            "param_value": obs["minimum_temperature"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["maximum_temperature_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}AT",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["maximum_temperature_severity"]
                            ),
                            "param_id": "AT",
                            "param_name": "Temperaturas máximas",
                            "param_value": obs["maximum_temperature"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["uniform_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.UNIFORME",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["uniform_precipitation_1h_severity"]
                            ),
                            "param_id": "PR_1H.UNIFORME",
                            "param_name": "Precipitación acumulada en una hora (estimación uniforme)",
                            "param_value": obs["uniform_precipitation_1h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["uniform_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.UNIFORME",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["uniform_precipitation_12h_severity"]
                            ),
                            "param_id": "PR_12H.UNIFORME",
                            "param_name": "Precipitación acumulada en 12 horas (estimación uniforme)",
                            "param_value": obs["uniform_precipitation_12h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["severe_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.SEVERA",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["severe_precipitation_1h_severity"]
                            ),
                            "param_id": "PR_1H.SEVERA",
                            "param_name": "Precipitación acumulada en una hora (estimación severa del 33% en 1 hora)",
                            "param_value": obs["severe_precipitation_1h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["severe_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.SEVERA",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["severe_precipitation_12h_severity"]
                            ),
                            "param_id": "PR_12H.SEVERA",
                            "param_name": "Precipitación acumulada en 12 horas (estimación severa del 85% en 12 horas)",
                            "param_value": obs["severe_precipitation_12h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["extreme_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.EXTREMA",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["extreme_precipitation_1h_severity"]
                            ),
                            "param_id": "PR_1H.EXTREMA",
                            "param_name": "Precipitación acumulada en una hora (estimación extrema del 50% en 1 hora)",
                            "param_value": obs["extreme_precipitation_1h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["extreme_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.EXTREMA",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["extreme_precipitation_12h_severity"]
                            ),
                            "param_id": "PR_12H.EXTREMA",
                            "param_name": "Precipitación acumulada en 12 horas (estimación extrema del 100% en 12 horas)",
                            "param_value": obs["extreme_precipitation_12h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["snowfall_24h_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}NE",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["snowfall_24h_severity"]
                            ),
                            "param_id": "NE",
                            "param_name": "Nieve acumulada en 24 horas",
                            "param_value": obs["snowfall_24h"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
            if obs["wind_speed_severity"] > 0:
                new_row = pd.DataFrame(
                    [
                        {
                            "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}VI",
                            "effective": obs["date"],
                            "severity": mapping_values_to_severity.get(
                                obs["wind_speed_severity"]
                            ),
                            "param_id": "VI",
                            "param_name": "Rachas máximas",
                            "param_value": obs["wind_speed"],
                            "geocode": obs["geocode"],
                        }
                    ]
                )
                self.__df_situations = pd.concat(
                    [self.__df_situations, new_row], ignore_index=True
                )
        self.__group_situations()

    def __group_situations(self):
        """
        Groups situations by id and effective date, and resolves conflicts within each group.
        """
        logging.info("Grouping situations by id and effective date")

        self.__df_situations = self.__df_situations.loc[
            self.__df_situations.groupby(["id", "effective"])["severity"].idxmax()
        ]

        def __resolve(group):
            if (group["param_id"] == "BT").any():
                return group.loc[group["param_value"].idxmin()]
            else:
                return group.loc[group["param_value"].idxmax()]

        self.__df_situations = (
            self.__df_situations.groupby(["id", "effective"])
            .apply(__resolve)
            .reset_index(drop=True)
        )

    def save_data(self):
        """
        Saves the analysis results to different files.

        The results are saved to the following files:

        - results: The observed data.
        - predictions: The predicted warnings.
        - situations: The real situations.

        The files are saved in a directory specified by the event ID.
        """
        self.__df_observations.to_csv(
            constants.get_path_to_file("results", self.__event_id), sep="\t"
        )
        self.__df_warnings.to_csv(
            constants.get_path_to_file("predictions", self.__event_id), sep="\t"
        )
        self.__df_situations.to_csv(
            constants.get_path_to_file("situations", self.__event_id), sep="\t"
        )
        self.__df_analysis.to_csv(
            constants.get_path_to_file("analysis", self.__event_id), sep="\t"
        )

    def preanalyze_data(self):
        """
        Prepares the geocode and severity data for analysis.

        This method converts the 'geocode' column in the geocodes, warnings, and 
        situations DataFrames to an integer and then to a string type. It maps 
        the severity values in the warnings and situations DataFrames using the 
        predefined severity mapping. It also fills missing parameter names with 
        an empty string in these DataFrames.

        Additionally, it expands the warnings DataFrame by creating new rows for 
        specific precipitation parameters ('PR_1H' and 'PR_12H') using existing 
        rows and appending them with unique parameter identifiers. It then removes 
        the original 'PR_1H' and 'PR_12H' rows from the warnings DataFrame.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.__df_geocodes["geocode"] = self.__df_geocodes["geocode"].astype(int)
        self.__df_geocodes["geocode"] = self.__df_geocodes["geocode"].astype(str)

        self.__df_warnings["geocode"] = self.__df_warnings["geocode"].astype(int)
        self.__df_warnings["geocode"] = self.__df_warnings["geocode"].astype(str)
        self.__df_warnings["severity"] = self.__df_warnings["severity"].map(
            constants.mapping_severity_values
        )
        self.__df_warnings["param_name"].fillna("", inplace=True)

        self.__df_situations["geocode"] = self.__df_situations["geocode"].astype(int)
        self.__df_situations["geocode"] = self.__df_situations["geocode"].astype(str)
        self.__df_situations["severity"] = self.__df_situations["severity"].map(
            constants.mapping_severity_values
        )
        self.__df_situations["param_name"].fillna("", inplace=True)

        precipitation_1h = self.__df_warnings[self.__df_warnings["param_id"] == "PR_1H"]
        precipitation_12h = self.__df_warnings[
            self.__df_warnings["param_id"] == "PR_12H"
        ]

        distinct_params = {
            key
            for key in constants.mapping_parameters.keys()
            if key.startswith("PR_1H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_1h.copy()
            repeating_rows["param_id"] = param
            self.__df_warnings = pd.concat(
                [self.__df_warnings, repeating_rows], ignore_index=True
            )

        distinct_params = {
            key
            for key in constants.mapping_parameters.keys()
            if key.startswith("PR_12H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_12h.copy()
            repeating_rows["param_id"] = param
            self.__df_warnings = pd.concat(
                [self.__df_warnings, repeating_rows], ignore_index=True
            )

        self.__df_warnings = self.__df_warnings[
            self.__df_warnings["param_id"] != "PR_1H"
        ]
        self.__df_warnings = self.__df_warnings[
            self.__df_warnings["param_id"] != "PR_12H"
        ]

    def analyze_data(self):
        """
        Analyzes the data by merging the warnings and situations dataframes and expanding
        the resulting dataframe with data from the geocodes, stations, and observations
        dataframes. The resulting dataframe is then saved to a file named 'analysis' in
        the directory specified by the event ID.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        self.preanalyze_data()
        merged_df = pd.merge(
            self.__df_warnings,
            self.__df_situations,
            how="outer",
            on=["geocode", "param_id", "effective"],
            suffixes=("_pre", "_obs"),
        )

        merged_df["param_name_obs"] = merged_df["param_id"].map(
            constants.mapping_parameters_description
        )
        merged_df["param_name_pre"] = merged_df["param_id"].map(
            constants.mapping_parameters_description
        )

        analysis = pd.DataFrame(
            columns=[
                "date",
                "geocode",
                "param_id",
                "param_name",
                "predicted_severity",
                "predicted_value",
                "region_severity",
                "region_value",
            ]
        )
        analysis["date"] = merged_df["effective"]
        analysis["geocode"] = merged_df["geocode"]
        analysis["param_id"] = merged_df["param_id"]
        analysis["param_name"] = merged_df["param_name_obs"].combine_first(
            merged_df["param_name_pre"]
        )
        analysis["predicted_severity"] = merged_df["severity_pre"]
        analysis["predicted_value"] = merged_df["param_value_pre"]
        analysis["region_severity"] = merged_df["severity_obs"]
        analysis["region_value"] = merged_df["param_value_obs"]

        analysis = pd.merge(
            analysis,
            self.__df_geocodes[["geocode", "region", "area", "province", "polygon"]],
            how="left",
            on=["geocode"],
        )

        analysis["geocode"] = analysis["geocode"].astype(float)
        analysis["geocode"] = analysis["geocode"].astype(str)
        self.__df_stations["geocode"] = self.__df_stations["geocode"].astype(str)
        analysis = analysis.merge(
            self.__df_stations[
                ["geocode", "idema", "name", "latitude", "longitude", "altitude"]
            ],
            how="left",
            on="geocode",
        )

        analysis = analysis.merge(
            self.__df_observations[
                [
                    "date",
                    "idema",
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
                    "extreme_precipitation_1h",
                    "extreme_precipitation_1h_severity",
                    "extreme_precipitation_12h",
                    "extreme_precipitation_12h_severity",
                    "snowfall_24h",
                    "snowfall_24h_severity",
                    "wind_speed",
                    "wind_speed_severity",
                ]
            ],
            how="left",
            on=["date", "idema"],
        )

        for k in analysis["param_id"].unique():
            analysis.loc[analysis["param_id"] == k, "observed_value"] = analysis[analysis["param_id"] == k][constants.mapping_parameters[k]["id"]]
            analysis.loc[analysis["param_id"] == k, "observed_severity"] = analysis[analysis["param_id"] == k][constants.mapping_parameters[k]["id"]+"_severity"]
            analysis[constants.mapping_parameters[k]["id"]+"_severity"].fillna(0, inplace=True)

        analysis["region_severity"].fillna(0, inplace=True)
        analysis["predicted_severity"].fillna(0, inplace=True)
        analysis.dropna(subset=["observed_value"], inplace=True)
        self.__df_analysis = analysis[self.__columns_analysis]

    def draw_maps(self):
        visuals.get_map(
            self.__event_id,
            self.__event_name,
            self.__df_analysis
        )
