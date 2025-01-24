"""
This module provides a class for performing data processing.

Imports:
- pandas as pd: For data manipulation and analysis.
- numpy as np: For numerical computations.
- datetime: For working with dates.
- aemet_opendata_connector: For connecting to the AEMET OpenData API.
- common_operations: For common operations and utilities.
- constants: For accessing global constants used throughout the project.
- logging: For logging and handling log messages.
"""

import pandas as pd
import numpy as np

from datetime import datetime

import aemet_opendata
import event_data_commons
import logging


class EventDataProcessor:
    __EVENT_ID__ = ""
    __EVENT_NAME__ = ""
    __EVENT_START__ = datetime.now()
    __EVENT_END__ = datetime.now()

    __FIELDS_PROCESSED_DATA__ = [
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
    __FIELDS_COMBINED_RESULTS__ = [
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

    __DATAFRAME_OBSERVED_DATA__ = pd.DataFrame(
        columns=event_data_commons.FIELDS_OBSERVATION_DATA
    )
    __DATAFRAME_WARNINGS__ = pd.DataFrame(
        columns=event_data_commons.FIELDS_WARNING_DATA
    )
    __DATAFRAME_WARNINGS_EXTENDED__ = pd.DataFrame(
        columns=event_data_commons.FIELDS_WARNING_DATA
    )
    __DATAFRAME_STATION_DATA = pd.DataFrame(
        columns=event_data_commons.FIELDS_STATION_DATA
    )
    __DATAFRAME_THRESHOLD_DATA__ = pd.DataFrame(
        columns=event_data_commons.FIELDS_THRESHOLD_DATA
    )
    __DATAFRAME_GEOCODES__ = pd.DataFrame(
        columns=event_data_commons.FIELDS_GEOCODE_DATA
    )
    __DATAFRAME_PREPARED_DATA__ = pd.DataFrame(columns=__FIELDS_PROCESSED_DATA__)

    __SEVERE_PRECIPITATION_BY_TIMEFRAME__ = {1: 0.33, 12: 0.75}
    __EXTREME_PRECIPITATION_BY_TIMEFRAME__ = {1: 0.75, 12: 1.00}

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        """
        Initializes an instance of the class.

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
        self.__EVENT_ID__ = event_id
        self.__EVENT_NAME__ = event_name
        self.__EVENT_START__ = event_start
        self.__EVENT_END__ = event_end

    def get_event_data(self) -> pd.DataFrame:
        """
        Retrieves the prepared data DataFrame.

        This method returns the DataFrame containing the processed data,
        which includes information such as date, geocode, region, area,
        province, polygon, and various parameter values and severities.

        Returns
        -------
        pd.DataFrame
            The DataFrame with processed event data.
        """
        return self.__DATAFRAME_PREPARED_DATA__

    def get_event_info(self) -> str:
        """
        Returns the event identifier, name, start date and end date of the analyzed event as a dictionary.

        Returns
        -------
        dict
            A dictionary containing the event identifier, name, start date and end date of the analyzed event.
        """
        return {
            "id": self.__EVENT_ID__,
            "name": self.__EVENT_NAME__,
            "start": self.__EVENT_START__,
            "end": self.__EVENT_END__,
        }

    def get_warnings_start(self) -> datetime:
        """
        Returns the start date and time of the warnings.

        This method first filters the warnings DataFrame to only include rows with a severity greater than or equal to 1.
        If this filtered DataFrame is not empty, it then returns the minimum effective date and time of the filtered warnings.
        Otherwise, it returns the start date and time of the analyzed event.

        Returns
        -------
        datetime
            The start date and time of the warnings.
        """

        warnings = self.__DATAFRAME_WARNINGS__.copy()
        warnings["severity_mapped"] = warnings["severity"].map(
            event_data_commons.MAPPING_SEVERITY_VALUE
        )
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].min()
        else:
            return self.__EVENT_START__

    def get_warnings_end(self) -> datetime:
        """
        Returns the end date and time of the warnings.

        This method first filters the warnings DataFrame to only include rows with a severity greater than or equal to 1.
        If this filtered DataFrame is not empty, it then returns the maximum effective date and time of the filtered warnings.
        Otherwise, it returns the end date and time of the analyzed event.

        Returns
        -------
        datetime
            The end date and time of the warnings.
        """

        warnings = self.__DATAFRAME_WARNINGS__.copy()
        warnings["severity_mapped"] = warnings["severity"].map(
            event_data_commons.MAPPING_SEVERITY_VALUE
        )
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].max()
        else:
            return self.__EVENT_END__

    def fetch_predicted_warnings(self):
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

        event_data_commons.ensure_directories(
            event=(self.__EVENT_ID__),
            start=(self.__EVENT_START__),
            end=(self.__EVENT_END__),
        )

        logging.info(f"Checking for warnings file ...")
        if not event_data_commons.exist_warnings(event=(self.__EVENT_ID__)):
            logging.info(f"... warnings file not found. Checking for caps ...")
            if not event_data_commons.exist_caps(event=(self.__EVENT_ID__)):
                logging.info(f"... caps not found. Downloading")
                aemet_opendata.fetch_caps(
                    event=(self.__EVENT_ID__),
                    start=(self.__EVENT_START__),
                    end=(self.__EVENT_END__),
                )
            event_data_commons.caps_to_warnings(event=(self.__EVENT_ID__))

        logging.info(f"Checking for geolocated stations file ...")
        if not event_data_commons.exist_gelocated_stations():
            logging.info(f"... file not found.")
            event_data_commons.geolocate_stations()
            logging.info(f"... stations updated.")

    def load_raw_data(self):
        """
        Loads the necessary data.

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
        stations = event_data_commons.get_geolocated_stations()
        self.__DATAFRAME_STATION_DATA = stations

        logging.info(f"Loading thresholds list ...")
        thresholds = event_data_commons.get_thresholds()
        self.__DATAFRAME_THRESHOLD_DATA__ = thresholds

        logging.info(f"Loading thresholds list ...")
        geocodes = event_data_commons.get_geocodes()
        self.__DATAFRAME_GEOCODES__ = geocodes

        logging.info(f"Loading warnings data ...")
        warnings = event_data_commons.get_warnings(event=self.__EVENT_ID__)
        self.__DATAFRAME_WARNINGS__ = warnings

        event_data_commons.clean_files(event=self.__EVENT_ID__)

    def fetch_observed_data(self):
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
        if not event_data_commons.exist_observations(event=(self.__EVENT_ID__)):
            logging.info(f"... observations file not found. Downloading")
            aemet_opendata.fetch_observations(
                event=(self.__EVENT_ID__),
                start=(self.get_warnings_start()),
                end=(self.get_warnings_end()),
            )

        logging.info(f"Loading observations data ...")
        observations = event_data_commons.get_observations(
            event=self.__EVENT_ID__,
            stations=self.__DATAFRAME_STATION_DATA["idema"].tolist(),
        )
        self.__DATAFRAME_OBSERVED_DATA__ = observations

    def prepare_event_data(self):
        """
        Prepares the data by performing several steps.

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

        logging.info("Starting data processing")
        logging.info("Estimating missings...")
        self.__estimate_missing_observations__()
        logging.info("Geolocating observations...")
        self.__geolocate_observed_data__()
        logging.info("Composing observations...")
        self.__evaluate_observed_severity__()
        logging.info("Extending warnings...")
        self.__extend_warning_data__()
        logging.info("Combining data...")
        self.__discretize_observed_data__()
        logging.info("Completing data...")
        self.__complete_empty_fields__()
        logging.info("Summarizing observations...")
        self.__summarize_observed_data__()
        logging.info("Preparation completed")

    def __estimate_snowfall_value__(
        self,
        precipitation: float,
        minimum_temperature: float,
        maximum_temperature: float,
        altitude: float,
    ) -> int:
        """
        Estimates snowfall given precipitation, minimum and maximum temperatures, and altitude.

        Parameters
        ----------
        precipitation : float
            The precipitation value.
        minimum_temperature : float
            The minimum temperature value.
        maximum_temperature : float
            The maximum temperature value.
        altitude : float
            The altitude value.

        Returns
        -------
        int
            The estimated snowfall value in centimeters.
        """
        snow_level = event_data_commons.get_snow_level()
        if not snow_level.index.name == "t":
            snow_level = snow_level.set_index("t")

        if precipitation > 0:
            lapse_rate = 6.5
            t_5500hpa = maximum_temperature + lapse_rate * (altitude - 5500) / 1000
            t_850hpa = int(
                max(
                    -10,
                    np.round(minimum_temperature - (1500 - altitude) / 1000 * 6.5, 0),
                )
            )

            if t_5500hpa > -16 or t_850hpa > 3:
                return 0
            else:
                t_5500hpa = min(-42, t_5500hpa)

            target_altidude = (
                float(snow_level.loc[f"{t_850hpa}", f"T{t_5500hpa}"]) + 1500
            )
            snow_liquid_rate = 1

            if altitude < target_altidude:
                return 0
            else:
                return int(np.round(precipitation * snow_liquid_rate, 0))
        return 0

    def __estimate_missing_observations__(self) -> pd.DataFrame:
        """
        Prepare observational data for analysis.

        Returns
        -------
        pd.DataFrame
            Processed observational data.
        """
        observations = self.__DATAFRAME_OBSERVED_DATA__.copy()
        logging.info("Calculating additional precipitation metrics...")
        observations["uniform_precipitation_1h"] = np.round(
            observations["precipitation"] * 1 / 24, 1
        )
        observations["severe_precipitation_1h"] = np.round(
            observations["precipitation"]
            * float(self.__SEVERE_PRECIPITATION_BY_TIMEFRAME__[1]),
            1,
        )
        observations["extreme_precipitation_1h"] = np.round(
            observations["precipitation"]
            * float(self.__EXTREME_PRECIPITATION_BY_TIMEFRAME__[1]),
            1,
        )
        observations["uniform_precipitation_12h"] = np.round(
            observations["precipitation"] * 12 / 24, 1
        )
        observations["severe_precipitation_12h"] = np.round(
            observations["precipitation"]
            * float(self.__SEVERE_PRECIPITATION_BY_TIMEFRAME__[12]),
            1,
        )
        observations["extreme_precipitation_12h"] = np.round(
            observations["precipitation"]
            * float(self.__EXTREME_PRECIPITATION_BY_TIMEFRAME__[12]),
            1,
        )
        observations["snowfall_24h"] = observations.apply(
            lambda row: self.__estimate_snowfall_value__(
                float(row["precipitation"]),
                float(row["minimum_temperature"]),
                float(row["maximum_temperature"]),
                float(row["altitude"]),
            ),
            axis=1,
        )

        observations.loc[
            observations["snowfall_24h"] > 0, "uniform_precipitation_1h"
        ] = 0
        observations.loc[
            observations["snowfall_24h"] > 0, "severe_precipitation_1h"
        ] = 0
        observations.loc[
            observations["snowfall_24h"] > 0, "extreme_precipitation_1h"
        ] = 0
        observations.loc[
            observations["snowfall_24h"] > 0, "uniform_precipitation_12h"
        ] = 0
        observations.loc[
            observations["snowfall_24h"] > 0, "severe_precipitation_12h"
        ] = 0
        observations.loc[
            observations["snowfall_24h"] > 0, "extreme_precipitation_12h"
        ] = 0

        observations["wind_speed"] = np.round(observations["wind_speed"] * 3.6, 1)

        logging.info("Observational data preparation complete.")
        self.__DATAFRAME_OBSERVED_DATA__ = observations

    def __geolocate_observed_data__(self):
        """
        Geolocates the observations by merging with station and geocode data.

        This method prepares the observations DataFrame for analysis by merging it
        with the stations DataFrame on the 'idema' column and with the geocodes
        DataFrame on the 'geocode' column. After merging, it ensures that all
        necessary columns are present in the observations by initializing any
        missing columns to NaN. The resulting DataFrame is then stored in
        self.__df_observations with columns reordered according to
        self.__columns_results.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info("Preparing observations for comparison")

        merged_observations = pd.merge(
            self.__DATAFRAME_OBSERVED_DATA__,
            self.__DATAFRAME_STATION_DATA,
            on="idema",
            suffixes=("", "stations_"),
        )

        merged_observations = pd.merge(
            merged_observations,
            self.__DATAFRAME_GEOCODES__,
            on="geocode",
            suffixes=("", "geocode_"),
        )

        logging.info("Reordering and initializing missing columns in observations")
        for col in self.__FIELDS_COMBINED_RESULTS__:
            if col not in merged_observations.columns:
                merged_observations[col] = np.nan
        self.__DATAFRAME_OBSERVED_DATA__ = merged_observations[
            self.__FIELDS_COMBINED_RESULTS__
        ]

    def __evaluate_observed_severity__(self):
        """
        Composes the definitive observations data.

        This method performs several data preparation steps on the observations
        DataFrame. It begins by ensuring proper data types for the 'geocode' and
        'date' columns. Then, it merges the observations with the thresholds
        DataFrame on the 'geocode' column. After merging, it removes unnecessary
        threshold columns from the observations. The method proceeds to calculate
        severity levels for various meteorological parameters such as minimum and
        maximum temperatures, precipitation over different time periods, snowfall,
        and wind speed, based on predefined warning thresholds. Finally, it
        reorders the columns to match the expected structure.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """

        logging.info("Preparing observed data for comparison")
        logging.info("Converting geocodes")
        observations = self.__DATAFRAME_OBSERVED_DATA__.copy()
        observations["geocode"] = observations["geocode"].astype(str)

        thresholds = self.__DATAFRAME_THRESHOLD_DATA__.copy()
        thresholds["geocode"] = thresholds["geocode"].astype(str)
        observations["date"] = pd.to_datetime(observations["date"])

        logging.info("Merging observed data with thresholds data")
        observations = pd.merge(
            observations,
            thresholds,
            on="geocode",
            suffixes=("", "thresholds_"),
        )

        logging.info("Dropping threshold columns from observations")
        observations = observations.loc[
            :, ~observations.columns.str.startswith("thresholds_")
        ]

        logging.info("Calculating minimum temperature severity")
        observations["minimum_temperature_severity"] = observations.apply(
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

        logging.info("Calculating maximum temperature severity")
        observations["maximum_temperature_severity"] = observations.apply(
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

        logging.info("Calculating uniform precipitation 1h severity")
        observations["uniform_precipitation_1h_severity"] = observations.apply(
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

        logging.info("Calculating severe precipitation 1h severity")
        observations["severe_precipitation_1h_severity"] = observations.apply(
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

        logging.info("Calculating extreme precipitation 1h severity")
        observations["extreme_precipitation_1h_severity"] = observations.apply(
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

        logging.info("Calculating uniform precipitation 12h severity")
        observations["uniform_precipitation_12h_severity"] = observations.apply(
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

        logging.info("Calculating severe precipitation 12h severity")
        observations["severe_precipitation_12h_severity"] = observations.apply(
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

        logging.info("Calculating extreme precipitation 12h severity")
        observations["extreme_precipitation_12h_severity"] = observations.apply(
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

        logging.info("Calculating snowfall 24h severity")
        observations["snowfall_24h_severity"] = observations.apply(
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
        observations["wind_speed_severity"] = observations.apply(
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

        self.__DATAFRAME_OBSERVED_DATA__ = observations[
            self.__FIELDS_COMBINED_RESULTS__
        ]

    def __extend_warning_data__(self):
        """
        Extends the warnings DataFrame to include warnings for distinct parameters.

        The original warnings DataFrame contains warnings for "PR_1H" and "PR_12H" parameters.
        This method extends the warnings DataFrame to include warnings for distinct parameters
        that start with "PR_1H." and "PR_12H." prefixes.

        The method first copies the original warnings DataFrame and maps the "severity" column
        to numeric values using the commons.mapping_severity_values dictionary.

        Then, it creates DataFrames for "PR_1H" and "PR_12H" parameters and repeats the rows
        for each distinct parameter, assigning the new parameter ID to each repeated row.

        Finally, it concatenates the original warnings DataFrame with the repeated DataFrames
        and filters out the "PR_1H" and "PR_12H" parameters from the resulting DataFrame.

        The resulting DataFrame contains warnings for all distinct parameters, including the
        original "PR_1H" and "PR_12H" parameters.

        Returns
        -------
        None
        """
        extended = self.__DATAFRAME_WARNINGS__.copy()
        extended.loc[:, "geocode"] = extended["geocode"].astype(str)
        extended.loc[:, "severity"] = extended["severity"].map(
            event_data_commons.MAPPING_SEVERITY_VALUE
        )
        extended.loc[:, "param_name"] = extended.loc[:, "param_name"].fillna("")

        precipitation_1h = extended[extended["param_id"] == "PR_1H"]
        precipitation_12h = extended[extended["param_id"] == "PR_12H"]

        distinct_params = {
            key
            for key in event_data_commons.MAPPING_PARAMETERS.keys()
            if key.startswith("PR_1H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_1h.copy()
            repeating_rows["param_id"] = param
            extended = pd.concat([extended, repeating_rows], ignore_index=True)

        distinct_params = {
            key
            for key in event_data_commons.MAPPING_PARAMETERS.keys()
            if key.startswith("PR_12H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_12h.copy()
            repeating_rows["param_id"] = param
            extended = pd.concat([extended, repeating_rows], ignore_index=True)

        extended = extended[extended["param_id"] != "PR_1H"]
        extended = extended[extended["param_id"] != "PR_12H"]

        self.__DATAFRAME_WARNINGS_EXTENDED__ = extended

    def __discretize_observed_data__(self):
        """
        Discretizes the observations DataFrame into distinct warnings.

        This method takes the observations DataFrame and discretizes it into distinct
        warnings for each parameter. The resulting DataFrame contains the date, idema,
        name, geocode, province, latitude, longitude, altitude, parameter ID, station
        severity, and station value for each distinct warning.

        The method first creates a new DataFrame with the columns mentioned above. Then,
        it iterates over each parameter and copies the relevant columns from the
        observations DataFrame to the new DataFrame. The parameter ID is added to each
        row and the columns are renamed accordingly.

        Finally, the method merges the discretized DataFrame with the extended warnings
        DataFrame and creates a new DataFrame for data. The DataFrame
        contains the date, geocode, region, area, province, polygon, idema, name,
        latitude, longitude, altitude, parameter ID, parameter name, predicted severity,
        predicted value, region severity, region value, observed severity, and observed
        value for each distinct warning.

        Returns
        -------
        None
        """
        discretized = pd.DataFrame(
            columns=[
                "date",
                "idema",
                "name",
                "geocode",
                "province",
                "latitude",
                "longitude",
                "altitude",
                "param_id",
                "station_severity",
                "station_value",
            ]
        )

        for p in list(event_data_commons.MAPPING_PARAMETERS.keys()):
            if p != "PR" and p != "PR_1H" and p != "PR_12H":
                value_column = event_data_commons.MAPPING_PARAMETERS[p]["id"]
                severity_column = (
                    event_data_commons.MAPPING_PARAMETERS[p]["id"] + "_severity"
                )
                new_rows = self.__DATAFRAME_OBSERVED_DATA__[
                    [
                        "date",
                        "idema",
                        "name",
                        "geocode",
                        "province",
                        "latitude",
                        "longitude",
                        "altitude",
                        severity_column,
                        value_column,
                    ]
                ]
                new_rows.loc[:, "param_id"] = p
                new_rows = new_rows.rename(
                    columns={
                        value_column: "station_value",
                        severity_column: "station_severity",
                    }
                )
                discretized = pd.concat([discretized, new_rows], ignore_index=True)

        merged_df = pd.merge(
            discretized,
            self.__DATAFRAME_WARNINGS_EXTENDED__,
            how="left",
            left_on=["date", "geocode", "param_id"],
            right_on=["effective", "geocode", "param_id"],
            suffixes=("_warn", "_obs"),
        )

        df = pd.DataFrame(columns=self.__FIELDS_PROCESSED_DATA__)
        df["date"] = merged_df["date"].combine_first(merged_df["effective"])
        df["geocode"] = merged_df["geocode"]
        df["region"] = ""
        df["area"] = ""
        df["province"] = merged_df["province"]
        df["polygon"] = ""
        df["idema"] = merged_df["idema"]
        df["name"] = merged_df["name"]
        df["latitude"] = merged_df["latitude"]
        df["longitude"] = merged_df["longitude"]
        df["altitude"] = merged_df["altitude"]
        df["param_id"] = merged_df["param_id"]
        df["param_name"] = ""
        df["predicted_severity"] = merged_df["severity"]
        df["predicted_value"] = merged_df["param_value"]
        df["region_severity"] = 0
        df["region_value"] = np.nan
        df["observed_severity"] = merged_df["station_severity"]
        df["observed_value"] = merged_df["station_value"]

        self.__DATAFRAME_PREPARED_DATA__ = df

    def __complete_empty_fields__(self):
        """
        Complete the data DataFrame with region, area, province and polygon data

        This method merges the data DataFrame with the geocodes DataFrame, and
        completes the 'region', 'area', 'province' and 'polygon' columns.

        The resulting DataFrame has the same columns as the original, but with
        the 'region', 'area', 'province' and 'polygon' columns completed.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        merged_df = pd.merge(
            self.__DATAFRAME_PREPARED_DATA__,
            self.__DATAFRAME_GEOCODES__,
            how="left",
            left_on=["geocode"],
            right_on=["geocode"],
        )

        merged_df["region"] = merged_df["region_y"].combine_first(merged_df["region_x"])
        merged_df["area"] = merged_df["area_y"].combine_first(merged_df["area_x"])
        merged_df["province"] = merged_df["province_y"].combine_first(
            merged_df["province_x"]
        )
        merged_df["polygon"] = merged_df["polygon_y"].combine_first(
            merged_df["polygon_x"]
        )

        self.__DATAFRAME_PREPARED_DATA__ = merged_df[self.__FIELDS_PROCESSED_DATA__]

    def __summarize_observed_data__(self):
        """
        Summarizes the data by computing the maximum severity and
        maximum/minimum value for each parameter and geocode.

        This method groups the DataFrame by date, param_id and geocode,
        and computes the maximum severity and the maximum/minimum value for each
        parameter. The results are merged with the original DataFrame, replacing
        the original region_severity and region_value columns.

        The resulting DataFrame has the same columns as the original, but with
        the region_severity and region_value columns replaced with the maximum
        severity and maximum/minimum value for each parameter and geocode.

        The maximum/minimum value is computed as follows:

        - For parameter 'BT', the minimum value is selected.
        - For other parameters, the maximum value is selected.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        situations_desc = self.__DATAFRAME_PREPARED_DATA__[
            self.__DATAFRAME_PREPARED_DATA__["param_id"] != "BT"
        ]
        situations_asc = self.__DATAFRAME_PREPARED_DATA__[
            self.__DATAFRAME_PREPARED_DATA__["param_id"] == "BT"
        ]

        situations_desc = situations_desc.sort_values(
            by=["observed_severity", "observed_value"], ascending=[False, False]
        )
        situations_asc = situations_asc.sort_values(
            by=["observed_severity", "observed_value"], ascending=[False, True]
        )

        situations_desc = (
            situations_desc.groupby(["date", "param_id", "geocode"])
            .agg(
                region_severity=("observed_severity", "first"),
                region_value=("observed_value", "first"),
            )
            .reset_index()
        )
        situations_asc = (
            situations_asc.groupby(["date", "param_id", "geocode"])
            .agg(
                region_severity=("observed_severity", "first"),
                region_value=("observed_value", "first"),
            )
            .reset_index()
        )

        situations = pd.concat([situations_desc, situations_asc], ignore_index=True)
        df = self.__DATAFRAME_PREPARED_DATA__
        df = df.drop(["region_severity", "region_value"], axis=1)

        df = pd.merge(df, situations, how="left", on=["date", "param_id", "geocode"])

        df.loc[:, "predicted_severity"] = df.loc[:, "predicted_severity"].fillna(0)
        df.loc[:, "region_severity"] = df.loc[:, "region_severity"].fillna(0)
        df.loc[:, "observed_severity"] = df.loc[:, "observed_severity"].fillna(0)
        df.loc[:, "predicted_severity"] = df["predicted_severity"].astype(int)
        df.loc[:, "region_severity"] = df["region_severity"].astype(int)
        df.loc[:, "observed_severity"] = df["observed_severity"].astype(int)

        df.loc[:, "param_name"] = df["param_id"].map(
            event_data_commons.MAPPING_PARAMETER_DESCRIPTION
        )

        self.__DATAFRAME_PREPARED_DATA__ = df

    def save_prepared_data(self):
        """
        Saves the data results to different files.

        The results are saved to the following files:

        - results: The observed data.
        - predictions: The predicted warnings.
        - situations: The real situations.

        The files are saved in a directory specified by the event ID.
        """
        df = self.__DATAFRAME_PREPARED_DATA__[
            [
                "date",
                "geocode",
                "region",
                "area",
                "province",
                "param_name",
                "predicted_severity",
            ]
        ]
        df = df[df["predicted_severity"] > 0]
        df["predicted_severity"] = df["predicted_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        df = df.drop_duplicates()
        df.to_csv(
            event_data_commons.get_path_to_file(
                "event_predicted_warnings", self.__EVENT_ID__
            ),
            sep="\t",
        )

        df = self.__DATAFRAME_PREPARED_DATA__[
            [
                "date",
                "geocode",
                "region",
                "area",
                "province",
                "param_name",
                "region_severity",
                "region_value",
            ]
        ]
        df = df[df["region_severity"] > 0]
        df["region_severity"] = df["region_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        df = df.drop_duplicates()
        df.to_csv(
            event_data_commons.get_path_to_file(
                "event_region_warnings", self.__EVENT_ID__
            ),
            sep="\t",
        )

        df = self.__DATAFRAME_PREPARED_DATA__[
            [
                "date",
                "geocode",
                "region",
                "area",
                "province",
                "param_name",
                "predicted_severity",
                "observed_severity",
                "observed_value",
            ]
        ]
        df = df[(df["predicted_severity"] > 0) | (df["observed_severity"] > 0)]
        df["predicted_severity"] = df["predicted_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        df["observed_severity"] = df["observed_severity"].map(
            event_data_commons.MAPPING_SEVERITY_TEXT
        )
        df = df.drop_duplicates()
        df.to_csv(
            event_data_commons.get_path_to_file(
                "event_resulting_data", self.__EVENT_ID__
            ),
            sep="\t",
        )

        self.__DATAFRAME_PREPARED_DATA__.to_csv(
            event_data_commons.get_path_to_file(
                "event_prepared_data", self.__EVENT_ID__
            ),
            columns=self.__DATAFRAME_PREPARED_DATA__.columns,
            sep="\t",
        )
