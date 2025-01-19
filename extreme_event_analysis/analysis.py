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
    __df_extended_warnings = pd.DataFrame(columns=constants.columns_warnings)
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
            logging.info(f"... stations updated.")

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
        stations = common.get_geolocated_stations()
        self.__df_stations = stations

        logging.info(f"Loading thresholds list ...")
        thresholds = common.get_thresholds()
        self.__df_thresholds = thresholds

        logging.info(f"Loading thresholds list ...")
        geocodes = common.get_geocodes()
        self.__df_geocodes = geocodes

        logging.info(f"Loading warnings data ...")
        warnings = common.get_warnings(event=self.__event_id)
        self.__df_warnings = warnings

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
        observations = common.get_observations(
            event=self.__event_id, stations=self.__df_stations["idema"].tolist()
        )
        self.__df_observations = observations

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
        self.__geolocate_observations_data()
        logging.info("Composing definitive observations data")
        self.__evaluate_observations_severity()
        logging.info("Extending warning data with estimation ids")
        self.__extend_warnings_parameters()
        logging.info("Combine warnings and observations")
        self.__discretize_observations_warnings()
        logging.info("Complete analysis data")
        self.__complete_analysis_data()           
        logging.info("Summarize observations by region")
        self.__summarize_observations_analysis()    
        logging.info("Analysis preparation completed")

    def __geolocate_observations_data(self):
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
            self.__df_observations,
            self.__df_stations,
            on="idema",
            suffixes=("", "stations_"),
        )

        merged_observations = pd.merge(
            merged_observations,
            self.__df_geocodes,
            on="geocode",
            suffixes=("", "geocode_"),
        )

        logging.info("Reordering and initializing missing columns in observations")
        for col in self.__columns_results:
            if col not in merged_observations.columns:
                merged_observations[col] = np.nan
        self.__df_observations = merged_observations[self.__columns_results]

    def __evaluate_observations_severity(self):
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
        observations = self.__df_observations.copy()
        observations["geocode"] = observations["geocode"].astype(str)

        thresholds = self.__df_thresholds.copy()
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

        self.__df_observations = observations[self.__columns_results]

    def __extend_warnings_parameters(self):
        """
        Extends the warnings DataFrame to include warnings for distinct parameters.

        The original warnings DataFrame contains warnings for "PR_1H" and "PR_12H" parameters.
        This method extends the warnings DataFrame to include warnings for distinct parameters
        that start with "PR_1H." and "PR_12H." prefixes.

        The method first copies the original warnings DataFrame and maps the "severity" column
        to numeric values using the constants.mapping_severity_values dictionary.

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
        extended = self.__df_warnings.copy()
        extended["geocode"] = extended["geocode"].astype(str)
        extended["severity"] = extended["severity"].map(
            constants.mapping_severity_values
        )
        extended["param_name"].fillna("", inplace=True)

        precipitation_1h = extended[extended["param_id"] == "PR_1H"]
        precipitation_12h = extended[extended["param_id"] == "PR_12H"]

        distinct_params = {
            key
            for key in constants.mapping_parameters.keys()
            if key.startswith("PR_1H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_1h.copy()
            repeating_rows["param_id"] = param
            extended = pd.concat([extended, repeating_rows], ignore_index=True)

        distinct_params = {
            key
            for key in constants.mapping_parameters.keys()
            if key.startswith("PR_12H.")
        }
        for param in distinct_params:
            repeating_rows = precipitation_12h.copy()
            repeating_rows["param_id"] = param
            extended = pd.concat([extended, repeating_rows], ignore_index=True)

        extended = extended[extended["param_id"] != "PR_1H"]
        extended = extended[extended["param_id"] != "PR_12H"]

        self.__df_extended_warnings = extended

    def __discretize_observations_warnings(self):
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
        DataFrame and creates a new DataFrame for analysis. The analysis DataFrame
        contains the date, geocode, region, area, province, polygon, idema, name,
        latitude, longitude, altitude, parameter ID, parameter name, predicted severity,
        predicted value, region severity, region value, observed severity, and observed
        value for each distinct warning.

        The resulting analysis DataFrame is stored in the self.__df_analysis attribute.

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

        for p in list(constants.mapping_parameters.keys()):
            if p != "PR" and p != "PR_1H" and p != "PR_12H":
                value_column = constants.mapping_parameters[p]["id"]
                severity_column = constants.mapping_parameters[p]["id"] + "_severity"
                new_rows = self.__df_observations[
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
                new_rows["param_id"] = p
                new_rows = new_rows.rename(
                    columns={
                        value_column: "station_value",
                        severity_column: "station_severity",
                    }
                )
                discretized = pd.concat([discretized, new_rows], ignore_index=True)

        merged_df = pd.merge(
            discretized,
            self.__df_extended_warnings,
            how="left",
            left_on=["date", "geocode", "param_id"],
            right_on=["effective", "geocode", "param_id"],
            suffixes=("_warn", "_obs"),
        )

        analysis = pd.DataFrame(columns=self.__columns_analysis)
        analysis["date"] =  merged_df["date"].combine_first(merged_df["effective"])
        analysis["geocode"] = merged_df["geocode"]
        analysis["region"] = ""
        analysis["area"] = ""
        analysis["province"] = merged_df["province"]
        analysis["polygon"] = ""
        analysis["idema"] = merged_df["idema"]
        analysis["name"] = merged_df["name"]
        analysis["latitude"] = merged_df["latitude"]
        analysis["longitude"] = merged_df["longitude"]
        analysis["altitude"] = merged_df["altitude"]
        analysis["param_id"] = merged_df["param_id"]
        analysis["param_name"] = ""
        analysis["predicted_severity"] = merged_df["severity"]
        analysis["predicted_value"]  = merged_df["param_value"]
        analysis["region_severity"] = 0
        analysis["region_value"] = np.nan
        analysis["observed_severity"] = merged_df["station_severity"]
        analysis["observed_value"] = merged_df["station_value"]

        self.__df_analysis = analysis

    def __complete_analysis_data(self):
        """
        Complete the analysis DataFrame with region, area, province and polygon data

        This method merges the analysis DataFrame with the geocodes DataFrame, and
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
            self.__df_analysis,
            self.__df_geocodes,
            how="left",
            left_on=["geocode"],
            right_on=["geocode"]
        )

        merged_df["region"] = merged_df["region_y"].combine_first(merged_df["region_x"])
        merged_df["area"] = merged_df["area_y"].combine_first(merged_df["area_x"])
        merged_df["province"] = merged_df["province_y"].combine_first(merged_df["province_x"])
        merged_df["polygon"] = merged_df["polygon_y"].combine_first(merged_df["polygon_x"])        
        
        self.__df_analysis = merged_df[self.__columns_analysis]

    def __summarize_observations_analysis(self):
        """
        Summarizes the analysis DataFrame by computing the maximum severity and
        maximum/minimum value for each parameter and geocode.

        This method groups the analysis DataFrame by date, param_id and geocode,
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
        situations_desc = self.__df_analysis[self.__df_analysis["param_id"] != "BT"]
        situations_asc = self.__df_analysis[self.__df_analysis["param_id"] == "BT"]

        situations_desc.sort_values(by=["observed_severity", "observed_value"], ascending=[False, False], inplace=True)
        situations_asc.sort_values(by=["observed_severity", "observed_value"], ascending=[False, True], inplace=True)        
        
        situations_desc = situations_desc.groupby(["date", "param_id", "geocode"]).agg(
            region_severity=("observed_severity", "first"),
            region_value=("observed_value", "first")).reset_index()
        situations_asc = situations_asc.groupby(["date", "param_id", "geocode"]).agg(
            region_severity=("observed_severity", "first"),
            region_value=("observed_value", "first")).reset_index()
        
        situations = pd.concat([situations_desc, situations_asc], ignore_index=True)
        analysis = self.__df_analysis
        analysis.drop(["region_severity", "region_value"], axis=1, inplace=True)

        analysis = pd.merge(analysis, situations, 
                            how="left", 
                            on=["date", "param_id", "geocode"])

        analysis["predicted_severity"].fillna(0, inplace=True)
        analysis["region_severity"].fillna(0, inplace=True)
        analysis["observed_severity"].fillna(0, inplace=True)
        analysis["predicted_severity"] = analysis["predicted_severity"].astype(int)
        analysis["region_severity"] = analysis["region_severity"].astype(int)
        analysis["observed_severity"] = analysis["observed_severity"].astype(int)                

        analysis["param_name"] = analysis["param_id"].map(constants.mapping_parameters_description)

        self.__df_analysis = analysis

    def save_data(self):
        """
        Saves the analysis results to different files.

        The results are saved to the following files:

        - results: The observed data.
        - predictions: The predicted warnings.
        - situations: The real situations.

        The files are saved in a directory specified by the event ID.
        """
        df = self.__df_analysis[["date", "geocode", "region", "area", "province",  "param_name", "predicted_severity", "predicted_value"]]
        df["predicted_severity"] = df["predicted_severity"].map(constants.mapping_severity_values)
        df.to_csv(
            constants.get_path_to_file("predictions", self.__event_id), sep="\t"
        )

        df = self.__df_analysis[["date", "geocode", "region", "area", "province",  "param_name", "region_severity", "region_value"]]
        df["region_severity"] = df["region_severity"].map(constants.mapping_severity_values)
        df.to_csv(
            constants.get_path_to_file("region", self.__event_id), sep="\t"
        )

        df = self.__df_analysis[["date", "geocode", "region", "area", "province",  "param_name", "observed_severity", "observed_value"]]
        df["observed_severity"] = df["observed_severity"].map(constants.mapping_severity_values)
        df.to_csv(
            constants.get_path_to_file("results", self.__event_id), sep="\t"
        )

        self.__df_analysis.to_csv(
            constants.get_path_to_file("analysis", self.__event_id), sep="\t"
        )

    def analyze_data(self):
        pass

    def draw_maps(self):
        visuals.get_map(self.__event_id, self.__event_name, self.__df_analysis)
