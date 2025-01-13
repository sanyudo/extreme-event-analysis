import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from shapely import Point, Polygon
import aemet_client
import commons
import constants
import logging


class EventAnalysis:

    __EVENT_ID = ""
    __EVENT_NAME = ""
    __EVENT_START = datetime.now()
    __EVENT_END = datetime.now()

    __COLUMNS_FOR_THRESHOLDS_ANALYSIS = [
        "geocode",
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

    __COLUMNS_FOR_STATIONS_ANALYSIS = [
        "idema",
        "name",
        "province",
        "latitude",
        "longitude",
        "altitude",
        "geocode",
    ]

    __COLUMNS_FOR_RESULTS_ANALYSIS = [
        "date",
        "idema",
        "min_temperature",
        "min_temperature_warning",
        "max_temperature",
        "max_temperature_warning",
        "precipitation",
        "uniform_precipitation_1h",
        "uniform_precipitation_1h_warning",
        "severe_precipitation_1h",
        "severe_precipitation_1h_warning",
        "uniform_precipitation_12h",
        "uniform_precipitation_12h_warning",
        "severe_precipitation_12h",
        "severe_precipitation_12h_warning",
        "snowfall_24h",
        "snowfall_24h_warning",
        "max_wind_speed",
        "max_wind_speed_warning",
    ]

    __SEVERE_PRECIPITATION_BY_TIMEFRAME = {1: 0.2, 12: 0.8}

    __ANALISYS_OBSERVATIONS = pd.DataFrame()
    __ANALISYS_WARNINGS = pd.DataFrame()
    __ANALISYS_STATIONS = pd.DataFrame()

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        self.__EVENT_ID = event_id
        self.__EVENT_NAME = event_name
        self.__EVENT_START = event_start
        self.__EVENT_END = event_end

    def get_event_name(self) -> str:
        return self.__EVENT_NAME

    def get_event_id(self) -> str:
        return self.__EVENT_ID

    def get_event_start(self) -> datetime:
        return self.__EVENT_START

    def get_event_end(self) -> datetime:
        return self.__EVENT_END

    def get_event_duration(self) -> timedelta:
        return self.get_event_end() - self.get_event_start()

    def fetch_data(self):
        commons.ensure_directories(
            self.get_event_id(), self.get_event_start(), self.get_event_end())

        logging.info(f"Checking for stations inventory file ...")
        if not commons.exists_data_stations():
            logging.info(f"... stations inventory file not found. Downloading")
            aemet_client.fetch_stations()

        logging.info(f"Checking for warnings file ...")
        if not commons.exists_data_warnings(self.get_event_id()):
            logging.info(f"... warnings file file not found. Checking for caps ...")
            if not commons.exists_files_caps(self.get_event_id()):
                logging.info(f"... caps not found. Downloading")
                aemet_client.fetch_caps(self.get_event_id(), self.get_event_start(), self.get_event_end())
            commons.convert_caps_to_warnings(self.get_event_id())

        logging.info(f"Checking for observations file ...")
        if not commons.exists_data_observations(self.get_event_id()):
            logging.info(f"... observations file not found. Downloading")
            aemet_client.fetch_observations(self.get_event_id(), self.get_event_start(), self.get_event_end())            

    def load_data(self):
        logging.info(f"Loading stations inventory ...")
        stations = commons.retrieve_data_stations()

        logging.info(f"Loading thresholds list ...")
        thresholds = commons.retrieve_data_thresholds()

        logging.info(f"Loading warnings data ...")
        warnings = commons.retrieve_data_warnings(self.get_event_id())

        logging.info("Filtering stations by regions affected ...")
        stations = self.__filter_stations(
            stations,
            warnings[["geocode", "polygon"]].drop_duplicates()
        )
  
        logging.info(f"Loading observations data ...")
        observations = commons.retrieve_data_observations(self.get_event_id(), stations["idema"].tolist())

        self.__ANALISYS_OBSERVATIONS = observations
        self.__ANALISYS_STATIONS = stations
        self.__ANALISYS_WARNINGS = warnings

        commons.clean_raw(self.get_event_id())

    def write_data(self):
        path = f"./results/{self.get_event_id()}/"
        self.__ANALISYS_OBSERVATIONS.to_csv(os.path.join(path, "observed_data.tsv"), sep="\t")
        self.__ANALISYS_STATIONS.to_csv(os.path.join(path, "filtered_stations.tsv"), sep="\t")
        self.__ANALISYS_WARNINGS.to_csv(os.path.join(path, "predicted_warnings.tsv"), sep="\t")

    def __filter_stations(
        self, stations: pd.DataFrame, areas: pd.DataFrame) -> pd.DataFrame:
        areas["area"] = areas["polygon"].apply(
            lambda coordinates: Polygon(
                [tuple(map(float, pair.split(","))) for pair in coordinates.split()]
            )
        )

        stations["geocode"] = None
        stations["point"] = stations.apply(
            lambda x: Point(x["latitude"], x["longitude"]), axis=1
        )

        for i, station in stations.iterrows():
            for _, area in areas.iterrows():
                if area["area"].contains(station["point"]):
                    stations.at[i, "geocode"] = areas["geocode"]

        stations = stations[stations["geocode"].notnull()]
        stations.drop(columns=["point"], inplace=True)
        return stations[self.__COLUMNS_FOR_STATIONS_ANALYSIS]