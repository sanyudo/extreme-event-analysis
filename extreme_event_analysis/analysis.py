import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from shapely import Point, Polygon
import extreme_event_analysis.aemet as aemet
import commons
import constants
import logging


class EventAnalysis:

    __EVENT_ID = ""
    __EVENT_NAME = ""
    __EVENT_START = datetime.now()
    __EVENT_END = datetime.now()

    __DF_OBSERVATIONS = pd.DataFrame()
    __DF_WARNINGS = pd.DataFrame()
    __DF_STATIONS = pd.DataFrame()
    __DF_THRESHOLDS = pd.DataFrame()
    __DF_GEOCODES = pd.DataFrame()

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        self.set_event_id(event_id)
        self.set_event_name(event_name)
        self.set_event_start(event_start)
        self.set_event_end(event_end)

    def get_event_name(self) -> str:
        return self.__EVENT_NAME

    def get_event_id(self) -> str:
        return self.__EVENT_ID

    def get_event_start(self) -> datetime:
        return self.__EVENT_START
    
    def get_warnings_start(self) -> datetime:
        warnings = self.get_warnings().copy()
        warnings["severity_mapped"] = warnings["severity"].map(constants.mapping_severity())
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].min()
        else:
            return self.__EVENT_START    

    def get_event_end(self) -> datetime:
        return self.__EVENT_END
    
    def get_warnings_end(self) -> datetime:
        warnings = self.get_warnings().copy()
        warnings["severity_mapped"] = warnings["severity"].map(constants.mapping_severity())
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].max()
        else:
            return self.__EVENT_END       

    def set_event_name(self, event_name: str):
        self.__EVENT_NAME = event_name

    def set_event_id(self, event_id: str):
        self.__EVENT_ID = event_id

    def set_event_start(self, event_start: datetime):
        self.__EVENT_START = event_start

    def set_event_end(self, event_end: datetime):
        self.__EVENT_END = event_end

    def get_event_duration(self) -> timedelta:
        return self.get_event_end() - self.get_event_start()

    def get_stations(self) -> pd.DataFrame:
        return self.__DF_STATIONS
       
    def get_warnings(self) -> pd.DataFrame:
        return self.__DF_WARNINGS

    def get_thresholds(self) -> pd.DataFrame:
        return self.__DF_THRESHOLDS
    
    def get_geocodes(self) -> pd.DataFrame:
        return self.__DF_GEOCODES

    def get_observations(self) -> pd.DataFrame:
        return self.__DF_OBSERVATIONS

    def set_stations(self, stations: pd.DataFrame):
        self.__DF_STATIONS = stations  
    
    def set_warnings(self, warnings: pd.DataFrame):
        self.__DF_WARNINGS = warnings

    def set_thresholds(self, thresholds: pd.DataFrame):
        self.__DF_THRESHOLDS = thresholds

    def set_geocodes(self, geocodes: pd.DataFrame):
        self.__DF_GEOCODES = geocodes        

    def set_observations(self, observations: pd.DataFrame):
        self.__DF_OBSERVATIONS = observations

    def fetch_data(self):
        commons.ensure_directories(
            event=(self.get_event_id()), start=(self.get_event_start()), end=(self.get_event_end()))

        logging.info(f"Checking for stations inventory file ...")
        if not commons.exists_data_stations():
            logging.info(f"... stations inventory file not found. Downloading")
            aemet.fetch_stations()

        logging.info(f"Checking for warnings file ...")
        if not commons.exists_data_warnings(event=(self.get_event_id())):
            logging.info(f"... warnings file file not found. Checking for caps ...")
            if not commons.exists_files_caps(event=(self.get_event_id())):
                logging.info(f"... caps not found. Downloading")
                aemet.fetch_caps(event=(self.get_event_id()), start=(self.get_event_start()), end=(self.get_event_end()))
            commons.convert_caps_to_warnings(event=(self.get_event_id()))

        logging.info(f"Checking for observations file ...")
        if not commons.exists_data_observations(event=(self.get_event_id())):
            logging.info(f"... observations file not found. Downloading")
            aemet.fetch_observations(event=(self.get_event_id()), start=(self.get_warnings_start()), end=(self.get_warnings_end()))            

    def load_data(self):
        logging.info(f"Loading stations inventory ...")
        self.set_stations(stations=commons.retrieve_data_stations())

        logging.info(f"Loading thresholds list ...")
        self.set_thresholds(thresholds=commons.retrieve_data_thresholds())

        logging.info(f"Loading thresholds list ...")
        self.set_geocodes(geocodes=commons.retrieve_data_geocodes())        

        logging.info(f"Loading warnings data ...")
        self.set_warnings(warnings=commons.retrieve_data_warnings(event=self.get_event_id()))

        logging.info(f"Loading observations data ...")
        self.set_observations(observations=commons.retrieve_data_observations(event=self.get_event_id(), stations=self.get_stations()["idema"].tolist()))

        commons.clean_raw(event=self.get_event_id())

    def prepare_analysis(self):
        logging.info("Starting analysis preparation")
        logging.info("Locating stations")
        self.__locate_stations()
        logging.info("Preparing observations data")
        self.__prepare_observations()
        logging.info("Analysis preparation completed")

    def __locate_stations(self):
        logging.info("Preparing stations for comparison")

        geocodes = self.get_geocodes().copy()
        geocodes["geocode"] = geocodes["geocode"].astype(str)        
        geocodes["area"] = geocodes["polygon"].apply(
            lambda coordinates: Polygon(
            [tuple(map(float, pair.split(","))) for pair in coordinates.split()]
            )
        )

        stations = self.get_stations().copy()
        stations["geocode"] = None
        stations["point"] = stations.apply(
            lambda x: Point(x["latitude"], x["longitude"]), axis=1
        )
        stations["geocode"] = stations.apply(
            lambda station: next(
                (a["geocode"] for _, a in geocodes.iterrows() if a["area"].contains(station["point"])),
                None
            ),
            axis=1
        )

        stations["geocode"] = stations["geocode"].astype(str)
        stations.drop(columns=["point"], inplace=True)
        self.set_stations(stations[constants.columns_analysis_stations()])        

    def __prepare_observations(self):
        logging.info("Preparing observed data for comparison")
        thresholds = self.get_thresholds()
        thresholds["geocode"] = thresholds["geocode"].astype(str)

        stations = self.get_stations()
        stations["geocode"] = stations["geocode"].astype(str)
        observations = self.get_observations()
        observations["date"] = pd.to_datetime(observations["date"])

        logging.info("Merging observed data with thresholds data")
        observations = pd.merge(
            observations,
            stations,
            on="idema"
        )

        logging.info("Merging observed data with thresholds data")
        observations = pd.merge(
            observations,
            thresholds,
            on="geocode",
            suffixes=("", "")
        )

        logging.info("Calculating minimum temperature severity")
        observations["minimum_temperature_severity"] = observations.apply(
            lambda row: 1 if row["minimum_temperature"] <= row["minimum_temperature_yellow_warning"] and row["minimum_temperature"] > row["minimum_temperature_orange_warning"]
            else 2 if row["minimum_temperature"] <= row["minimum_temperature_orange_warning"] and row["minimum_temperature"] > row["minimum_temperature_red_warning"]
            else 3 if row["minimum_temperature"] <= row["minimum_temperature_red_warning"]
            else 0,
            axis=1
        )

        logging.info("Calculating maximum temperature severity")
        observations["maximum_temperature_severity"] = observations.apply(
            lambda row: 1 if row["maximum_temperature"] >= row["maximum_temperature_yellow_warning"] and row["maximum_temperature"] < row["maximum_temperature_orange_warning"]
            else 2 if row["maximum_temperature"] >= row["maximum_temperature_orange_warning"] and row["maximum_temperature"] < row["maximum_temperature_red_warning"]
            else 3 if row["maximum_temperature"] >= row["maximum_temperature_red_warning"]
            else 0,
            axis=1
        ) 

        logging.info("Calculating uniform precipitation 1h severity")
        observations["uniform_precipitation_1h_severity"] = observations.apply(
            lambda row: 1 if row["uniform_precipitation_1h"] >= row["precipitation_1h_yellow_warning"] and row["uniform_precipitation_1h"] < row["precipitation_1h_orange_warning"]
            else 2 if row["uniform_precipitation_1h"] >= row["precipitation_1h_orange_warning"] and row["uniform_precipitation_1h"] < row["precipitation_1h_red_warning"]
            else 3 if row["uniform_precipitation_1h"] >= row["precipitation_1h_red_warning"]
            else 0,
            axis=1
        )        

        logging.info("Calculating severe precipitation 1h severity")
        observations["severe_precipitation_1h_severity"] = observations.apply(
            lambda row: 1 if row["severe_precipitation_1h"] >= row["precipitation_1h_yellow_warning"] and row["severe_precipitation_1h"] < row["precipitation_1h_orange_warning"]
            else 2 if row["severe_precipitation_1h"] >= row["precipitation_1h_orange_warning"] and row["severe_precipitation_1h"] < row["precipitation_1h_red_warning"]
            else 3 if row["severe_precipitation_1h"] >= row["precipitation_1h_red_warning"]
            else 0,
            axis=1
        )     

        logging.info("Calculating uniform precipitation 12h severity")
        observations["uniform_precipitation_12h_severity"] = observations.apply(
            lambda row: 1 if row["uniform_precipitation_12h"] >= row["precipitation_12h_yellow_warning"] and row["uniform_precipitation_12h"] < row["precipitation_12h_orange_warning"]
            else 2 if row["uniform_precipitation_12h"] >= row["precipitation_12h_orange_warning"] and row["uniform_precipitation_12h"] < row["precipitation_12h_red_warning"]
            else 3 if row["uniform_precipitation_12h"] >= row["precipitation_12h_red_warning"]
            else 0,
            axis=1
        )        

        logging.info("Calculating severe precipitation 12h severity")
        observations["severe_precipitation_12h_severity"] = observations.apply(
            lambda row: 1 if row["severe_precipitation_12h"] >= row["precipitation_12h_yellow_warning"] and row["severe_precipitation_12h"] < row["precipitation_12h_orange_warning"]
            else 2 if row["severe_precipitation_12h"] >= row["precipitation_12h_orange_warning"] and row["severe_precipitation_12h"] < row["precipitation_12h_red_warning"]
            else 3 if row["severe_precipitation_12h"] >= row["precipitation_12h_red_warning"]
            else 0,
            axis=1
        )      

        logging.info("Calculating snowfall 24h severity")
        observations["snowfall_24h_severity"] = observations.apply(
            lambda row: 1 if row["snowfall_24h"] >= row["snowfall_24h_yellow_warning"] and row["snowfall_24h"] < row["snowfall_24h_orange_warning"]
            else 2 if row["snowfall_24h"] >= row["snowfall_24h_orange_warning"] and row["snowfall_24h"] < row["snowfall_24h_red_warning"]
            else 3 if row["snowfall_24h"] >= row["snowfall_24h_red_warning"]
            else 0,
            axis=1
        ) 

        logging.info("Calculating maximum wind speed severity")
        observations["wind_speed_severity"] = observations.apply(
            lambda row: 1 if row["wind_speed"] >= row["wind_speed_yellow_warning"] and row["wind_speed"] < row["wind_speed_orange_warning"]
            else 2 if row["wind_speed"] >= row["wind_speed_orange_warning"] and row["wind_speed"] < row["wind_speed_red_warning"]
            else 3 if row["wind_speed"] >= row["wind_speed_red_warning"]
            else 0,
            axis=1
        )           

        self.set_observations(observations[constants.columns_analysis_observations()])

    def write_data(self):
        path = f"P:\\TFM/results/{self.get_event_id()}/"
        os.makedirs(path)
        self.get_observations().to_csv(os.path.join(path, "observed_data.tsv"), sep="\t")
        self.get_warnings().to_csv(os.path.join(path, "predicted_warnings.tsv"), sep="\t")

