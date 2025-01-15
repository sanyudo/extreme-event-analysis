import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime, timedelta
from shapely import Point, Polygon
import aemet_client
import common_tasks
import common_constants
import logging


class EventAnalysis:

    __event_id = ""
    __event_name = ""
    __event_start = datetime.now()
    __event_end = datetime.now()

    __df_observations = pd.DataFrame()
    __df_warnings = pd.DataFrame()
    __df_situations = pd.DataFrame()
    __df_stations = pd.DataFrame()
    __df_thresholds = pd.DataFrame()
    __df_geocodes = pd.DataFrame()

    def __init__(
        self, event_id: str, event_name: str, event_start: datetime, event_end: datetime
    ):
        self.__event_id = event_id
        self.__event_name = event_name
        self.__event_start = event_start
        self.__event_end = event_end

    def get_event_name(self) -> str:
        return self.__event_name

    def get_event_id(self) -> str:
        return self.__event_id

    def get_event_start(self) -> datetime:
        return self.__event_start
    
    def get_warnings_start(self) -> datetime:
        warnings = self.__df_warnings.copy()
        warnings["severity_mapped"] = warnings["severity"].map(common_constants.mapping_severity_values)
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].min()
        else:
            return self.__event_start    

    def get_event_end(self) -> datetime:
        return self.__event_end
    
    def get_warnings_end(self) -> datetime:
        warnings = self.__df_warnings.copy()
        warnings["severity_mapped"] = warnings["severity"].map(common_constants.mapping_severity_values)
        filtered_warnings = warnings[warnings["severity_mapped"] >= 1]
        if not filtered_warnings.empty:
            return filtered_warnings["effective"].max()
        else:
            return self.__event_end       

    def get_event_duration(self) -> timedelta:
        return self.__event_end - self.__event_start

    def fetch_warnings(self):
        common_tasks.ensure_directories(
            event=(self.__event_id), start=(self.__event_start), end=(self.__event_end))

        logging.info(f"Checking for warnings file ...")
        if not common_tasks.exists_data_warnings(event=(self.__event_id)):
            logging.info(f"... warnings file not found. Checking for caps ...")
            if not common_tasks.exists_files_caps(event=(self.__event_id)):
                logging.info(f"... caps not found. Downloading")
                aemet_client.fetch_caps(event=(self.__event_id), start=(self.__event_start), end=(self.__event_end))
            common_tasks.convert_caps_to_warnings(event=(self.__event_id))

        logging.info(f"Checking for geolocated stations file ...")
        if not common_tasks.exists_geolocated_stations():
            logging.info(f"... file not found.")
            common_tasks.geolocate_stations()
          
    def load_data(self):
        logging.info(f"Loading stations inventory ...")
        self.__df_stations = common_tasks.retrieve_geolocated_stations()
        
        logging.info(f"Loading thresholds list ...")
        self.__df_thresholds =common_tasks.retrieve_data_thresholds()

        logging.info(f"Loading thresholds list ...")
        self.__df_geocodes = common_tasks.retrieve_data_geocodes()

        logging.info(f"Loading warnings data ...")
        self.__df_warnings = common_tasks.retrieve_data_warnings(event=self.__event_id)

        common_tasks.clean_raw(event=self.__event_id)

    def fetch_observations(self):
        logging.info(f"Checking for observations file ...")
        if not common_tasks.exists_data_observations(event=(self.__event_id)):
            logging.info(f"... observations file not found. Downloading")
            aemet_client.fetch_observations(event=(self.__event_id), start=(self.get_warnings_start()), end=(self.get_warnings_end()))

        logging.info(f"Loading observations data ...")
        self.__df_observations = common_tasks.retrieve_data_observations(event=self.__event_id, stations=self.__df_stations["idema"].tolist())

    def prepare_analysis(self):
        logging.info("Starting analysis preparation")
        logging.info("Geolocating stations")
        self.__geolocate_observations()
        logging.info("Preparing observations data")
        self.__prepare_observations()
        logging.info("Generating real situations")
        self.__generate_situations()
        logging.info("Analysis preparation completed")

    def __geolocate_observations(self):
        logging.info("Preparing observations for comparison")
     
        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_stations,
            on="idema",
            suffixes=("", "stations_")
        )
     
        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_geocodes,
            on="geocode",
            suffixes=("", "geocode_")
        )            

        logging.info("Reordering and initializing missing columns in observations")
        for col in common_constants.analysis_columns_observations:
            if col not in self.__df_observations.columns:
                self.__df_observations[col] = np.nan
        self.__df_observations = self.__df_observations[common_constants.analysis_columns_observations]      

    def __prepare_observations(self):
        logging.info("Preparing observed data for comparison")
        logging.info("Converting geocodes")
        self.__df_observations["geocode"] = self.__df_observations["geocode"].astype(int)
        self.__df_observations["geocode"] = self.__df_observations["geocode"].astype(str)
        self.__df_thresholds["geocode"] = self.__df_thresholds["geocode"].astype(int)
        self.__df_thresholds["geocode"] = self.__df_thresholds["geocode"].astype(str)
        self.__df_observations["date"] = pd.to_datetime(self.__df_observations["date"])

        logging.info("Merging observed data with thresholds data")
        self.__df_observations = pd.merge(
            self.__df_observations,
            self.__df_thresholds,
            on="geocode",
            suffixes=("", "thresholds_")
        )

        logging.info("Dropping threshold columns from observations")
        self.__df_observations = self.__df_observations.loc[:, ~self.__df_observations.columns.str.startswith("thresholds_")]

        logging.info("Calculating minimum temperature severity")
        self.__df_observations["minimum_temperature_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["minimum_temperature"] <= row["minimum_temperature_yellow_warning"] and row["minimum_temperature"] > row["minimum_temperature_orange_warning"]
            else 2 if row["minimum_temperature"] <= row["minimum_temperature_orange_warning"] and row["minimum_temperature"] > row["minimum_temperature_red_warning"]
            else 3 if row["minimum_temperature"] <= row["minimum_temperature_red_warning"]
            else 0,
            axis=1
        )

        logging.info("Calculating maximum temperature severity")
        self.__df_observations["maximum_temperature_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["maximum_temperature"] >= row["maximum_temperature_yellow_warning"] and row["maximum_temperature"] < row["maximum_temperature_orange_warning"]
            else 2 if row["maximum_temperature"] >= row["maximum_temperature_orange_warning"] and row["maximum_temperature"] < row["maximum_temperature_red_warning"]
            else 3 if row["maximum_temperature"] >= row["maximum_temperature_red_warning"]
            else 0,
            axis=1
        ) 

        logging.info("Calculating uniform precipitation 1h severity")
        self.__df_observations["uniform_precipitation_1h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["uniform_precipitation_1h"] >= row["precipitation_1h_yellow_warning"] and row["uniform_precipitation_1h"] < row["precipitation_1h_orange_warning"]
            else 2 if row["uniform_precipitation_1h"] >= row["precipitation_1h_orange_warning"] and row["uniform_precipitation_1h"] < row["precipitation_1h_red_warning"]
            else 3 if row["uniform_precipitation_1h"] >= row["precipitation_1h_red_warning"]
            else 0,
            axis=1
        )        

        logging.info("Calculating severe precipitation 1h severity")
        self.__df_observations["severe_precipitation_1h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["severe_precipitation_1h"] >= row["precipitation_1h_yellow_warning"] and row["severe_precipitation_1h"] < row["precipitation_1h_orange_warning"]
            else 2 if row["severe_precipitation_1h"] >= row["precipitation_1h_orange_warning"] and row["severe_precipitation_1h"] < row["precipitation_1h_red_warning"]
            else 3 if row["severe_precipitation_1h"] >= row["precipitation_1h_red_warning"]
            else 0,
            axis=1
        )    

        logging.info("Calculating extreme precipitation 1h severity")
        self.__df_observations["extreme_precipitation_1h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["extreme_precipitation_1h"] >= row["precipitation_1h_yellow_warning"] and row["extreme_precipitation_1h"] < row["precipitation_1h_orange_warning"]
            else 2 if row["extreme_precipitation_1h"] >= row["precipitation_1h_orange_warning"] and row["extreme_precipitation_1h"] < row["precipitation_1h_red_warning"]
            else 3 if row["extreme_precipitation_1h"] >= row["precipitation_1h_red_warning"]
            else 0,
            axis=1
        )              

        logging.info("Calculating uniform precipitation 12h severity")
        self.__df_observations["uniform_precipitation_12h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["uniform_precipitation_12h"] >= row["precipitation_12h_yellow_warning"] and row["uniform_precipitation_12h"] < row["precipitation_12h_orange_warning"]
            else 2 if row["uniform_precipitation_12h"] >= row["precipitation_12h_orange_warning"] and row["uniform_precipitation_12h"] < row["precipitation_12h_red_warning"]
            else 3 if row["uniform_precipitation_12h"] >= row["precipitation_12h_red_warning"]
            else 0,
            axis=1
        )        

        logging.info("Calculating severe precipitation 12h severity")
        self.__df_observations["severe_precipitation_12h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["severe_precipitation_12h"] >= row["precipitation_12h_yellow_warning"] and row["severe_precipitation_12h"] < row["precipitation_12h_orange_warning"]
            else 2 if row["severe_precipitation_12h"] >= row["precipitation_12h_orange_warning"] and row["severe_precipitation_12h"] < row["precipitation_12h_red_warning"]
            else 3 if row["severe_precipitation_12h"] >= row["precipitation_12h_red_warning"]
            else 0,
            axis=1
        ) 

        logging.info("Calculating extreme precipitation 12h severity")
        self.__df_observations["extreme_precipitation_12h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["extreme_precipitation_12h"] >= row["precipitation_12h_yellow_warning"] and row["extreme_precipitation_12h"] < row["precipitation_12h_orange_warning"]
            else 2 if row["extreme_precipitation_12h"] >= row["precipitation_12h_orange_warning"] and row["extreme_precipitation_12h"] < row["precipitation_12h_red_warning"]
            else 3 if row["extreme_precipitation_12h"] >= row["precipitation_12h_red_warning"]
            else 0,
            axis=1
        )                 

        logging.info("Calculating snowfall 24h severity")
        self.__df_observations["snowfall_24h_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["snowfall_24h"] >= row["snowfall_24h_yellow_warning"] and row["snowfall_24h"] < row["snowfall_24h_orange_warning"]
            else 2 if row["snowfall_24h"] >= row["snowfall_24h_orange_warning"] and row["snowfall_24h"] < row["snowfall_24h_red_warning"]
            else 3 if row["snowfall_24h"] >= row["snowfall_24h_red_warning"]
            else 0,
            axis=1
        ) 

        logging.info("Calculating maximum wind speed severity")
        self.__df_observations["wind_speed_severity"] = self.__df_observations.apply(
            lambda row: 1 if row["wind_speed"] >= row["wind_speed_yellow_warning"] and row["wind_speed"] < row["wind_speed_orange_warning"]
            else 2 if row["wind_speed"] >= row["wind_speed_orange_warning"] and row["wind_speed"] < row["wind_speed_red_warning"]
            else 3 if row["wind_speed"] >= row["wind_speed_red_warning"]
            else 0,
            axis=1
        )           

        self.__df_observations = self.__df_observations[common_constants.analysis_columns_observations]

    def __generate_situations(self):
        self.__df_situations = pd.DataFrame(columns=common_constants.columns_warnings)
        mapping_values_to_severity = {v: k for k, v in common_constants.mapping_severity_values.items()}      
        for _, obs in self.__df_observations.iterrows():
            if obs["minimum_temperature_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}BT",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["minimum_temperature_severity"]),
                    "param_id": "BT",
                    "param_name": "Temperaturas mínimas",
                    "param_value": obs["minimum_temperature"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)
            if obs["maximum_temperature_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}AT",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["maximum_temperature_severity"]),
                    "param_id": "AT",
                    "param_name": "Temperaturas máximas",
                    "param_value": obs["maximum_temperature"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True) 
            if obs["uniform_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.UNIFORME",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["uniform_precipitation_1h_severity"]),
                    "param_id": "PR_1H.UNIFORME",
                    "param_name": "Precipitación acumulada en una hora (estimación uniforme)",
                    "param_value": obs["uniform_precipitation_1h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)   
            if obs["uniform_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.UNIFORME",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["uniform_precipitation_12h_severity"]),
                    "param_id": "PR_12H.UNIFORME",
                    "param_name": "Precipitación acumulada en 12 horas (estimación uniforme)",
                    "param_value": obs["uniform_precipitation_12h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)   
            if obs["severe_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.SEVERA",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["severe_precipitation_1h_severity"]),
                    "param_id": "PR_1H.SEVERA",
                    "param_name": "Precipitación acumulada en una hora (estimación severa del 33% en 1 hora)",
                    "param_value": obs["severe_precipitation_1h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)   
            if obs["severe_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.SEVERA",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["severe_precipitation_12h_severity"]),
                    "param_id": "PR_12H.SEVERA",
                    "param_name": "Precipitación acumulada en 12 horas (estimación severa del 85% en 12 horas)",
                    "param_value": obs["severe_precipitation_12h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)
            if obs["extreme_precipitation_1h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_1H.EXTREMA",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["extreme_precipitation_1h_severity"]),
                    "param_id": "PR_1H.EXTREMA",
                    "param_name": "Precipitación acumulada en una hora (estimación extrema del 50% en 1 hora)",
                    "param_value": obs["extreme_precipitation_1h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)   
            if obs["extreme_precipitation_12h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}PR_12H.EXTREMA",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["extreme_precipitation_12h_severity"]),
                    "param_id": "PR_12H.EXTREMA",
                    "param_name": "Precipitación acumulada en 12 horas (estimación extrema del 100% en 12 horas)",
                    "param_value": obs["extreme_precipitation_12h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)                
            if obs["snowfall_24h_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}NE",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["snowfall_24h_severity"]),
                    "param_id": "NE",
                    "param_name": "Nieve acumulada en 24 horas",
                    "param_value": obs["snowfall_24h"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True) 
            if obs["wind_speed_severity"] > 0:
                new_row = pd.DataFrame([{
                    "id": f"{self.__event_id}.{obs['date'].strftime('%Y%m%d')}000000.{obs['geocode']}VI",
                    "effective": obs["date"],
                    "severity": mapping_values_to_severity.get(obs["wind_speed_severity"]),
                    "param_id": "VI",
                    "param_name": "Rachas máximas",
                    "param_value": obs["wind_speed"],
                    "geocode": obs["geocode"],
                    "polygon": obs["polygon"]
                }])
                self.__df_situations = pd.concat([self.__df_situations, new_row], ignore_index=True)  

    def __group_situations(self):
        logging.info("Grouping situations by id and effective date")
        self.__df_situations = self.__df_situations.groupby(["id", "effective"]).apply(self.__resolve_conflict).reset_index(drop=True)
        
    def __resolve_conflict(group):
        if len(group) == 1:
            return group.iloc[0]
        max_severity = group["severity"].max()
        candidates = group[group["severity"] == max_severity]
        if len(candidates) == 1:
            return candidates.iloc[0]
        if candidates.iloc[0]["param_id"] == "BT":
            return candidates.loc[candidates["param_value"].idxmin()]
        else:
            return candidates.loc[candidates["param_value"].idxmax()]

    def write_data(self):
        path = f"P:\\TFM/results/{self.__event_id}/"
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)
        self.__df_observations.to_csv(os.path.join(path, "observed_data.tsv"), sep="\t")
        self.__df_warnings.to_csv(os.path.join(path, "predicted_warnings.tsv"), sep="\t")
        self.__df_situations.to_csv(os.path.join(path, "observed_warnings.tsv"), sep="\t")