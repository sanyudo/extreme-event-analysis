import pandas as pd
import datetime
from shapely import Point, Polygon
import aemet_opendata_client as aemet
import logging


class EventAnalysis:

    __EVENT_NAME = ""
    __EVENT_START = datetime.datetime.now()
    __EVENT_END = datetime.datetime.now()

    __STATIONS = pd.DataFrame()
    __OBSERVATIONS = pd.DataFrame()
    __WARNINGS = pd.DataFrame()    
    __THRESHOLDS = pd.DataFrame()    

    def __init__(self, event_name: str, event_start: datetime, event_end: datetime):
        self.__EVENT_NAME = event_name
        self.__EVENT_START = event_start
        self.__EVENT_END = event_end
        aemet.ensure_directories_exist(self.__EVENT_NAME)

        aemet.fetch_stations()
        self.__STATIONS = self.prepare_stations(pd.read_csv(aemet.get_file('stations'), sep="\t"))

        aemet.fetch_warnings(self.__EVENT_NAME, self.__EVENT_START, self.__EVENT_END)
        self.__WARNINGS = self.prepare_warnings(pd.read_csv(aemet.get_file('warnings', self.__EVENT_NAME), sep="\t"))

        self.__THRESHOLDS = self.prepare_thresholds(pd.read_csv(aemet.get_file('thresholds'), sep="\t"))

        self.__filter_data()
        stations = self.__STATIONS["idema"].tolist()

        aemet.fetch_observations(self.__EVENT_NAME, self.__EVENT_START, self.__EVENT_END, stations)
        self.__OBSERVATIONS = self.prepare_observations(pd.read_csv(aemet.get_file('observations', self.__EVENT_NAME), sep="\t"))

    def get_name(self) -> str:
        return self.__EVENT_NAME

    def prepare_thresholds(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Preprocesando datos de umbrales...")
        try:
            columns = aemet.columns_on_thresholds()
            for column in columns:
                if column not in ["geocode", "region", "area","province"]:
                    df[column] = pd.to_numeric(df[column], errors="coerce")
            return df
        except Exception as e:
            logging.error(f"Error preprocesando datos de umbrales: {e}")

    def prepare_stations(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Preprocesando datos de estaciones...")
        try:
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            df["altitude"] = pd.to_numeric(df["altitude"], errors="coerce")
            return df
        except Exception as e:
            logging.error(f"Error preprocesando datos de estaciones: {e}")

    def prepare_warnings(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Preprocesando datos de avisos...")
        try:
            df["effective"] = pd.to_datetime(df["effective"], format="%Y-%m-%d")
            df["param_value"] = pd.to_numeric(
                df["param_value"], errors="coerce"
            )
            df["geocode"] = pd.to_numeric(
                df["geocode"], downcast="integer", errors="coerce"
            )
            return df
        except Exception as e:
            logging.error(f"Error preprocesando datos de avisos: {e}")

    def prepare_observations(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Preprocesando datos de observaciones...")
        try:
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
            df["min_temperature"] = pd.to_numeric(df["min_temperature"], errors="coerce")
            df["max_temperature"] = pd.to_numeric(df["max_temperature"], errors="coerce")
            df["max_wind_speed"] = pd.to_numeric(df["max_wind_speed"], errors="coerce")
            df["precipitation"] = pd.to_numeric(df["precipitation"], errors="coerce")
            return df
        except Exception as e:
            logging.error(f"Error preprocesando datos de observaciones: {e}")            

    def __filter_data(self):
        min_date = pd.Timestamp(self.__EVENT_START)
        max_date = pd.Timestamp(self.__EVENT_END)
        revised_areas = set()
        filtered_stations = pd.DataFrame()
        for i, warning_item in self.__WARNINGS.iterrows():
            area_str = warning_item["polygon"].split()
            area = [tuple(map(float, a.split(','))) for a in area_str]
            if Polygon(area) not in revised_areas:
                for j, station_item in self.__STATIONS.iterrows():
                    if Polygon(area).contains(Point(station_item["longitude"], station_item["latitude"])) and station_item not in filtered_stations:
                        filtered_stations = pd.concat(filtered_stations, station_item)
                revised_areas.add(Polygon(area))

        logging.debug(f"... regiones con aviso: {len(revised_areas)}.")
        logging.debug(f"... estaciones en regiones con aviso: {len(filtered_stations)}.")
        
        self.__WARNINGS = pd.merge(self.__WARNINGS, self.__THRESHOLDS, on='geocode', how='inner')

"""
    def __load_observations(self, df: pd.DataFrame):
        logging.info("Cargando observaciones...")
        for i, item in df.iterrows():
            try:
                observation = WeatherObservation(
                    station=item["indicativo"],
                    date=item["fecha"],
                    min_temperature=item["tmin"],
                    max_temperature=item["tmax"],
                    max_wind_speed=item["racha"],
                    precipitation=item["prec"],
                )
                self.__OBSERVATIONS.add(observation)
            except KeyError as e:
                logging.error(f"Error creando observación desde fila {i}: {e}")
        logging.info(f"... cargadas {len(self.__OBSERVATIONS)} observaciones.")

    def __load_warnings(self, df: pd.DataFrame):
        logging.info("Cargando avisos...")
        for i, item in df.iterrows():
            try:
                area_polygon = [
                    tuple(map(float, par.split(",")))
                    for par in item["area_polygon"].split()
                ]
                weather_warning = WeatherWarning(
                    file_id=item["identifier"],
                    date_effective=item["dt_effective"],
                    warning_level=item["warning_level"],
                    parameter_id=item["parameter_id"],
                    parameter_description=item["parameter_description"],
                    parameter_value=item["parameter_value"],
                    area_geocode=item["area_code"],
                    area_polygon=Polygon(area_polygon),
                )
                self.__WARNINGS.add(weather_warning)
            except KeyError as e:
                logging.error(f"Error creando aviso desde fila {i}: {e}")
        logging.info(f"... cargados {len(self.__WARNINGS)} avisos.")

    def __load_stations(self, df: pd.DataFrame):
        logging.info("Cargando estaciones...")
        for i, item in df.iterrows():
            try:
                ws = WeatherStation(
                    code=item["indicativo"],
                    name=item["nombre"],
                    province=item["provincia"],
                    latitude=item["latitud"],
                    longitude=item["longitud"],
                    altitude=item["altitud"],
                )
                self.__STATIONS.add(ws)
            except KeyError as e:
                logging.error(f"Error creando estación desde fila {i}: {e}")
        logging.info(f"... cargadas {len(self.__STATIONS)} estaciones.")        

    def __load_thresholds(self, df: pd.DataFrame):
        logging.info("Cargando umbrales...")
        for i, item in df.iterrows():
            try:
                wt = WeatherThreshold(
                    geocode=item["codigo"],
                    low_temp_yellow = item["temperatura mínima - aviso amarillo"],
                    low_temp_orange = item["temperatura mínima - aviso naranja"],
                    low_temp_red = item["temperatura mínima - aviso rojo"],
                    high_temp_yellow = item["temperatura máxima - aviso amarillo"],
                    high_temp_orange = item["temperatura máxima - aviso naranja"],
                    high_temp_red = item["temperatura máxima - aviso rojo"],
                    wind_speed_yellow = item["velocidad viento - aviso amarillo"],
                    wind_speed_orange = item["velocidad viento - aviso naranja"],
                    wind_speed_red = item["velocidad viento - aviso rojo"],
                    precipitation_1h_yellow = item["precipitacion 1 hora - aviso amarillo"],
                    precipitation_1h_orange = item["precipitacion 1 hora - aviso naranja"],
                    precipitation_1h_red = item["precipitacion 1 hora - aviso rojo"],
                    precipitation_12h_yellow = item["precipitacion 12 horas - aviso amarillo"],
                    precipitation_12h_orange = item["precipitacion 12 horas - aviso naranja"],
                    precipitation_12h_red = item["precipitacion 12 horas - aviso rojo"],
                    snowfall_yellow = item["nieve 24 horas - aviso amarillo"],
                    snowfall_orange = item["nieve 24 horas - aviso naranja"],
                    snowfall_red = item["nieve 24 horas - aviso rojo"],
                )
                self.__THRESHOLDS.add(wt)
            except KeyError as e:
                logging.error(f"Error creando umbral desde fila {i}: {e}")
        logging.info(f"... cargados {len(self.__THRESHOLDS)} umbrales.")
"""

