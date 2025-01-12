# Módulos del lenguaje
import datetime  # Manejo de fechas y tiempos
from datetime import timedelta  # Operaciones con fechas
import gzip  # Comprimir y descomprimir archivos gzip
import os  # Operaciones del sistema operativo
import re  # Operaciones con expresiones regulares
import shutil  # Copiar y mover archivos
import tarfile  # Manejar archivos tar

# Bibliotecas
import pandas as pd  # Manipulación y análisis de datos
import tenacity  # Reintentar operaciones fallidas
import requests  # Realizar solicitudes HTTP
from requests.exceptions import RequestException  # Excepciones de la biblioteca request
import xml.etree.ElementTree as ET  # Módulo para manejo de XML

# Módulo para manejo de logs
import logging

# Clave de la API para acceder a los datos abiertos de AEMET
__API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbHZhcm8uc2FudWRvQGFsdW1ub3MudWkxLmVzIiwianRpIjoiMzMzMWQ4YjgtMjc3OS00NzNmLWFjNDEtYTI0Zjg1NzczOTc4IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3MzExNzA2NzgsInVzZXJJZCI6IjMzMzFkOGI4LTI3NzktNDczZi1hYzQxLWEyNGY4NTc3Mzk3OCIsInJvbGUiOiIifQ.bNt0gjOKShj0PAf2XZ0IUMspaaKVlmdAxy4koTY7gjo"

# Encabezados utilizados en las solicitudes HTTP
__REQUEST_HEADERS = {"cache-control": "no-cache"}

# Cadena de consulta para las solicitudes HTTP
__REQUEST_QUERYSTRING = {"api_key": "{api_key}".format(api_key=__API_KEY)}

# URL del servidor de datos abiertos de AEMET
__REQUEST_SERVER = "https://opendata.aemet.es/opendata"

# Puntos de acceso a la API de AEMET
__REQUEST_ENDPOINTS = {
    "stations": "/api/valores/climatologicos/inventarioestaciones/todasestaciones/",
    "warnings": "/api/avisos_cap/archivo/fechaini/{start_date}/fechafin/{end_date}",
    "observations": "/api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones",
}

# Extensión de archivo para los datos
__DATA_EXTENSION = ".tsv"

__RENAMING = str.maketrans(
    {
        " ": "_",  # Reemplazar espacios con guiones bajos
        "(": "",  # Eliminar paréntesis de apertura
        ")": "",  # Eliminar paréntesis de cierre
    }
)

# Directorios utilizados para almacenar diversos tipos de datos
__DIRS = {
    "root": os.path.dirname(os.path.abspath(__file__)),
    "data": "./data/",
    "warnings": "./data/warnings/{event}/",
    "observations": "./data/observations/{event}/",
}

# Archivos utilizados para almacenar datos específicos
__FILES = {
    "stations": os.path.join(
        __DIRS["root"], __DIRS["data"], "weather_stations" + __DATA_EXTENSION
    ),
    "thresholds": os.path.join(
        __DIRS["root"], __DIRS["data"], "parameter_thresholds" + __DATA_EXTENSION
    ),
    "events": os.path.join(
        __DIRS["root"], __DIRS["data"], "events_list" + __DATA_EXTENSION
    ),
    "warnings": os.path.join(
        __DIRS["root"], __DIRS["warnings"], "warnings" + __DATA_EXTENSION
    ),
    "observations": os.path.join(
        __DIRS["root"], __DIRS["observations"], "observations" + __DATA_EXTENSION
    ),
}

__NAMESPACES = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

# Parámetros de las observaciones climatológicas
__OBSERVATION_PARAMETERS = ["PR", "VI", "BT", "TA", "NE"]
# Mapeo de los niveles de aviso
__WARNINGS_MAPPING = {"amarillo": 1, "naranja": 2, "rojo": 3}
# Número máximo de estaciones por solicitud
__MAX_STATIONS_PER_REQUEST = 5

__COLUMNS_STATIONS = {
    "indicativo": "idema",
    "nombre": "name",
    "provincia": "province",
    "latitud": "latitude",
    "longitud": "longitude",
    "altitud": "altitude",
}

# Columnas de los datos de observaciones climatológicas
__COLUMNS_OBSERVATIONS = {
    "fecha": "date",
    "indicativo": "idema",
    "tmin": "min_temperature",
    "tmax": "max_temperature",
    "prec": "precipitation",
    "racha": "max_wind_speed",
}

# Columnas de los datos de avisos meteorológicos
__COLUMNS_WARNINGS = [
    "id",
    "effective",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

__COLUMNS_THRESHOLDS = [
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

# Columnas de los datos de avisos meteorológicos en formato CAP
__COLUMNS_CAP = [
    "id",
    "sent",
    "effective",
    "expires",
    "severity",
    "param_id",
    "param_name",
    "param_value",
    "geocode",
    "polygon",
]

__COLUMNS_EVENTS = {"season", "category", "name", "start", "end"}

def columns_on_stations() -> list:
    return list(__COLUMNS_STATIONS.values())

def columns_on_observations() -> list:
    return list(__COLUMNS_OBSERVATIONS.values())

def columns_on_warnings() -> list:
    return __COLUMNS_WARNINGS

def columns_on_thresholds() -> list:
    return __COLUMNS_THRESHOLDS

def ensure_directories_exist(event: str) -> None:
    """Aseguramos que existe cada directorio especificado"""
    for d in __DIRS.values():
        p = d.format(event=event.lower().translate(__RENAMING))
        logging.debug(f"Comprobando existencia de {p}.")
        if not os.path.exists(p):
            try:
                logging.debug(f"... no se ha encontrado. Creando {p}.")
                os.makedirs(p)
            except OSError as e:
                logging.error(
                    f"Ha ocurrido un error al comprobar el directorio {p}: {e}"
                )
                raise
        else:
            logging.debug(f"... ¡encontrado!")

def get_file(file: str, event: str = "") -> str:
    """Devuelve la ruta completa del archivo de datos especificado"""
    try:
        return __FILES[file].format(event=event.lower().translate(__RENAMING))
    except Exception as e:
        logging.error(f"Ha ocurrido un error al obtener el archivo de datos: {e}")
        raise

def get_dir(dir: str, event: str = "") -> str:
    """Devuelve la ruta completa del directorio especificado"""
    try:
        return os.path.join(
            __DIRS["root"],
            __DIRS[dir].format(event=event.lower().translate(__RENAMING)),
        )
    except Exception as e:
        logging.error(f"Ha ocurrido un error al obtener la ruta del directorio: {e}")
        raise

def exists_stations() -> bool:
    if not os.path.exists(get_file("stations")):
        logging.debug(f"... inventario de estaciones no encontrado.")
        return False
    logging.debug(f"... inventario de estaciones disponible.")
    return True

def exists_warnings(event: str) -> bool:
    if os.path.exists(get_file("warnings", event)):
        logging.debug(f"... fichero de avisos encontrado.")
        return True
    else:
        logging.debug(f"... fichero de avisos no encontrados.")
        return False

def __exists_caps(event: str) -> bool:
    if os.path.exists(event):
        try:
            logging.debug(f"... directorio de avisos disponible.")
            by_date_dir = [
                d
                for d in os.listdir(get_dir("warnings", event))
                if os.path.isdir(os.path.join(get_dir("warnings", event), d))
            ]
            for date_dir in by_date_dir:
                by_prediction_dir = [
                    d
                    for d in os.listdir(
                        os.path.join(get_dir("warnings", event), date_dir)
                    )
                    if os.path.isdir(
                        os.path.join(get_dir("warnings", event), date_dir, d)
                    )
                ]
                for prediction_dir in by_prediction_dir:
                    cap_files = [
                        f
                        for f in os.listdir(
                            os.path.join(
                                get_dir("warnings", event), date_dir, prediction_dir
                            )
                        )
                        if f.endswith(".xml")
                    ]
                    for cap in cap_files:
                        if cap:
                            logging.info(f"... ficheros cap encontrados.")
                            return True
        except Exception as e:
            logging.error(f"Error comprobando los datos brutos del evento: {e}")
            raise
        return True
    else:
        logging.debug(f"... ficheros cap no disponibles.")
        return False

def exists_observations(event: str) -> bool:
    if not os.path.exists(get_file("observations", event)):
        logging.debug(f"... observaciones no encontradas.")
        return False
    logging.debug(f"... observaciones disponibles.")
    return True

def __build_request_url(endpoint: str) -> str:
    """Construye una URL para realizar una solicitud a la API de AEMET"""
    return f"{__REQUEST_SERVER}{__REQUEST_ENDPOINTS[endpoint]}?api_key={__API_KEY}"

@tenacity.retry(
    wait=tenacity.wait_fixed(5),  # Espera 5 segundos entre reintentos
    stop=tenacity.stop_after_attempt(3),  # Detiene después de 3 intentos
    retry=tenacity.retry_if_exception_type(
        RequestException
    ),  # Reintenta si ocurren estas excepciones
)
def __request_stations() -> pd.DataFrame:
    try:
        response = requests.get(
            __build_request_url("stations"), headers=__REQUEST_HEADERS
        )
        response.raise_for_status()
        logging.info("... datos obtenidos correctamente.")
        return pd.DataFrame(response.json()["datos"])
    except RequestException as e:
        logging.error(f"Error al obtener datos: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error al parsear JSON: {e}")
        raise
    except Exception as e:
        logging.error(f"Error. {e}.")
        raise

def __convert_dms_to_degrees(dms_coordinate: str, hemisphere: str) -> float:
    dms_value = dms_coordinate.replace(hemisphere, "")
    degrees, minutes, seconds = map(int, [dms_value[:2], dms_value[2:4], dms_value[4:]])
    result = degrees + minutes / 60 + seconds / 3600
    return -result if hemisphere in {"S", "W"} else result

def __transform_stations(stations_df: pd.DataFrame) -> pd.DataFrame:
    df = stations_df.copy()
    df.rename(columns=columns_on_stations(), inplace=True)
    df[["province", "name"]] = df[["province", "name"]].applymap(
        str.title
    )
    df["latitude"] = df["latitude"].apply(
        lambda x: __convert_dms_to_degrees(x, hemisphere=x[-1])
    )
    df["longitude"] = df["longitude"].apply(
        lambda x: __convert_dms_to_degrees(x, hemisphere=x[-1])
    )
    df = df[columns_on_stations]
    return df

def fetch_stations():
    logging.info(f"Comprobando inventario de estaciones.")
    if not exists_stations():
        logging.info("... descargando nuevos datos del inventario.")
        stations_df = __request_stations()
        logging.info("... preparando datos del inventario de estaciones.")
        stations_df = __transform_stations(stations_df)
        try:
            logging.debug(f"... almacenando datos en {get_file('stations')}.")
            stations_df.to_csv(
                get_file("stations"),
                index=False,
                sep="\t",
            )
        except Exception as e:
            logging.error(f"Error al guardar los datos: {e}")
            raise

@tenacity.retry(
    wait=tenacity.wait_fixed(5),  # Espera 5 segundos entre reintentos
    stop=tenacity.stop_after_attempt(3),  # Detiene después de 3 intentos
    retry=tenacity.retry_if_exception_type(
        RequestException
    ),  # Reintenta si ocurren estas excepciones
)
def __request_warnings(event: str, date: datetime):
    try:
        response = requests.get(
            __build_request_url("warnings").format(
                start_date=date.strftime("%Y-%m-%dT%H:%M:%SUTC"),
                end_date=date.strftime("%Y-%m-%dT23:59:59UTC"),
            ),
            headers=__REQUEST_HEADERS,
        )
        response.raise_for_status()
        download_url = response.json()["datos"]
    except RequestException as e:
        logging.error(f"Error al obtener datos: {e}")
        raise

    except ValueError as e:
        logging.error(f"Error al parsear JSON: {e}")
        raise

    except Exception as e:
        logging.error(f"Error. {e}.")
        raise

    try:
        download_file = os.path.join(
            get_dir("warnings", event), date.strftime("%Y%m%d"), "caps.tar"
        )
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()
            with open(download_file, "wb") as dl:
                dl.write(response.content)
        logging.info("... caps descargados correctamente.")
    except RequestException as e:
        logging.error(f"Error al descargar datos: {e}")
        raise

    except ValueError as e:
        logging.error(f"Error al reparsear JSON: {e}")
        raise

    except Exception as e:
        logging.error(f"Error. {e}.")
        raise

def fetch_warnings(event: str, start: datetime, end: datetime):
    logging.info(f"Comprobando existencia de avisos para evento {event}.")
    if not exists_warnings(event):
        if not __exists_caps(event):
            logging.info(f"Descargando avisos entre {start:%Y-%m-%d} y {end:%Y-%m-%d}.")
            if not os.path.exists(os.path.join(get_dir("warnings", event))):
                logging.info(
                    f"... creando nuevo directorio de avisos en {get_dir('warnings', event)}."
                )
                os.makedirs(get_dir("warnings", event))
            else:
                try:
                    logging.info(
                        f"... eliminando directorio existente en {get_dir('warnings', event)}."
                    )
                    shutil.rmtree(get_dir("warnings", event))
                    os.makedirs(get_dir("warnings", event))
                except Exception as e:
                    logging.error(f"Error al limpiar el directorio de avisos: {e}")
                    raise

            for n in range(int((end - start).days) + 1):
                logging.info(
                    f"... creando subdirectorio de avisos para {(start + timedelta(n)).strftime('%Y-%m-%d')}."
                )
                if not os.path.exists(
                    os.path.join(
                        get_dir("warnings", event),
                        (start + timedelta(n)).strftime("%Y%m%d"),
                    )
                ):
                    os.makedirs(
                        os.path.join(
                            get_dir("warnings", event),
                            (start + timedelta(n)).strftime("%Y%m%d"),
                        )
                    )

                try:
                    logging.debug(
                        f"... descargando avisos para el día {(start + timedelta(n)):%Y-%m-%d}."
                    )
                    __request_warnings(event, (start + timedelta(n)))
                except Exception as e:
                    logging.error(f"Error al obtener avisos: {e}")
                    raise

                try:
                    logging.debug(f"... descomprimiendo avisos.")
                    __extract_warnings(event, (start + timedelta(n)))
                except Exception as e:
                    logging.error(f"Error al descomprimir avisos: {e}")
                    raise

        try:
            logging.info("... preparando datos de avisos.")
            warnings_df = __merge_warnings_data(get_dir("warnings", event))
        except Exception as e:
            logging.error(f"Error al unificar avisos: {e}")
            raise

        try:
            logging.info("... transformando datos de avisos.")
            warnings_df = __transform_warnings_data(warnings_df)
        except Exception as e:
            logging.error(f"Error al transformar avisos: {e}")
            raise

        try:
            logging.info("... limpiando datos de avisos.")
            warnings_df = __clean_warnings_data(warnings_df)
        except Exception as e:
            logging.error(f"Error al limpiar avisos: {e}")
            raise

        warnings_df = warnings_df[columns_on_warnings()]

        try:
            logging.debug(f"... almacenando datos en {get_file('warnings', event)}.")
            warnings_df.to_csv(
                get_file("warnings", event),
                index=False,
                sep="\t",
            )
        except Exception as e:
            logging.error(f"Error al guardar los datos: {e}")
            raise

def __extract_warnings(event: str, date: datetime):
    extraction_path = os.path.join(get_dir("warnings", event), date.strftime("%Y%m%d"))
    tar_path = os.path.join(
        get_dir("warnings", event), date.strftime("%Y%m%d"), "caps.tar"
    )
    try:
        logging.info(f"Descomprimiendo tars de warnings en {extraction_path}.")
        with tarfile.open(tar_path, "r") as t:
            members = t.getmembers()
            members_str = ", ".join(m.name for m in members)
            logging.debug(f"... abriendo tar: {members_str}.")
            for member in members:
                logging.debug(f"... extrayendo {member.name} a {extraction_path}.")
                t.extract(member, path=extraction_path)
            logging.debug(f"... cerrando tar.")
            t.close()
            try:
                logging.debug(f"... eliminando tar.")
                os.remove(tar_path)
            except Exception as e:
                logging.error(f"Error al eliminar tar: {e}")
                raise

            try:
                __extract_gzips(extraction_path)
            except:
                logging.error(f"Error al descomprimir gzips: {e}")
                raise

    except Exception as e:
        logging.error(f"Error durante la descompresión de avisos: {e}")
        raise

def __extract_gzips(path: str):
    logging.info(f"Descomprimiendo gzips de previsiones en {path}.")
    try:
        logging.info(f"... examinando {path}.")
        gz_files = [f for f in os.listdir(path) if f.endswith(".gz")]
        for gz in gz_files:
            logging.info(f"Descomprimiendo {gz}.")
            file = os.path.join(path, re.search(r"\d+", gz).group() + ".tar")
            with gzip.open(os.path.join(path, gz), "rb") as gz_in, open(
                file, "wb"
            ) as gz_out:
                gz_out.write(gz_in.read())
            gz_in.close()
            gz_out.close()
            logging.debug(f"... eliminando {gz}.")
            try:
                os.remove(os.path.join(path, gz))
            except Exception as e:
                logging.error(f"Error al eliminar gzip {gz}: {e}")
                raise

        __extract_caps(path)
    except Exception as e:
        logging.error(f"Error durante la descompresión de gzip: {e}")
        raise

def __extract_caps(path: str) -> bool:
    logging.info(f"Descomprimiendo tars con ficheros cap en {path}.")
    try:
        logging.info(f"... examinando {path}.")
        tar_files = [f for f in os.listdir(path) if f.endswith(".tar")]
        for tar in tar_files:
            logging.info(f"Descomprimiendo {tar}.")
            with tarfile.open(os.path.join(path, tar), "r") as t:
                members = t.getmembers()
                members_str = ", ".join(m.name for m in members)
                logging.debug(f"... abriendo {tar}: {members_str}.")
                for member in members:
                    logging.debug(f"... extrayendo {member.name} a {path}.")
                    t.extract(member, path=path)
                logging.debug(f"... cerrando {tar}.")
                t.close()
                logging.debug(f"... eliminando {tar}.")
                try:
                    os.remove(os.path.join(path, tar))
                except Exception as e:
                    logging.error(f"Error al eliminar {tar}: {e}")
                    raise

    except Exception as e:
        logging.error(f"Error durante la descompresión de ficheros cap: {e}")
        raise

def __merge_warnings_data(path: str) -> pd.DataFrame:
    logging.info(f"Unificando archivos cap en {path}.")

    warnings_df = pd.DataFrame(columns=__COLUMNS_CAP)
    date_list = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]
    for event_day in date_list:
        cap_file_list = [
            f for f in os.listdir(os.path.join(path, event_day)) if f.endswith(".xml")
        ]
        for cap in cap_file_list:
            try:
                logging.debug(
                    f"... leyendo archivo {os.path.join(path, event_day, cap)}."
                )
                tree = ET.parse(os.path.join(path, event_day, cap))
                root = tree.getroot()

                cap_identifier = root.find("cap:identifier", __NAMESPACES).text
                cap_sent = root.find(".//cap:sent", __NAMESPACES).text
                info_elements = root.findall(".//cap:info", __NAMESPACES)
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
                    ".//cap:effective", __NAMESPACES
                ).text
                cap_expires = selected_info.find(".//cap:expires", __NAMESPACES).text
                cap_event_code = selected_info.find(
                    ".//cap:eventCode/cap:value", __NAMESPACES
                ).text
                if cap_event_code:
                    cap_event_code = cap_event_code.split(";")

                parameters = {}
                for param in selected_info.findall(".//cap:parameter", __NAMESPACES):
                    param_name = param.find("cap:valueName", __NAMESPACES).text
                    param_value = param.find("cap:value", __NAMESPACES).text
                    parameters[param_name] = param_value

                    cap_severity = parameters.get("AEMET-Meteoalerta nivel", None)
                    cap_parameter = parameters.get("AEMET-Meteoalerta parametro", None)
                    if cap_parameter:
                        cap_parameter = cap_parameter.split(";")
                    else:
                        cap_parameter = ["", "", "0"]

                cap_polygon = []
                for area in selected_info.findall(".//cap:area", __NAMESPACES):
                    geocode = area.find("cap:geocode", __NAMESPACES)
                    cap_geocode = geocode.find("cap:value", __NAMESPACES).text
                    cap_polygon = area.find("cap:polygon", __NAMESPACES).text

                if cap_severity in list(__WARNINGS_MAPPING.keys()):
                    warnings_df.loc[len(warnings_df)] = {
                        "id": cap_identifier,
                        "sent": cap_sent,
                        "effective": cap_effective,
                        "expires": cap_expires,
                        "severity": cap_severity,
                        "param_id": cap_event_code[0],
                        "param_name": cap_parameter[1],
                        "param_value": re.sub(r"[^\d]", "", cap_parameter[2]),
                        "geocode": cap_geocode,
                        "polygon": cap_polygon,
                    }
            except Exception as e:
                logging.error(
                    f"Error leyendo {os.path.join(path, event_day, cap)}: {e}"
                )
                raise

            try:
                logging.debug(f"... eliminando {os.path.join(path, event_day, cap)}.")
                os.remove(os.path.join(path, event_day, cap))
            except Exception as e:
                logging.error(
                    f"Error al eliminar {os.path.join(path, event_day, cap)}: {e}"
                )

    try:
        logging.debug(f"... eliminando directorio {os.path.join(path, event_day)}.")
        shutil.rmtree(os.path.join(path, event_day))
    except Exception as e:
        logging.error(
            f"Error al eliminar directorio {os.path.join(path, event_day)}: {e}"
        )
        raise
    
    return warnings_df

def __transform_warnings_data(warnings_df: pd.DataFrame) -> pd.DataFrame:
    df = warnings_df.copy()
    df = df[df["param_id"].isin(__OBSERVATION_PARAMETERS)]
    df["priority"] = df["severity"].map(__WARNINGS_MAPPING)
    df.dropna(subset=["priority"], inplace=True)

    print(df.head(1))

    df["sent"] = pd.to_datetime(
        df["sent"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    )

    df["effective"] = pd.to_datetime(
        df["effective"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    ).dt.date

    df["expires"] = pd.to_datetime(
        df["expires"], format="%Y-%m-%dT%H:%M:%S%z", errors="coerce", utc=True
    ).dt.date

    df["param_value"] = pd.to_numeric(df["param_value"], errors="coerce")
    df["geocode"] = pd.to_numeric(df["geocode"], downcast="integer", errors="coerce")

    processed_rows = []
    for _, row in df.iterrows():
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

    return pd.DataFrame(processed_rows)

def __clean_warnings_data(warnings_df: pd.DataFrame) -> pd.DataFrame:
    df = warnings_df.copy()
    df.sort_values(
        by=[
            "geocode",
            "param_id",
            "effective",
            "severity",
            "sent",
        ],
        ascending=[True, True, True, False, False], inplace=True,
    ).reset_index(drop=True, inplace=True)

    df.groupby(
        ["geocode", "param_id", "effective"], as_index=False, inplace=True
    ).apply(
        lambda x: x.sort_values(by=["severity", "sent"], ascending=[False, False]).head(
            1
        )
    )
    return df

@tenacity.retry(
    wait=tenacity.wait_fixed(5),  # Espera 5 segundos entre reintentos
    stop=tenacity.stop_after_attempt(3),  # Detiene después de 3 intentos
    retry=tenacity.retry_if_exception_type(
        RequestException
    ),  # Reintenta si ocurren estas excepciones
)
def __request_observations(date: datetime) -> pd.DataFrame:
    try:
        response = requests.get(
            __build_request_url("observations").format(
                start_date=date.strftime("%Y-%m-%dT%H:%M:%SUTC"),
                end_date=date.strftime("%Y-%m-%dT23:59:59UTC"),
            ),
            headers=__REQUEST_HEADERS,
        )
        response.raise_for_status()
        logging.info("... enlace obtenido correctamente.")
    except RequestException as e:
        logging.error(f"Error al soliticar enlaces: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error al parsear JSON: {e}")
        raise
    except Exception as e:
        logging.error(f"Error. {e}.")

    try:
        data_link = response.json()["datos"]
        response = requests.get(data_link)
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except RequestException as e:
        logging.error(f"Error al solicitar datos: {e}")
        raise
    except ValueError as e:
        logging.error(f"Error al parsear JSON: {e}")
        raise
    except Exception as e:
        logging.error(f"Error. {e}.")
        raise

def fetch_observations(
    event: str, start: datetime, end: datetime, stations=[str]
) -> None:
    logging.info(f"Comprobando existencia de los datos observados del evento.")
    if not exists_observations(event):
        observations_df = pd.DataFrame(columns=list(__COLUMNS_OBSERVATIONS.keys()))
        for n in range((end - start).days):
            logging.debug(
                f"... obteniendo observaciones para el dia {(start + timedelta(n)):%Y-%m-%d}."
            )
            response_df = __request_observations(start + timedelta(n))
            observations_df = pd.concat([observations_df, response_df], ignore_index=True)

        observations_df.rename(columns=__COLUMNS_OBSERVATIONS, inplace=True)
        observations_df = observations_df[columns_on_observations()]
        try:
            logging.info("... filtrando datos de observaciones.")
            observations_df = __filter_observations_data(observations_df, stations)
        except Exception as e:
            logging.error(f"Error al filtrar observaciones: {e}")
            raise

        try:
            logging.info("... transformando datos de observaciones.")
            observations_df = __transform_observations_data(observations_df)
        except Exception as e:
            logging.error(f"Error al transformar observaciones: {e}")
            raise

        try:
            logging.info("... limpiando datos de observaciones.")
            observations_df = __clean_observations_data(observations_df)
        except Exception as e:
            logging.error(f"Error al limpiar observaciones: {e}")

        try:
            observations_df.to_csv(
                get_file("observations", event),
                index=False,
                sep="\t",
            )
        except Exception as e:
            logging.error(f"Error al guardar los datos: {e}")
            raise

def __filter_observations_data(observations_df: pd.DataFrame, stations=[]) -> pd.DataFrame:
    df = observations_df.copy()
    df = df[df["idema"].isin(stations)]
    return df

def __transform_observations_data(observations_df: pd.DataFrame) -> pd.DataFrame:
    df = observations_df.copy()
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["min_temperature"] = pd.to_numeric(
        df["min_temperature"].str.replace(",", "."), errors="coerce"
    )
    df["max_temperature"] = pd.to_numeric(
        df["max_temperature"].str.replace(",", "."), errors="coerce"
    )
    df["precipitation"] = pd.to_numeric(
        df["precipitation"].str.replace(",", "."), errors="coerce"
    )
    df["max_wind_speed"] = pd.to_numeric(
        df["max_wind_speed"].str.replace(",", "."), errors="coerce"
    )
    df.dropna(inplace=True)
    return df

def __clean_observations_data(observations_df: pd.DataFrame) -> pd.DataFrame:
    df = observations_df.copy()
    df = df[columns_on_observations()]
    return df
