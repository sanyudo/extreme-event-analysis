# Importación de módulos y bibliotecas
# dataclasses: biblioteca para definir estructuras de datos.
# datetime: biblioteca para manejar fechas y tiempos.
# typing: biblioteca para definir tipos de datos.
# shapely: biblioteca para manejar geometrías (puntos, polígonos, etc. en un espacio bidimensional).
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple
from shapely.geometry import Point, Polygon

@dataclass(frozen=True)
class WeatherStation:
    """Estación meteorológica"""
    code: str  # Código de la estación
    name: str  # Nombre de la estación
    province: str  # Provincia en la que se encuentra la estación
    latitude: float  # Latitud de la estación
    longitude: float  # Longitud de la estación
    altitude: float  # Altitud sobre el nivel del mar en metros

    def point(self) -> Point:
        """Devuelve la ubicación de la estación como un objeto Point de shapely."""
        return Point(self.latitude, self.longitude)

@dataclass(frozen=True)
class WeatherThreshold:
    """Umbral de aviso"""
    geocode: str  # Código del área
    low_temp_yellow: float  # Umbral de aviso amarillo para la temperatura baja
    low_temp_orange: float  # Umbral de aviso naranja para la temperatura baja
    low_temp_red: float  # Umbral de aviso rojo para la temperatura baja
    high_temp_yellow: float  # Umbral de aviso amarillo para la temperatura alta
    high_temp_orange: float  # Umbral de aviso naranja para la temperatura alta
    high_temp_red: float  # Umbral de aviso rojo para la temperatura alta
    wind_speed_yellow: float  # Umbral de aviso amarillo para la velocidad del viento (km/h)
    wind_speed_orange: float  # Umbral de aviso naranja para la velocidad del viento (km/h)
    wind_speed_red: float  # Umbral de aviso rojo para la velocidad del viento (km/h)
    precipitation_1h_yellow: float  # Umbral de aviso amarillo para la precipitación en 1 hora (mm)
    precipitation_1h_orange: float  # Umbral de aviso naranja para la precipitación en 1 hora (mm)
    precipitation_1h_red: float  # Umbral de aviso rojo para la precipitación en 1 hora (mm)
    precipitation_12h_yellow: float  # Umbral de aviso amarillo para la precipitación en 12 horas (mm)
    precipitation_12h_orange: float  # Umbral de aviso naranja para la precipitación en 12 horas (mm)
    precipitation_12h_red: float  # Umbral de aviso rojo para la precipitación en 12 horas (mm)
    snowfall_yellow: float  # Umbral de aviso amarillo para la nevada (cm)
    snowfall_orange: float  # Umbral de aviso naranja para la nevada (cm)
    snowfall_red: float  # Umbral de aviso rojo para la nevada (cm)

    def low_temperature(self) -> Tuple[int, int, int]:
        return Tuple[self.low_temp_yellow, self.low_temp_orange, self.low_temp_red]
    
    def high_temperature(self) -> Tuple[int, int, int]:
        return Tuple[self.high_temp_yellow, self.high_temp_orange, self.high_temp_red]
    
    def wind_speed(self) -> Tuple[int, int, int]:  
        return Tuple[self.wind_speed_yellow, self.wind_speed_orange, self.wind_speed_red]
    
    def precipitation_1h(self) -> Tuple[int, int, int]:
        return Tuple[self.precipitation_1h_yellow, self.precipitation_1h_orange, self.precipitation_1h_red]
    
    def precipitation_12h(self) -> Tuple[int, int, int]:
        return Tuple[self.precipitation_12h_yellow, self.precipitation_12h_orange, self.precipitation_12h_red]

    def snowfall(self) -> Tuple[int, int, int]:
        return Tuple[self.snowfall_yellow, self.snowfall_orange, self.snowfall_red]

@dataclass(frozen=True)
class WeatherObservation:
    """Observación meteorológica"""
    station: str  # Código de la estación meteorológica
    date: datetime  # Fecha de la observación
    min_temperature: float  # Temperatura mínima en grados Celsius
    max_temperature: float  # Temperatura máxima en grados Celsius
    max_wind_speed: float  # Velocidad máxima del viento en km/h
    precipitation: float  # Precipitación acumulada en 24 horas en mm

    # Relación de la precipitación extrema en 1 hora con la precipitación total.
    extreme_precipitation_1h_ratio: float = 0.40
    # Relación de la precipitación extrema en 12 horas con la precipitación total.
    extreme_precipitation_12h_ratio: float = 0.95

    def uniform_precipitation(self, hours: int) -> float:
        """Calcula la precipitación uniforme en un número dado de horas."""
        if hours <= 0:
            raise ValueError("El número de horas debe ser mayor que cero.")
        return self.precipitation / hours

    def uniform_precipitation_1h(self) -> float:
        """Calcula la precipitación uniforme en 1 hora."""
        return self.precipitation * self.extreme_precipitation_1h_ratio

    def uniform_precipitation_12h(self) -> float:
        """Calcula la precipitación uniforme en 12 horas."""
        return self.precipitation * self.extreme_precipitation_12h_ratio

@dataclass(frozen=True)
class WeatherWarning:
    """Aviso meteorológico"""
    file_id: str
    date_effective: datetime
    warning_level: int
    parameter_id: str
    parameter_description: str
    parameter_value: float
    area_geocode: int
    area_polygon: Polygon

@dataclass
class WeatherEvent:
    """Evento meteorológico"""
    name: str
    season: str
    category: str
    start: datetime
    end: datetime
