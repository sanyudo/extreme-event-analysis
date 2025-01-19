import pandas as pd
import numpy as np
import os

import folium
import geopandas as gpd
from shapely import Point, Polygon
from datetime import datetime, timedelta
from branca.element import Template, MacroElement

import common
import constants
import logging

TEXT_COLORS = ["green", "yellow", "orange", "red"]
RGB_COLORS = ["#228B22", "#FFFF00", "#FF8C00", "#FF0000"]
COLORS_DESCRIPTION = ["verde", "amarillo", "naranja", "rojo"]
ICONS = {
    "BT": "thermometer-empty",
    "AT": "thermometer-full",
    "PR": "tint",
    "PR_1H": "tint",
    "PR_12H": "tint",
    "PR_1H.UNIFORME": "tint",
    "PR_12H.UNIFORME": "tint",    
    "PR_1H.SEVERA": "tint",
    "PR_12H.SEVERA": "tint",       
    "PR_1H.EXTREMA": "tint",
    "PR_12H.EXTREMA": "tint",     
    "NE": "snowflake-o",
    "VI": "refresh",
}

MAP_CENTER = [40.42, -3.70]
MAP_TILES = "CartoDB Positron"

def get_network(geocodes: pd.DataFrame, stations: pd.DataFrame):
    """
    Generates a map of AEMET's weather stations and warning regions.

    Parameters
    ----------
    geocodes : pd.DataFrame
        A DataFrame with geocodes, provinces, regions, and their respective polygons.
    stations : pd.DataFrame
        A DataFrame with the stations data, including their respective geocodes.

    Returns
    -------
    None
    """
    geo_map = folium.Map(location=MAP_CENTER, zoom_start=6, tiles=folium.TileLayer(
        tiles="CartoDB Positron",
        name="CartoDB Positron", 
    ))
    folium.TileLayer(
        tiles="Cartodb dark_matter",
        name="CartoDB Dark Matter", 
    ).add_to(geo_map)             
    folium.TileLayer(
        tiles="OpenTopoMap",
        name="OpenTopoMap"
    ).add_to(geo_map)            
    folium.TileLayer(
        tiles="OpenStreetMap",
        name="OpenStreetMap"
    ).add_to(geo_map)

    geocodes["geometry"] = geocodes["polygon"].apply(
        lambda coordinates: 
            ([tuple(map(float, pair.split(","))) for pair in coordinates.split()])
        )

    layer_regions = folium.FeatureGroup(name=f"Regiones", show=True)
    layer_stations = folium.FeatureGroup(name=f"Estaciones", show=False)

    for _, geo in geocodes.iterrows():
        folium.Polygon(
            locations=geo["geometry"],
            color="black",
            weight=1,
            dash_array=10,
            fill_color="blue",
            fill_opacity=0.66,
            tooltip=f"{geo['geocode']}: {geo['area']} ({geo['province']}), {geo['region']}"
        ).add_to(layer_regions)

    for _, sta in stations.iterrows():
        folium.Marker(
            location=[sta["latitude"], sta["longitude"]],
            icon=folium.Icon(color="lightgray", icon="circle", icon_color="blue", prefix="fa", opacity=1),
            weight=1,
            tooltip=f"<b>{sta['idema']}: {sta['name']} ({sta['province']})"
        ).add_to(layer_stations)

    layer_regions.add_to(geo_map)
    layer_stations.add_to(geo_map)
    
    folium.LayerControl().add_to(geo_map)

    title = f"""
        <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;"><b>Red de estaciones de AEMET</b><br></h4>
        <h5 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Regiones de aviso y estaciones meteorológicas automáticas</h5>
    """
    geo_map.get_root().html.add_child(folium.Element(title))
    geo_map.save(os.path.join(constants.get_path_to_dir("data"), f"mapa_aemet.html"))
    logging.info(f"Map saved")        
        
def get_map(event_id:str, event_name:str, analysis: pd.DataFrame):

    """
    Creates a map comparing predicted and observed data for a given event id and param id.

    Parameters
    ----------
    event_id : str
        The id of the event to visualize.
    event_name : str
        The name of the event to visualize.
    analysis : pd.DataFrame
        The analysis DataFrame with the columns
            - date: The date of the observation.
            - param_id: The id of the parameter to visualize.
            - param_name: The name of the parameter to visualize.
            - geocode: The geocode of the region to visualize.
            - observed_value: The observed value of the parameter.
            - observed_severity: The observed severity of the parameter.
            - predicted_value: The predicted value of the parameter.
            - predicted_severity: The predicted severity of the parameter.

    Returns
    -------
    None
    """
    analysis["geometry"] = analysis["polygon"].apply(
        lambda coordinates: 
            ([tuple(map(float, pair.split(","))) for pair in coordinates.split()])
        )
    
    analysis = analysis[(analysis["predicted_severity"] > 0) | (analysis["observed_severity"] > 0) | (analysis["region_severity"] > 0)]
    for d in analysis["date"].unique():
        for p in analysis[analysis["date"] == d]["param_id"].unique():
            subset = analysis[(analysis["date"] == d) & (analysis["param_id"] == p)]
            geo_map = folium.Map(location=MAP_CENTER, zoom_start=6, tiles=folium.TileLayer(
                tiles="CartoDB Positron",
                name="CartoDB Positron", 
            ))
            folium.TileLayer(
                tiles="Cartodb dark_matter",
                name="CartoDB Dark Matter", 
            ).add_to(geo_map)             
            folium.TileLayer(
                tiles="OpenTopoMap",
                name="OpenTopoMap"
            ).add_to(geo_map)            
            folium.TileLayer(
                tiles="OpenStreetMap",
                name="OpenStreetMap"
            ).add_to(geo_map)

            layer_warnings = folium.FeatureGroup(name=f"Avisos | {d.strftime('%d/%m/%Y')} | {constants.mapping_parameters[p]['description']}", show=True)
            layer_results = folium.FeatureGroup(name=f"Situación | {d.strftime('%d/%m/%Y')} | {constants.mapping_parameters[p]['description']}", show=False)
            layer_stations = folium.FeatureGroup(name=f"Estaciones | {d.strftime('%d/%m/%Y')} | {constants.mapping_parameters[p]['description']}", show=False)

            for _, obs in subset.iterrows():
                folium.Marker(
                    location=[obs["latitude"], obs["longitude"]],
                    icon=folium.Icon(color="lightgray", icon="circle", icon_color=TEXT_COLORS[int(obs['observed_severity'])], prefix="fa", opacity=1),
                    weight=1,
                    tooltip=f"<b>Datos observados</b><br>{obs['idema']}: {obs['name']} ({obs['province']})<br><b>{constants.mapping_parameters[p]['description']}</b>: {float(obs['observed_value'])} {constants.mapping_parameters[p]['units']} ({obs['date'].strftime('%d/%m/%Y')})"
                ).add_to(layer_stations)

            subset.drop(["observed_value", "observed_severity"], axis=1, inplace=True)
            subset.drop_duplicates(subset=["geocode", "date", "param_id"], inplace=True)

            for _, row in subset.iterrows():
                folium.Polygon(
                    locations=row["geometry"],
                    color="black",
                    weight=1,
                    dash_array=10,
                    fill_color=TEXT_COLORS[int(row["predicted_severity"])],
                    fill_opacity=0.66,
                    tooltip=f"Predicción para {row['area']} ({row['province']}), {row['region']}<br><b>{constants.mapping_parameters[p]['description']}</b>: {float(row['predicted_value'])} {constants.mapping_parameters[p]['units']} ({row['date'].strftime('%d/%m/%Y')})"
                ).add_to(layer_warnings)

                folium.Polygon(
                    locations=row["geometry"],
                    color=TEXT_COLORS[int(row["region_severity"])],
                    weight=2.5,
                    fill_color=TEXT_COLORS[int(row["region_severity"])],
                    fill_opacity=0.66,         
                    tooltip=f"Situación para {row['area']} ({row['province']}) {row['region']}<br><b>{constants.mapping_parameters[p]['description']}</b>: {float(row['region_value'])} {constants.mapping_parameters[p]['units']} ({row['date'].strftime('%d/%m/%Y')})"
                ).add_to(layer_results)

            layer_warnings.add_to(geo_map)
            layer_results.add_to(geo_map)
            layer_stations.add_to(geo_map)
           
            folium.LayerControl().add_to(geo_map)

            title = f"""
                <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;"><b>{event_name.upper()}</b><br></h4>
                <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Día: <b>{d.strftime('%d/%m/%Y')}</b> | Parámetro: <b>{constants.mapping_parameters[p]['description']}</b> ({constants.mapping_parameters[p]['units']})<br></h4>
                <h5 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Comparativa por regiones entre predicción avisos y datos observados</h5>
            """
            geo_map.get_root().html.add_child(folium.Element(title))
            geo_map.save(os.path.join(constants.get_path_to_dir("maps", event_id), f"{d.strftime('%Y%m%d')}_{p}.html"))
            logging.info(f"Map saved")