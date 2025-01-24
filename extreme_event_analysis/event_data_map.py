"""
This module provides functions for visualizing data related to extreme weather events.

Provides a function to generate a map with the stations and warning regions.
Provides a function to generate a map comparing predicted and observed data for a given event id and parameter id.

"""

import pandas as pd
import os

import folium

import event_data_commons
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
    geo_map = folium.Map(
        location=MAP_CENTER,
        zoom_start=6,
        tiles=folium.TileLayer(
            tiles="CartoDB Positron",
            name="CartoDB Positron",
        ),
    )
    folium.TileLayer(
        tiles="Cartodb dark_matter",
        name="CartoDB Dark Matter",
    ).add_to(geo_map)
    folium.TileLayer(tiles="OpenTopoMap", name="OpenTopoMap").add_to(geo_map)
    folium.TileLayer(tiles="OpenStreetMap", name="OpenStreetMap").add_to(geo_map)

    geocodes["geometry"] = geocodes["polygon"].apply(
        lambda coordinates: (
            [tuple(map(float, pair.split(","))) for pair in coordinates.split()]
        )
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
            tooltip=f"{geo['geocode']}: {geo['area']} ({geo['province']}), {geo['region']}",
        ).add_to(layer_regions)

    for _, sta in stations.iterrows():
        folium.Marker(
            location=[sta["latitude"], sta["longitude"]],
            icon=folium.Icon(
                color="lightgray",
                icon="circle",
                icon_color="blue",
                prefix="fa",
                opacity=1,
            ),
            weight=1,
            tooltip=f"<b>{sta['idema']}: {sta['name']} ({sta['province']})",
        ).add_to(layer_stations)

    layer_regions.add_to(geo_map)
    layer_stations.add_to(geo_map)

    folium.LayerControl().add_to(geo_map)

    title = f"""
        <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;"><b>Red de estaciones de AEMET</b><br></h4>
        <h5 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Regiones de aviso y estaciones meteorológicas automáticas</h5>
    """
    geo_map.get_root().html.add_child(folium.Element(title))
    geo_map.save(
        os.path.join(event_data_commons.get_path_to_dir("data"), f"mapa_aemet.html")
    )
    logging.info(f"Map saved")


def get_map(event_id: str, event_name: str, event_data: pd.DataFrame = None):
    """
    Creates a map comparing predicted and observed data for a given event id and param id.

    Parameters
    ----------
    event_id : str
        The id of the event to visualize.
    event_name : str
        The name of the event to visualize.
    Returns
    -------
    None
    """
    if event_data is None:
        event_data = pd.read_csv(
            event_data_commons.get_path_to_file("event_prepared_data", event=event_id)
        )
    event_data["geometry"] = event_data["polygon"].apply(
        lambda coordinates: (
            [tuple(map(float, pair.split(","))) for pair in coordinates.split()]
        )
    )

    event_data = event_data[
        (event_data["predicted_severity"] > 0)
        | (event_data["observed_severity"] > 0)
        | (event_data["region_severity"] > 0)
    ]
    for d in event_data["date"].unique():
        for p in event_data[event_data["date"] == d]["param_id"].unique():
            subset = event_data[
                (event_data["date"] == d) & (event_data["param_id"] == p)
            ]
            geo_map = folium.Map(
                location=MAP_CENTER,
                zoom_start=6,
                tiles=folium.TileLayer(
                    tiles="CartoDB Positron",
                    name="CartoDB Positron",
                ),
            )
            folium.TileLayer(
                tiles="Cartodb dark_matter",
                name="CartoDB Dark Matter",
            ).add_to(geo_map)
            folium.TileLayer(tiles="OpenTopoMap", name="OpenTopoMap").add_to(geo_map)
            folium.TileLayer(tiles="OpenStreetMap", name="OpenStreetMap").add_to(
                geo_map
            )

            layer_warnings = folium.FeatureGroup(
                name=f"Avisos | {d.strftime('%d/%m/%Y')} | {event_data_commons.MAPPING_PARAMETERS[p]['description']}",
                show=True,
            )
            layer_results = folium.FeatureGroup(
                name=f"Situación | {d.strftime('%d/%m/%Y')} | {event_data_commons.MAPPING_PARAMETERS[p]['description']}",
                show=False,
            )
            layer_stations = folium.FeatureGroup(
                name=f"Estaciones | {d.strftime('%d/%m/%Y')} | {event_data_commons.MAPPING_PARAMETERS[p]['description']}",
                show=False,
            )

            for _, obs in subset.iterrows():
                folium.Marker(
                    location=[obs["latitude"], obs["longitude"]],
                    icon=folium.Icon(
                        color="lightgray",
                        icon="circle",
                        icon_color=TEXT_COLORS[int(obs["observed_severity"])],
                        prefix="fa",
                        opacity=1,
                    ),
                    weight=1,
                    tooltip=f"<b>Datos observados</b><br>{obs['idema']}: {obs['name']} ({obs['province']})<br><b>{event_data_commons.MAPPING_PARAMETERS[p]['description']}</b>: {float(obs['observed_value'])} {event_data_commons.MAPPING_PARAMETERS[p]['units']} ({obs['date'].strftime('%d/%m/%Y')})",
                ).add_to(layer_stations)

            reduced = subset.drop(["observed_value", "observed_severity"], axis=1)
            reduced = reduced.drop_duplicates(subset=["geocode", "date", "param_id"])

            for _, row in reduced.iterrows():
                folium.Polygon(
                    locations=row["geometry"],
                    color="black",
                    weight=1,
                    dash_array=10,
                    fill_color=TEXT_COLORS[int(row["predicted_severity"])],
                    fill_opacity=0.66,
                    tooltip=f"Predicción para {row['area']} ({row['province']}), {row['region']}<br><b>{event_data_commons.MAPPING_PARAMETERS[p]['description']}</b>: {float(row['predicted_value'])} {event_data_commons.MAPPING_PARAMETERS[p]['units']} ({row['date'].strftime('%d/%m/%Y')})",
                ).add_to(layer_warnings)

                folium.Polygon(
                    locations=row["geometry"],
                    color=TEXT_COLORS[int(row["region_severity"])],
                    weight=2.5,
                    fill_color=TEXT_COLORS[int(row["region_severity"])],
                    fill_opacity=0.66,
                    tooltip=f"Situación para {row['area']} ({row['province']}) {row['region']}<br><b>{event_data_commons.MAPPING_PARAMETERS[p]['description']}</b>: {float(row['region_value'])} {event_data_commons.MAPPING_PARAMETERS[p]['units']} ({row['date'].strftime('%d/%m/%Y')})",
                ).add_to(layer_results)

            layer_warnings.add_to(geo_map)
            layer_results.add_to(geo_map)
            layer_stations.add_to(geo_map)

            folium.LayerControl().add_to(geo_map)

            title = f"""
                <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;"><b>{event_name.upper()}</b><br></h4>
                <h4 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Día: <b>{d.strftime('%d/%m/%Y')}</b> | Parámetro: <b>{event_data_commons.MAPPING_PARAMETERS[p]['description']}</b> ({event_data_commons.MAPPING_PARAMETERS[p]['units']})<br></h4>
                <h5 style="font-size: 20px; text-align: center; font-family: Arial, sans-serif;">Comparativa por regiones entre predicción avisos y datos observados</h5>
            """
            geo_map.get_root().html.add_child(folium.Element(title))
            geo_map.save(
                os.path.join(
                    event_data_commons.get_path_to_dir("maps", event_id),
                    f"Dia-{d.strftime('%Y%m%d')}_Parametro-{p}.html",
                )
            )
            logging.info(f"Map saved")
