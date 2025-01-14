import os
import pandas as pd
import xml.etree.ElementTree as ET

directory = r'.\Z_CAP_C_LEMM_20250113225001_AFAE'
namespace = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}

geocode_polygons = pd.DataFrame(columns=["geocode", "polygon"])

for filename in os.listdir(directory):
    if filename.endswith('.xml'):
        file_path = os.path.join(directory, filename)
        tree = ET.parse(file_path)
        root = tree.getroot()

        for info in root.findall(".//cap:info", namespace):
            for area in info.findall(".//cap:area", namespace):
                geocode = area.find("cap:geocode", namespace)
                cap_geocode = geocode.find("cap:value", namespace).text
                cap_polygon = area.find("cap:polygon", namespace).text
                if not cap_geocode.endswith("C"):
                    geocode_polygons = pd.concat([geocode_polygons, pd.DataFrame([{
                        "geocode": cap_geocode,
                        "polygon": cap_polygon
                    }])], ignore_index=True)

geocode_polygons.drop_duplicates(inplace=True)                
geocode_polygons.to_csv(r'.\data\geocode_polygons.tsv', sep='\t', index=False)