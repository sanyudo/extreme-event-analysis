import logging
import pandas as pd
import aemet_opendata_client as aemet
from extreme_event_analysis import EventAnalysis
import os


logging.basicConfig(
    filename="main.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s::%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

df = pd.read_csv(aemet.get_file("events"), sep = "\t")
df["start"] = pd.to_datetime(df["start"], format="%d/%m/%Y")
df["end"] = pd.to_datetime(df["end"], format="%d/%m/%Y")
df.sort_values(by=['start', 'end'], inplace=True)

print(df)
for i, event in df.iterrows():
    print("Iniciando análisis ...")
    analysis = EventAnalysis(f"{event['name']} ({event['season']})", event['start'], event['end'])
    print(analysis.get_name())


