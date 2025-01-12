import logging
from datetime import datetime
import pandas as pd
import aemet_opendata_client as aemet
from extreme_event_analysis import EventAnalysis


logging.basicConfig(
    filename=f"main_{datetime.now().strftime('%Y%m%d%H%M')}.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s::%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

aemet.set_root("P:\\TFM\\")

df = pd.read_csv(aemet.get_file("events"), sep = "\t")
df["start"] = pd.to_datetime(df["start"], format="%d/%m/%Y")
df["end"] = pd.to_datetime(df["end"], format="%d/%m/%Y")
df.sort_values(by=["start", "end"], inplace=True)

for i, event in df.iterrows():
    print(f"Iniciando análisis: {event['name']} ({event['start']} - {event['end']}). ID = {event['id']}")
    analysis = EventAnalysis(event["id"], event["name"], event["start"], event["end"])
    print("................")


