import numpy as np
import logging
from datetime import datetime
import event_data_commons, event_data_map, aemet_opendata
from event_data_processor import EventDataProcessor
from event_data_analysis import EventDataAnalysis

# Configure logging
logging.basicConfig(
    filename=f"main_{datetime.now().strftime('%Y%m%d%H%M')}.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s::%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Set the root directory for constants
event_data_commons.set_path_to_root("")

# Set API Key
aemet_opendata.set_api_key(
    ""
)

# Retrieve data events
events = event_data_commons.get_events()

# Iterate over each event and perform analysis
for i, event in events.iterrows():
    logging.info(
        f"Starting analysis: {event['name']} ({event['start']} - {event['end']}). ID = {event['id']}"
    )

    event_processor = EventDataProcessor(
        event["id"], event["name"], event["start"], event["end"]
    )
    event_processor.fetch_predicted_warnings()
    event_processor.load_raw_data()
    event_processor.fetch_observed_data()
    event_processor.prepare_event_data()
    event_processor.save_prepared_data()
    event_data_map.get_map(
        event_processor.get_event_info()["id"],
        event_processor.get_event_info()["name"],
        event_processor.get_event_data(),
    )
    event_analysis = EventDataAnalysis(
        event["id"], event["name"], event["start"], event["end"]
    )
    event_analysis.load_prepared_data()
    event_analysis.get_confusion_matrix()
    event_analysis.get_distribution_chart()
    event_analysis.get_error_map()
    event_analysis.get_analysis_stats()
    event_analysis.save_analisys_data()
