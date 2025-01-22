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
event_data_commons.set_path_to_root("P:\\TFM\\")
aemet_opendata.set_api_key("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbHZhcm8uc2FudWRvQGFsdW1ub3MudWkxLmVzIiwianRpIjoiMzMzMWQ4YjgtMjc3OS00NzNmLWFjNDEtYTI0Zjg1NzczOTc4IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3MzExNzA2NzgsInVzZXJJZCI6IjMzMzFkOGI4LTI3NzktNDczZi1hYzQxLWEyNGY4NTc3Mzk3OCIsInJvbGUiOiIifQ.bNt0gjOKShj0PAf2XZ0IUMspaaKVlmdAxy4koTY7gjo")

# Retrieve data events
events = event_data_commons.get_events()

# Iterate over each event and perform analysis
for i, event in events.sample(1, random_state=42).iterrows():
    logging.info(f"Starting analysis: {event['name']} ({event['start']} - {event['end']}). ID = {event['id']}")
    
    event_processor = EventDataProcessor(event["id"], event["name"], event["start"], event["end"])
    logging.info(f"Starting data processing...")
    logging.info(f"Fetching data.")
    event_processor.fetch_predicted_warnings()
    logging.info(f"Loading data.")
    event_processor.load_raw_data()
    logging.info(f"Obtain observations.")
    event_processor.fetch_observed_data()     
    logging.info(f"Preparing data.")
    event_processor.prepare_event_data()       
    logging.info(f"Saving data.")
    event_processor.save_prepared_data()
    logging.info(f"Draw maps.")
    event_data_map.get_map(event_processor.get_event_info()["id"], event_processor.get_event_info()["name"], event_processor.get_event_data())
    logging.info(f"...")
    logging.info("Data processing completed!")

    logging.info(f"Starting data analysis...")
    event_analysis = EventDataAnalysis(event["id"], event["name"], event["start"], event["end"])
    logging.info(f"Loading data.")    
    event_analysis.load_prepared_data()
    logging.info(f"Confusion matrix.")
    event_analysis.get_confusion_matrix()      
    logging.info(f"Distribution chart.")
    event_analysis.get_distribution_chart()        
    logging.info(f"Error clustering.")
    event_analysis.get_error_map()     
    logging.info(f"Other stats.")
    event_analysis.get_analysis_stats()
    logging.info(f"Saving data.")
    event_analysis.save_analisys_data()        
    logging.info(f"...")
    logging.info("Data analysis completed!")