import logging
from datetime import datetime
import constants
import common_operations
from event_analysis import EventAnalysis

# Configure logging
logging.basicConfig(
    filename=f"main_{datetime.now().strftime('%Y%m%d%H%M')}.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s::%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Set the root directory for constants
constants.set_path_to_root("P:\\TFM\\")

# Retrieve data events
events = common_operations.get_events()

# Iterate over each event and perform analysis
for i, event in events.iterrows():
    logging.info(f"Starting analysis: {event['name']} ({event['start']} - {event['end']}). ID = {event['id']}")
    analysis = EventAnalysis(event["id"], event["name"], event["start"], event["end"])
    logging.info(f"Fetching data.")
    analysis.fetch_warnings()
    logging.info(f"Loading data.")
    analysis.load_data()
    logging.info(f"Obtain observations.")
    analysis.fetch_observations()     
    logging.info(f"Preparing data.")
    analysis.prepare_analysis()   
    logging.info(f"Analyzing data.")
    analysis.analyze_data()        
    logging.info(f"Writing data.")
    analysis.save_data()
    logging.info(f"Draw maps.")
    analysis.draw_maps()    
    logging.info("Analysis completed")



