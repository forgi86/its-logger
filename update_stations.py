import requests
from datetime import datetime
import pandas as pd
from pathlib import Path
import json
import gzip
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
STATIC_DATA_URL = "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/data/ch.bfe.ladestellen-elektromobilitaet.json"
DATA_FOLDER = "data"
STATIC_FOLDER = "stations"
UPDATE_INTERVAL = 24 * 60 * 60  # 24 hours in seconds


def update_stations():
    """Fetch and save station data."""
    try:
        # Create directory structure
        static_path = Path(DATA_FOLDER) / STATIC_FOLDER
        static_path.mkdir(parents=True, exist_ok=True)
        
        # Fetch data
        logging.info("Fetching station data...")
        response = requests.get(STATIC_DATA_URL, timeout=30)
        response.raise_for_status()
        
        # Get timestamp
        timestamp = pd.Timestamp.now(tz="Europe/Zurich").floor("s")
        
        # Save compressed JSON file
        time_str = timestamp.strftime("%Y%m%d%H%M%S")
        filename = static_path / f"stations_{time_str}.json.gz"
        
        with gzip.open(filename, "wt", encoding="utf-8") as f:
            f.write(response.text)
        
        logging.info(f"Successfully saved data to {filename}")
        return True
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return False
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return False


def main():
    """Main loop to update stations daily."""
    logging.info("Starting daily station update loop...")
    
    while True:
        try:
            # Update stations
            update_stations()
            
            # Wait for 24 hours
            logging.info(f"Waiting {UPDATE_INTERVAL / 3600} hours until next update...")
            time.sleep(UPDATE_INTERVAL)
            
        except KeyboardInterrupt:
            logging.info("Shutting down gracefully...")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            logging.info("Retrying in 1 hour...")
            time.sleep(3600)


if __name__ == "__main__":
    main()
