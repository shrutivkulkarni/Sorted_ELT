import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_data():
    file_path = f"./data/SORTED_YT_data_{date.today()}.json()"

    try:
        logger.info(f" Processing file: SORTED_YT_data_{date.today()}")

        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)

        return data #for smaller json file, need other ways for larger json files
    
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        raise