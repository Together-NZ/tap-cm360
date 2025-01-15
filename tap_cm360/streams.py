"""Stream type classes for tap-dv360."""

from __future__ import annotations
from singer_sdk.typing import PropertiesList, Property, StringType, DateTimeType, NumberType
import typing as t
from importlib import resources
from typing import Iterable, Dict, Optional, Any, List

from singer_sdk import typing as th  # JSON Schema typing helpers
from datetime import datetime, timedelta
from tap_cm360.client import cm360Stream,GoogleOAuthAuthenticator
import csv
import json
import logging
import requests
from typing import Iterable, Dict, Any
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

    # Add a console handler to see logs in the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
current_date = datetime.now()



class CM360StandardStream(cm360Stream):
    def __init__(self, tap, name=None, schema=None, path=None):
        super().__init__(tap, name, schema, path)
        self.shared_data = tap.shared_data
    name = "cm360_standard"
    path = f'https://www.googleapis.com/auth/dfareporting/v4'
    replication_key = "Date"
    records_jsonpath = "$[*]"  # Adjust based on DV360 API's response
    next_page_token_jsonpath = None  # Assuming no pagination for this example
    @property
    def authenticator(self):
        self.google_account = self.config.get("google_account")
        return GoogleOAuthAuthenticator(self.google_account)



    def _parse_csv_to_records(self, csv_content: str) -> Iterable[Dict[str, Any]]:
        """Convert CSV content into a list of records, dynamically extracting metric names."""
        logger.info("Starting to parse CSV content.")

        # If the content looks like JSON, extract the `googleCloudStoragePath`
        if csv_content.strip().startswith("{"):
            logger.info("Response appears to be JSON; attempting to extract CSV URL.")
            try:
                response_json = json.loads(csv_content)
                csv_url = response_json.get("metadata", {}).get("googleCloudStoragePath")
                if not csv_url:
                    logger.error("CSV URL not found in response.")
                    raise ValueError("CSV URL not found in response.")

                logger.info(f"Downloading CSV from URL: {csv_url}")
                response = requests.get(csv_url)
                if response.status_code != 200:
                    logger.error(f"Failed to download CSV: {response.status_code} - {response.text}")
                    raise RuntimeError(f"Failed to download CSV: {response.status_code} - {response.text}")

                csv_content = response.text  # Replace with downloaded CSV content
                logger.info("CSV content downloaded successfully.")

            except json.JSONDecodeError as e:
                logger.exception("Failed to decode JSON response.")
                raise RuntimeError("Invalid JSON in API response.") from e

        # Parse the CSV content
        try:
            filters=set()
            reader = csv.DictReader(csv_content.splitlines())
            logger.info("Parsing CSV content into rows.")
            # Reinitialize reader to start from the beginning
            reader = csv.DictReader(csv_content.splitlines())            
            self.shared_data["filters"] = filters
            self._tap.shared_data["filters"] = filters
            # Process all rows and extract metric data
            for row in reader:
                yield row
            
        except Exception as e:
            logger.exception("Error occurred while parsing CSV content.")
            raise e


