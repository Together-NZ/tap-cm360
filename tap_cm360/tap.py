import os
import time
import io
import csv
import datetime
import httplib2
from datetime import date, timedelta,datetime

# Google & OAuth
from oauth2client.file import Storage
from oauth2client import tools, client
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Singer SDK
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
import jsonschema
from jsonschema import validate
import tempfile
from google.cloud import secretmanager
from oauth2client import client

from tap_cm360.streams import CM360ReportStream



# ------------------------------
# Main Tap class
# ------------------------------
class Tapcm360(Tap):
    """Singer Tap for CM360."""

    name = "tap-cm360"

    # Define config JSON schema so Meltano/singer know how to validate
    config_jsonschema = {
        "type": "object",
        "properties": {
            "profile_id": {
                "type": "string",
                "description": "The CM360 user profile ID."
            },
            "client_secrets_file": {
                "type": "string",
                "description": "Path to OAuth2 client secrets JSON."
            },
            "credential_store_file": {
                "type": "string",
                "description": "Path to store (and reuse) OAuth2 credentials."
            },
            "oauth_scopes": {
                "type": "array",
                "items": {"type": "string"},
                "default": ["https://www.googleapis.com/auth/dfareporting"]
            },
            "start_date": {
                "type": "string",
                "format": "date",
                "description": "The date from which you'd like to replicate data for all incremental streams, in the format YYYY-MM-DD."},
            "end_date": {
                "type": "string",
                "format": "date",
                "description": "The date until which you'd like to replicate data for all incremental streams, in the format YYYY-MM-DD."
            }   
        },
        "required": [
            "profile_id",
            "client_secrets_file",
            "credential_store_file",
            "start_date",
        ]
    }

    def discover_streams(self):
        """Return a list of streams."""
        return [CM360ReportStream(self)]

# If running directly (i.e., python tap_cm360.py --config …)
if __name__ == "__main__":
    Tapcm360.cli()

