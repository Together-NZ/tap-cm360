from __future__ import annotations
from typing import Any, Dict, Optional
from collections.abc import Iterable
import io
import time
import logging
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload  # Correct import
from oauth2client.file import Storage
from oauth2client import tools, client
import httplib2
from singer_sdk.streams import Stream
import google.auth.credentials
logger = logging.getLogger(__name__)
from google.auth.transport.requests import Request
from google.cloud import secretmanager
import tempfile
from tap_cm360.auth import authorize_in_memory
class cm360Stream(Stream):
    """Stream class for Campaign Manager 360 (CM360) API."""
    
    name = "tap-cm360"
    records_jsonpath = "$[*]"  # Adjust based on API's response structure
    next_page_token_jsonpath = None  # Assuming no pagination

    def __init__(self, tap, name=None, schema=None, path=None):
        super().__init__(tap, name, schema, path)
        self.version = 'v4'

    @property
    def url_base(self) -> str:
        """Base URL for CM360 API."""
        return f"https://www.googleapis.com/auth/dfareporting/{self.version}"

    def fetch_secret_from_secret_manager(self, secret_id, project_id, version_id="1"):
        """
        Fetch a secret (JSON content) from Google Secret Manager.
        """
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": secret_name})
        return response.payload.data.decode("UTF-8")
    def get_flow_from_client_secrets(self, secret_id, project_id, scopes):
        """
        Create a flow object using client secrets fetched from Google Secret Manager.
        """
        # Fetch the secret
        secret_content = self.fetch_secret_from_secret_manager(secret_id, project_id)

        # Write the secret to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            temp_file.write(secret_content.encode())
            temp_file_path = temp_file.name

        # Create a flow object using the temporary file
        flow = client.flow_from_clientsecrets(
            filename=temp_file_path,
            scope=scopes
        )

        return flow
    def generate_credential(self, context: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        config = self.config
            # Example usage
        SECRET_ID = "airflow-variables-meltano-cm360"
        PROJECT_ID = "739679429225"
        SCOPES = ["https://www.googleapis.com/auth/dfareporting"]
        secret_content = self.fetch_secret_from_secret_manager(SECRET_ID, PROJECT_ID)
        return authorize_in_memory(secret_content, SCOPES)



