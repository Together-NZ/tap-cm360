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
from singer_sdk.streams import RESTStream
import google.auth.credentials
logger = logging.getLogger(__name__)
from google.auth.transport.requests import Request

class GoogleOAuthAuthenticator:
    """Custom authenticator using Google OAuth login token."""

    def __init__(self, google_account):
        """Initialize with the relevant Google account's OAuth token.

        :param google_account: The Google account passed from the tap that provides the OAuth token.
        """
        # Assume that the google_account has 'access_token' that can be used directly.
        self.credentials = google.auth.credentials.Credentials.from_authorized_user_info(
            google_account
        )

        if not self.credentials.valid:
            self.credentials.refresh(Request())

    def __call__(self, request):
        """Add Authorization header."""
        if not self.credentials.valid:
            self.credentials.refresh(Request())
        request.headers["Authorization"] = f"Bearer {self.credentials.token}"
        return request

class cm360Stream(RESTStream):
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

    def prepare_request_payload(self, context: Optional[Dict[str, Any]], next_page_token: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """Prepare the request payload for POST requests."""
        request = {
            'dimensionName': 'campaign',
            'endDate': context['criteria']['dateRange']['endDate'],
            'startDate': context['criteria']['dateRange']['startDate']
        }
        return request

    def generate_credential(self, context: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Generate OAuth credentials."""
        path_to_client_secrets_file = 'secret.json'
        OAUTH_SCOPES = ['https://www.googleapis.com/auth/dfareporting']
        CREDENTIAL_STORE_FILE = 'credentials/app.json'

        # Set up authentication
        flow = client.flow_from_clientsecrets(path_to_client_secrets_file, scope=OAUTH_SCOPES)
        storage = Storage(CREDENTIAL_STORE_FILE)
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, tools.argparser.parse_known_args()[0])
        
        return credentials

    def download_report(self, service, report_id, file_id):
        """Download the report file to an in-memory file object."""
        request = service.files().get_media(reportId=report_id, fileId=file_id)
        memory_file = io.BytesIO()

        downloader = MediaIoBaseDownload(memory_file, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.info(f"Download {int(status.progress() * 100)}% complete.")

        memory_file.seek(0)  # Rewind the file before reading
        return memory_file

    def generate_service(self, context: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Generate an authenticated service object."""
        credentials = self.generate_credential(context)
        http = credentials.authorize(httplib2.Http())
        return build('dfareporting', 'v4', http=http)

    def parse_response(self, context: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Parse the report response."""
        service = self.generate_service(context)
        request = self.prepare_request_payload(context)

        values = service.dimensionValues().query(profileId=context['profile_id'], body=request).execute()

        report = context['report_path']

        fields = service.reports().compatibleFields().query(
            profileId=context['profile_id'], body=report).execute()

        report_fields = fields['reportCompatibleFields']

        if report_fields['dimensions']:
            report['criteria']['dimensions'].append({
                'name': report_fields['dimensions'][0]['name']
            })
        elif report_fields['metrics']:
            report['criteria']['metricNames'].append(
                report_fields['metrics'][0]['name'])

        inserted_report = service.reports().insert(profileId=context["profile_id"], body=report).execute()

        report_file = service.reports().run(profileId=context["profile_id"], reportId=inserted_report['id']).execute()
        report_file_id = report_file['id']

        return inserted_report, report_file_id

    def request_records(self, context: Optional[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        """Download the report and parse the CSV content."""
        MAX_RETRY_ELAPSED_TIME = 3600
        initial_sleep = 10

        sleep = initial_sleep
        start_time = time.time()
        service = self.generate_service(context)
        inserted_report, report_file_id = self.parse_response(context)

        while True:
            report_file = service.files().get(reportId=inserted_report['id'], fileId=report_file_id).execute()
            status = report_file['status']

            if status == 'REPORT_AVAILABLE':
                logger.info(f'File status is {status}, ready to download.')
                break
            elif status != 'PROCESSING':
                logger.error(f'File status is {status}, processing failed.')
                return
            elif time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                logger.error('File processing deadline exceeded.')
                return

            sleep = self.next_sleep_interval(sleep)
            logger.info(f'File status is {status}, sleeping for {sleep} seconds.')
            time.sleep(sleep)

        return self.download_report(service, inserted_report["id"], report_file_id)

