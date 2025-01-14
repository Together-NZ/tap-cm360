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
class CM360ReportStream(Stream):
    """Streamer that downloads and parses a CM360 CSV report."""

    name = "cm360_report_stream"
    # Minimal schema. Adjust to your needs:
    schema = th.PropertiesList(
        th.Property("placementId", th.StringType, description="Unique ID for the placement"),
        th.Property("advertiser", th.StringType, description="Name of the advertiser"),
        th.Property("creativeType", th.StringType, description="Type of creative (e.g., Banner, Video)"),
        th.Property("creativeId", th.StringType, description="Unique ID for the creative"),
        th.Property("creative", th.StringType, description="Name or identifier for the creative"),
        th.Property("advertiserId", th.StringType, description="Unique ID for the advertiser"),
        th.Property("campaignEndDate", th.StringType, description="End date of the campaign in YYYY-MM-DD format"),
        th.Property("campaignId", th.StringType, description="Unique ID for the campaign"),
        th.Property("campaign", th.StringType, description="Name of the campaign"),
        th.Property("campaignStartDate", th.StringType, description="Start date of the campaign in YYYY-MM-DD format"),
        th.Property("clickThroughUrl", th.StringType, description="URL associated with the click-through action"),
        th.Property("date", th.StringType, description="Date of the data in YYYY-MM-DD format"),
        th.Property("placementCostStructure", th.StringType, description="Cost structure for the placement (e.g., CPM, CPC)"),
        th.Property("placementEndDate", th.StringType, description="End date of the placement in YYYY-MM-DD format"),
        th.Property("placement", th.StringType, description="Name or identifier for the placement"),
        th.Property("packageRoadblockId", th.StringType, description="Unique ID for the package or roadblock"),
        th.Property("packageRoadblock", th.StringType, description="Name or identifier for the package or roadblock"),
        th.Property("placementSize", th.StringType, description="Size of the placement (e.g., 300x250)"),
        th.Property("placementStartDate", th.StringType, description="Start date of the placement in YYYY-MM-DD format"),
        th.Property("placementStrategy", th.StringType, description="Strategy used for the placement"),
        th.Property("siteKeyname", th.StringType, description="Key name of the site for the placement"),
        th.Property("clicks", th.IntegerType, description="Number of clicks"),
        th.Property("impressions", th.IntegerType, description="Number of impressions")
    ).to_dict()

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


    def get_records(self, context):
        """Main function to create/run/download the CM360 report and yield row data."""
        config = self.config
            # Example usage
        SECRET_ID = "airflow-variables-meltano-cm360"
        PROJECT_ID = "739679429225"
        SCOPES = ["https://www.googleapis.com/auth/dfareporting"]

        flow = self.get_flow_from_client_secrets(SECRET_ID, PROJECT_ID, SCOPES)
        # 1. OAuth2 Flow using oauth2client
        flow = client.flow_from_clientsecrets(
            filename=config["client_secrets_file"],
            scope=config["oauth_scopes"]
        )
        storage = Storage(config["credential_store_file"])
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            # Will prompt for OAuth if running interactively
            credentials = tools.run_flow(
                flow,
                storage,
                tools.argparser.parse_known_args()[0]
            )
        http = credentials.authorize(httplib2.Http())

        # 2. Build CM360 (a.k.a. DFA Reporting) service
        service = build("dfareporting", "v4", http=http)

        profile_id = config["profile_id"]  # Must be a string
        str_start_date = config["start_date"]  # e.g. "2025-01-14"
        parsed_datetime = datetime.strptime(str_start_date, "%Y-%m-%d")  # Convert string → datetime
        start_date = parsed_datetime.date()  # Extract date object
        if not config.get("end_date"):
            end_date = date.today()
        else:
            end_date = config["end_date"]

        # 3. Prepare a new (or updated) report definition
        report_body = {
            'name': 'Example Standard Report',
            'type': 'STANDARD',
            'fileName': 'example_report',
            'format': 'CSV',
            'criteria': {
                'dateRange': {
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d')
                },
                'dimensions': [{
                    'name':'placementId'
                },
                {'name':'advertiser'},
                {'name':'creativeType'},
                {'name':'creativeId'},
                {'name':'creative'},
                {'name':'advertiserId'},
                {'name':'campaignEndDate'},
                {'name':'campaignId'},
                {'name':'campaign'},
                {'name':'campaignStartDate'},
                {'name':'clickThroughUrl'},
                {'name':'date'},
                {'name':'placementCostStructure'},
                {'name':'placementEndDate'},
                {'name':'placement'},
                {'name':'packageRoadblockId'},
                {'name':'packageRoadblock'},
                {'name':'placementSize'},
                {'name':'placementStartDate'},
                {'name':'placementStrategy'},
                {'name':'siteKeyname'}],
                'metricNames': ['clicks', 'impressions']
            }
        }

        # 4. Insert & run the report
        inserted_report = service.reports().insert(
            profileId=profile_id, body=report_body
        ).execute()

        run_response = service.reports().run(
            profileId=profile_id, reportId=inserted_report["id"]
        ).execute()

        report_file_id = run_response["id"]

        # 5. Poll until ready (or fail)
        start_time = time.time()
        max_wait = 3600
        sleep = 20
        max_sleep = 300

        def next_sleep_interval(s):
            return min(s * 2, max_sleep)

        while True:
            # Get file info
            status_response = service.files().get(
                reportId=inserted_report["id"],
                fileId=report_file_id
            ).execute()

            status = status_response["status"]

            # Success: File is available
            if status == "REPORT_AVAILABLE":
                self.logger.info("Report is ready for download.")
                break

            # Still processing or queued
            elif status in ("PROCESSING", "QUEUED"):
                pass  # just keep waiting

            # Failure or unknown status
            else:
                self.logger.error(f"Report failed with status: {status}")
                return

            # Check total wait time
            elapsed = time.time() - start_time
            if elapsed > max_wait:
                self.logger.error(
                    f"Report took too long to process. Waited {elapsed:.1f} seconds."
                )
                return

            # Otherwise, sleep with exponential backoff
            self.logger.info(f"Report status is {status}; sleeping {sleep} seconds.")
            time.sleep(sleep)
            sleep = next_sleep_interval(sleep)

        # 6. Download the CSV file into memory
        request = service.files().get_media(
            reportId=inserted_report["id"], fileId=report_file_id
        )
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                self.logger.info(f"Download {int(status.progress() * 100)}%.")

        # Define the schema fields in order
        schema_fields = [
            "placementId", "advertiser", "creativeType", "creativeId", "creative",
            "advertiserId", "campaignEndDate", "campaignId", "campaign", "campaignStartDate",
            "clickThroughUrl", "date", "placementCostStructure", "placementEndDate", "placement",
            "packageRoadblockId", "packageRoadblock", "placementSize", "placementStartDate",
            "placementStrategy", "siteKeyname", "clicks", "impressions"
        ]

        # Define the JSON schema
        schema = {
            "properties": {
                "placementId": {"type": "string"},
                "advertiser": {"type": "string"},
                "creativeType": {"type": "string"},
                "creativeId": {"type": "string"},
                "creative": {"type": "string"},
                "advertiserId": {"type": "string"},
                "campaignEndDate": {"type": "string"},
                "campaignId": {"type": "string"},
                "campaign": {"type": "string"},
                "campaignStartDate": {"type": "string"},
                "clickThroughUrl": {"type": "string"},
                "date": {"type": "string"},
                "placementCostStructure": {"type": "string"},
                "placementEndDate": {"type": "string"},
                "placement": {"type": "string"},
                "packageRoadblockId": {"type": "string"},
                "packageRoadblock": {"type": "string"},
                "placementSize": {"type": "string"},
                "placementStartDate": {"type": "string"},
                "placementStrategy": {"type": "string"},
                "siteKeyname": {"type": "string"},
                "clicks": {"type": "integer"},
                "impressions": {"type": "integer"}
            },
            "required": schema_fields
        }
        buf.seek(0)
        # Verify buffer content
        buf.seek(0)  # Reset buffer

        csv_reader = csv.reader(io.TextIOWrapper(buf, "utf-8"))
        start_reading = False  # Flag to start reading data rows

        for row in csv_reader:
            # Check for "Report Fields" marker to start processing rows
            if not start_reading:
                if  "Placement ID" in row:  # Strict header detection
                    start_reading = True
                continue

            # Ensure the row has the correct number of fields
            if len(row) != len(schema_fields):
                continue

            # Map the row values to schema fields using index
            row_dict = {}
            for i, value in enumerate(row):
                try:
                    if schema_fields[i] in ["clicks", "impressions"]:
                        row_dict[schema_fields[i]] = int(value)
                    else:
                        row_dict[schema_fields[i]] = value
                except ValueError:
                    row_dict[schema_fields[i]] = None  # Handle invalid numbers
            if row_dict.get("placementId") == "Grand Total":
                print("Skipping row:", row_dict)  # Debug skipped rows
                continue
            try:
                # Validate the mapped row against the schema
                validate(instance=row_dict, schema=schema)

                yield row_dict  # Yield the validated row as a dictionary
            except jsonschema.exceptions.ValidationError as e:
                # Handle validation errors (e.g., log or skip invalid rows)
                print(f"Validation error in row {row}: {e}")




