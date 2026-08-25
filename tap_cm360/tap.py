import os
import time
import io
import json
import csv
import datetime
import httplib2
from datetime import date, timedelta,datetime
from oauth2client.client import OAuth2Credentials
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

REPORT_DIMENSIONS = [
    "placementId", "advertiser", "dv360CreativeId", "dv360Creative",
    "dv360LineItem", "dv360LineItemId", "creativeType", "creativeId", "creative",
    "advertiserId", "campaignEndDate", "campaignId", "campaign", "campaignStartDate",
    "clickThroughUrl", "date", "placementCostStructure", "placementEndDate", "placement",
    "packageRoadblockId", "packageRoadblock", "placementSize", "placementStartDate",
    "placementStrategy", "site", "siteKeyname",
]
REPORT_METRICS = [
    "clicks", "impressions", "dv360Cost", "totalConversions",
    "richMediaVideoFirstQuartileCompletes", "richMediaVideoMidpoints",
    "richMediaVideoThirdQuartileCompletes", "richMediaVideoCompletions",
    "richMediaVideoPlays", "richMediaTrueViewViews",
]
INTEGER_METRICS = {
    "clicks", "impressions", "richMediaVideoFirstQuartileCompletes",
    "richMediaVideoMidpoints", "richMediaVideoThirdQuartileCompletes",
    "richMediaVideoCompletions", "richMediaVideoPlays", "richMediaTrueViewViews",
}
FLOAT_METRICS = {"dv360Cost", "totalConversions"}


def _report_fields(include_dv360: bool):
    if include_dv360:
        return list(REPORT_DIMENSIONS), list(REPORT_METRICS)
    dimensions = [name for name in REPORT_DIMENSIONS if "dv360" not in name.lower()]
    metrics = [name for name in REPORT_METRICS if "dv360" not in name.lower()]
    return dimensions, metrics


def _build_validation_schema(schema_fields):
    properties = {}
    for name in schema_fields:
        if name in INTEGER_METRICS:
            properties[name] = {"type": ["integer", "null"]}
        elif name in FLOAT_METRICS:
            properties[name] = {"type": ["number", "null"]}
        else:
            properties[name] = {"type": "string"}
    return {"properties": properties, "required": schema_fields}


def _fetch_report_records(stream, include_dv360: bool):
    """Create/run/download a CM360 CSV report and yield row data."""
    config = stream.config
    credentials_data = config["credential"]

    if isinstance(credentials_data, str):
        credentials_data = json.loads(credentials_data)

    if "user_agent" not in credentials_data:
        credentials_data["user_agent"] = "tap-cm360/1.0"

    credentials = OAuth2Credentials.from_json(json.dumps(credentials_data))
    http = credentials.authorize(httplib2.Http())
    service = build("dfareporting", "v5", http=http)

    profile_id = config["profile_id"]
    parsed_datetime = datetime.strptime(config["start_date"], "%Y-%m-%d")
    start_date = parsed_datetime.date()
    end_date = config.get("end_date") or date.today()

    dimension_names, metric_names = _report_fields(include_dv360)
    report_body = {
        "name": f"{stream.name} Standard Report",
        "type": "STANDARD",
        "fileName": f"{stream.name}_report",
        "format": "CSV",
        "criteria": {
            "dateRange": {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
            },
            "dimensions": [{"name": name} for name in dimension_names],
            "metricNames": metric_names,
        },
    }

    inserted_report = service.reports().insert(
        profileId=profile_id, body=report_body
    ).execute()

    run_response = service.reports().run(
        profileId=profile_id, reportId=inserted_report["id"]
    ).execute()

    report_file_id = run_response["id"]

    start_time = time.time()
    max_wait = 3600
    sleep = 20
    max_sleep = 300

    def next_sleep_interval(current_sleep):
        return min(current_sleep * 2, max_sleep)

    while True:
        status_response = service.files().get(
            reportId=inserted_report["id"],
            fileId=report_file_id,
        ).execute()

        status = status_response["status"]

        if status == "REPORT_AVAILABLE":
            stream.logger.info("Report is ready for download.")
            break

        if status not in ("PROCESSING", "QUEUED"):
            stream.logger.error(f"Report failed with status: {status}")
            return

        elapsed = time.time() - start_time
        if elapsed > max_wait:
            stream.logger.error(
                f"Report took too long to process. Waited {elapsed:.1f} seconds."
            )
            return

        stream.logger.info(f"Report status is {status}; sleeping {sleep} seconds.")
        time.sleep(sleep)
        sleep = next_sleep_interval(sleep)

    request = service.files().get_media(
        reportId=inserted_report["id"], fileId=report_file_id
    )
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            stream.logger.info(f"Download {int(status.progress() * 100)}%.")

    schema_fields = dimension_names + metric_names
    validation_schema = _build_validation_schema(schema_fields)

    buf.seek(0)
    csv_reader = csv.reader(io.TextIOWrapper(buf, "utf-8"))
    start_reading = False

    for row in csv_reader:
        if not start_reading:
            if "Placement ID" in row:
                start_reading = True
            continue

        if len(row) != len(schema_fields):
            continue

        row_dict = {}
        for i, value in enumerate(row):
            field_name = schema_fields[i]
            try:
                if field_name in INTEGER_METRICS:
                    row_dict[field_name] = int(value) if value else None
                elif field_name in FLOAT_METRICS:
                    row_dict[field_name] = float(value) if value else None
                else:
                    row_dict[field_name] = value
            except (ValueError, TypeError):
                row_dict[field_name] = None

        try:
            validate(instance=row_dict, schema=validation_schema)
            yield row_dict
        except jsonschema.exceptions.ValidationError as e:
            print(f"Validation error in row {row}: {e}")


class CM360ReportStream(Stream):
    """Streamer that downloads and parses a CM360 CSV report."""

    name = "cm360_report_stream"
    # Minimal schema. Adjust to your needs:
    schema = th.PropertiesList(
        th.Property("placementId", th.StringType, description="Unique ID for the placement"),
        th.Property("advertiser", th.StringType, description="Name of the advertiser"),
        th.Property("dv360CreativeId", th.StringType, description="DV360 Creative ID"),
        th.Property("dv360Creative", th.StringType, description="DV360 Creative name"),
        th.Property("dv360LineItem", th.StringType, description="DV360 Line Item name"),
        th.Property("dv360LineItemId", th.StringType, description="DV360 Line Item ID"),
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
        th.Property("site", th.StringType, description="Site Name"),
        th.Property("siteKeyname", th.StringType, description="Key name of the site for the placement"),
        th.Property("clicks", th.IntegerType, description="Number of clicks"),
        th.Property("impressions", th.IntegerType, description="Number of impressions"),
        th.Property("dv360Cost", th.NumberType, description="DV360 Cost in account currency"),
        th.Property("totalConversions", th.NumberType, description="Total conversions"),
        th.Property("richMediaVideoFirstQuartileCompletes", th.IntegerType, description="Video first quartile completions"),
        th.Property("richMediaVideoMidpoints", th.IntegerType, description="Video midpoint completions"),
        th.Property("richMediaVideoThirdQuartileCompletes", th.IntegerType, description="Video third quartile completions"),
        th.Property("richMediaVideoCompletions", th.IntegerType, description="Video completions"),
        th.Property("richMediaVideoPlays", th.IntegerType, description="Video plays"),
        th.Property("richMediaTrueViewViews", th.IntegerType, description="Video views"),
    ).to_dict()

    def fetch_secret_from_secret_manager(self, secret_id, project_id, version_id="1"):
        """
        Fetch a secret (JSON content) from Google Secret Manager.
        """
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.config["secret-content"]
        response = json.dumps(response)
        return response



    def get_records(self, context):
        """Main function to create/run/download the CM360 report and yield row data."""
        yield from _fetch_report_records(self, include_dv360=True)


class CM360NoDv360ReportStream(Stream):
    """CM360 report stream without DV360 dimensions or metrics."""

    name = "no_dv360_stream"

    @property
    def schema(self):
        dimension_names, metric_names = _report_fields(include_dv360=False)
        properties = []
        for name in dimension_names:
            properties.append(th.Property(name, th.StringType))
        for name in metric_names:
            prop_type = th.IntegerType if name in INTEGER_METRICS else th.NumberType
            properties.append(th.Property(name, prop_type))
        return th.PropertiesList(*properties).to_dict()

    def get_records(self, context):
        yield from _fetch_report_records(self, include_dv360=False)


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
            "secret-content": {
                "description": "Path to OAuth2 client secrets JSON."
            },
            "credential": {
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
            "secret-content",
            "credential",
            "start_date",
        ]
    }

    def discover_streams(self):
        """Return a list of streams."""
        return [CM360ReportStream(self), CM360NoDv360ReportStream(self)]

# If running directly (i.e., python tap_cm360.py --config …)
if __name__ == "__main__":
    Tapcm360.cli()
