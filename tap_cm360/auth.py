"""cm360 Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import flow_from_clientsecrets
from oauth2client.tools import run_flow
from oauth2client.client import Credentials
from io import StringIO
import json
import httplib2
# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
def authorize_in_memory(client_secrets_content, stored_credentials=None, scopes=None):
    """
    Authorizes the application using client secrets and credentials provided in memory.

    :param client_secrets_content: JSON string containing the client secrets.
    :param stored_credentials: JSON string containing previously stored credentials, if any.
    :param scopes: List of scopes required for the authorization.
    :return: Authorized HTTP object.
    """
    # Load client secrets from the provided content
    client_secrets_io = StringIO(client_secrets_content)

    # Create the OAuth2 flow
    flow = flow_from_clientsecrets(
        client_secrets_io,
        scope=scopes
    )

    # Load credentials if provided
    credentials = None
    if stored_credentials:
        credentials = Credentials.new_from_json(stored_credentials)

    # If no credentials or invalid credentials, run the OAuth flow
    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, None)

    # Authorize HTTP
    http = credentials.authorize(httplib2.Http())
    return http, credentials.to_json()