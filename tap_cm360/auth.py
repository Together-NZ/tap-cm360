from google.auth import default

def authorize_with_adc(scopes):
    """
    Authorizes using Application Default Credentials for Google Cloud environments.

    :param scopes: List of scopes required for the authorization.
    :return: Credentials object.
    """
    credentials, _ = default(scopes=scopes)
    return credentials

