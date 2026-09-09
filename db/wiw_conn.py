"""
When I Work credentials, pulled from SSM Parameter Store.

Same shape as easebase_conn.py: one function that reads a SecureString
parameter and hands back something you can use. easebase_conn() returns a
live psycopg2 connection; wiw_conn() returns an authenticated API token.

Parameter (SecureString), default name 'api_wheniwork_internal':

    {
      "key": "<developer key>",
      "email": "<service account email>",
      "password": "<service account password>",
      "user_id": 53397802
    }
"""

import json
import os
import time

import boto3

from whereiwork_v2 import DEFAULT_USER_ID, login

PARAM_NAME = os.environ.get("WIW_SSM_PARAM", "api_wheniwork_internal")

# The API token outlives a single invocation, so a warm container reuses
# it instead of logging in again. Short enough that a revoked password
# stops working within the hour.
TOKEN_TTL = 1800

_ssm = None
_credentials = None
_token = None
_token_at = 0.0


def _client():
    global _ssm
    if _ssm is None:
        _ssm = boto3.client("ssm")
    return _ssm


def wiw_credentials(refresh=False):
    """{'key', 'email', 'password', 'user_id'} from Parameter Store."""
    global _credentials
    if _credentials is not None and not refresh:
        return _credentials

    param = _client().get_parameter(Name=PARAM_NAME, WithDecryption=True)
    stored = json.loads(param["Parameter"]["Value"])

    missing = [f for f in ("key", "email", "password") if not stored.get(f)]
    if missing:
        raise RuntimeError("SSM parameter %s is missing: %s"
                           % (PARAM_NAME, ", ".join(missing)))

    _credentials = {
        "key": stored["key"],
        "email": stored["email"],
        "password": stored["password"],
        "user_id": stored.get("user_id") or DEFAULT_USER_ID,
    }
    return _credentials


def wiw_conn(refresh=False):
    """(token, user_id) for the When I Work API."""
    global _token, _token_at
    if _token and not refresh and (time.time() - _token_at) < TOKEN_TTL:
        return _token, wiw_credentials()["user_id"]

    creds = wiw_credentials(refresh=refresh)
    _token = login(creds["key"], creds["email"], creds["password"])
    _token_at = time.time()
    return _token, creds["user_id"]