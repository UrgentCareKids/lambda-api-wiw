"""
Destination side of the sync: schema management and upserts.

Tables are stg.s_wiw_<entity> (WIW_SCHEMA / WIW_TABLE_PREFIX to change).
They are created on first run and ALTERed additively on every run, so a
field added to COLUMNS in whereiwork_v2.py appears without a migration.

Column types are best-effort -- the API is inconsistent about a few
fields. A value that doesn't fit its column lands as NULL and is logged
once, but every row also keeps the untouched API record in `raw` jsonb,
so nothing is actually lost.
"""

import datetime as dt
import json
import os

from psycopg2.extras import Json, execute_values

from whereiwork_v2 import COLUMNS, SENSITIVE_FIELDS

UTC = dt.timezone.utc

# stg.s_wiw_users, stg.s_wiw_shifts, ... Identifiers are quoted in the
# generated SQL, so these are used exactly as written -- keep them
# lowercase unless you want to have to quote the names when querying.
SCHEMA = os.environ.get("WIW_SCHEMA", "stg")
PREFIX = os.environ.get("WIW_TABLE_PREFIX", "s_wiw_")
PAGE_SIZE = 500


# ---------------------------------------------------------------- typing

# Column name -> Postgres type. Names are consistent across the six
# entities, so one map covers all of them. Anything not listed is text,
# which is the safe default: the API occasionally returns a string where
# you'd expect a number and text never rejects it.
COLUMN_TYPES = {
    # keys and references
    "id": "bigint", "account_id": "bigint", "login_id": "bigint",
    "user_id": "bigint", "location_id": "bigint", "position_id": "bigint",
    "site_id": "bigint", "shift_id": "bigint", "creator_id": "bigint",
    "created_by": "bigint", "updated_by": "bigint", "modified_by": "bigint",
    "openshift_approval_request_id": "bigint", "sort": "bigint",

    # measures
    "hours_max": "numeric", "hours_preferred": "numeric",
    "hourly_rate": "numeric", "latitude": "numeric", "longitude": "numeric",
    "radius": "numeric", "max_hours": "numeric", "break_time": "numeric",
    "length": "numeric", "rounded_length": "numeric", "cash_tips": "numeric",
    "break_hours": "numeric",

    # flags
    "is_active": "boolean", "is_deleted": "boolean", "is_hidden": "boolean",
    "is_private": "boolean", "is_trusted": "boolean", "is_payroll": "boolean",
    "is_onboarded": "boolean", "exclude_from_payrolls": "boolean",
    "is_default": "boolean", "place_confirmed": "boolean",
    "published": "boolean", "alerted": "boolean", "acknowledged": "boolean",
    "is_open": "boolean", "is_shared": "boolean", "is_trimmed": "boolean",
    "requires_openshift_approval": "boolean",
    "is_approved_without_time": "boolean", "is_alerted": "boolean",
    "is_approved": "boolean",

    # timestamps
    "created_at": "timestamptz", "updated_at": "timestamptz",
    "deleted_at": "timestamptz", "terminated_at": "timestamptz",
    "hired_on": "timestamptz", "start_date": "timestamptz",
    "last_login": "timestamptz", "invited_at": "timestamptz",
    "start_time": "timestamptz", "end_time": "timestamptz",
    "rounded_start_time": "timestamptz", "rounded_end_time": "timestamptz",
    "published_date": "timestamptz", "notified_at": "timestamptz",
    "acknowledged_at": "timestamptz", "split_time": "timestamptz",

    # nested structures
    "positions": "jsonb", "locations": "jsonb", "position_rates": "jsonb",
    "coordinates": "jsonb", "instances": "jsonb", "breaks": "jsonb",
    "linked_users": "jsonb",
}

# Deliberately left as text because the API is inconsistent about them:
# role, type, employment_type, alert_type, tips_tracking, activated,
# place_id, uuid, sync_id, sync_hash, block_id, shiftchain_key,
# employee_code, phone_number.

INDEXES = {
    "users": ["account_id", "is_active"],
    "locations": ["account_id"],
    "positions": ["account_id"],
    "sites": ["location_id"],
    "shifts": ["user_id", "location_id", "start_time"],
    "times": ["user_id", "shift_id", "start_time"],
}

NULL_TIMESTAMPS = {"", "0", "null", "none", "0000-00-00",
                   "0000-00-00 00:00:00", "0000-00-00t00:00:00+00:00"}
TRUE_VALUES = {"1", "t", "true", "y", "yes"}
FALSE_VALUES = {"0", "f", "false", "n", "no", ""}

_warnings = {}


def _warn(column, value):
    """Record the first bad value per column instead of spamming the log."""
    _warnings.setdefault(column, repr(value)[:80])


def column_type(name):
    return COLUMN_TYPES.get(name, "text")


def coerce(value, sqltype, column):
    if value is None:
        return None

    if sqltype == "jsonb":
        return None if value == "" else Json(value)

    if isinstance(value, str) and not value.strip():
        return None

    if sqltype == "timestamptz":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return dt.datetime.fromtimestamp(value, UTC)
        text = str(value).strip().lower()
        if text in NULL_TIMESTAMPS or text.startswith("0000-00-00"):
            return None
        return value  # Postgres parses the ISO-8601 string itself

    if sqltype == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in TRUE_VALUES:
            return True
        if text in FALSE_VALUES:
            return False
        _warn(column, value)
        return None

    if sqltype == "bigint":
        if isinstance(value, bool):
            return int(value)
        try:
            return int(float(value))
        except (TypeError, ValueError):
            _warn(column, value)
            return None

    if sqltype == "numeric":
        try:
            return float(value)
        except (TypeError, ValueError):
            _warn(column, value)
            return None

    if isinstance(value, (dict, list)):
        # An unexpected structure in a text column -- keep it readable.
        return json.dumps(value, separators=(",", ":"))
    return value


# ------------------------------------------------------------------ ddl

def table_columns(entity):
    """Exported columns for an entity, credential fields removed."""
    return [c for c in COLUMNS[entity] if c not in SENSITIVE_FIELDS]


def table_name(entity):
    return '"%s"."%s%s"' % (SCHEMA, PREFIX, entity)


def ensure_table(cur, entity):
    columns = table_columns(entity)
    body = ['"id" bigint PRIMARY KEY']
    body += ['"%s" %s' % (c, column_type(c))
             for c in columns if c != "id"]
    body += ['"raw" jsonb',
             '"_api_deleted" boolean NOT NULL DEFAULT false',
             '"_synced_at" timestamptz NOT NULL DEFAULT now()']
    
    cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (SCHEMA,))
    if not cur.fetchone():
        raise RuntimeError(
            'Schema "%s" does not exist. Create it (and grant this role '
            "USAGE, CREATE on it) before running the sync." % SCHEMA)

    cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)"
                % (table_name(entity), ", ".join(body)))

    # Additive migration: a field added to COLUMNS later appears here on
    # the next run. Nothing is ever dropped or retyped automatically.
    for column in columns:
        if column == "id":
            continue
        cur.execute('ALTER TABLE %s ADD COLUMN IF NOT EXISTS "%s" %s'
                    % (table_name(entity), column, column_type(column)))

    for column in INDEXES.get(entity, []):
        if column not in columns:
            continue
        cur.execute('CREATE INDEX IF NOT EXISTS "idx_%s%s_%s" ON %s ("%s")'
                    % (PREFIX, entity, column, table_name(entity), column))


# --------------------------------------------------------------- upsert

def upsert(cur, entity, records, synced_at):
    columns = table_columns(entity)
    insert_columns = columns + ["raw", "_synced_at"]

    # ON CONFLICT cannot touch the same row twice in one statement. Keep
    # the first occurrence of an id, which is what fetch() does for
    # windowed entities; this covers the rest.
    unique, skipped = {}, 0
    for record in records:
        rid = record.get("id")
        if rid is None:
            skipped += 1
            continue
        unique.setdefault(rid, record)

    rows = []
    for record in unique.values():
        values = [coerce(record.get(c), column_type(c), c) for c in columns]
        values.append(Json({k: v for k, v in record.items()
                            if k not in SENSITIVE_FIELDS}))
        values.append(synced_at)
        rows.append(tuple(values))

    if not rows:
        return 0, skipped

    assignments = ", ".join('"%s" = EXCLUDED."%s"' % (c, c)
                            for c in insert_columns if c != "id")
    sql = ('INSERT INTO %s (%s) VALUES %%s '
           'ON CONFLICT ("id") DO UPDATE SET %s'
           % (table_name(entity),
              ", ".join('"%s"' % c for c in insert_columns),
              assignments))

    execute_values(cur, sql, rows, page_size=PAGE_SIZE)
    return len(rows), skipped


def flag_deleted(cur, entity, deleted_ids, synced_at):
    ids = [int(i) for i in deleted_ids if str(i).strip().isdigit()]
    if not ids:
        return 0
    cur.execute('UPDATE %s SET "_api_deleted" = true, "_synced_at" = %%s '
                'WHERE "id" = ANY(%%s) AND "_api_deleted" IS DISTINCT FROM true'
                % table_name(entity), (synced_at, ids))
    return cur.rowcount


def warnings():
    """Columns whose values didn't fit, with a sample of the offender."""
    return dict(_warnings)


def clear_warnings():
    _warnings.clear()