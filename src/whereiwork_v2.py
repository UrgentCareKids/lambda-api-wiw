#!/usr/bin/env python3
"""
Export When I Work data to CSV / JSON files.

Standard library only -- no pip install, no database.

    set -a; source .env; set +a
    python3 wiw_export.py                       # last 14 days + next 28
    python3 wiw_export.py --lookback 90 --lookahead 0
    python3 wiw_export.py --start 2026-01-01 --end 2026-07-01
    python3 wiw_export.py --entities shifts times --format csv

Writes one file per entity into ./export (override with --out).

Notes that come from the API spec rather than guesswork:

  * There is no pagination. `limit` on /2/shifts is a hard cap on
    results, not a page size, so it is never sent -- passing it would
    silently truncate the export. Shifts and times are pulled in date
    chunks instead.

  * /2/shifts returns only published, assigned, single-location shifts
    unless unpublished / include_open / include_allopen / all_locations
    are set. All four are set here; on your account they add roughly
    20% more rows.

  * Rate limiting comes back as HTTP 403, not 429, so 403 is retried
    with backoff rather than treated as an auth failure.
"""

import argparse
import csv
import datetime as dt
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_USER_ID = 53397802

LOGIN_URL = "https://api.login.wheniwork.com/login"
API_BASE = "https://api.wheniwork.com"
TIMEOUT = 60
MAX_RETRIES = 6
CHUNK_DAYS = 30

UTC = dt.timezone.utc
API_TS = "%Y-%m-%dT%H:%M:%S+00:00"


# Credential-ish fields the API returns that have no business sitting in
# an export. Dropped unless --include-secrets is passed.
SENSITIVE_FIELDS = {"password", "c2dm_auth_key", "token_valid_after",
                    "is_internal_login", "frontegg_id"}


# Column order for the CSVs, taken from the spec's component schemas.
# Any field the API returns that isn't listed gets appended on the end,
# so a new field shows up in the export instead of being dropped.
COLUMNS = {
    "users": ["id", "login_id", "account_id", "role", "type",
              "employment_type", "first_name", "middle_name", "last_name",
              "email", "phone_number", "employee_code", "hours_max",
              "hours_preferred", "hourly_rate", "activated", "is_active",
              "is_deleted", "is_hidden", "is_private", "is_trusted",
              "is_payroll", "is_onboarded", "hired_on", "start_date",
              "terminated_at", "timezone_name", "country_code",
              "exclude_from_payrolls", "notes", "positions", "locations",
              "position_rates", "last_login", "invited_at", "deleted_at",
              "delete_reason", "created_at", "created_by", "updated_at",
              "uuid"],
    "locations": ["id", "account_id", "name", "address", "latitude",
                  "longitude", "coordinates", "radius", "max_hours",
                  "place_id", "place_confirmed", "is_default", "is_deleted",
                  "deleted_at", "sort", "created_at", "updated_at",
                  "updated_by"],
    # is_deleted is absent from the spec's Position schema but the API
    # does return it.
    "positions": ["id", "account_id", "name", "color", "sort",
                  "tips_tracking", "is_deleted", "created_at", "updated_at",
                  "updated_by"],
    "sites": ["id", "account_id", "location_id", "name", "color",
              "description", "address", "postal_code", "latitude",
              "longitude", "coordinates", "radius", "place_id", "is_deleted",
              "deleted_at", "created_at", "updated_at", "updated_by"],
    "shifts": ["id", "account_id", "user_id", "location_id", "position_id",
               "site_id", "start_time", "end_time", "break_time", "color",
               "notes", "alerted", "shiftchain_key", "published",
               "published_date", "notified_at", "instances", "acknowledged",
               "acknowledged_at", "creator_id", "is_open", "is_shared",
               "is_trimmed", "block_id", "breaks", "linked_users",
               "requires_openshift_approval",
               "openshift_approval_request_id", "is_approved_without_time",
               "created_at", "updated_at"],
    "times": ["id", "account_id", "user_id", "creator_id", "position_id",
              "location_id", "site_id", "shift_id", "start_time", "end_time",
              "rounded_start_time", "rounded_end_time", "length",
              "rounded_length", "hourly_rate", "cash_tips", "notes",
              "break_hours", "alert_type", "is_alerted", "is_approved",
              "modified_by", "sync_id", "sync_hash", "split_time",
              "created_at", "updated_at"],
}

ENTITIES = {
    "users": {"path": "/2/users", "key": "users", "windowed": False,
              "params": {"show_deleted": "true", "show_pending": "true"}},
    "locations": {"path": "/2/locations", "key": "locations",
                  "windowed": False, "params": {}},
    "positions": {"path": "/2/positions", "key": "positions",
                  "windowed": False, "params": {"show_deleted": "true"}},
    "sites": {"path": "/2/sites", "key": "sites", "windowed": False,
              "params": {"include_deleted": "true"}},
    "shifts": {"path": "/2/shifts", "key": "shifts", "windowed": True,
               "params": {"unpublished": "true", "include_open": "true",
                          "include_allopen": "true", "all_locations": "true",
                          "deleted": "true"}},
    "times": {"path": "/2/times", "key": "times", "windowed": True,
              "params": {}},
}

ALL = ["users", "locations", "positions", "sites", "shifts", "times"]


# ------------------------------------------------------------------ http

class ApiError(RuntimeError):
    pass


def request(method, url, headers=None, body=None):
    last = None
    for attempt in range(MAX_RETRIES):
        req = urllib.request.Request(url, data=body, method=method,
                                     headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read().decode("utf-8").strip()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:400]
            if exc.code in (403, 429) or 500 <= exc.code < 600:
                if attempt < MAX_RETRIES - 1:
                    wait = min(2 ** attempt, 60)
                    retry_after = exc.headers.get("Retry-After") if exc.headers else None
                    if retry_after and str(retry_after).isdigit():
                        wait = min(int(retry_after), 120)
                    print("   HTTP %s, retrying in %ss" % (exc.code, wait))
                    time.sleep(wait)
                    last = exc
                    continue
            raise ApiError("%s %s -> HTTP %s: %s"
                           % (method, url, exc.code, detail))
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt < MAX_RETRIES - 1:
                time.sleep(min(2 ** attempt, 30))
                last = exc
                continue
            raise ApiError("%s %s -> %s" % (method, url, exc))
    raise ApiError("%s %s exhausted retries: %s" % (method, url, last))


def login(key, email, password):
    payload = request("POST", LOGIN_URL,
                      headers={"Content-Type": "application/json",
                               "Accept": "application/json", "W-Key": key},
                      body=json.dumps({"email": email,
                                       "password": password}).encode("utf-8"))
    for holder in (payload, payload.get("login") or {},
                   payload.get("person") or {}):
        if isinstance(holder, dict) and holder.get("token"):
            return holder["token"]
    raise ApiError("Login returned no token. Keys: %s" % sorted(payload.keys()))


def get(path, token, user_id, params=None):
    url = "%s/%s" % (API_BASE, path.lstrip("/"))
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {"Accept": "application/json",
               "Authorization": "Bearer %s" % token}
    if user_id:
        headers["W-UserId"] = str(user_id)
    return request("GET", url, headers=headers)


# ------------------------------------------------------------------ pull

def chunks(start, end, days):
    cursor = start
    step = dt.timedelta(days=days)
    while cursor < end:
        stop = min(cursor + step, end)
        yield cursor, stop
        cursor = stop


def fetch(name, token, user_id, window):
    spec = ENTITIES[name]
    base = dict(spec["params"])

    if not spec["windowed"]:
        body = get(spec["path"], token, user_id, base)
        return body.get(spec["key"]) or [], []

    rows, seen, deleted = [], set(), []
    for chunk_start, chunk_end in chunks(window[0], window[1], CHUNK_DAYS):
        params = dict(base)
        params["start"] = chunk_start.strftime(API_TS)
        params["end"] = chunk_end.strftime(API_TS)
        print("   %s -> %s" % (chunk_start.date(), chunk_end.date()))

        body = get(spec["path"], token, user_id, params)
        deleted.extend(body.get("deleted_ids") or [])

        # Records straddling a chunk boundary come back twice.
        for rec in (body.get(spec["key"]) or []):
            rid = rec.get("id")
            if rid is not None and rid in seen:
                continue
            if rid is not None:
                seen.add(rid)
            rows.append(rec)

    return rows, sorted(set(deleted))


# ----------------------------------------------------------------- write

def flatten(value):
    """CSV cells must be scalar; nest anything else as compact JSON."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"))
    return value


def write_csv(path, name, records, include_secrets=False):
    columns = list(COLUMNS.get(name, []))
    extra = sorted({k for r in records for k in r} - set(columns))
    columns += extra
    if not include_secrets:
        columns = [c for c in columns if c not in SENSITIVE_FIELDS]
        extra = [c for c in extra if c not in SENSITIVE_FIELDS]

    # utf-8-sig so Excel opens accented names correctly instead of
    # showing mojibake.
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns,
                                extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow({c: flatten(rec.get(c)) for c in columns})

    return columns, extra


def write_json(path, records, include_secrets=False):
    if not include_secrets:
        records = [{k: v for k, v in r.items() if k not in SENSITIVE_FIELDS}
                   for r in records]
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=1, ensure_ascii=False)


# ------------------------------------------------------------------ main

def parse_date(text):
    d = dt.datetime.fromisoformat(text)
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d


def main():
    ap = argparse.ArgumentParser(
        description="Export When I Work data to files.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--key", default=os.environ.get("WIW_KEY"))
    ap.add_argument("--email", default=os.environ.get("WIW_EMAIL"))
    ap.add_argument("--password", default=os.environ.get("WIW_PASSWORD"))
    ap.add_argument("--user-id",
                    default=os.environ.get("WIW_USER_ID") or DEFAULT_USER_ID)
    ap.add_argument("--entities", nargs="+", default=ALL, choices=ALL,
                    metavar="NAME")
    ap.add_argument("--lookback", type=int, default=14, metavar="DAYS")
    ap.add_argument("--lookahead", type=int, default=28, metavar="DAYS")
    ap.add_argument("--start", help="YYYY-MM-DD, overrides --lookback")
    ap.add_argument("--end", help="YYYY-MM-DD, overrides --lookahead")
    ap.add_argument("--out", default="./export", metavar="DIR")
    ap.add_argument("--format", default="csv",
                    choices=["csv", "json", "both"])
    ap.add_argument("--stamp", action="store_true",
                    help="Append the run date to filenames.")
    ap.add_argument("--include-secrets", action="store_true",
                    help="Keep credential fields (%s) that are dropped by "
                         "default." % ", ".join(sorted(SENSITIVE_FIELDS)))
    args = ap.parse_args()

    if not args.key:
        args.key = input("Developer key: ").strip()
    if not args.email:
        args.email = input("Email: ").strip()
    if not args.password:
        args.password = getpass.getpass("Password: ")

    today = dt.datetime.now(UTC).replace(hour=0, minute=0, second=0,
                                         microsecond=0)
    start = parse_date(args.start) if args.start else today - dt.timedelta(days=args.lookback)
    end = parse_date(args.end) if args.end else today + dt.timedelta(days=args.lookahead + 1)
    if end <= start:
        print("end must be after start", file=sys.stderr)
        return 2

    os.makedirs(args.out, exist_ok=True)
    suffix = "_" + dt.datetime.now().strftime("%Y%m%d") if args.stamp else ""

    print("\nWindow  %s -> %s" % (start.date(), end.date()))
    print("Output  %s\n" % os.path.abspath(args.out))

    token = login(args.key, args.email, args.password)
    print("Logged in as %s (user_id %s)\n" % (args.email, args.user_id))

    written, failures = [], []

    for name in args.entities:
        print("%s" % name)
        try:
            records, deleted = fetch(name, token, args.user_id, (start, end))
        except ApiError as exc:
            print("   FAILED: %s\n" % exc)
            failures.append(name)
            continue

        base = os.path.join(args.out, name + suffix)
        extra = []

        if args.format in ("csv", "both"):
            _, extra = write_csv(base + ".csv", name, records,
                                 args.include_secrets)
            written.append((name, len(records), base + ".csv"))
        if args.format in ("json", "both"):
            write_json(base + ".json", records, args.include_secrets)
            written.append((name, len(records), base + ".json"))

        dropped = sorted({k for r in records for k in r} & SENSITIVE_FIELDS)
        if dropped and not args.include_secrets:
            print("   dropped credential fields: %s" % ", ".join(dropped))
        if extra:
            print("   extra fields appended as columns: %s" % ", ".join(extra))
        if deleted:
            path = os.path.join(args.out, "%s_deleted_ids%s.csv" % (name, suffix))
            with open(path, "w", newline="", encoding="utf-8") as handle:
                w = csv.writer(handle)
                w.writerow(["id"])
                w.writerows([[i] for i in deleted])
            print("   %d ids reported deleted -> %s"
                  % (len(deleted), os.path.basename(path)))

        print("   %d records\n" % len(records))

    print("Written")
    for name, count, path in written:
        size = os.path.getsize(path)
        print("   %-26s %7d rows  %8.1f KB"
              % (os.path.basename(path), count, size / 1024.0))

    if failures:
        print("\nFailed: %s" % ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except ApiError as exc:
        print("\n%s" % exc, file=sys.stderr)
        sys.exit(1)