"""
Lambda entry point: pull When I Work, load it into easebase.

    CMD [ "app.handler" ]   # matches the existing Dockerfile

Event (all optional):
    {"entities": ["shifts", "times"]}   default: all six
    {"lookback": 90, "lookahead": 0}    default: 14 / 28 (WIW_LOOKBACK,
                                        WIW_LOOKAHEAD to change)
    {"start": "2026-01-01", "end": "2026-07-01"}    explicit window

Credentials come from Parameter Store on both sides:
    api_wheniwork_internal          -> db/wiw_conn.py
    db_postgres_easebase_internal   -> db/easebase_conn.py

Commits are per entity, so a failure loading `times` doesn't roll back
`shifts`. Failures are collected, printed, then raised at the end so the
invocation shows as errored and your alarm fires.
"""

import argparse
import datetime as dt
import json
import os
import sys

import psycopg2

from db.easebase_conn import get_conn
from db.wiw_conn import wiw_conn
import loader
from whereiwork_v2 import ALL, ApiError, fetch, parse_date

UTC = dt.timezone.utc


def resolve_window(event):
    today = dt.datetime.now(UTC).replace(hour=0, minute=0, second=0,
                                         microsecond=0)
    lookback = int(event.get("lookback", os.environ.get("WIW_LOOKBACK", 14)))
    lookahead = int(event.get("lookahead", os.environ.get("WIW_LOOKAHEAD", 28)))

    start = (parse_date(event["start"]) if event.get("start")
             else today - dt.timedelta(days=lookback))
    end = (parse_date(event["end"]) if event.get("end")
           else today + dt.timedelta(days=lookahead + 1))
    if end <= start:
        raise ValueError("end must be after start (%s -> %s)" % (start, end))
    return start, end


def lambda_handler(event, context=None):
    event = event or {}
    entities = event.get("entities") or ALL
    unknown = [e for e in entities if e not in ALL]
    if unknown:
        raise ValueError("Unknown entities: %s" % ", ".join(unknown))

    loader.clear_warnings()
    start, end = resolve_window(event)
    synced_at = dt.datetime.now(UTC)

    token, user_id = wiw_conn()
    conn = get_conn()

    summary = {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "target": "%s.%s*" % (loader.SCHEMA, loader.PREFIX),
        "entities": {},
        "failures": [],
    }

    for name in entities:
        print(name)
        try:
            records, deleted = fetch(name, token, user_id, (start, end))
        except ApiError as exc:
            print("   FETCH FAILED: %s" % exc)
            summary["failures"].append({"entity": name, "stage": "fetch",
                                        "error": str(exc)})
            continue

        try:
            with conn.cursor() as cur:
                loader.ensure_table(cur, name)
                written, skipped = loader.upsert(cur, name, records, synced_at)
                flagged = loader.flag_deleted(cur, name, deleted, synced_at)
            conn.commit()
        except psycopg2.Error as exc:
            conn.rollback()
            print("   LOAD FAILED: %s" % exc)
            summary["failures"].append({"entity": name, "stage": "load",
                                        "error": str(exc)})
            continue

        summary["entities"][name] = {"fetched": len(records),
                                     "upserted": written,
                                     "skipped_no_id": skipped,
                                     "flagged_deleted": flagged}
        print("   %d fetched, %d upserted, %d flagged deleted\n"
              % (len(records), written, flagged))

    warned = loader.warnings()
    if warned:
        summary["coercion_warnings"] = warned
        print("Values that didn't fit their column (kept in raw):")
        for column, sample in sorted(warned.items()):
            print("   %-32s e.g. %s" % (column, sample))

    print(json.dumps(summary, indent=1))

    if summary["failures"]:
        raise RuntimeError("Sync completed with failures: %s"
                           % ", ".join(f["entity"]
                                       for f in summary["failures"]))
    return summary


# The Dockerfile CMD is app.handler; lambda_handler is kept as an alias so
# either name works if the CMD is ever overridden in the console.
handler = lambda_handler


def main():
    """Local runs: python app.py --lookback 7 --entities shifts"""
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--entities", nargs="+", default=None, choices=ALL,
                    metavar="NAME")
    ap.add_argument("--lookback", type=int)
    ap.add_argument("--lookahead", type=int)
    ap.add_argument("--start")
    ap.add_argument("--end")
    args = ap.parse_args()

    event = {k: v for k, v in vars(args).items() if v is not None}
    try:
        lambda_handler(event)
    except (ApiError, RuntimeError, ValueError) as exc:
        print("\n%s" % exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())