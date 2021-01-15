import datetime
import os
import requests
import shelve
import time

ENDPOINT = "https://api.openaq.org/v1/measurements"
MAX_RESULTS_TO_FETCH = 10  # small limit for now
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


# TODO: Enable sending diffs.
def fetch_data(dt, use_cache):
    # Use cache if user specifies the option and the cache for the date exists.
    if use_cache and os.path.isfile(filename(dt)):
        return fetch_from_cache(dt)

    date_str = dt.strftime("%Y-%m-%d")
    # TODO: Add timeout/retries.
    response = requests.get(
        ENDPOINT,
        params={
            "country": "US",  # limit to US for now
            "has_geo": True,  # limit to records that have geographic info attached
            "date_from": date_str,
            "date_to": date_str,
            "limit": MAX_RESULTS_TO_FETCH,
            "page": 1,  # TODO: Add support for overflow.
        },
    )
    # TODO: catch errors.
    records = persist_to_cache(dt, response.json()["results"])
    return records


def record_key(record):
    return ";".join(
        (
            record["location"],
            record["city"],
            record["country"],
            record["parameter"],
            record["date"]["utc"],
        )
    )


def filename(dt):
    return os.path.join(ROOT_DIR, "cached", dt.strftime("openaq_%Y%m%d"))


def persist_to_cache(dt, records):
    with shelve.open(filename(dt)) as db:
        db.clear()
        for record in records:
            db[record_key(record)] = record
        return list(db.values())


def fetch_from_cache(dt):
    with shelve.open(filename(dt)) as db:
        return list(db.values())
