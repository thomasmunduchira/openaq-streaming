import datetime
import logging
import os
import requests
from requests.adapters import HTTPAdapter
import shelve
import time
from urllib3.util.retry import Retry

# logging.basicConfig(level=logging.DEBUG)

ENDPOINT = "https://api.openaq.org/v1/measurements"
MAX_RESULTS_TO_FETCH = 10000  # OpenAQ server side limit
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


http = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
http.mount("https://", HTTPAdapter(max_retries=retries))

# TODO: Enable sending diffs.
def fetch_data(dt, use_cache):
    # Use cache if user specifies the option and the cache for the date exists.
    if use_cache and os.path.isfile(filename(dt)):
        return fetch_from_cache(dt)

    date_str = dt.strftime("%Y-%m-%d")
    response = http.get(
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
