import requests
import time

ENDPOINT = "https://api.openaq.org/v1/measurements"
MAX_RESULTS_TO_FETCH = 5  # small limit for now


# TODO: Save seen records to enable sending diffs and also to not have to rely on endpoint being up to run this.
def fetch_data(date):
    date_str = date.strftime("%Y-%m-%d")
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
    return response.json()["results"]
