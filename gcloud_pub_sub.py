import argparse
from datetime import date
import json
import time

from dateutil.rrule import rrule, DAILY
from google.cloud import pubsub_v1

import stream_openaq

# Resolve the publish future in a separate thread.
def callback(future):
    message_id = future.result()
    print(message_id)


def publish_messages(publisher, topic_path, start_date, end_date, pull_frequency):
    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        dt_str = dt.strftime("%Y-%m-%d")
        print(f"Fetching records from {dt_str}.")
        records = stream_openaq.fetch_data(dt)
        print(f"{len(records)} records fetched for {dt_str}.")
        for record in records:
            # Data must be a bytestring
            data = json.dumps(record, separators=(",", ":"))  # compact encoding
            future = publisher.publish(topic_path, data.encode("UTF-8"))
            # Non-blocking. Allow the publisher client to batch multiple messages.
            future.add_done_callback(callback)
        print(f"Added records from {dt_str} to batch queue.")
        time.sleep(pull_frequency)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream data from OpenAQ to Google Cloud Pub/Sub."
    )
    parser.add_argument(
        "--project_id",
        help="Google Cloud project ID that the pubsub topic falls under",
        required=True,
    )
    parser.add_argument(
        "--topic_id", help="Google Cloud Pub/Sub topic ID", required=True
    )
    parser.add_argument(
        "--start_date",
        type=date.fromisoformat,
        default=date.today(),
        help="start date to stream results for",
    )
    parser.add_argument(
        "--end_date",
        type=date.fromisoformat,
        default=date.today(),
        help="end date to stream results for",
    )
    parser.add_argument(
        "--pull_frequency",
        type=int,
        default=10,
        help="how often to pull a day's worth of data from the OpenAQ API in seconds",
    )
    parser.add_argument(
        "--batch_max_messages",
        type=int,
        default=1000,
        help="max amount of messages before a batch of messages should be sent to the pubsub topic",
    )
    parser.add_argument(
        "--batch_max_bytes",
        type=int,
        default=1024,
        help="max amount of bytes before a batch of messages should be sent to the pubsub topic",
    )
    parser.add_argument(
        "--batch_max_latency",
        type=int,
        default=10,
        help="max amount of latency in seconds before a batch of messages should be sent to the pubsub topic",
    )
    args = parser.parse_args()

    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=args.batch_max_messages,  # default 100
        max_bytes=args.batch_max_bytes,  # default 1 MB
        max_latency=args.batch_max_latency,  # default 0.01 s
    )
    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(args.project_id, args.topic_id)

    publish_messages(
        publisher, topic_path, args.start_date, args.end_date, args.pull_frequency
    )
