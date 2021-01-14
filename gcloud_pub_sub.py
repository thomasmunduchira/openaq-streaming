import argparse
from google.cloud import pubsub_v1
from datetime import date

# Resolve the publish future in a separate thread.
def callback(future):
    message_id = future.result()
    print(message_id)


def publish_messages(publisher, topic_path):
    for n in range(1, 10):
        data = "Message number {}".format(n)
        # Data must be a bytestring
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        # Non-blocking. Allow the publisher client to batch multiple messages.
        future.add_done_callback(callback)
    print(f"Added messages for {topic_path} to batch queue.")


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

    publish_messages(publisher, topic_path)
