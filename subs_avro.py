import avro.schema as schema
from avro.io import BinaryDecoder, DatumReader
from concurrent.futures import TimeoutError
import io
import json
from google.cloud.pubsub import SubscriberClient

# TODO(developer)
project_id = "ketupub"
subscription_id = "ketu-new-topic-sub"
avsc_file = "user.avsc"
# Number of seconds the subscriber listens for messages
timeout = 8.0

subscriber = SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

with open(avsc_file, "rb") as file:
    avro_schema = schema.parse(file.read())

def callback(message) -> None:
    # Get the message serialization type.
    encoding = message.attributes.get("googclient_schemaencoding")
    # Deserialize the message data accordingly.
    if encoding == "BINARY":
        bout = io.BytesIO(message.data)
        decoder = BinaryDecoder(bout)
        reader = DatumReader(avro_schema)
        message_data = reader.read(decoder)
        print(f"Received a binary-encoded message:\n{message_data}")
    elif encoding == "JSON":
        message_data = json.loads(message.data)
        print(f"Received a JSON-encoded message:\n{message_data}")
    else:
        print(f"Received a message with no encoding:\n{message}")

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception occurs first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.