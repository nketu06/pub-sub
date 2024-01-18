from avro.io import BinaryEncoder, DatumWriter
import avro.schema as schema
import io
from google.cloud.pubsub import PublisherClient
import threading

# TODO(developer): Replace these variables before running the sample.
# project_id = "ketupub"
# topic_id = "ketu-new-topic"
avsc_file = "user.avsc"

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

# Prepare to write Avro records to the binary output stream.
avro_schema = schema.parse(open(avsc_file, "rb").read())
writer = DatumWriter(avro_schema)

# Prepare some data using a Python dictionary that matches the Avro schema
records = [
    {"name": "Alaska", "favorite_number": 76, "favorite_color": "black"},
    {"name": "ram", "favorite_number": 64, "favorite_color": "red"},
    {"name": "mohit", "favorite_number": 9, "favorite_color": "gr"},
    {"name": "raman", "favorite_number": 11, "favorite_color": "ora"},
    {"name": "ketu", "favorite_number": 6, "favorite_color": "cyan"}
]

#publish to topic
def publish_message(record):
    bout = io.BytesIO()  # Reset the buffer for each message
    encoder = BinaryEncoder(bout)
    writer.write(record, encoder)
    data = bout.getvalue()
    print(f"Preparing a binary-encoded message:\n{data.hex()}")
    try:
        future = publisher_client.publish(topic_path, data=data)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"Error publishing message: {e}")

# Loop through records and publish each one
def publish_messages(messages):
    for message in messages: 
        print(message)
        publish_message(message)


# used to publish using array with multithreading
def pubish_to_topic_arr(records):
    num_threads=3
    threads = []
    for i in range(num_threads):
        start_index = i * (len(records) // num_threads)
        if i==num_threads-1:
            end_index=len(records)
        else:
            end_index = (i + 1) * (len(records) // num_threads)
        thread_messages = records[start_index:end_index]
        
        thread = threading.Thread(target=publish_messages, args=(thread_messages,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

# used to publish to topic using queue
def pubish_to_topic_queue(data_queue):
    while not data_queue.empty():
        message = vars(data_queue.get())
        publish_message(message)


# used to publish to topic using multithreading with queue
def pubish_to_topic_queue_threading(data_queue):
    def publish_worker():
        while True:
            message = data_queue.get()
            if message==None:
                break
            publish_message(vars(message))

    # Number of threads
    num_threads = 3
    for _ in range(num_threads):
        data_queue.put(None)

    # Create and start worker threads
    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=publish_worker)
        thread.start()
        threads.append(thread)

    # Wait for all worker threads to finish
    for thread in threads:
        thread.join()





