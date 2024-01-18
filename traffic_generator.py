from dataclasses import dataclass
import threading
import queue
from generator import generateData
from publish_avro import pubish_to_topic_queue_threading,pubish_to_topic_queue


def data_generator_worker(q, num_data):
    data_generator = generateData()
    for _ in range(num_data):
        data = data_generator.getData()
        q.put(data)

# Number of threads
num_threads = 5
total_data = 27  # Set to any desired number

# Calculate the number of data per thread
num_data_per_thread = total_data // num_threads

# Create a queue
data_queue = queue.Queue()

# Create and start data generation threads
threads = []

for _ in range(num_threads):
    thread = threading.Thread(target=data_generator_worker, args=(data_queue, num_data_per_thread))
    thread.start()
    threads.append(thread)

# If the total data is not divisible by the number of threads, handle the remaining data
remaining_data = total_data % num_threads
if remaining_data > 0:
    last_thread = threading.Thread(target=data_generator_worker, args=(data_queue, remaining_data))
    last_thread.start()
    threads.append(last_thread)

# Wait for all data generation threads to finish
for thread in threads:
    thread.join()

# Print the data from the queue (just for verification)
# print(data_queue.qsize())
# while not data_queue.empty():
#     data = data_queue.get()
#     print(vars(data))


# print("All data generated and put in the queue.")
# pubish_to_topic_queue(data_queue)
# pubish_to_topic_queue_threading(data_queue)
