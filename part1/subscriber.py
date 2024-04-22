import json
import os
from datetime import datetime
from google.cloud import pubsub_v1

project_id = "devm-420400"
subscription_id = "my-sub"
message_count = 0
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    global message_count
    print(f"Received message: {message.data.decode('utf-8')}")
    data = json.loads(message.data.decode('utf-8'))
    save_data_to_daily_file(data)
    message.ack()
    message_count += 1

def save_data_to_daily_file(data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    directory = '/home/snutheti/data'
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = f"{directory}/{date_str}.json"
    with open(filename, 'a') as file:
        json.dump(data, file)
        file.write('\n')

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

with subscriber:
    try:
        # Removed the timeout parameter so the listener runs indefinitely
        streaming_pull_future.result()
    except KeyboardInterrupt:
        # Handles Ctrl+C interrupt, allowing for a graceful shutdown
        streaming_pull_future.cancel()
        streaming_pull_future.result()

print(f"Total messages received: {message_count}")

