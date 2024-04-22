import json
import requests
from google.cloud import pubsub_v1

# Setup Google Cloud Pub/Sub
project_id = "devm-420400"
topic_id = "my-topic"
message_count = 0
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def read_vehicle_ids(file_path):
    """Read vehicle IDs from a text file."""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def fetch_and_publish_data(api_url, vehicle_ids):
    global message_count
    """Fetch and publish breadcrumb data for a list of vehicle IDs."""
    for vehicle_id in vehicle_ids:
        try:
            response = requests.get(f"{api_url}?vehicle_id={vehicle_id}")
            response.raise_for_status()  # Check for HTTP request errors
            vehicle_data = response.json()
            if vehicle_data:
                for record in vehicle_data:
                    message_count += 1
                    data_str = json.dumps(record)
                    data_bytes = data_str.encode('utf-8')
                    publisher.publish(topic_path, data_bytes)
                    print(f"Published: {data_str} to {topic_path}")
            else:
                print(f"No data returned for Vehicle ID: {vehicle_id}")
        except requests.exceptions.RequestException as err:
            print(f"Error for Vehicle ID {vehicle_id}: {err}")

if __name__ == "__main__":
    api_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"
    vehicle_ids = read_vehicle_ids('vehicle_ids.txt')  # Read vehicle IDs from file
    fetch_and_publish_data(api_url, vehicle_ids)
    print(f"Total message sent {message_count}")
