import os
from bs4 import BeautifulSoup
import pandas as pd
from google.cloud import pubsub_v1
import json
import requests

def fetch_html_content(url):
    """Fetch HTML content from a URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Failed to fetch data from URL {url}: {str(e)}")
        return None

def parse_html_to_dataframe(html_content):
    """Parse HTML content to create a DataFrame for each trip section found."""
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    sections = soup.find_all('h2')
    all_records = []
    
    for section in sections:
        trip_id = section.get_text().split()[-1] 
        table = section.find_next('table') 
        if table:
            headers = [th.get_text(strip=True) for th in table.find_all('th')]
            rows = table.find_all('tr')
            if len(rows) > 1:  
                first_row = rows[1]  
                record = {**{'trip_id': trip_id}, **{headers[i]: cell.get_text(strip=True) for i, cell in enumerate(first_row.find_all('td'))}}
                all_records.append(record)  
    
    if all_records:
        return pd.DataFrame(all_records)
    return None

def publish_to_pubsub(df, publisher, topic_path):
    """Publish DataFrame records to Google Cloud Pub/Sub."""
    if df is not None:
        for record in df.to_dict(orient='records'):
            data_bytes = json.dumps(record).encode('utf-8')
            future = publisher.publish(topic_path, data_bytes)
            future.result()

def main():
    vehicle_ids = [line.strip() for line in open('vehicle_ids.txt') if line.strip()]
    base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('devm-420400', 'my-topic1')

    for vehicle_id in vehicle_ids:
        url = f"{base_url}{vehicle_id}"
        html_content = fetch_html_content(url)
        if html_content:
            df = parse_html_to_dataframe(html_content)
            if df is not None:
                publish_to_pubsub(df, publisher, topic_path)
            else:
                print(f"No data to publish for vehicle {vehicle_id}.")
        else:
            print(f"Skipping vehicle {vehicle_id} due to fetch failure.")

if __name__ == "__main__":
    main()
