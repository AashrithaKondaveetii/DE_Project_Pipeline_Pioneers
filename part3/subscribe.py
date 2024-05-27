import json
import pandas as pd
import psycopg2
from google.cloud import pubsub_v1

project_id = "devm-420400"
subscription_id = "my-sub1"
db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "pipeline",
    "host": "127.0.0.1",
    "port": "5432"
}

def map_service_key(service_key):
    """Map service_key values to the corresponding enum."""
    if service_key == 'S':
        return 'Saturday'
    elif service_key == 'U':
        return 'Sunday'
    else:
        return 'Weekday'

def validate_data(df):
    """Perform validation on DataFrame with additional assertions."""
    essential_columns = ['vehicle_number', 'trip_id', 'route_number', 'direction', 'service_key']
    missing_columns = [col for col in essential_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing essential columns: {missing_columns}")
    
    try:
        
        assertion1 = "leave_time should always be less than or equal to arrive_time, and arrive_time should be less than or equal to stop_time."
        assert (df['leave_time'] <= df['arrive_time']).all() and (df['arrive_time'] <= df['stop_time']).all(), assertion1

        assertion2 = "Vehicle ID should be non-null and a positive number."
        assert df['vehicle_id'].notnull().all() and (df['vehicle_id'] > 0).all(), assertion2

        assertion3 = "Route ID should be a positive number."
        assert (df['route_number'] > 0).all(), assertion3

        assertion4 = "Arrive time should not be earlier than the previous stop time."
        assert df.groupby('trip_id')['arrive_time'].apply(lambda x: x.shift(1) <= x).all(), assertion4

        assertion5 = "Direction must be either 0 or 1."
        assert df['direction'].isin([0, 1]).all(), assertion5

        return True
    
    except AssertionError as e:
        print(str(e))
        
        return False



def transform_data(df):
    """Transform data according to specific rules."""
    if 'direction' in df.columns:
        df['direction'] = df['direction'].apply(lambda x: 'Out' if x == '0' else ('Back' if x == '1' else 'Unknown'))
    else:
        df['direction'] = 'Unknown'  
    if 'service_key' in df.columns:
        df['service_key'] = df['service_key'].apply(map_service_key)
    return df

def update_trip_table(df, conn):
    """Update the new_trip table in the PostgreSQL database."""
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            cursor.execute(
                "UPDATE new_trip SET route_id = %s, direction = %s, service_key = %s WHERE vehicle_id = %s AND trip_id = %s;",
                (int(row['route_number']), row['direction'], row['service_key'], int(row['vehicle_number']), int(row['trip_id']))
            )
        conn.commit()
        print("New_trip table updated successfully for batch.")

def callback(message):
    """Process each message received from the Pub/Sub topic."""
    data = json.loads(message.data.decode('utf-8'))
    df = pd.DataFrame([data])

    try:
        if validate_data(df):
            df = transform_data(df)
            with psycopg2.connect(**db_params) as conn:
                update_trip_table(df, conn)
                message.ack()
    except Exception as e:
        print(f"Data processing error: {e}")
        message.nack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Subscription cancelled due to: {e}")

if __name__ == "__main__":
    main()
