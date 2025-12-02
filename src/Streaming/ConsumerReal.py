import json
import os
from kafka import KafkaConsumer
from google.cloud import bigquery
import math
import time

# --- CONFIGURATION ---
KAFKA_TOPIC = 'car_data'
KAFKA_BROKER = "34.56.180.242:9092" # The VM's External IP
BQ_TABLE_ID = 'stream-pipeline-dea2.Cars_Stream.Cars_Table_Stream' 
BATCH_SIZE = 100 
# Path to your Service Account Key (JSON file) with BigQuery Data Editor permissions
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/olivier/Documents/Master_JADS/Data_Engineering/A2/A2_real/stream-pipeline-dea2-19f99a79b5a7.json" 
# ---------------------

# Helper function to not use NA (as these are not recognized for JSON data)
def clean_nan_values(record):
    """Recursively replaces float('nan') with None in a dictionary."""
    cleaned_record = {}
    for key, value in record.items():
        if isinstance(value, dict):
            cleaned_record[key] = clean_nan_values(value)
        elif isinstance(value, float) and math.isnan(value):
            # Replace NaN float with None, which BigQuery accepts as null
            cleaned_record[key] = None
        else:
            cleaned_record[key] = value
    return cleaned_record
    
def consume_and_stream_to_bq():
    # 1. Initialize BigQuery Client
    bq_client = bigquery.Client()
    rows_buffer = []

    WINDOW_TIMEOUT_S = 5 
    # Tracker for when the last batch was successfully inserted
    last_flush_time = time.time()

    # 2. Initialize Kafka Consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', 
        # Make sure to timeout after 1 min of no data received
        consumer_timeout_ms=8000,
        enable_auto_commit=True,
        # Deserialize the incoming bytes (from your producer) into a Python dictionary
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f" Consumer ready. Listening to {KAFKA_TOPIC} and streaming to {BQ_TABLE_ID}...")

    # Create a placeholder to print latency messages later
    latest_kafka_timestamp_ms = 0

    for msg in consumer:
        # Get the current time in miliseconds
        current_time = int(time.time() * 1000)
        
        # 1. Deserialize the data from Kafka
        data = msg.value
        
        # 2. CLEAN THE DATA (NEW STEP HERE)
        cleaned_data = clean_nan_values(data)
        
        # 3. Add to our mini-batch (use the cleaned data)
        rows_buffer.append(cleaned_data)

        # Get the timestamp in miliseconds
        latest_kafka_timestamp_ms = msg.timestamp 
        
        # Insert in batches for efficiency
        if len(rows_buffer) >= BATCH_SIZE:
            # Calculate the batch latency in seconds
            batch_latency = (current_time - latest_kafka_timestamp_ms) / 1000
            
            errors = bq_client.insert_rows_json(BQ_TABLE_ID, rows_buffer)
            
            if errors == []:
                print(f"   [Streaming] Pushed {len(rows_buffer)} rows. End-to-End Latency (Latest Msg): {batch_latency:.3f}s")
            else:
                print(f" Encountered errors during BigQuery insert: {errors}")
            
            rows_buffer = [] # Reset buffer
            latest_kafka_timestamp_ms = 0
            
    # --- FINAL BUFFER FLUSH ---
    if rows_buffer:
        print(f"\nTime out reached. Flushing final {len(rows_buffer)} rows...")
        errors = bq_client.insert_rows_json(BQ_TABLE_ID, rows_buffer)
        if errors == []:
            print(" Final buffer successfully pushed to BigQuery.")
        else:
            print(f" Error during final buffer push: {errors}")
    print("\nConsumer finished.")

if __name__ == '__main__':
    consume_and_stream_to_bq()