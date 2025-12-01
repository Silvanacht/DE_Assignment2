import pandas as pd
import json
from kafka import KafkaProducer
import time # Import time for rate limiting (good practice for streaming)


# 1. Csv file path to the (test set of the) data
CSV_FILE_PATH = 'vehicles.csv' 
# CSV_FILE_PATH = 'gs://vehicles-stream-temp-data-001/first100_vehicles.csv'
# 2. Use the VM's External IP 
KAFKA_BROKER = "34.56.180.242:9092"
# 3. New topic name for the real assignment
KAFKA_TOPIC = 'car_data'
# 4. Specify the amount of rows to process from the data
NUM_ROWS_TO_PROCESS = 1000


if __name__ == '__main__':
    # Initialize producer with a JSON serializer (for consistency)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # The value_serializer ensures we can send Python dicts/objects if we want, 
        # though we will serialize manually below.
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Producer connected. Reading data from {CSV_FILE_PATH}...")

    try:
        # Load the CSV file using pandas
        df = pd.read_csv(CSV_FILE_PATH,
        nrows=NUM_ROWS_TO_PROCESS)
        
        # Iterate over DataFrame rows
        for index, row in df.iterrows():
            # Convert the Pandas Series (row) into a Python dictionary

            record_dict = row.to_dict()

            # The producer's 'value_serializer' will handle the conversion to JSON bytes
            producer.send(KAFKA_TOPIC, value=record_dict)  

            # Every 100 cars (data points)
            if(index % 100 == 0):
                # Print an update message with the manufacturer 
                print(f"Sending record {index + 1}: {record_dict['manufacturer']}...")

            # Optional: Add a short delay to simulate a real-time stream
            time.sleep(0.001)

    except FileNotFoundError:
        print(f"\n ERROR: CSV file not found at {CSV_FILE_PATH}.")
    except Exception as e:
        print(f"\n An unexpected error occurred: {e}")
    finally:
        # Ensure the producer flushes any remaining messages and closes the connection
        producer.close()
        print("\n Producer finished and closed.")