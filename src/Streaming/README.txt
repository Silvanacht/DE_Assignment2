# Option A:
# 0) The 'vehicles.csv' should be in the same directory as the python scripts to run the ProducerReal.py

# 1) To run the SparkStream.py that is also on the VM (and contains a consumer), first run the following command on the VM (not in the venv!):
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 SparkFinal.py

# 2) Run the ProducerReal.py script locally (to save GCS credits) with the following command:
# python ProducerReal.py

# 3) Enjoy!



# Option B:
# To run the Producer and Consumer(!) logic fully local and send to BigQuery table:

# 0a) The 'vehicles.csv' should be in the same directory as the python scripts to run the ProducerReal.py
# 0b) The 'vehicles.csv' and ProducerReal.py should be in the same directory as ConsumerReal.py

# 1) Run the ConsumerReal.py with the following command
# python ProducerReal.py

# 2) Run the ProducerReal.py script locally with the following command:
# python ProducerReal.py
# 3) Enjoy!



# (Option C:)
To run the Producer (or Consumer) on the vm:

# 0) Move to the venv by using:
# source stream_pipeline_env/bin/activate

# 1) Run the producer file on the vm (and in the venv) with:
# python ProducerReal.py

(# 2) Run the consumer file on the vm (and in the venv) with:
# python ConsumerReal.py)

# Enjoy!
