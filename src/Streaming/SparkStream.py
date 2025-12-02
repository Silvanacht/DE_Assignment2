from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, current_timestamp, avg, count, when, lit, substring, abs as spark_abs, mean, stddev, round, upper, lower, window, max as spark_max
from pyspark.sql.window import Window
import logging

# Set up logging for better visibility in the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 1. Define Schema ---
dataSchema = StructType([
    StructField("id", IntegerType()),
    StructField("url", StringType()),
    StructField("region", StringType()),
    StructField("region_url", StringType()),
    StructField("price", IntegerType()),
    StructField("year", StringType()), 
    StructField("manufacturer", StringType()),
    StructField("model", StringType()),
    StructField("condition", StringType()),
    StructField("cylinders", StringType()),
    StructField("fuel", StringType()),
    StructField("odometer", DoubleType()),
    StructField("title_status", StringType()),
    StructField("transmission", StringType()),
    StructField("VIN", StringType()),
    StructField("drive", StringType()),
    StructField("size", StringType()),
    StructField("type", StringType()),
    StructField("paint_color", StringType()),
    StructField("image_url", StringType()),
    StructField("description", StringType()),
    StructField("county", StringType()),
    StructField("state", StringType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("posting_date", StringType())
])


# --------------------------------------------------------
## Advanced Analytics Function (Run on Each Micro-Batch)
# --------------------------------------------------------

def advanced_analytics_batch(current_batch_df, batch_id):
    """
    Applies complex windowing and statistical logic to each micro-batch,
    performs scoring, and prints alert lists.
    """
    
    # Calculate the count first
    batch_count = current_batch_df.count()

    # Check for empty batch and log/return if empty
    if batch_count == 0:
        logger.info(f"Batch {batch_id}: No data to process. Skipping analysis.")
        return

    # If the batch is NOT empty, log the count and start processing
    logger.info(f"||New Micro-Batch!||\n")
    logger.info(f"Micro-Batch {batch_id}: Records received in this batch: {batch_count}")
    logger.info(f"--- Starting Advanced Analysis for Batch {batch_id} ---")

    # Rename for clarity within the function
    df_working = current_batch_df 

    # --- YEAR IMPUTATION ---
    df_working = df_working.withColumn(
        "year_temp", 
        col("year").cast(IntegerType())
    ).withColumn(
        "year", 
        when(col("year_temp").isNull() | (col("year_temp") == 0), 1900) 
        .otherwise(col("year_temp"))
    ).drop("year_temp")
    # -----------------------

    # --- 0. Data Pre-Cleaning: Standardization and Outlier/Null Imputation ---
    logger.info("  0. Data Pre-Cleaning: Lowercase Status, Imputation, and Price Outlier Removal")
    
    # 1. Standardize Strings: Lowercase title_status
    df_working = df_working \
        .withColumn("title_status", lower(col("title_status")))

    # 2. Impute Nulls for odometer and title_status
    df_working = df_working.fillna({
        "odometer": -1.0, 
        "title_status": "clean" 
    }) 

    # 3. Filter Price Outliers
    df_working = df_working \
        .filter(col("price") > 500) \
        .filter(col("price") < 200000)
    
    # ----------------------------------------------------------------------------------


    # --------------------------------------------------------
    # STEP 1: Statistical Scoring (Price and Odometer Z-Scores)
    # --------------------------------------------------------
    logger.info("  1. Statistical Scoring: Calculating Market Averages (manufacturer, model, year)")
    
    # The partitioning now uses the cleaned 'year' integer column
    windowSpec = Window.partitionBy("manufacturer", "model", "year")
    
    # Calculate Group Statistics (Average, StdDev, Count)
    df_stats = df_working \
        .withColumn("market_avg_price", round(mean("price").over(windowSpec), 2)) \
        .withColumn("market_std_dev_price", round(stddev("price").over(windowSpec), 2)) \
        .withColumn("market_avg_odometer", round(mean("odometer").over(windowSpec), 0)) \
        .withColumn("market_std_dev_odometer", round(stddev("odometer").over(windowSpec), 0)) \
        .withColumn("group_count", count("id").over(windowSpec))
    
    # Calculate Price Z-Score
    df_scored = df_stats.withColumn("price_z_score",
        (col("price") - col("market_avg_price")) / when(col("market_std_dev_price").isNull() | (col("market_std_dev_price") == 0), 1).otherwise(col("market_std_dev_price")))
    
    # Calculate Odometer Z-Score (How much mileage deviates from the norm for this car type)
    df_scored = df_scored.withColumn("odometer_z_score",
        (col("odometer") - col("market_avg_odometer")) / when(col("market_std_dev_odometer").isNull() | (col("market_std_dev_odometer") == 0), 1).otherwise(col("market_std_dev_odometer")))
    
    # --------------------------------------------------------
    # STEP 2: VIN Forensics & Fraud Detection (Trust Score)
    # --------------------------------------------------------
    logger.info("  2. VIN Forensics: Decoding VIN Year and Checking Fraud Flags")
    current_yr = 2025 # Define the current year for mileage checks

    # 2.1 VIN Year Decoding
    df_fraud_check = df_scored.withColumn("vin_year_code", substring(col("VIN"), 10, 1))
    
    df_fraud_check = df_fraud_check.withColumn("decoded_vin_year",
        when(col("vin_year_code") == "A", 2010)
        .when(col("vin_year_code") == "B", 2011)
        .when(col("vin_year_code") == "C", 2012)
        .when(col("vin_year_code") == "D", 2013)
        .when(col("vin_year_code") == "E", 2014)
        .when(col("vin_year_code") == "F", 2015)
        .when(col("vin_year_code") == "G", 2016)
        .when(col("vin_year_code") == "H", 2017)
        .when(col("vin_year_code") == "J", 2018)
        .when(col("vin_year_code") == "K", 2019)
        .when(col("vin_year_code") == "L", 2020)
        .when(col("vin_year_code") == "M", 2021)
        .when(col("vin_year_code") == "N", 2022)
        .when(col("vin_year_code") == "P", 2023)
        .otherwise(0))

    # 2.2 Fraud Logic Flags
    df_scored_with_fraud = df_fraud_check \
        .withColumn("is_year_mismatch", 
            when((col("decoded_vin_year") > 0) & (spark_abs(col("decoded_vin_year") - col("year")) > 1), True)
            .otherwise(False)
        ) \
        .withColumn("is_suspicious_mileage", 
            when((col("year") < (current_yr - 5)) & (col("odometer") > 0) & (col("odometer") < 2000), True)
            .otherwise(False)
        )
    
    # 2.3 Trust Score
    df_scored_with_fraud = df_scored_with_fraud.withColumn("trust_score", 
        lit(100) - 
        when(col("is_year_mismatch"), 50).otherwise(0) - 
        when(col("is_suspicious_mileage"), 30).otherwise(0))

    # --------------------------------------------------------
    # STEP 3: Geospatial Arbitrage (National vs. Local Price)
    # --------------------------------------------------------
    logger.info("  3. Geospatial Arbitrage: Calculating Opportunity")
    national_window = Window.partitionBy("manufacturer", "model", "year")

    
    # Recalculate National Average Price
    df_geo = df_scored_with_fraud.withColumn("national_avg_price", round(mean("price").over(national_window), 2))
    
    # Arbitrage Value
    df_geo = df_geo.withColumn("arbitrage_opportunity", col("national_avg_price") - col("price"))

    # --------------------------------------------------------
    # STEP 4: Final Adjusted Deal Scoring (Including Volatility and Outliers)
    # --------------------------------------------------------
    logger.info("  4. Final Scoring: Adjusting Deal Rating based on Trust Score, Odometer, and Market Volatility")
    
    df_final = df_geo.withColumn("deal_rating",
        when(col("trust_score") < 70, "RISKY_BUY") 
        .when(col("group_count") < 5, "INSUFFICIENT_DATA")
        
        # --- NEW ALERT 1: Market Volatility Alert ---
        # Flag segments where std dev is > 20% of the average price
        .when((col("market_std_dev_price") / col("market_avg_price")) > 0.20, "VOLATILE_MARKET") 
        # --------------------------------------------
        
        # Odometer Downgrade Logic: Downgrade a "GREAT_DEAL" if it has excessively high mileage
        .when((col("price_z_score") < -1.5) & (col("odometer_z_score") > 1.5), "GOOD_PRICE")
        
        # Original Great Deal Threshold
        .when(col("price_z_score") < -1.5, "GREAT_DEAL")
        
        # Remaining ratings
        .when(col("price_z_score").between(-1.5, -0.5), "GOOD_PRICE")
        .when(col("price_z_score").between(-0.5, 0.5), "FAIR_MARKET")
        
        # --- NEW ALERT 2: Extreme Overprice Alert ---
        .when(col("price_z_score") >= 3.0, "EXTREME_OVERPRICE")
        # --------------------------------------------
        
        .otherwise("OVERPRICED"))
        
    # --------------------------------------------------------
    # STEP 5: Time Window Aggregation (Summary of Current Batch for Display)
    # --------------------------------------------------------
    logger.info("  5. Calculating Time Window Aggregation (Summary of Current Batch)")
    
    # Group by manufacturer/model to get a summary of the current 10-second batch
    df_window_aggregation = current_batch_df \
        .groupBy(
            col("manufacturer"), 
            col("model")
        ).agg(
            # Use max event_time as a proxy for the window end time
            spark_max(col("event_time")).alias("last_data_received (kafka)"), 
            round(avg("price"), 2).alias("minutely_avg_price"),
            round(stddev("price"), 2).alias("minutely_price_standard_dev"),
            count("*").alias("listings_count")
        )
    
    # Calculate the number of unique market segments (unique manufacturer/model combinations)
    segment_count = df_window_aggregation.count()

    # Re-order and sort the columns
    df_window_aggregation = df_window_aggregation.select(
        col("manufacturer"), 
        col("model"), 
        col("minutely_avg_price"),
        col("minutely_price_standard_dev"),
        col("listings_count"),
        col("last_data_received (kafka)")
    ).sort(col("listings_count").desc()) 


    # --------------------------------------------------------
    # STEP 6: Console Output (The Sink & Alerts)
    # --------------------------------------------------------
    logger.info(f"Micro-Batch (Window) {batch_id}: Analysis Complete")
    logger.info(f"\n||That's the batch! Here is the statistics:||")
    
    # 6.1. Print Aggregated Summary Table
    # Log now shows both the total records and the segment count
    logger.info(f"\n--- Latest 10 seconds market summary: (Current Batch: {batch_count} Records Processed, {segment_count} Unique Segments) ---")
    df_window_aggregation.show(20, truncate=False) # Display limit is now 20
    
    # 6.2. Print Great Deal Alerts
    great_deals = df_final.filter((col("deal_rating") == "GREAT_DEAL") | (col("deal_rating") == "GOOD_PRICE") & (col("price_z_score") < -1.5))
    great_deal_count = great_deals.count()
    
    if great_deal_count > 0:
        logger.info(f"\n--------------------------------------------------------------------")
        logger.info(f"--- DEAL ALERT! :o ({great_deal_count} Listings) - BATCH {batch_id} ---")
        logger.info(f"------------------------------------------------------")
        great_deals.select(
            "manufacturer", 
            "model", 
            "price",
            round(col("price_z_score"), 2).alias("Price_Z_Score"), 
            round(col("odometer_z_score"), 2).alias("Odometer_Z_Score"),
            round(col("arbitrage_opportunity"), 2).alias("Arbitrage_Value"),
            "deal_rating"
        ).orderBy(col("price_z_score")).show(great_deal_count, truncate=False)
    else:
        logger.info(f"\n--- No GREAT DEAL listings found in Batch {batch_id} :( ---")

    # 6.3. Print Risky Buy Alerts
    risky_buys = df_final.filter(col("deal_rating") == "RISKY_BUY") 
    risky_buy_count = risky_buys.count()
    
    if risky_buy_count > 0:
        logger.info(f"\n--------------------------------------------------------------------")
        logger.info(f"--- RISKY BUY ALERT! :o ({risky_buy_count} Listings) - BATCH {batch_id} ---")
        logger.info(f"------------------------------------------------------")
        risky_buys.select(
            "manufacturer", 
            "model", 
            "price",
            "trust_score",
            when(col("is_year_mismatch"), "Year Mismatch").otherwise("").alias("Fraud_Flag")
        ).show(risky_buy_count, truncate=False)
    else:
        logger.info(f"\n--- No RISKY BUY listings found in Batch {batch_id} :) ---")
    
    # 6.4. Print Volatile Market Alert (NEW)
    volatile_markets = df_final.filter(col("deal_rating") == "VOLATILE_MARKET").select("manufacturer", "model", "year", "market_avg_price", "market_std_dev_price").distinct()
    volatile_market_count = volatile_markets.count()
    
    if volatile_market_count > 0:
        logger.info(f"\n--------------------------------------------------------------------")
        logger.info(f"--- VOLATILE MARKET ALERT! :o ({volatile_market_count} Segments) - BATCH {batch_id} ---")
        logger.info(f"------------------------------------------------------")
        volatile_markets.withColumn("StdDev_as_Pct_of_Avg", round(col("market_std_dev_price") / col("market_avg_price") * 100, 1)).select(
            "manufacturer", 
            "model", 
            "year",
            col("market_avg_price").alias("Avg_Price"),
            col("market_std_dev_price").alias("Std_Dev_Price"),
            "StdDev_as_Pct_of_Avg"
        ).orderBy(col("Std_Dev_Price").desc()).show(volatile_market_count, truncate=False)
    else:
        logger.info(f"\n\n--- Price deviations seem to be normal! :) ---")
    
    # 6.5. Print Extreme Overprice Alert (NEW)
    extreme_overprices = df_final.filter(col("deal_rating") == "EXTREME_OVERPRICE")
    extreme_overprice_count = extreme_overprices.count()
    
    if extreme_overprice_count > 0:
        logger.info(f"\n--------------------------------------------------------------------")
        logger.info(f"--- EXTREME OVERPRICE ALERT! :o ({extreme_overprice_count} Listings) - BATCH {batch_id} ---")
        logger.info(f"------------------------------------------------------")
        extreme_overprices.select(
            "manufacturer", 
            "model", 
            "price",
            round(col("price_z_score"), 2).alias("Price_Z_Score"),
            col("market_avg_price").alias("Avg_Price")
        ).orderBy(col("price_z_score").desc()).show(extreme_overprice_count, truncate=False)
    else:
        logger.info(f"\n\n--- No highly overpriced listings found! :) ---")
    
# (End of advanced_analytics_batch function)

# --------------------------------------------------------
# --- MAIN EXECUTION START ---
# --------------------------------------------------------

# --- 2. Initialize Spark Session ---
spark = SparkSession.builder \
    .appName("KafkaStreamingAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", 8) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")
logger.info("--- Spark Session Initialized and Kafka Package Loaded ---")

# --- 3. Read from Kafka Broker (The Consumer) ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "car_data") \
    .option("startingOffsets", "latest") \
    .load()
logger.info("--- Streaming Data Source Defined: Kafka Topic 'car_data' ---")


# --- 4. Stream Pre-processing (Parse, Clean, Filter) ---

# 4.1 Parse JSON Data and get event_time
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_string", "timestamp as event_time") \
    .select(from_json(col("json_string"), dataSchema).alias("data"), col("event_time")) \
    .select("data.*", "event_time")
logger.info("--- JSON Data Parsed and Schema Applied ---")

# 4.2 Stream Standardization
df_standardized_stream = parsed_stream \
    .withColumn("manufacturer", upper(col("manufacturer"))) \
    .withColumn("model", upper(col("model"))) \
    .withColumn("VIN", upper(col("VIN")))
logger.info("--- Key String Fields Standardized to UPPERCASE for Filtering ---")


# --- 5. Custom Sink: Call Advanced Analytics on Each Micro-Batch ---

logger.info("--- Starting Streaming Query to ForeachBatch Sink (Append Mode) ---")
# Use the raw, standardized stream (df_standardized_stream) as the source
query = df_standardized_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(advanced_analytics_batch) \
    .trigger(processingTime='10 seconds') \
    .start()

# Await Termination to keep the Spark job running
query.awaitTermination()