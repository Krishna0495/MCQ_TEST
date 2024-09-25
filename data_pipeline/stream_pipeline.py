# Assumptions:
# Apache Kafka is set up, and there’s a Kafka topic where session data is being published.
# Kafka messages are in JSON format (for session data).
# Kafka cluster's bootstrap servers (localhost:9092) and topic names (session_data_topic).

from pyspark.sql import SparkSession
from pyspark.sql.functions import F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Initialize Spark session with Kafka support
spark = SparkSession.builder \
    .appName("RealTimeSessionIngestionKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()


# Define schema for session data
session_schema = StructType([
    StructField("session_id", IntegerType(), True),
    StructField("student_id", IntegerType(), True),
    StructField("test_id", IntegerType(), True),
    StructField("question_id", IntegerType(), True),
    StructField("choice_id", IntegerType(), True),  # "started", "completed", etc.
    StructField("session_start_time", TimestampType(), True),
    StructField("session_end_time", TimestampType(), True)
])

# Kafka topic and broker settings
kafka_brokers = "localhost:9092"  
kafka_topic = "session_data_topic"  

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest").load() # Start from the earliest message (or use "latest")


def process_batch(df, batchID):

    # Convert Kafka 'value' from binary to string (JSON format)
    session_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data")

    parsed_df = session_df.withColumn("data", F.from_json(F.col("json_data"), session_schema)).select("data.*")

    # Add processing columns (e.g., current processing time)
    processed_df = parsed_df.withColumn("processed_time", F.current_timestamp())

    # Example transformation: Filter for sessions that have completed
    answered_sessions_df = processed_df.filter(F.col("choice_id").isNotNull())

    answered_sessions_df = answered_sessions_df\
        .withColumnRenamed("answer_timestamp","session_end_time")\
            .withColumn("answer_id",F.concat(F.col("student_id").cast('string'),
                                             F.col("test_id").cast('string'),
                                             F.col("question_id").cast('string')
                                             ).cast('int'))\
                                             .drop("session_end_time","session_start_time","processed_time","session_id")

    processed_df = processed_df.drop("choice_id")

    # Define the path to Delta Lake (or other storage)
    delta_table_path = "gs://delta/session_log"

    # Write the streaming data to Delta Lake in append mode
    processed_df.write \
        .format("delta") \
        .mode("append") \
        .start(delta_table_path)
    
    answered_sessions_input_path = "gs://input_bucket/answer/"
    
    answered_sessions_df.write \
        .format("parquet") \
        .mode("append") \
        .start(answered_sessions_input_path)
    



query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

# Wait for the stream to finish (it will run indefinitely in real-time)
query.awaitTermination()