import os
import joblib
import re
import nltk
import time
import sys
import typing
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Critical Java 17 compatibility fix - must be BEFORE any Spark imports
os.environ["JAVA_TOOL_OPTIONS"] = (
    "-Dio.netty.tryReflectionSetAccessible=true "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/sun.util=ALL-UNNAMED"
)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from json import loads
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel

# Establish connection to MongoDB Atlas (Cloud)
MONGODB_URI = os.getenv(
    "MONGODB_URI",
    "mongodb+srv://user_420:useradmin@cluster0.adjdner.mongodb.net/?retryWrites=true&w=majority",
)


def connect_mongodb(retries=3):
    """Connect to MongoDB with retry logic"""
    for attempt in range(retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            client.server_info()  # Test connection
            db = client["big_data_project"]
            collection = db["tweets"]
            print("[MongoDB] ✓ Connected successfully")
            return client, db, collection
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            print(f"[MongoDB] Connection attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise


try:
    mongodb_client, db, collection = connect_mongodb()
except Exception as e:
    print(f"[ERROR] Failed to connect to MongoDB after 3 attempts: {e}")
    print("[INFO] Consumer will continue but data will NOT be saved to MongoDB")
    mongodb_client = None
    collection = None

# Download stopwords
nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)

# Initialize Spark Session
cwd = os.path.abspath(os.path.dirname(__file__))
print("[Spark] Initializing SparkSession...")

# Check Java version
java_version = os.environ.get("JAVA_VERSION")
if java_version:
    print(f"[Java] Detected Java version: {java_version}")
else:
    print("[WARNING] JAVA_VERSION not set - Java detection may not work as expected")

# Check PySpark version
pyspark_version = os.environ.get("PYSPARK_PYTHON")
if pyspark_version:
    print(f"[PySpark] Detected PySpark version: {pyspark_version}")
else:
    print(
        "[WARNING] PYSPARK_PYTHON not set - PySpark detection may not work as expected"
    )

# Check if JAVA_HOME is set
if "JAVA_HOME" not in os.environ:
    print("[WARNING] JAVA_HOME not set - Java detection may not work as expected")
else:
    print(f"[INFO] JAVA_HOME is set to: {os.environ['JAVA_HOME']}")

# Check if SPARK_HOME is set
if "SPARK_HOME" not in os.environ:
    print("[WARNING] SPARK_HOME not set - Spark detection may not work as expected")
else:
    print(f"[INFO] SPARK_HOME is set to: {os.environ['SPARK_HOME']}")

# Update SparkSession initialization
try:
    spark = (
        SparkSession.builder.appName("classify tweets")
        .config("spark.sql.warehouse.dir", os.path.join(cwd, "spark-warehouse"))
        .config("spark.driver.memory", "2g")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/sun.util=ALL-UNNAMED",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/sun.util=ALL-UNNAMED",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .getOrCreate()
    )
    print("[Spark] ✓ SparkSession created successfully")
except Exception as e:
    print(f"[ERROR] Failed to create SparkSession: {e}")
    print("[INFO] Please ensure PySpark 3.4.3 is installed")
    sys.exit(1)

# Load the model - Spark ML Pipeline saved as directory
model_path = os.path.join(cwd, "logistic_regression_model")
pipeline = None

print("[Model] Loading ML model...")
try:
    # Load Spark PipelineModel from directory
    if os.path.isdir(model_path):
        pipeline = PipelineModel.load(model_path)
        print("[Model] ✓ Loaded Spark PipelineModel successfully")
    else:
        raise FileNotFoundError(
            f"Model directory not found at {model_path}. "
            f"Expected a Spark ML Pipeline directory. "
            f"Run Big_Data.ipynb to train and save the model."
        )
except Exception as e:
    print(f"[ERROR] {e}")
    sys.exit(1)


def clean_text(text):
    """Clean and preprocess tweet text"""
    if text is not None:
        # Remove links starting with https://, http://, www., or containing .com
        text = re.sub(r"https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+", "", text)

        # Remove words starting with # or @
        text = re.sub(r"(@|#)\w+", "", text)

        # Convert to lowercase
        text = text.lower()

        # Remove non-alphanumeric characters
        text = re.sub(r"[^a-zA-Z\s]", "", text)

        # Remove extra whitespaces
        text = re.sub(r"\s+", " ", text).strip()
        return text
    else:
        return ""


class_index_mapping = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}

# Kafka Consumer with error handling
print("[Kafka] Connecting to Kafka consumer...")
try:
    kafka_bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    ).split(",")
    kafka_topic = os.getenv("KAFKA_TOPIC", "numtest")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "my-group")

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=kafka_group_id,
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=5000,
        session_timeout_ms=30000,
    )
    print("[Kafka] ✓ Connected to Kafka consumer")
except Exception as e:
    print(f"[ERROR] Failed to connect to Kafka: {e}")
    print("[INFO] Ensure Kafka is running on localhost:9092")
    sys.exit(1)

print("[INFO] Starting to consume tweets...")
print("=" * 60)

# Main consumer loop
message_count = 0
try:
    for message in consumer:
        try:
            tweet = message.value[-1]  # get the Text from the list
            preprocessed_tweet = clean_text(tweet)

            # Create a DataFrame from the string
            data = [
                (preprocessed_tweet,),
            ]
            data = spark.createDataFrame(data, ["Text"])

            # Apply the pipeline to the new text
            processed_validation = pipeline.transform(data)
            prediction = processed_validation.collect()[0][6]

            prediction_label = class_index_mapping[int(prediction)]

            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Tweet #{message_count + 1}"
            )
            print(f"  Tweet: {tweet}")
            print(f"  Cleaned: {preprocessed_tweet}")
            print(f"  Prediction: {prediction_label} (Score: {prediction})")

            # Prepare document to insert into MongoDB
            tweet_doc = {
                "tweet": tweet,
                "prediction": prediction_label,
                "timestamp": datetime.now(),
                "preprocessed": preprocessed_tweet,
            }

            # Insert document into MongoDB collection
            if collection is not None:
                try:
                    collection.insert_one(tweet_doc)
                    print(f"  ✓ Saved to MongoDB")
                except Exception as e:
                    print(f"  ✗ Failed to save to MongoDB: {e}")
            else:
                print(f"  ⚠ MongoDB not connected - not saving")

            print("-" * 60)
            message_count += 1

        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")
            import traceback

            traceback.print_exc()
            continue

except KeyboardInterrupt:
    print(
        f"\n[INFO] Consumer stopped by user after processing {message_count} messages"
    )
except Exception as e:
    print(f"[ERROR] Consumer loop error: {e}")
    import traceback

    traceback.print_exc()
finally:
    print("[INFO] Closing consumer...")
    consumer.close()
    if mongodb_client:
        mongodb_client.close()
    print("[INFO] Done")
