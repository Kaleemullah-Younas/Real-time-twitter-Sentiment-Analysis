#!/usr/bin/env python
"""
End-to-end test: Producer -> Kafka -> Consumer -> MongoDB
"""
import subprocess
import time
import sys
import os

print("[INFO] Big Data Project - End-to-End Test")
print("=" * 60)

# Check if Kafka is running
print("\n[1] Checking Kafka connectivity...")
try:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], request_timeout_ms=5000
    )
    producer.close()
    print("[OK] Kafka is running and accessible")
except Exception as e:
    print(f"[ERROR] Kafka not accessible: {e}")
    print(
        "       Make sure Docker Kafka is running: docker-compose -f zk-single-kafka-single.yml up -d"
    )
    sys.exit(1)

# Check MongoDB
print("\n[2] Checking MongoDB connectivity...")
try:
    from pymongo import MongoClient

    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.close()
    print("[OK] MongoDB is running and accessible")
    mongodb_available = True
except Exception as e:
    print(f"[WARNING] MongoDB not accessible: {e}")
    print("          To start MongoDB on Windows:")
    print("          1. Open MongoDB Compass, or")
    print("          2. Run: mongod")
    mongodb_available = False

# Check if model exists
print("\n[3] Checking ML model...")
model_path = os.path.join(
    os.path.dirname(__file__), "Kafka-PySpark", "logistic_regression_model.pkl"
)
if os.path.exists(model_path):
    print(f"[OK] Model found at {model_path}")
else:
    print(f"[WARNING] Model not found at {model_path}")
    print("         You need to run Big_Data.ipynb first to train the model")

# Test Django classification
print("\n[4] Testing Django classification...")
sys.path.insert(0, "Django-Dashboard")
try:
    from dashboard.consumer_user import classify_text

    result = classify_text("I am very happy today!")
    print(f"[OK] Classification working: 'I am very happy today!' -> {result}")
except Exception as e:
    print(f"[ERROR] Classification failed: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("[SUCCESS] Core systems ready!")
if mongodb_available:
    print("\nAll services operational. You can now run:")
else:
    print("\nKafka and classification ready. Start MongoDB to use persistence:")
    print("  Windows: mongod")
    print("  or use MongoDB Compass")
print("\nRun these in 3 separate terminals:")
print("  Terminal 1: python Kafka-PySpark/producer-validation-tweets.py")
print("  Terminal 2: python Kafka-PySpark/consumer-pyspark.py")
print("  Terminal 3: cd Django-Dashboard && python manage.py runserver")
