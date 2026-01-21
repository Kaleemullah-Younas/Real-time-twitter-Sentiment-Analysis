#!/usr/bin/env python3
"""
Verification script to test all components before running the full pipeline
Run this script to diagnose any issues with the setup
"""

import sys
import time
from datetime import datetime

print("=" * 70)
print("BIG DATA PROJECT - COMPONENT VERIFICATION")
print("=" * 70)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting verification...\n")

# Test 1: Python & Basic Imports
print("[TEST 1] Python Version & Basic Imports")
print("-" * 70)
try:
    print(f"  Python Version: {sys.version}")
    import pandas as pd

    print(f"  ✓ Pandas: {pd.__version__}")
    import nltk

    print(f"  ✓ NLTK: {nltk.__version__}")
    import pymongo

    print(f"  ✓ PyMongo: {pymongo.__version__}")
    from kafka import KafkaConsumer, KafkaProducer

    print(f"  ✓ Kafka-Python: Available")
except ImportError as e:
    print(f"  ✗ Import Error: {e}")
    sys.exit(1)

print()

# Test 2: PySpark & Py4J
print("[TEST 2] PySpark & Py4J Versions")
print("-" * 70)
try:
    import pyspark

    print(f"  ✓ PySpark: {pyspark.__version__}")
    if pyspark.__version__ != "3.4.3":
        print(f"  ⚠ WARNING: Expected PySpark 3.4.3, got {pyspark.__version__}")
        print(f"     Run: pip install pyspark==3.4.3")

    import py4j

    print(f"  ✓ Py4J: {py4j.__version__}")
    if py4j.__version__ != "0.10.9.7":
        print(f"  ⚠ WARNING: Expected Py4J 0.10.9.7, got {py4j.__version__}")
        print(f"     Run: pip install py4j==0.10.9.7")
except ImportError as e:
    print(f"  ✗ Import Error: {e}")
    sys.exit(1)

print()

# Test 3: Java Installation
print("[TEST 3] Java Installation")
print("-" * 70)
try:
    import subprocess

    result = subprocess.run(
        ["java", "-version"], capture_output=True, text=True, timeout=5
    )
    java_info = result.stderr if result.stderr else result.stdout
    print(f"  ✓ Java available:")
    for line in java_info.split("\n")[:2]:
        if line.strip():
            print(f"     {line.strip()}")
except Exception as e:
    print(f"  ✗ Java Error: {e}")
    print(f"     Please install Java 17 or higher")
    sys.exit(1)

print()

# Test 4: Model File
print("[TEST 4] ML Model File")
print("-" * 70)
import os

model_path = "Kafka-PySpark/logistic_regression_model.pkl"
if os.path.isdir(model_path):
    print(f"  ✓ Model directory found: {model_path}")
    files = os.listdir(model_path)
    print(f"     Contains {len(files)} files")
elif os.path.isfile(model_path):
    size = os.path.getsize(model_path) / (1024 * 1024)
    print(f"  ✓ Model file found: {model_path}")
    print(f"     Size: {size:.2f} MB")
else:
    print(f"  ✗ Model not found at {model_path}")
    print(f"     Please run: ML PySpark Model/Big_Data.ipynb")

print()

# Test 5: MongoDB Connection
print("[TEST 5] MongoDB Connection")
print("-" * 70)
try:
    from pymongo import MongoClient

    MONGODB_URI = "mongodb+srv://user_420:useradmin@cluster0.adjdner.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
    print(f"  ✓ MongoDB connected successfully")
    db = client["big_data_project"]
    collections = db.list_collection_names()
    print(f"  ✓ Database 'big_data_project' exists")
    if "tweets" in collections:
        count = db.tweets.count_documents({})
        print(f"  ✓ Collection 'tweets' exists with {count} documents")
    else:
        print(f"  ℹ Collection 'tweets' does not exist yet (will be created)")
    client.close()
except Exception as e:
    print(f"  ✗ MongoDB Connection Error: {e}")
    print(f"     Check your internet connection and MongoDB Atlas cluster status")

print()

# Test 6: Kafka Connection
print("[TEST 6] Kafka Connection")
print("-" * 70)
try:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"], request_timeout_ms=5000
    )
    print(f"  ✓ Kafka accessible at localhost:9092")
    producer.close()
except Exception as e:
    print(f"  ✗ Kafka Connection Error: {e}")
    print(f"     Ensure Kafka is running on localhost:9092")
    print(f"     Run Docker Compose: docker-compose up -d")

print()

# Test 7: Django Setup
print("[TEST 7] Django Project")
print("-" * 70)
try:
    os.chdir("Django-Dashboard")
    import django
    from django.conf import settings

    print(f"  ✓ Django: {django.__version__}")
    if os.path.isfile("db.sqlite3"):
        print(f"  ✓ Database file exists: db.sqlite3")
    else:
        print(f"  ⚠ Database not initialized. Run: python manage.py migrate")
    os.chdir("..")
except Exception as e:
    print(f"  ✗ Django Error: {e}")
    os.chdir("..")

print()

# Summary
print("=" * 70)
print("VERIFICATION COMPLETE")
print("=" * 70)
print("\nNext Steps:")
print("1. Ensure Kafka is running (docker-compose up -d)")
print("2. Run producer:   python Kafka-PySpark/producer-validation-tweets.py")
print("3. Run consumer:   python Kafka-PySpark/consumer-pyspark.py")
print("4. Run Django:     cd Django-Dashboard && python manage.py runserver")
print("5. Visit:          http://127.0.0.1:8000/dashboard/")
print()
