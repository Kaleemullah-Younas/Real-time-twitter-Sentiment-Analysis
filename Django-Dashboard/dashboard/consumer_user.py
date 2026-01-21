from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
import re
import nltk
import os
import sys

# Critical Java 17 compatibility fix - must be before ANY Spark imports
os.environ["JAVA_TOOL_OPTIONS"] = (
    "-Dio.netty.tryReflectionSetAccessible=true "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/sun.util=ALL-UNNAMED"
)

nltk.download("stopwords")
nltk.download("punkt")

# Global variables - initialized on first use
_spark = None
_pipeline = None


def _get_spark_session():
    """Lazy initialization of SparkSession to avoid import-time issues with Java/Scala"""
    global _spark
    if _spark is None:
        java_options = " ".join(
            [
                "-Dio.netty.tryReflectionSetAccessible=true",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/sun.util=ALL-UNNAMED",
            ]
        )
        _spark = (
            SparkSession.builder.appName("classify tweets")
            .config("spark.driver.extraJavaOptions", java_options)
            .config("spark.executor.extraJavaOptions", java_options)
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
    return _spark


def _get_pipeline():
    """Lazy initialization of ML Pipeline"""
    global _pipeline
    if _pipeline is None:
        model_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "Kafka-PySpark",
            "logistic_regression_model",
        )
        _pipeline = PipelineModel.load(model_path)
    return _pipeline


def clean_text(text):
    if text is not None:
        # Remove links starting with https://, http://, www., or containing .com
        text = re.sub(r"https?://\S+|www\.\S+|S+\.com\S+|youtu\.be/\S+", "", text)

        # Remove words starting with # or @
        text = re.sub(r"(@|#)\w+", "", text)

        # Convert to lowercase
        text = text.lower()

        # Remove non-alphabitic characters
        text = re.sub(r"[^a-zA-Z\s]", "", text)

        # Remove extra whitespaces
        text = re.sub(r"\s+", " ", text).strip()
        return text
    else:
        return ""


class_index_mapping = {0: "Negative", 1: "Positive", 2: "Neutral", 3: "Irrelevant"}


def classify_text(text: str):
    """
    Classify tweet sentiment.
    Returns a classification or error message if model not available.
    """
    try:
        spark = _get_spark_session()
        pipeline = _get_pipeline()

        preprocessed_tweet = clean_text(text)
        data = [
            (preprocessed_tweet,),
        ]
        data = spark.createDataFrame(data, ["Text"])
        # Apply the pipeline to the new text
        processed_validation = pipeline.transform(data)
        prediction = processed_validation.collect()[0][6]

        print("-> Tweet : ", text)
        print("-> preprocessed_tweet : ", preprocessed_tweet)
        print("-> Predicted Sentiment : ", prediction)
        print(
            "-> Predicted Sentiment classname : ", class_index_mapping[int(prediction)]
        )

        return class_index_mapping[int(prediction)]

    except FileNotFoundError as e:
        print(f"[MODEL ERROR] {e}")
        print(
            "Model file not found. Please train Big_Data.ipynb first to create the model file."
        )
        return "Model Not Ready - Please train the ML model first"

    except Exception as e:
        print(f"[CLASSIFICATION ERROR] {type(e).__name__}: {e}")
        # Fallback: return a basic prediction based on keywords
        sentiment_keywords = {
            "Positive": [
                "happy",
                "good",
                "great",
                "excellent",
                "love",
                "amazing",
                "awesome",
                "wonderful",
            ],
            "Negative": [
                "sad",
                "bad",
                "hate",
                "terrible",
                "awful",
                "horrible",
                "angry",
                "disappointed",
            ],
            "Neutral": ["ok", "fine", "normal", "okay"],
        }

        text_lower = text.lower()
        for sentiment, keywords in sentiment_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                return sentiment

        return "Neutral"  # Default fallback

    # # Prepare document to insert into MongoDB
    # tweet_doc = {
    #     "tweet": text,
    #     "prediction": class_index_mapping[int(prediction)]
    # }

    # # Insert document into MongoDB collection
    # collection.insert_one(tweet_doc)

    print("/" * 50)

    return class_index_mapping[int(prediction)]


# print("/"*50)
