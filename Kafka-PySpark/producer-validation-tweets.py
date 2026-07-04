import argparse
import csv
import json
from pathlib import Path
from time import sleep

from kafka import KafkaProducer
from xquik_tweets import iter_xquik_jsonl


def positive_float(value):
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("sleep seconds must be 0 or greater")
    return parsed


def optional_positive_int(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("limit must be 1 or greater")
    return parsed


def iter_csv_rows(path, limit=None):
    csv_path = Path(path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {path}")

    emitted = 0
    with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
        reader_obj = csv.reader(file_obj)
        for row_number, data in enumerate(reader_obj, 1):
            if not data or not any(cell.strip() for cell in data):
                print(f"Skipping empty CSV row {row_number}")
                continue
            if not data[-1].strip():
                print(f"Skipping CSV row {row_number} without tweet text")
                continue
            yield data
            emitted += 1
            if limit is not None and emitted >= limit:
                return


def main():
    parser = argparse.ArgumentParser(description="Stream tweet rows to Kafka")
    parser.add_argument("--csv", default="twitter_validation.csv", help="CSV file to stream")
    parser.add_argument("--xquik-jsonl", help="Xquik tweet JSON Lines file to stream")
    parser.add_argument("--topic", default="numtest", help="Kafka topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--sleep-seconds", default=3, type=positive_float, help="Delay between rows")
    parser.add_argument("--limit", type=optional_positive_int, help="Maximum rows to send")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    records = (
        iter_xquik_jsonl(args.xquik_jsonl, limit=args.limit)
        if args.xquik_jsonl
        else iter_csv_rows(args.csv, limit=args.limit)
    )
    for data in records:
        producer.send(args.topic, value=data)
        print(f"Produced: {data}")
        sleep(args.sleep_seconds)
    producer.flush()


if __name__ == "__main__":
    main()
