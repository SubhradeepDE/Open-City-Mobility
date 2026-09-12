import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"
KAFKA_GROUP_ID = "bronze-storage-consumer"

BRONZE_DIR = "data/bronze/stream"


def create_consumer() -> KafkaConsumer:
    """Create Kafka consumer."""

    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


def save_event(event: dict, partition: int, offset: int) -> None:
    """Save a Kafka event as raw JSONL."""

    os.makedirs(BRONZE_DIR, exist_ok=True)

    now = datetime.now(timezone.utc)

    # Partition Bronze files by date.
    date_dir = os.path.join(
        BRONZE_DIR,
        now.strftime("%Y/%m/%d"),
    )

    os.makedirs(date_dir, exist_ok=True)

    output_file = os.path.join(
        date_dir,
        "vehicle_positions.jsonl",
    )

    raw_record = {
        **event,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "bronze_ingestion_timestamp": now.isoformat(),
    }

    with open(
        output_file,
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(raw_record)
            + "\n"
        )


def main():

    print("========================================")
    print("Kafka → Bronze Consumer")
    print("========================================")

    print(
        f"Topic: {KAFKA_TOPIC}"
    )

    print(
        f"Consumer group: {KAFKA_GROUP_ID}"
    )

    print(
        f"Bronze location: {BRONZE_DIR}"
    )

    print("\nWaiting for events...\n")

    consumer = create_consumer()

    processed = 0

    try:

        for message in consumer:

            save_event(
                event=message.value,
                partition=message.partition,
                offset=message.offset,
            )

            processed += 1

            if processed <= 10 or processed % 1000 == 0:

                print(
                    f"Processed: {processed} | "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset} | "
                    f"Vehicle: "
                    f"{message.value.get('vehicle_id')}"
                )

    except KeyboardInterrupt:

        print(
            f"\nStopping consumer."
        )

        print(
            f"Total processed: {processed}"
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()