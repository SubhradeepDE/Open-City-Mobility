import json

from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vehicle_positions"


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mobility-debug-consumer",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


print("========================================")
print("Kafka Vehicle Position Consumer")
print("========================================")
print(f"Topic: {KAFKA_TOPIC}")
print("Waiting for messages...\n")


try:
    for message in consumer:

        vehicle = message.value

        print(
            f"Partition: {message.partition} | "
            f"Offset: {message.offset}"
        )

        print(
            f"Vehicle: {vehicle.get('vehicle_id')} | "
            f"Route: {vehicle.get('route_id')} | "
            f"Lat: {vehicle.get('latitude')} | "
            f"Lon: {vehicle.get('longitude')} | "
            f"Timestamp: {vehicle.get('vehicle_timestamp')}"
        )

        print("-" * 80)

except KeyboardInterrupt:

    print("\nStopping consumer...")

finally:

    consumer.close()
    print("Consumer closed.")