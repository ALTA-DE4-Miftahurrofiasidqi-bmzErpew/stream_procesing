import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import StringDeserializer


def consume():
    string_deserializer = StringDeserializer("utf_8")

    config = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "my-consumer5",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(config)
    consumer.subscribe(
        ["postgres.public.sample_table"]
    )  # Sesuaikan dengan topic yang digunakan oleh Debezium

    while True:
        try:
            msg = consumer.poll(1)

            if msg is None:
                continue

            key = string_deserializer(msg.key(), None)
            value = json.loads(msg.value().decode("utf-8"))

            print(f"Key: {key}")
            print(f"Value: {value}")
            print("-------------------------")

        except KafkaException as e:
            print("Kafka failure " + str(e))

    consumer.close()


def main():
    consume()


if __name__ == "__main__":
    main()
