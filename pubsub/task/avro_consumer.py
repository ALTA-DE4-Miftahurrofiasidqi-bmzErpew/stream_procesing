from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer


def consume():
    schema_registry_conf = {"url": "http://localhost:18081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client)
    string_deserializer = StringDeserializer("utf_8")
    config = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "my-consumer1",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(config)
    consumer.subscribe(["clickstream"])

    while True:
        try:
            msg = consumer.poll(1)

            if msg is None:
                continue

            # Deserialisasi key dan value menggunakan AvroDeserializer
            key = string_deserializer(msg.key(), None)
            value = avro_deserializer(msg.value(), None)

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
