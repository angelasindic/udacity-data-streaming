import asyncio

from dataclasses import asdict, dataclass, field
from io import BytesIO
import random

from confluent_kafka import Producer, Consumer
from faker import Faker
from fastavro import parse_schema, writer
import io
from fastavro import reader


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"


@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    schema = parse_schema(
        {
            "type": "record",
            "name": "click_event",
            "namespace": "com.udacity.lesson3.exercise2",
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
                #
                # TODO: Add the attributes map!
                {"name": "attributes",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "attribute",
                            "fields": [
                                {"name": "element", "type": "string"},
                                {"name": "content", "type": "string"}
                            ]
                        }
                    }
                 }
            ]
        }
    )

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        out = BytesIO()
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(0)
    event_dict = [m for m in (reader(message_bytes))]
    return event_dict

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        #
        # Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        messages = c.consume(10, timeout=1.)
        if messages is None:
            print("no messages received by consumer")
        else:
            for message in messages:
                if message.error() is not None:
                    print(f"error from consumer {message.error()}")
                else:
                    event_dict = decode(message.value())


                    print(f"consumed message: {event_dict}")


        await asyncio.sleep(0.01)

def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("com.udacity.lesson3.exercise3.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2

if __name__ == "__main__":
    main()
