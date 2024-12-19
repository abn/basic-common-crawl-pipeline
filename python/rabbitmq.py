import os
import time

from abc import ABC
from abc import abstractmethod

import pika
import pika.exceptions


QUEUE_NAME = "batches"
RABBITMQ_PUBLISH_RETRIES = int(os.environ.get("RABBITMQ_PUBLISH_RETRIES", "3"))
MAX_PUBLISH_BACKOFF = float(os.environ.get("RABBITMQ_PUBLISH_MAX_BACKOFF", "15"))


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.channel = rabbitmq_channel()

    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        # this should be the reconnect interval from the rabbitMQ config
        # retries can be cleaner async if moving to aio-pika
        backoff = 1

        while (attempts := 0) < RABBITMQ_PUBLISH_RETRIES:
            try:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=body.encode(),
                )
                break
            except pika.exceptions.AMQPConnectionError:
                # this should really move to a logger
                print(  # noqa: T201
                    f"Connection error on attempt {attempts + 1}/{RABBITMQ_PUBLISH_RETRIES}"
                )

            attempts += 1

            # this should really be a deferred task
            backoff = min(backoff * 1.5, MAX_PUBLISH_BACKOFF)
            time.sleep(backoff)
        else:
            # this should be modified to a specific exception and handled appropriately for retry/investigation
            raise RuntimeError("Failed to publish message")


def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:
    connection = pika.BlockingConnection(
        pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    return channel
