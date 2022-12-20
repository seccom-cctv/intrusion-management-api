import ssl
import threading
import cv2
import imutils
import kombu
import datetime
from kombu.mixins import ConsumerMixin
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv() 

RABBIT_MQ_URL = os.getenv("RABBIT_MQ_URL")
RABBIT_MQ_USERNAME = os.getenv("RABBIT_MQ_USERNAME")
RABBIT_MQ_PASSWORD = os.getenv("RABBIT_MQ_PASSWORD")
RABBIT_MQ_EXCHANGE_NAME = os.getenv("RABBIT_MQ_EXCHANGE_NAME")
RABBIT_MQ_QUEUE_NAME = os.getenv("RABBIT_MQ_QUEUE_NAME")


class myConsumer(ConsumerMixin):

    def __init__(self):
        pass

    def start_connection(self):
        connection_string = f"amqps://{RABBIT_MQ_USERNAME}:{RABBIT_MQ_PASSWORD}" \
            f"@{RABBIT_MQ_URL}/"
        print(connection_string)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

        # Kombu Connection
        self.connection = kombu.Connection(connection_string, ssl=ssl_context)
        self.kombu_channel = self.connection.channel()

        # Kombu Exchange
        exchange1 = kombu.Exchange(
            name=RABBIT_MQ_EXCHANGE_NAME,
            type="direct",
            delivery_mode=1
        )
        prod_exchange1 = kombu.Exchange(
            name='camera-exchange',
            type="direct",
            delivery_mode=1
        )
        prod_exchange2 = kombu.Exchange(
            name='alarm-exchange',
            type="direct",
            delivery_mode=1
        )

        # Kombu Producer
        self.producer1 = kombu.Producer(
            exchange=prod_exchange1,
            channel=self.kombu_channel
        )

        self.producer2 = kombu.Producer(
            exchange=prod_exchange2,
            channel=self.kombu_channel
        )

        # Kombu Queue
        self.queues = kombu.Queue(
                    name="intrusion-detected",
                    exchange=exchange1,
                    bindings=[
                        kombu.binding(exchange1, routing_key='intrusion-detected'),
                    ],
                )

        self.queues.maybe_bind(self.connection)
        self.queues.declare()

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=self.queues,
                callbacks=[self.on_message],
                )
            ]

    def on_message(self, body, message):
        # Get message headers' information
        
        # TODO: Create intrusion
        logging.debug("MESSAGE RECEIVED")
        logging.debug(body)
        print(body)

        self.producer1.publish(
            body,
            routing_key='create-snapshot'
        )

        self.producer2.publish(
            body,
            routing_key='activate-alarm'
        )

        # Remove Message From Queue
        message.ack()

