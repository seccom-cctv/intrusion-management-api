import numpy as np
import cv2
import sys
import kombu
from kombu.mixins import ConsumerMixin
import datetime
import os
import glob
import boto3


# Kombu Message Consuming
class Worker(ConsumerMixin):

    def __init__(self, connection, queues, s3, output_dir):
        self.connection = connection
        self.queues = queues
        self.s3 = s3
        self.output_dir = output_dir
        self.HOGCV = cv2.HOGDescriptor()
        self.HOGCV.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
        self.out = None

        


    def get_consumers(self, Consumer, channel):
        return [
            Consumer(
                queues=self.queues,
                callbacks=[self.on_message],
                accept=['image/jpeg']
                )
            ]

    def upload_to_s3(self, key):
        self.s3.upload_file(
            Filename="output_video.avi",
            Bucket="seccom-video-store",
            Key=key,
        )
        


    def on_message(self, body, message):
        # Get message headers' information

        msg_end = message.headers["end"]
        msg_source = message.headers["source"]
        frame_timestamp = message.headers["timestamp"]
        frame_count = message.headers["frame_count"]
        frame_id = message.headers["frame_id"]

        # Debug
        print(f"I received the frame number {frame_count} from {msg_source}" +
              f", with the timestamp {frame_timestamp}.")

        # Process the Frame
        # Get the original  byte array size
        size = sys.getsizeof(body) - 33
        # Jpeg-encoded byte array into numpy array
        np_array = np.frombuffer(body, dtype=np.uint8)
        np_array = np_array.reshape((size, 1))
        # Decode jpeg-encoded numpy array
        image = cv2.imdecode(np_array, 1)

        height, width, _ = image.shape
        
        if not self.out:
            self.out = cv2.VideoWriter('output_video.avi',cv2.VideoWriter_fourcc(*'DIVX'), message.headers['fps'], (width, height))
        self.out.write(image)

        if msg_end:
            try:
                self.out.release()
            except Exception as e:
                print(e)
                quit()
            print("RELEASED")
            self.out = None
            key = f"{msg_source}-{frame_timestamp}"
            print(f"Upload video {key}")
            self.upload_to_s3(key)
            message.ack()
            return

        # Remove Message From Queue
        message.ack()


class Video_Consumer:

    def __init__(self, output_dir):
        self.database = {}
        self.output_dir = output_dir
        self.__bootstrap_output_directory()

    def __bootstrap_output_directory(self):
        if os.path.isdir(self.output_dir):
            files = os.listdir(self.output_dir)
            for f in files:
                os.remove(os.path.join(self.output_dir, f))
        else:
            os.mkdir(self.output_dir)

    def start_processing(self, broker_url, broker_username,
                         broker_password, exchange_name, queue_name):

        # Create Connection String
        connection_string = f"amqp://{broker_username}:{broker_password}" \
            f"@{broker_url}/"

        # Kombu Exchange
        self.kombu_exchange = kombu.Exchange(
            name=exchange_name,
            type="direct",
        )

        # Kombu Queues
        self.kombu_queues = [
            kombu.Queue(
                name=queue_name,
                exchange=self.kombu_exchange,
                
            )
        ]

        # Kombu Connection
        self.kombu_connection = kombu.Connection(
            connection_string,
            heartbeat=4
        )

        s3 = boto3.client("s3")

        # Start Human Detection Workers
        self.human_detection_worker = Worker(
            connection=self.kombu_connection,
            queues=self.kombu_queues,
            s3=s3,
            output_dir=self.output_dir
        )
        self.human_detection_worker.run()


if __name__ == '__main__':
    # AMQP Variables
    RABBIT_MQ_URL = "localhost:5672"
    RABBIT_MQ_USERNAME = "myuser"
    RABBIT_MQ_PASSWORD = "mypassword"
    RABBIT_MQ_EXCHANGE_NAME = "human-detection-exchange"
    RABBIT_MQ_QUEUE_NAME = "human-detection-queue"

    # OUTPUT
    OUTPUT_DIR = "intruders"

    worker = Video_Consumer(OUTPUT_DIR)

    worker.start_processing(
        broker_url=RABBIT_MQ_URL,
        broker_username=RABBIT_MQ_USERNAME,
        broker_password=RABBIT_MQ_PASSWORD,
        exchange_name=RABBIT_MQ_EXCHANGE_NAME,
        queue_name=RABBIT_MQ_QUEUE_NAME
        )