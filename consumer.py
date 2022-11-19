import numpy as np
import cv2
import sys
import kombu
from kombu.mixins import ConsumerMixin
import datetime
import os
import glob


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

        if msg_end:
            self.out.release()
            self.out = None
            key = f"{msg_source}-{frame_timestamp}"
            self.upload_to_s3(key)

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
        if not self.out:
            self.out = cv2.VideoWriter('output_video.avi',cv2.VideoWriter_fourcc(*'DIVX'), 60, (size, 1))
        self.out.write(image)

        # Remove Message From Queue
        message.ack()


