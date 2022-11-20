from typing import Union
from fastapi import FastAPI, HTTPException
from consumer import Video_Consumer
import asyncio
import threading

app = FastAPI()

items = {"foo": "The Foo Wrestlers"}

consumer_thread = None

@app.on_event('startup')
async def startup_event():
    print("Startup...")
    # AMQP Variables
    RABBIT_MQ_URL = "localhost:5672"
    RABBIT_MQ_USERNAME = "myuser"
    RABBIT_MQ_PASSWORD = "mypassword"
    RABBIT_MQ_EXCHANGE_NAME = "human-detection-exchange"
    RABBIT_MQ_QUEUE_NAME = "human-detection-queue"

    # OUTPUT
    OUTPUT_DIR = "intruders"

    worker = Video_Consumer(OUTPUT_DIR)

    kwargs = {
            "broker_url": RABBIT_MQ_URL,
            "broker_username": RABBIT_MQ_USERNAME,
            "broker_password": RABBIT_MQ_PASSWORD,
            "exchange_name": RABBIT_MQ_EXCHANGE_NAME,
            "queue_name": RABBIT_MQ_QUEUE_NAME
    }

    consumer_thread = threading.Thread(
        target=worker.start_processing, 
        kwargs=kwargs
    )
    consumer_thread.start() 

    print("End startup...")
    
@app.on_event('shutdown')
async def shutdown():
    print("Shutdown")
    quit()

@app.get("/")
async def read_root():
    '''Document endpoint usage here'''

    return {"Hello": "World"}


