from typing import Union
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.responses import RedirectResponse
import boto3
import shutil
import os
import threading
import logging
from event_handler import myConsumer
import os
from dotenv import load_dotenv
from pathlib import Path
import requests

# os.environ
# AMQP Variables
RABBIT_MQ_URL = os.getenv("RABBIT_MQ_URL")

app = FastAPI()

items = {"foo": "The Foo Wrestlers"}

load_dotenv()

API_URL = os.getenv("NOTIFICATIONS_API_URL")
#API_URL = "http://localhost:8081/send?camera_id"
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

logging.basicConfig(level=logging.INFO)

consumer_thread = None
#client = boto3.client('secretsmanager')
s3 = boto3.client("s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key= SECRET_KEY)

@app.on_event("startup") 
async def startup_event():
    consumer = myConsumer()
    consumer.start_connection()
    consumer_thread  =threading.Thread(target=consumer.run)
    consumer_thread.start()

@app.get("/")
async def root():
    return RedirectResponse(url='/docs')

@app.post("/uploadfile/{id}")
async def create_upload_file(id: int, file: UploadFile =  File(...)):
    fname = f"intruders/{file.filename}"
    with open(fname, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    s3.upload_file(
        Filename=fname,
        Bucket="seccom.video.store.1",
        Key=file.filename
    )
    os.remove(fname)

    dispatch_notification(id)

    return {"filename": file.filename}


def dispatch_notification(camera_id):
    global API_URL

    url = f'{API_URL}={camera_id}'
    
    r = requests.post(url = url)
  
    # extracting data in json format
    data = r.json()
    print(data)

    pass
