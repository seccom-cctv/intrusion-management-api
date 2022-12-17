from typing import Union
from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.responses import RedirectResponse
import boto3
import shutil
import os
import threading
from event_handler import myConsumer

app = FastAPI()

items = {"foo": "The Foo Wrestlers"}

consumer_thread = None
#client = boto3.client('secretsmanager')
s3 = boto3.client("s3")

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
        Bucket="seccom.video.store",
        Key=file.filename
    )
    os.remove(fname)

    return {"filename": file.filename}


def dispatch_alarm(alarm_id):
    
    
    pass
