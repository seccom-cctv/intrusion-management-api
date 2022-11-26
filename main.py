from typing import Union
from fastapi import FastAPI, HTTPException, File, UploadFile
from consumer import Video_Consumer
import threading
import boto3
import shutil
import os

app = FastAPI()

items = {"foo": "The Foo Wrestlers"}

consumer_thread = None
#client = boto3.client('secretsmanager')
s3 = boto3.client("s3")

@app.get("/")
async def read_root():
    '''Document endpoint usage here'''

    return {"Hello": "World"}

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile =  File(...)):
    fname = f"intruders/{file.filename}"
    with open(fname, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    s3.upload_file(
        Filename=fname,
        Bucket="seccom-video-store",
        Key=file.filename
    )
    os.remove(fname)

    return {"filename": file.filename}


