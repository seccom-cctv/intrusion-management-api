from typing import Union
from fastapi import FastAPI, HTTPException

app = FastAPI()

items = {"foo": "The Foo Wrestlers"}


@app.get("/")
async def read_root():
    '''Document endpoint usage here'''

    return {"Hello": "World"}


@app.get("/items/{item_id}")
async def read_item(item_id: int, q: Union[str, None] = None):
    '''Document endpoint usage here'''

    if item_id not in items:
        raise HTTPException(status_code=404, detail="Item not found")

    return {"item_id": item_id, "q": q}