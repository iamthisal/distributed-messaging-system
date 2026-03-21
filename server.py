import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException

from database import add_message, get_messages_for, get_all_messages, clear_all
from models import MessageRequest, MessageResponse
from replication import REPLICAS, register_replica, replicate_to_all, register_with_primary


import sys

port = int(sys.argv[-1]) if sys.argv[-1].isdigit() else 8000
OWN_URL = f"http://localhost:{port}"
IS_PRIMARY = (port == 8000)
PRIMARY_URL = "http://localhost:8000"




# ─────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    if not IS_PRIMARY:
        await register_with_primary(PRIMARY_URL, OWN_URL)
    yield

app = FastAPI(title="Distributed Chat System", lifespan=lifespan)

# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "status": "running",
        "is_primary": IS_PRIMARY,
        "own_url": OWN_URL,
        "replicas": REPLICAS
    }

@app.post("/register")
def register(url: str):
    register_replica(url)
    return {"status": "registered", "replicas": REPLICAS}

@app.post("/replicate", response_model=MessageResponse)
def receive_replicated_message(message: MessageResponse):
    add_message(message.dict())
    print(f"[REPLICATED] {message.sender} → {message.receiver}: {message.content}")
    return message

@app.post("/send", response_model=MessageResponse)
def send_message(request: MessageRequest):
    if not request.content.strip():
        raise HTTPException(status_code=400, detail="Message text cannot be empty")

    message = {
        "id": str(uuid.uuid4()),
        "sender": request.sender,
        "receiver": request.receiver,
        "content": request.content,
        "timestamp": datetime.utcnow().isoformat()
    }

    add_message(message)
    print(f"[NEW MESSAGE] {message['sender']} → {message['receiver']}: {message['content']}")

    if IS_PRIMARY:
        replicate_to_all(message)

    return message

@app.get("/messages", response_model=List[MessageResponse])
def get_messages(receiver: str = None):
    if receiver:
        return get_messages_for(receiver)
    return get_all_messages()

@app.delete("/messages")
def clear_messages():
    clear_all()
    return {"status": "All messages cleared"}







