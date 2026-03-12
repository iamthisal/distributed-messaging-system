import uuid
from datetime import datetime
from typing import List

from fastapi import FastAPI, HTTPException

from database import add_message, get_messages_for, get_all_messages, clear_all
from models import MessageRequest,MessageResponse

app = FastAPI(title="Distributed Chat System")

@app.post("/send", response_model=MessageResponse)
def send_message(request: MessageRequest):
    """
    Client calls this to send a message.
    The server assigns a unique ID and timestamp, then stores it.
    """
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Message text cannot be empty")

    message = {
        "id": str(uuid.uuid4()),  # unique message ID
        "sender": request.sender,
        "receiver": request.recipient,
        "content": request.text,
        "timestamp": datetime.utcnow().isoformat()
    }

    add_message(message)

    print(f"[NEW MESSAGE] {message['sender']} → {message['receiver']}: {message['content']}")

    return message


@app.get("/messages", response_model=List[MessageResponse])
def get_messages(receiver: str = None):
    if receiver:
        return  get_messages_for(receiver)
    return get_all_messages()



@app.delete("/messages")
def clear_messages():
    """
    Utility endpoint to clear all messages (useful for testing).
    """
    clear_all()
    return {"status": "All messages cleared"}










