from typing import Optional

from pydantic import BaseModel

class MessageRequest(BaseModel):
    sender : str
    receiver : str
    content : str

class MessageResponse(BaseModel):
    id : str
    sender : str
    receiver : str
    content : str
    timestamp : str

class ReplicatedMessageResponse(MessageResponse):
    replication_status: str = "full"
    warning: Optional[str] = None


