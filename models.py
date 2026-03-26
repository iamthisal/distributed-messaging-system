from typing import List, Optional

from pydantic import BaseModel, Field

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


class LeaderAnnouncement(BaseModel):
    new_primary_url: str
    known_nodes: List[str] = Field(default_factory=list)


