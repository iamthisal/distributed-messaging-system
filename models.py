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


