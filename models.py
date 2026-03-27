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
    corrected_timestamp: str
    logical_timestamp: int


class TimeSyncRequest(BaseModel):
    client_send_time_ms: int


class TimeSyncResponse(BaseModel):
    current_primary_url: str
    own_url: str
    server_receive_time_ms: int
    server_send_time_ms: int


