from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import leader_election as le

router = APIRouter()

class ElectionMessage(BaseModel):
    candidate_id: int
    term: int

class VictoryMessage(BaseModel):
    leader_id: int
    term: int

class HeartbeatMessage(BaseModel):
    leader_id: int
    term: int

@router.get("/")
def get_leader():
    s = le.state
    if s.current_leader is None:
        raise HTTPException(status_code=503, detail={"error": "No leader elected yet", "election_in_progress": s.election_in_progress})
    return {"leader_id": s.current_leader, "is_me": s.is_leader, "node_id": le.NODE_ID, "term": s.term, "last_heartbeat": s.last_heartbeat, "election_in_progress": s.election_in_progress}

@router.post("/elect")
async def trigger_election():
    s = le.state
    if s.election_in_progress:
        raise HTTPException(status_code=409, detail={"error": "Election already in progress", "term": s.term})
    await le.start_election()
    return {"message": "Election started", "initiator_id": le.NODE_ID, "term": s.term}

@router.post("/election")
async def receive_election(msg: ElectionMessage):
    return await le.handle_election_message(msg.candidate_id, msg.term)

@router.post("/victory")
def receive_victory(msg: VictoryMessage):
    return le.handle_victory_message(msg.leader_id, msg.term)

@router.post("/heartbeat")
def receive_heartbeat(msg: HeartbeatMessage):
    return le.handle_heartbeat(msg.leader_id, msg.term)
