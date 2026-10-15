import asyncio
import httpx
import logging
import os
import json
import random
from dataclasses import dataclass
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

NODE_ID = int(os.getenv("NODE_ID", "4"))
NODE_PORT = int(os.getenv("PORT", "3004"))

_peers_raw = os.getenv("PEERS", '[{"id":1,"url":"http://localhost:3001"},{"id":2,"url":"http://localhost:3002"},{"id":3,"url":"http://localhost:3003"}]')
PEERS = json.loads(_peers_raw)

ELECTION_TIMEOUT_S = 3.0
HEARTBEAT_INTERVAL_S = 2.0
HEARTBEAT_MISS_LIMIT = 3

@dataclass
class NodeState:
    current_leader: Optional[int] = None
    is_leader: bool = False
    election_in_progress: bool = False
    term: int = 0
    last_heartbeat: Optional[float] = None
    missed_heartbeats: int = 0

state = NodeState()

_heartbeat_task = None
_watchdog_task = None

def higher_peers():
    return [p for p in PEERS if p["id"] > NODE_ID]

def all_peers():
    return PEERS

async def post(url, path, body, timeout=2.0):
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(f"{url}{path}", json=body, timeout=timeout)
            return res.json()
    except Exception:
        return None

async def _send_heartbeats_loop():
    log.info(f"[Node {NODE_ID}] Sending heartbeats")
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)
        if not state.is_leader:
            break
        payload = {"leader_id": NODE_ID, "term": state.term}
        await asyncio.gather(*[post(p["url"], "/leader/heartbeat", payload) for p in all_peers()], return_exceptions=True)

def start_sending_heartbeats():
    global _heartbeat_task
    stop_sending_heartbeats()
    _heartbeat_task = asyncio.create_task(_send_heartbeats_loop())

def stop_sending_heartbeats():
    global _heartbeat_task
    if _heartbeat_task and not _heartbeat_task.done():
        _heartbeat_task.cancel()
    _heartbeat_task = None

async def _watchdog_loop():
    state.missed_heartbeats = 0
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_S)
        if state.is_leader:
            break
        state.missed_heartbeats += 1
        log.info(f"[Node {NODE_ID}] Missed heartbeats: {state.missed_heartbeats}/{HEARTBEAT_MISS_LIMIT}")
        if state.missed_heartbeats >= HEARTBEAT_MISS_LIMIT:
            log.info(f"[Node {NODE_ID}] Leader down — starting election")
            stop_heartbeat_watchdog()
            asyncio.create_task(start_election())
            break

def start_heartbeat_watchdog():
    global _watchdog_task
    stop_heartbeat_watchdog()
    _watchdog_task = asyncio.create_task(_watchdog_loop())

def stop_heartbeat_watchdog():
    global _watchdog_task
    if _watchdog_task and not _watchdog_task.done():
        _watchdog_task.cancel()
    _watchdog_task = None

async def start_election():
    if state.election_in_progress:
        return
    state.election_in_progress = True
    state.term += 1
    log.info(f"[Node {NODE_ID}] Starting election for term {state.term}")
    higher = higher_peers()
    if not higher:
        await declare_victory()
        return
    responses = await asyncio.gather(
        *[post(p["url"], "/leader/election", {"candidate_id": NODE_ID, "term": state.term}, ELECTION_TIMEOUT_S) for p in higher],
        return_exceptions=True
    )
    any_ok = any(isinstance(r, dict) and r.get("ok") is True for r in responses)
    if any_ok:
        log.info(f"[Node {NODE_ID}] Higher node responded — stepping back")
        state.election_in_progress = False
        start_heartbeat_watchdog()
    else:
        await declare_victory()

async def declare_victory():
    state.is_leader = True
    state.current_leader = NODE_ID
    state.election_in_progress = False
    log.info(f"[Node {NODE_ID}] *** Node {NODE_ID} is now the LEADER (term {state.term}) ***")
    stop_heartbeat_watchdog()
    start_sending_heartbeats()
    payload = {"leader_id": NODE_ID, "term": state.term}
    await asyncio.gather(*[post(p["url"], "/leader/victory", payload) for p in all_peers()], return_exceptions=True)

async def handle_election_message(candidate_id, term):
    log.info(f"[Node {NODE_ID}] ELECTION from node {candidate_id} (term {term})")
    if term > state.term:
        state.term = term
    asyncio.create_task(start_election())
    return {"ok": True, "responder_id": NODE_ID}

def handle_victory_message(leader_id, term):
    log.info(f"[Node {NODE_ID}] VICTORY from node {leader_id} (term {term})")
    state.current_leader = leader_id
    state.is_leader = (leader_id == NODE_ID)
    state.election_in_progress = False
    state.term = max(state.term, term)
    state.last_heartbeat = asyncio.get_event_loop().time()
    state.missed_heartbeats = 0
    if not state.is_leader:
        stop_sending_heartbeats()
        start_heartbeat_watchdog()
    return {"acknowledged": True}

def handle_heartbeat(leader_id, term):
    if state.current_leader != leader_id:
        log.info(f"[Node {NODE_ID}] Heartbeat: leader is node {leader_id}")
        state.current_leader = leader_id
    state.last_heartbeat = asyncio.get_event_loop().time()
    state.missed_heartbeats = 0
    state.term = max(state.term, term)
    return {"ok": True}

async def bootstrap():
    jitter = random.uniform(0, 2)
    log.info(f"[Node {NODE_ID}] Bootstrap: starting election in {jitter:.2f}s")
    await asyncio.sleep(jitter)
    await start_election()
