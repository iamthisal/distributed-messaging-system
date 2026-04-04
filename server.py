import os
import random
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List

import httpx
from fastapi import FastAPI, HTTPException

from database import (
    add_message,
    clear_all,
    get_all_messages,
    get_highest_logical_timestamp,
    get_messages_for,
)
from models import (
    LeaderAnnouncement,
    MessageRequest,
    MessageResponse,
    RaftAppendEntriesRequest,
    RaftAppendEntriesResponse,
    RaftVoteRequest,
    RaftVoteResponse,
    TimeSyncRequest,
    TimeSyncResponse,
)
from replication import (
    BOOTSTRAP_PRIMARY_URL,
    DEFAULT_NODE_URLS,
    REPLICAS,
    announce_new_primary,
    discover_primary,
    fetch_node_status,
    fetch_time_sync,
    register_replica,
    register_with_primary,
    replicate_to_all,
    set_replicas,
    sort_nodes,
)


SYNC_INTERVAL_SECONDS = 3.0
HEARTBEAT_INTERVAL_SECONDS = 2.0
MAX_ACCEPTABLE_RTT_MS = 2_000
RAFT_REQUEST_TIMEOUT_SECONDS = 2.0
RAFT_ELECTION_RETRY_LIMIT = 2
MANUAL_CLOCK_SKEW_MS = int(os.getenv("CHAT_CLOCK_SKEW_MS", "0"))

port = int(sys.argv[-1]) if sys.argv[-1].isdigit() else 8000
NODE_ID = port
OWN_URL = f"http://localhost:{port}"
CURRENT_PRIMARY_URL = BOOTSTRAP_PRIMARY_URL
KNOWN_NODES = sort_nodes(DEFAULT_NODE_URLS + [OWN_URL])
state_lock = threading.Lock()
heartbeat_stop_event = threading.Event()
heartbeat_thread = None
clock_offset_ms = 0
logical_clock = 0
last_sync_at = None
sync_status = "bootstrap"
best_sync_rtt_ms = None
raft_current_term = 0
raft_voted_for = None
raft_role = "follower"
raft_commit_index = 0
raft_last_leader_contact_monotonic = time.monotonic()

print(f"[STARTUP] port={port} own_url={OWN_URL} clock_skew_ms={MANUAL_CLOCK_SKEW_MS}")


def current_physical_time_ms() -> int:
    return (time.time_ns() // 1_000_000) + MANUAL_CLOCK_SKEW_MS


def corrected_time_ms() -> int:
    with state_lock:
        offset = clock_offset_ms
    return current_physical_time_ms() + offset


def iso_from_ms(value_ms: int) -> str:
    return datetime.utcfromtimestamp(value_ms / 1000).isoformat(timespec="milliseconds")


def majority_count(node_urls: list[str]) -> int:
    return (len(set(node_urls)) // 2) + 1


def get_state_snapshot():
    with state_lock:
        return {
            "own_url": OWN_URL,
            "current_primary_url": CURRENT_PRIMARY_URL,
            "known_nodes": list(KNOWN_NODES),
            "is_primary": CURRENT_PRIMARY_URL == OWN_URL,
            "clock_offset_ms": clock_offset_ms,
            "logical_clock": logical_clock,
            "last_sync_at": last_sync_at,
            "sync_status": sync_status,
            "best_sync_rtt_ms": best_sync_rtt_ms,
            "manual_clock_skew_ms": MANUAL_CLOCK_SKEW_MS,
            "raft_current_term": raft_current_term,
            "raft_voted_for": raft_voted_for,
            "raft_role": raft_role,
            "raft_commit_index": raft_commit_index,
        }


def ensure_logical_clock_floor():
    global logical_clock

    with state_lock:
        logical_clock = max(logical_clock, get_highest_logical_timestamp())
        return logical_clock


def ensure_raft_commit_index_floor():
    global raft_commit_index

    with state_lock:
        raft_commit_index = max(raft_commit_index, get_highest_logical_timestamp())
        return raft_commit_index


def update_cluster_state(primary_url: str = None, known_nodes: list[str] = None):
    global CURRENT_PRIMARY_URL, KNOWN_NODES, logical_clock, raft_commit_index

    with state_lock:
        if known_nodes is not None:
            KNOWN_NODES = sort_nodes(known_nodes + [OWN_URL])
        if primary_url is not None:
            CURRENT_PRIMARY_URL = primary_url
        if CURRENT_PRIMARY_URL not in KNOWN_NODES:
            KNOWN_NODES = sort_nodes(KNOWN_NODES + [CURRENT_PRIMARY_URL])

        if CURRENT_PRIMARY_URL == OWN_URL:
            logical_clock = max(logical_clock, get_highest_logical_timestamp())
            raft_commit_index = max(raft_commit_index, get_highest_logical_timestamp())

        set_replicas(KNOWN_NODES, OWN_URL)
        return {
            "own_url": OWN_URL,
            "current_primary_url": CURRENT_PRIMARY_URL,
            "known_nodes": list(KNOWN_NODES),
            "is_primary": CURRENT_PRIMARY_URL == OWN_URL,
            "clock_offset_ms": clock_offset_ms,
            "logical_clock": logical_clock,
            "last_sync_at": last_sync_at,
            "sync_status": sync_status,
            "best_sync_rtt_ms": best_sync_rtt_ms,
            "manual_clock_skew_ms": MANUAL_CLOCK_SKEW_MS,
            "raft_current_term": raft_current_term,
            "raft_voted_for": raft_voted_for,
            "raft_role": raft_role,
            "raft_commit_index": raft_commit_index,
        }


def update_time_sync(offset_ms: int = None, sync_time_ms: int = None, status: str = None, rtt_ms: int = None):
    global clock_offset_ms, last_sync_at, sync_status, best_sync_rtt_ms

    with state_lock:
        if offset_ms is not None:
            if best_sync_rtt_ms is None or rtt_ms is None or rtt_ms <= best_sync_rtt_ms:
                clock_offset_ms = int(offset_ms)
                best_sync_rtt_ms = rtt_ms
            else:
                clock_offset_ms = int((clock_offset_ms * 3 + offset_ms) / 4)
                if best_sync_rtt_ms is not None and rtt_ms is not None:
                    best_sync_rtt_ms = int((best_sync_rtt_ms * 3 + rtt_ms) / 4)
        if sync_time_ms is not None:
            last_sync_at = iso_from_ms(sync_time_ms)
        if status is not None:
            sync_status = status


def update_raft_state(
    term: int = None,
    voted_for: int = None,
    role: str = None,
    commit_index: int = None,
    reset_vote: bool = False,
    leader_contact: bool = False,
):
    global raft_current_term, raft_voted_for, raft_role, raft_commit_index, raft_last_leader_contact_monotonic

    with state_lock:
        if term is not None:
            if term > raft_current_term:
                raft_current_term = term
                if reset_vote:
                    raft_voted_for = None
            else:
                raft_current_term = max(raft_current_term, term)
        if voted_for is not None:
            raft_voted_for = voted_for
        elif reset_vote:
            raft_voted_for = None
        if role is not None:
            raft_role = role
        if commit_index is not None:
            raft_commit_index = max(raft_commit_index, commit_index)
        if leader_contact:
            raft_last_leader_contact_monotonic = time.monotonic()


def next_logical_timestamp() -> int:
    global logical_clock

    with state_lock:
        logical_clock = max(logical_clock, get_highest_logical_timestamp()) + 1
        return logical_clock


def build_message(request: MessageRequest) -> dict:
    raw_timestamp_ms = current_physical_time_ms()
    corrected_timestamp_ms = corrected_time_ms()
    logical_timestamp = next_logical_timestamp()
    update_raft_state(commit_index=logical_timestamp)
    return {
        "id": str(uuid.uuid4()),
        "sender": request.sender,
        "receiver": request.receiver,
        "content": request.content,
        "timestamp": iso_from_ms(raw_timestamp_ms),
        "corrected_timestamp": iso_from_ms(corrected_timestamp_ms),
        "logical_timestamp": logical_timestamp,
    }


def remove_known_node(node_url: str):
    global KNOWN_NODES

    with state_lock:
        if node_url in KNOWN_NODES and node_url != OWN_URL:
            KNOWN_NODES = [node for node in KNOWN_NODES if node != node_url]
            set_replicas(KNOWN_NODES, OWN_URL)


def post_raft_request_vote(node_url: str, payload: dict):
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{node_url}/raft/request-vote",
                json=payload,
                timeout=RAFT_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        return response.json()
    except Exception:
        return None


def send_raft_heartbeats():
    state = get_state_snapshot()
    payload = {
        "leader_id": NODE_ID,
        "leader_url": OWN_URL,
        "term": state["raft_current_term"],
        "commit_index": state["raft_commit_index"],
        "known_nodes": state["known_nodes"],
    }

    for node_url in state["known_nodes"]:
        if node_url == OWN_URL:
            continue
        try:
            with httpx.Client() as client:
                response = client.post(
                    f"{node_url}/raft/append-entries",
                    json=payload,
                    timeout=2.0,
                )
                response.raise_for_status()
                body = response.json()
            responder_term = int(body.get("term", state["raft_current_term"]))
            if responder_term > state["raft_current_term"]:
                update_raft_state(term=responder_term, role="follower", reset_vote=True)
                update_cluster_state(primary_url=node_url, known_nodes=state["known_nodes"])
                print(f"[RAFT] Stepping down due to higher term from {node_url}")
                break
        except Exception:
            continue


def become_raft_leader(term: int):
    update_raft_state(term=term, voted_for=NODE_ID, role="leader", leader_contact=True)
    update_cluster_state(primary_url=OWN_URL, known_nodes=get_state_snapshot()["known_nodes"])
    ensure_logical_clock_floor()
    ensure_raft_commit_index_floor()
    update_time_sync(offset_ms=0, sync_time_ms=current_physical_time_ms(), status="leader-local", rtt_ms=0)
    surviving_nodes = announce_new_primary(OWN_URL, get_state_snapshot()["known_nodes"], OWN_URL)
    update_cluster_state(primary_url=OWN_URL, known_nodes=surviving_nodes)
    print(f"[RAFT] Node {NODE_ID} became leader for term {term}")
    return OWN_URL


def start_raft_election() -> str | None:
    state = get_state_snapshot()
    known_nodes = state["known_nodes"]

    if OWN_URL not in known_nodes:
        known_nodes = sort_nodes(known_nodes + [OWN_URL])

    new_term = state["raft_current_term"] + 1
    update_raft_state(term=new_term, voted_for=NODE_ID, role="candidate", leader_contact=True)

    last_logical_timestamp = get_highest_logical_timestamp()
    payload = {
        "candidate_id": NODE_ID,
        "candidate_url": OWN_URL,
        "term": new_term,
        "last_logical_timestamp": last_logical_timestamp,
    }

    votes = 1
    quorum = majority_count(known_nodes)

    for node_url in known_nodes:
        if node_url == OWN_URL:
            continue
        response = post_raft_request_vote(node_url, payload)
        if not response:
            continue

        response_term = int(response.get("term", new_term))
        if response_term > new_term:
            update_raft_state(term=response_term, role="follower", reset_vote=True)
            update_cluster_state(primary_url=node_url, known_nodes=known_nodes)
            return node_url

        if response.get("vote_granted"):
            votes += 1

    if votes >= quorum:
        return become_raft_leader(new_term)

    update_raft_state(role="follower", leader_contact=False)
    return None


def elect_new_primary(failed_primary_url: str):
    remove_known_node(failed_primary_url)

    for _ in range(RAFT_ELECTION_RETRY_LIMIT):
        time.sleep(random.uniform(0.15, 0.45))
        elected = start_raft_election()
        if elected:
            if elected == OWN_URL:
                return elected
            update_cluster_state(primary_url=elected, known_nodes=get_state_snapshot()["known_nodes"])
            print(f"[RAFT] Observed new leader {elected}")
            return elected

        discovered_primary = discover_primary(get_state_snapshot()["known_nodes"], OWN_URL)
        if discovered_primary and discovered_primary != failed_primary_url:
            update_cluster_state(primary_url=discovered_primary, known_nodes=get_state_snapshot()["known_nodes"])
            return discovered_primary

    if len(get_state_snapshot()["known_nodes"]) == 1:
        return become_raft_leader(get_state_snapshot()["raft_current_term"] + 1)

    return None


def perform_time_sync(primary_url: str):
    if primary_url == OWN_URL:
        update_time_sync(offset_ms=0, sync_time_ms=current_physical_time_ms(), status="leader-local", rtt_ms=0)
        return

    client_send_time_ms = current_physical_time_ms()
    response = fetch_time_sync(primary_url, client_send_time_ms)
    client_receive_time_ms = current_physical_time_ms()

    if not response:
        update_time_sync(status="sync-failed")
        return

    server_receive_time_ms = int(response["server_receive_time_ms"])
    server_send_time_ms = int(response["server_send_time_ms"])
    rtt_ms = max(0, client_receive_time_ms - client_send_time_ms)

    if rtt_ms > MAX_ACCEPTABLE_RTT_MS:
        update_time_sync(status=f"sync-ignored-high-rtt-{rtt_ms}ms")
        return

    estimated_offset_ms = int(
        ((server_receive_time_ms - client_send_time_ms) + (server_send_time_ms - client_receive_time_ms)) / 2
    )
    update_time_sync(
        offset_ms=estimated_offset_ms,
        sync_time_ms=client_receive_time_ms,
        status="synced",
        rtt_ms=rtt_ms,
    )


def initialize_node():
    discovered_primary = discover_primary(KNOWN_NODES, OWN_URL)
    update_cluster_state(primary_url=discovered_primary, known_nodes=KNOWN_NODES)

    state = get_state_snapshot()
    if not state["is_primary"]:
        registration = register_with_primary(state["current_primary_url"], OWN_URL)
        if registration:
            update_cluster_state(
                primary_url=registration.get("current_primary_url", state["current_primary_url"]),
                known_nodes=registration.get("known_nodes", state["known_nodes"]),
            )
        else:
            rediscovered_primary = discover_primary(state["known_nodes"], OWN_URL)
            update_cluster_state(primary_url=rediscovered_primary, known_nodes=state["known_nodes"])
            state = get_state_snapshot()
            if not state["is_primary"]:
                registration = register_with_primary(state["current_primary_url"], OWN_URL)
                if registration:
                    update_cluster_state(
                        primary_url=registration.get("current_primary_url", state["current_primary_url"]),
                        known_nodes=registration.get("known_nodes", state["known_nodes"]),
                    )

    state = get_state_snapshot()
    if state["is_primary"]:
        update_raft_state(term=max(1, state["raft_current_term"]), voted_for=NODE_ID, role="leader", leader_contact=True)
        ensure_logical_clock_floor()
        ensure_raft_commit_index_floor()
    else:
        update_raft_state(role="follower", leader_contact=True)
    perform_time_sync(state["current_primary_url"])


def heartbeat_loop():
    missed_heartbeats = 0
    last_sync_monotonic = 0.0

    while not heartbeat_stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
        state = get_state_snapshot()
        if state["is_primary"]:
            missed_heartbeats = 0
            update_time_sync(offset_ms=0, sync_time_ms=current_physical_time_ms(), status="leader-local", rtt_ms=0)
            send_raft_heartbeats()
            continue

        status = fetch_node_status(state["current_primary_url"])
        if status:
            missed_heartbeats = 0
            update_cluster_state(
                primary_url=status.get("current_primary_url", state["current_primary_url"]),
                known_nodes=status.get("known_nodes", state["known_nodes"]),
            )
            update_raft_state(term=status.get("raft_current_term", state["raft_current_term"]), role="follower", leader_contact=True)

            now_monotonic = time.monotonic()
            if now_monotonic - last_sync_monotonic >= SYNC_INTERVAL_SECONDS:
                perform_time_sync(get_state_snapshot()["current_primary_url"])
                last_sync_monotonic = now_monotonic
            continue

        missed_heartbeats += 1
        update_time_sync(status=f"heartbeat-miss-{missed_heartbeats}")
        print(f"[HEARTBEAT MISS] {state['current_primary_url']} missed {missed_heartbeats}/3")
        if missed_heartbeats >= 3:
            elected_url = elect_new_primary(state["current_primary_url"])
            if elected_url:
                update_cluster_state(primary_url=elected_url, known_nodes=get_state_snapshot()["known_nodes"])
            missed_heartbeats = 0
            last_sync_monotonic = 0.0


def forward_send_to_primary(request: MessageRequest):
    state = get_state_snapshot()

    try:
        with httpx.Client() as client:
            response = client.post(
                f"{state['current_primary_url']}/send",
                json=request.dict(),
                params={"forwarded_from": OWN_URL},
                timeout=5.0,
            )
            response.raise_for_status()
        print(f"[FORWARDED] to primary {state['current_primary_url']}")
        return response.json()
    except Exception:
        new_primary_url = elect_new_primary(state["current_primary_url"])
        if new_primary_url == OWN_URL:
            return None
        if new_primary_url is None:
            raise HTTPException(status_code=503, detail="No Raft leader elected")

    try:
        with httpx.Client() as client:
            response = client.post(
                f"{new_primary_url}/send",
                json=request.dict(),
                params={"forwarded_from": OWN_URL},
                timeout=5.0,
            )
            response.raise_for_status()
        print(f"[FORWARDED] to new primary {new_primary_url}")
        return response.json()
    except Exception as exc:
        print(f"[FORWARD ERROR] exact error: {exc}")
        raise HTTPException(status_code=503, detail="Primary server is unreachable") from exc


def forward_register_to_primary(url: str):
    state = get_state_snapshot()

    try:
        with httpx.Client() as client:
            response = client.post(
                f"{state['current_primary_url']}/register",
                params={"url": url},
                timeout=3.0,
            )
            response.raise_for_status()
        return response.json()
    except Exception:
        new_primary_url = elect_new_primary(state["current_primary_url"])
        if new_primary_url == OWN_URL:
            return None
        if new_primary_url is None:
            raise HTTPException(status_code=503, detail="No Raft leader elected")

    try:
        with httpx.Client() as client:
            response = client.post(
                f"{new_primary_url}/register",
                params={"url": url},
                timeout=3.0,
            )
            response.raise_for_status()
        return response.json()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Primary server is unreachable: {exc}") from exc


@asynccontextmanager
async def lifespan(app: FastAPI):
    global heartbeat_thread

    initialize_node()
    heartbeat_stop_event.clear()
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    yield
    heartbeat_stop_event.set()


app = FastAPI(title="Distributed Chat System", lifespan=lifespan)


@app.get("/")
def root():
    state = get_state_snapshot()
    return {"status": "running", **state, "replicas": REPLICAS}


@app.get("/heartbeat")
def heartbeat():
    state = get_state_snapshot()
    return {"status": "alive", **state}


@app.get("/leader")
def leader():
    state = get_state_snapshot()
    return {
        "current_primary_url": state["current_primary_url"],
        "own_url": state["own_url"],
        "is_primary": state["is_primary"],
        "logical_clock": state["logical_clock"],
        "raft_current_term": state["raft_current_term"],
        "raft_role": state["raft_role"],
        "raft_commit_index": state["raft_commit_index"],
    }


@app.get("/time-status")
def time_status():
    state = get_state_snapshot()
    return {
        "status": "time-ok" if state["sync_status"] in {"synced", "leader-local"} else "time-pending",
        "own_url": state["own_url"],
        "current_primary_url": state["current_primary_url"],
        "clock_offset_ms": state["clock_offset_ms"],
        "logical_clock": state["logical_clock"],
        "last_sync_at": state["last_sync_at"],
        "sync_status": state["sync_status"],
        "best_sync_rtt_ms": state["best_sync_rtt_ms"],
        "manual_clock_skew_ms": state["manual_clock_skew_ms"],
        "raft_current_term": state["raft_current_term"],
        "raft_commit_index": state["raft_commit_index"],
    }


@app.get("/raft/state")
def raft_state():
    state = get_state_snapshot()
    return {
        "node_id": NODE_ID,
        "term": state["raft_current_term"],
        "role": state["raft_role"],
        "voted_for": state["raft_voted_for"],
        "commit_index": state["raft_commit_index"],
        "current_primary_url": state["current_primary_url"],
        "known_nodes": state["known_nodes"],
    }


@app.post("/raft/request-vote", response_model=RaftVoteResponse)
def request_vote(payload: RaftVoteRequest):
    local_last_logical_timestamp = get_highest_logical_timestamp()
    state = get_state_snapshot()

    if payload.term < state["raft_current_term"]:
        return {
            "term": state["raft_current_term"],
            "vote_granted": False,
            "responder_id": NODE_ID,
        }

    if payload.term > state["raft_current_term"]:
        update_raft_state(term=payload.term, role="follower", reset_vote=True)

    state = get_state_snapshot()
    can_vote = state["raft_voted_for"] in {None, payload.candidate_id}
    candidate_up_to_date = payload.last_logical_timestamp >= local_last_logical_timestamp
    vote_granted = can_vote and candidate_up_to_date

    if vote_granted:
        update_raft_state(voted_for=payload.candidate_id, role="follower", leader_contact=True)

    return {
        "term": get_state_snapshot()["raft_current_term"],
        "vote_granted": vote_granted,
        "responder_id": NODE_ID,
    }


@app.post("/raft/append-entries", response_model=RaftAppendEntriesResponse)
def append_entries(payload: RaftAppendEntriesRequest):
    state = get_state_snapshot()
    if payload.term < state["raft_current_term"]:
        return {"term": state["raft_current_term"], "success": False}

    update_raft_state(
        term=payload.term,
        role="follower",
        commit_index=payload.commit_index,
        reset_vote=payload.term > state["raft_current_term"],
        leader_contact=True,
    )
    update_cluster_state(primary_url=payload.leader_url, known_nodes=payload.known_nodes)
    return {"term": get_state_snapshot()["raft_current_term"], "success": True}


@app.post("/time-sync", response_model=TimeSyncResponse)
def time_sync(payload: TimeSyncRequest):
    server_receive_time_ms = current_physical_time_ms()
    state = get_state_snapshot()
    server_send_time_ms = current_physical_time_ms()
    return {
        "current_primary_url": state["current_primary_url"],
        "own_url": state["own_url"],
        "server_receive_time_ms": server_receive_time_ms,
        "server_send_time_ms": server_send_time_ms,
    }


@app.post("/register")
def register(url: str):
    state = get_state_snapshot()
    if not state["is_primary"]:
        forwarded = forward_register_to_primary(url)
        if forwarded is not None:
            return forwarded
        state = get_state_snapshot()

    update_cluster_state(known_nodes=state["known_nodes"] + [url])
    register_replica(url)

    synced_messages = 0
    try:
        with httpx.Client() as client:
            for message in get_all_messages():
                response = client.post(
                    f"{url}/replicate",
                    json=message,
                    timeout=2.0,
                )
                response.raise_for_status()
                synced_messages += 1
        print(f"[SYNC] Sent {synced_messages} existing messages to {url}")
    except Exception:
        print(f"[SYNC FAILED] Could not sync to {url}")

    state = get_state_snapshot()
    return {
        "status": "registered",
        "replicas": REPLICAS,
        "synced_messages": synced_messages,
        "current_primary_url": state["current_primary_url"],
        "known_nodes": state["known_nodes"],
    }


@app.post("/announce-primary")
def announce_primary(payload: LeaderAnnouncement):
    state = update_cluster_state(
        primary_url=payload.new_primary_url,
        known_nodes=payload.known_nodes,
    )
    if state["is_primary"]:
        update_raft_state(term=max(1, state["raft_current_term"]), voted_for=NODE_ID, role="leader", leader_contact=True)
        ensure_logical_clock_floor()
        ensure_raft_commit_index_floor()
        update_time_sync(offset_ms=0, sync_time_ms=current_physical_time_ms(), status="leader-local", rtt_ms=0)
    else:
        update_raft_state(role="follower", leader_contact=True)
    return {"status": "updated", **get_state_snapshot()}


@app.post("/replicate", response_model=MessageResponse)
def receive_replicated_message(message: MessageResponse):
    stored = add_message(message.dict())
    update_raft_state(commit_index=message.logical_timestamp)
    if stored:
        print(
            f"[REPLICATED] {message.sender} -> {message.receiver}: "
            f"{message.content} (logical={message.logical_timestamp})"
        )
    else:
        print(f"[DEDUP] Skipped already replicated message {message.id[:8]}")
    return message


@app.post("/send", response_model=MessageResponse)
def send_message(request: MessageRequest, forwarded_from: str = None):
    if not request.content.strip():
        raise HTTPException(status_code=400, detail="Message text cannot be empty")

    state = get_state_snapshot()
    if not state["is_primary"]:
        forwarded = forward_send_to_primary(request)
        if forwarded is not None:
            return forwarded

    message = build_message(request)
    add_message(message)
    print(
        f"[NEW MESSAGE] {message['sender']} -> {message['receiver']}: "
        f"{message['content']} (logical={message['logical_timestamp']}, term={get_state_snapshot()['raft_current_term']})"
    )
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
    with state_lock:
        global logical_clock, raft_commit_index
        logical_clock = 0
        raft_commit_index = 0
    return {"status": "All messages cleared"}
