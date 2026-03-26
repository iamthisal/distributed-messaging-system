import sys
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List

import httpx
from fastapi import FastAPI, HTTPException

from database import add_message, clear_all, get_all_messages, get_messages_for
from models import LeaderAnnouncement, MessageRequest, MessageResponse
from replication import (
    BOOTSTRAP_PRIMARY_URL,
    DEFAULT_NODE_URLS,
    REPLICAS,
    announce_new_primary,
    choose_lowest_port_leader,
    discover_primary,
    fetch_node_status,
    register_replica,
    register_with_primary,
    replicate_to_all,
    set_replicas,
    sort_nodes,
)


port = int(sys.argv[-1]) if sys.argv[-1].isdigit() else 8000
OWN_URL = f"http://localhost:{port}"
CURRENT_PRIMARY_URL = BOOTSTRAP_PRIMARY_URL
KNOWN_NODES = sort_nodes(DEFAULT_NODE_URLS + [OWN_URL])
state_lock = threading.Lock()
heartbeat_stop_event = threading.Event()
heartbeat_thread = None

print(f"[STARTUP] port={port} own_url={OWN_URL}")


def get_state_snapshot():
    with state_lock:
        return {
            "own_url": OWN_URL,
            "current_primary_url": CURRENT_PRIMARY_URL,
            "known_nodes": list(KNOWN_NODES),
            "is_primary": CURRENT_PRIMARY_URL == OWN_URL,
        }


def update_cluster_state(primary_url: str = None, known_nodes: list[str] = None):
    global CURRENT_PRIMARY_URL, KNOWN_NODES

    with state_lock:
        if known_nodes is not None:
            KNOWN_NODES = sort_nodes(known_nodes + [OWN_URL])
        if primary_url is not None:
            CURRENT_PRIMARY_URL = primary_url
        if CURRENT_PRIMARY_URL not in KNOWN_NODES:
            KNOWN_NODES = sort_nodes(KNOWN_NODES + [CURRENT_PRIMARY_URL])

        set_replicas(KNOWN_NODES, OWN_URL)
        return {
            "own_url": OWN_URL,
            "current_primary_url": CURRENT_PRIMARY_URL,
            "known_nodes": list(KNOWN_NODES),
            "is_primary": CURRENT_PRIMARY_URL == OWN_URL,
        }


def remove_known_node(node_url: str):
    global KNOWN_NODES

    with state_lock:
        if node_url in KNOWN_NODES and node_url != OWN_URL:
            KNOWN_NODES = [node for node in KNOWN_NODES if node != node_url]
            set_replicas(KNOWN_NODES, OWN_URL)


def elect_new_primary(failed_primary_url: str):
    state = get_state_snapshot()
    alive_nodes = [OWN_URL]

    for node_url in state["known_nodes"]:
        if node_url in {OWN_URL, failed_primary_url}:
            continue

        status = fetch_node_status(node_url)
        if status:
            alive_nodes.append(node_url)

    alive_nodes = sort_nodes(alive_nodes)
    new_primary_url = choose_lowest_port_leader(alive_nodes)
    update_cluster_state(primary_url=new_primary_url, known_nodes=alive_nodes)

    if new_primary_url == OWN_URL:
        surviving_nodes = announce_new_primary(new_primary_url, alive_nodes, OWN_URL)
        update_cluster_state(primary_url=new_primary_url, known_nodes=surviving_nodes)
        print(f"[FAILOVER] Promoted self to primary at {OWN_URL}")
    else:
        print(f"[FAILOVER] Switched primary to {new_primary_url}")

    return new_primary_url


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


def heartbeat_loop():
    missed_heartbeats = 0

    while not heartbeat_stop_event.wait(2.0):
        state = get_state_snapshot()
        if state["is_primary"]:
            missed_heartbeats = 0
            continue

        status = fetch_node_status(state["current_primary_url"])
        if status:
            missed_heartbeats = 0
            update_cluster_state(
                primary_url=status.get("current_primary_url", state["current_primary_url"]),
                known_nodes=status.get("known_nodes", state["known_nodes"]),
            )
            continue

        missed_heartbeats += 1
        print(f"[HEARTBEAT MISS] {state['current_primary_url']} missed {missed_heartbeats}/3")
        if missed_heartbeats >= 3:
            remove_known_node(state["current_primary_url"])
            elect_new_primary(state["current_primary_url"])
            missed_heartbeats = 0


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
    }


@app.post("/register")
def register(url: str):
    state = get_state_snapshot()
    if state["is_primary"]:
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
    return {"status": "updated", **state}


@app.post("/replicate", response_model=MessageResponse)
def receive_replicated_message(message: MessageResponse):
    stored = add_message(message.dict())
    if stored:
        print(f"[REPLICATED] {message.sender} -> {message.receiver}: {message.content}")
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

    message = {
        "id": str(uuid.uuid4()),
        "sender": request.sender,
        "receiver": request.receiver,
        "content": request.content,
        "timestamp": datetime.utcnow().isoformat(),
    }

    add_message(message)
    print(f"[NEW MESSAGE] {message['sender']} -> {message['receiver']}: {message['content']}")
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
