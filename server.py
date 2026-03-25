import uuid
from datetime import datetime
from typing import List
import os

import httpx
from fastapi import FastAPI, HTTPException

from database import add_message, get_messages_for, get_all_messages, clear_all
from models import MessageRequest, MessageResponse

from node_manager import (
    start_heartbeat, is_primary, MY_URL,
    get_alive_servers, get_primary,
    on_node_down, on_node_recover, get_role
)
from replication import (
    replicate_to_all, register_replica,
    register_with_primary, mark_replica_down,
    mark_replica_recovered, get_replica_status
)
from fault_tolerance import (
    wal_write, wal_commit, wal_fail,
    recover_from_wal, start_recovery_manager,
    get_fault_tolerance_status
)

app = FastAPI(title="Distributed Chat System")


# ─────────────────────────────────────────
# Startup — Wire Everything Together
# ─────────────────────────────────────────

@app.on_event("startup")
def startup():
    # Step 1: Wire node_manager failure callbacks → replication module
    on_node_down(mark_replica_down)
    on_node_recover(mark_replica_recovered)

    # Step 2: Recover any messages that were mid-write when this node last crashed.
    # get_primary and MY_URL are passed so replica nodes can re-replicate
    # recovered messages to the primary (primary's /replicate deduplicates).
    recover_from_wal(add_message, get_primary, MY_URL)

    # Step 3: Start heartbeat — probes all nodes every 5s, elects primary
    start_heartbeat()

    # Step 4: Start automatic recovery manager
    start_recovery_manager(get_primary)

    # Step 5: Pre-register the OTHER two nodes as replicas (not self)
    for url in ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]:
        if url != MY_URL:
            register_replica(url)

    # Step 6: If this node is a replica, register itself with the current primary
    if not is_primary():
        primary = get_primary()
        if primary:
            register_with_primary(primary, MY_URL)


# ─────────────────────────────────────────
# Health Check
# ─────────────────────────────────────────

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "node": MY_URL,
        "timestamp": datetime.utcnow().isoformat()
    }


# ─────────────────────────────────────────
# Node Role
# ─────────────────────────────────────────

@app.get("/role")
def role():
    return {
        "role": get_role(),
        "node": MY_URL,
        "primary": get_primary()
    }


# ─────────────────────────────────────────
# Send Message (Primary only)
# ─────────────────────────────────────────

@app.post("/send", response_model=MessageResponse)
def send_message(request: MessageRequest):
    """
    Only the primary accepts new messages.
    Uses Write-Ahead Log (WAL) to survive crashes mid-write.
    Replicates to all alive nodes (excluding self) after storing.
    """
    if not is_primary():
        raise HTTPException(
            status_code=403,
            detail={"error": "Not primary node", "primary": get_primary()}
        )

    if not request.content.strip():
        raise HTTPException(status_code=400, detail="Empty message")

    message = {
        "id": str(uuid.uuid4()),
        "sender": request.sender,
        "receiver": request.receiver,
        "content": request.content,
        "timestamp": datetime.utcnow().isoformat()
    }

    # Write to WAL BEFORE storing — guarantees crash recovery
    wal_write(message)

    try:
        # Store locally
        add_message(message)

        # Replicate to all alive nodes except self
        alive = get_alive_servers()
        for replica in alive:
            if replica == MY_URL:
                continue  # never replicate to self
            try:
                with httpx.Client() as client:
                    client.post(f"{replica}/replicate", json=message, timeout=2.0)
            except Exception as e:
                print(f"[REPLICATION] Failed to reach {replica} — ignored: {e}")

        # Commit WAL after successful store
        wal_commit(message["id"])
        print(f"[SERVER] ✓ Message stored+replicated: {message['sender']} → {message['receiver']}")

    except Exception as e:
        wal_fail(message["id"])
        raise HTTPException(status_code=500, detail=str(e))

    return message


# ─────────────────────────────────────────
# Receive Replicated Message
# ─────────────────────────────────────────

@app.post("/replicate")
def receive_replica(message: dict):
    """
    Accept a replicated message pushed from the primary.
    Deduplication check prevents storing the same message twice.
    """
    existing_ids = {m["id"] for m in get_all_messages()}
    if message.get("id") in existing_ids:
        print(f"[SERVER] Duplicate skipped: {message.get('id', '')[:8]}")
        return {"status": "duplicate skipped"}

    add_message(message)
    print(f"[SERVER] Replica received: {message['sender']} → {message['receiver']}")
    return {"status": "replicated"}


# ─────────────────────────────────────────
# Get Messages
# ─────────────────────────────────────────

@app.get("/messages", response_model=List[MessageResponse])
def get_messages(receiver: str = None):
    if receiver:
        return get_messages_for(receiver)
    return get_all_messages()


# ─────────────────────────────────────────
# Clear Messages (Testing only)
# ─────────────────────────────────────────

@app.delete("/messages")
def clear_messages():
    clear_all()
    return {"status": "All messages cleared"}


# ─────────────────────────────────────────
# Register Replica
# ─────────────────────────────────────────

@app.post("/register")
def register(url: str):
    register_replica(url)
    return {"status": f"Replica {url} registered"}


# ─────────────────────────────────────────
# Sync — Delta catch-up by timestamp
# ─────────────────────────────────────────

@app.get("/sync")
def sync_messages(since: str = None):
    """
    Returns messages after a given timestamp.
    A recovering node calls this to get only messages it missed.
    """
    all_messages = get_all_messages()
    if not since:
        return all_messages

    missed = [m for m in all_messages if m["timestamp"] > since]
    print(f"[SERVER] Sync: since={since} → {len(missed)} messages returned")
    return missed


# ─────────────────────────────────────────
# Rejoin — Smart delta recovery
# ─────────────────────────────────────────

@app.post("/rejoin")
def rejoin(url: str, since: str = None):
    """
    Called when a crashed node comes back online.
    since=<timestamp> → delta sync (only missed messages)
    since not provided → full sync (entire history)
    """
    all_messages = get_all_messages()

    if since:
        messages_to_send = [m for m in all_messages if m["timestamp"] > since]
        print(f"[SERVER] Rejoin delta: {len(messages_to_send)} missed messages for {url}")
    else:
        messages_to_send = all_messages
        print(f"[SERVER] Rejoin full: {len(messages_to_send)} messages for {url}")

    recovered = 0
    failed = 0

    for msg in messages_to_send:
        try:
            with httpx.Client() as client:
                client.post(f"{url}/replicate", json=msg, timeout=2.0)
            recovered += 1
        except Exception:
            failed += 1

    # Re-register the rejoining node as an active replica
    register_replica(url)

    print(f"[SERVER] Recovery done → sent={recovered}, failed={failed}, node={url}")
    return {
        "status": "recovery complete",
        "messages_sent": recovered,
        "messages_failed": failed,
        "node": url,
        "delta_sync": since is not None
    }


# ─────────────────────────────────────────
# Cluster Status
# ─────────────────────────────────────────

@app.get("/status")
def get_status():
    return {
        "own_url": MY_URL,
        "is_primary": is_primary(),
        "current_primary": get_primary(),
        "alive_servers": get_alive_servers(),
        "replicas": get_replica_status(),
        "message_count": len(get_all_messages()),
        "timestamp": datetime.utcnow().isoformat()
    }


# ─────────────────────────────────────────
# Fault Tolerance Status
# ─────────────────────────────────────────

@app.get("/ft-status")
def fault_tolerance_status():
    """
    Full fault tolerance subsystem snapshot.
    Shows WAL stats and any nodes queued for recovery.
    """
    return get_fault_tolerance_status()