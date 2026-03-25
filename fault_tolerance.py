import json
import threading
import time
import httpx
from datetime import datetime
import os
from pathlib import Path

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────

# WAL file is per-node — each node gets its own log file
# MY_URL env var must be set when starting each server instance:
#   MY_URL=http://localhost:8001 uvicorn server:app --port 8001
_my_port = os.environ.get("MY_URL", "http://localhost:8000").split(":")[-1]
WAL_FILE = f"wal_node_{_my_port}.log"
WAL_LOCK = threading.Lock()

RECOVERY_CHECK_INTERVAL = 6    # seconds between recovery manager cycles
RECOVERY_TIMEOUT        = 5.0  # seconds to wait for /rejoin response

# ─────────────────────────────────────────
# WAL Entry States
# ─────────────────────────────────────────
# Each WAL entry has one of three states:
#
#   PENDING   → written before storage attempt
#   COMMITTED → storage + replication succeeded
#   FAILED    → storage or replication failed
#
# On restart, any PENDING entries = crash happened mid-write → recover them

WAL_PENDING   = "PENDING"
WAL_COMMITTED = "COMMITTED"
WAL_FAILED    = "FAILED"


# ─────────────────────────────────────────
# Write-Ahead Log (WAL)
# ─────────────────────────────────────────

def wal_write(message: dict, state: str = WAL_PENDING) -> None:
    """
    Append a log entry to the WAL file.
    Called BEFORE storing the message — guarantees we can recover
    even if the process crashes immediately after.
    Each line is a self-contained JSON object (newline-delimited JSON).
    """
    entry = {
        "state":     state,
        "message":   message,
        "logged_at": datetime.utcnow().isoformat()
    }
    with WAL_LOCK:
        with open(WAL_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")


def wal_commit(message_id: str) -> None:
    """
    Mark a WAL entry as COMMITTED.
    Called after the message is successfully stored AND replicated.
    """
    _wal_update_state(message_id, WAL_COMMITTED)


def wal_fail(message_id: str) -> None:
    """
    Mark a WAL entry as FAILED.
    Called if storage or replication throws an unrecoverable error.
    """
    _wal_update_state(message_id, WAL_FAILED)


def _wal_update_state(message_id: str, new_state: str) -> None:
    """
    Internal: append a state-update tombstone entry for a specific message ID.
    Avoids rewriting the full file on every commit (O(1) instead of O(n)).
    wal_get_pending() and wal_get_stats() resolve the final state by
    taking the LAST entry for each message ID — later entries win.
    Uses a lock so concurrent commits don't interleave writes.
    """
    tombstone = {
        "state":       new_state,
        "message":     {"id": message_id},
        "resolved_at": datetime.utcnow().isoformat()
    }
    with WAL_LOCK:
        with open(WAL_FILE, "a") as f:
            f.write(json.dumps(tombstone) + "\n")


def wal_get_pending() -> list[dict]:
    """
    Return all messages whose final resolved state is PENDING.
    Because _wal_update_state appends tombstones rather than rewriting,
    a message can have multiple entries — the LAST one wins.
    Called on startup to recover in-flight messages.
    """
    if not os.path.exists(WAL_FILE):
        return []

    # Build a dict: message_id -> latest entry seen
    latest: dict[str, dict] = {}
    with WAL_LOCK:
        lines = Path(WAL_FILE).read_text().strip().splitlines()

    for line in lines:
        try:
            entry = json.loads(line)
            msg_id = entry.get("message", {}).get("id")
            if msg_id:
                latest[msg_id] = entry   # later lines overwrite earlier ones
        except json.JSONDecodeError:
            continue

    return [
        e["message"] for e in latest.values()
        if e.get("state") == WAL_PENDING
    ]


def wal_get_stats() -> dict:
    """
    Return a summary of WAL entries by their final resolved state.
    Uses last-entry-wins to match the append-only tombstone approach.
    Used by /ft-status endpoint and benchmark script.
    """
    stats = {WAL_PENDING: 0, WAL_COMMITTED: 0, WAL_FAILED: 0, "total": 0}

    if not os.path.exists(WAL_FILE):
        return stats

    with WAL_LOCK:
        lines = Path(WAL_FILE).read_text().strip().splitlines()

    # Resolve final state per message ID (last entry wins)
    latest: dict[str, str] = {}
    for line in lines:
        try:
            entry = json.loads(line)
            msg_id = entry.get("message", {}).get("id")
            state  = entry.get("state")
            if msg_id and state:
                latest[msg_id] = state
        except json.JSONDecodeError:
            continue

    for state in latest.values():
        if state in stats:
            stats[state] += 1
        stats["total"] += 1

    return stats


def wal_clear_committed() -> int:
    """
    Compact the WAL: resolve final state per message ID (last-entry-wins),
    then write back only non-COMMITTED entries as single canonical lines.
    Returns the number of message IDs removed.
    """
    if not os.path.exists(WAL_FILE):
        return 0

    with WAL_LOCK:
        lines = Path(WAL_FILE).read_text().strip().splitlines()

        # Resolve final entry per message ID preserving insertion order
        latest: dict[str, dict] = {}
        for line in lines:
            try:
                entry = json.loads(line)
                msg_id = entry.get("message", {}).get("id")
                if msg_id:
                    latest[msg_id] = entry
            except json.JSONDecodeError:
                continue

        kept    = []
        removed = 0
        for entry in latest.values():
            if entry.get("state") == WAL_COMMITTED:
                removed += 1
            else:
                kept.append(json.dumps(entry))

        with open(WAL_FILE, "w") as f:
            f.write("\n".join(kept) + "\n" if kept else "")

    print(f"[WAL] Cleaned {removed} committed entries from {WAL_FILE}")
    return removed


# ─────────────────────────────────────────
# WAL Crash Recovery (called on startup)
# ─────────────────────────────────────────

def recover_from_wal(add_message_fn, get_primary_fn=None, my_url: str = None) -> int:
    """
    Called once on server startup BEFORE accepting any requests.
    Finds all PENDING WAL entries and re-inserts them into the database.

    If this node is a replica (get_primary_fn provided and primary != my_url),
    recovered messages are also pushed to the primary so the cluster stays
    consistent — the primary's deduplication check prevents double-storage.

    Returns the number of messages recovered.
    """
    pending = wal_get_pending()

    if not pending:
        print("[WAL] No pending entries — clean startup")
        return 0

    print(f"[WAL] ⚠ Found {len(pending)} PENDING entries — node crashed mid-write!")
    recovered = 0
    for message in pending:
        try:
            add_message_fn(message)
            wal_commit(message["id"])
            recovered += 1
            print(f"[WAL] ✓ Recovered message {message['id'][:8]}... "
                  f"({message['sender']} → {message['receiver']})")

            # If this node is a replica, push recovered messages to the primary
            # so the cluster doesn't miss them. The primary's /replicate endpoint
            # will silently skip duplicates it already has.
            if get_primary_fn and my_url:
                primary = get_primary_fn()
                if primary and primary != my_url:
                    try:
                        with httpx.Client() as client:
                            client.post(
                                f"{primary}/replicate",
                                json=message,
                                timeout=2.0
                            )
                        print(f"[WAL] ✓ Re-replicated recovered message to primary {primary}")
                    except Exception as re_err:
                        print(f"[WAL] ✗ Could not re-replicate to primary: {re_err}")

        except Exception as e:
            wal_fail(message["id"])
            print(f"[WAL] ✗ Could not recover message {message['id'][:8]}: {e}")

    print(f"[WAL] Recovery complete — {recovered}/{len(pending)} messages restored")
    return recovered


# ─────────────────────────────────────────
# Automatic Recovery Manager
# ─────────────────────────────────────────

_recovery_manager_running = False
_pending_recoveries: dict[str, str] = {}   # url -> down_since timestamp
_recovery_lock = threading.Lock()
_schedule_recovery_registered = False      # guard against double-registration


def schedule_recovery(node_url: str, down_since: str):
    """
    Called by node_manager's on_node_recover callback.
    Schedules a delta-sync recovery for the given node.
    """
    with _recovery_lock:
        _pending_recoveries[node_url] = down_since
        print(f"[RECOVERY MANAGER] Recovery scheduled for {node_url} "
              f"(missed messages since {down_since})")


def _do_recovery(node_url: str, down_since: str, primary_url: str):
    """
    Ask the primary to push missed messages to the recovering node.
    Uses delta sync — only messages after down_since are transferred.
    """
    print(f"[RECOVERY MANAGER] Starting recovery: {node_url} (since={down_since})")
    try:
        with httpx.Client() as client:
            params = {"url": node_url}
            if down_since:
                params["since"] = down_since

            response = client.post(
                f"{primary_url}/rejoin",
                params=params,
                timeout=RECOVERY_TIMEOUT
            )
            result = response.json()

        print(f"[RECOVERY MANAGER] ✓ Recovery complete for {node_url}: "
              f"{result.get('messages_sent', 0)} messages sent")
        return True

    except Exception as e:
        print(f"[RECOVERY MANAGER] ✗ Recovery failed for {node_url}: {e}")
        return False


def _recovery_manager_loop(get_primary_fn):
    """
    Background loop that processes scheduled recoveries.
    """
    print("[RECOVERY MANAGER] Started — watching for node recoveries")

    while _recovery_manager_running:
        with _recovery_lock:
            pending = dict(_pending_recoveries)

        for node_url, down_since in pending.items():
            primary = get_primary_fn()
            if not primary:
                print("[RECOVERY MANAGER] No primary available — will retry")
                break

            success = _do_recovery(node_url, down_since, primary)

            if success:
                with _recovery_lock:
                    _pending_recoveries.pop(node_url, None)

        # WAL housekeeping — clean committed entries every 10 cycles (~60s)
        if not hasattr(_recovery_manager_loop, "_cycle"):
            _recovery_manager_loop._cycle = 0
        _recovery_manager_loop._cycle += 1
        if _recovery_manager_loop._cycle % 10 == 0:
            wal_clear_committed()

        time.sleep(RECOVERY_CHECK_INTERVAL)


def start_recovery_manager(get_primary_fn):
    """
    Start the background recovery manager thread.
    Call once from server.py startup.
    Guards against double-registration of the schedule_recovery callback
    so that hot-reloads (--reload) don't fire duplicate recoveries per event.
    """
    global _recovery_manager_running, _schedule_recovery_registered

    # Import here to avoid circular import at module level
    from node_manager import on_node_recover
    if not _schedule_recovery_registered:
        on_node_recover(schedule_recovery)
        _schedule_recovery_registered = True
        print("[RECOVERY MANAGER] schedule_recovery callback registered")
    else:
        print("[RECOVERY MANAGER] schedule_recovery already registered — skipping")

    _recovery_manager_running = True
    thread = threading.Thread(
        target=_recovery_manager_loop,
        args=(get_primary_fn,),
        daemon=True
    )
    thread.start()
    print("[RECOVERY MANAGER] Background thread started")


# ─────────────────────────────────────────
# Fault Tolerance Status Snapshot
# ─────────────────────────────────────────

def get_fault_tolerance_status() -> dict:
    """
    Returns a full snapshot of the fault tolerance subsystem.
    Exposed via /ft-status endpoint in server.py.
    """
    with _recovery_lock:
        pending = dict(_pending_recoveries)

    wal_stats = wal_get_stats()

    return {
        "wal": {
            "total":     wal_stats.get("total", 0),
            "COMMITTED": wal_stats.get(WAL_COMMITTED, 0),
            "PENDING":   wal_stats.get(WAL_PENDING, 0),
            "FAILED":    wal_stats.get(WAL_FAILED, 0),
        },
        "wal_file":                WAL_FILE,
        "pending_recoveries":      pending,
        "recovery_manager_running": _recovery_manager_running,
        "timestamp":               datetime.utcnow().isoformat()
    }