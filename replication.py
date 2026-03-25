import httpx
import threading
import time
from datetime import datetime

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────

MAX_RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 0.5
RETRY_BACKOFF = 2.0
REPLICATE_TIMEOUT = 2.0

# ─────────────────────────────────────────
# Shared State
# ─────────────────────────────────────────

REPLICAS = []                # List of replica URLs
REPLICA_STATUS = {}          # True=UP, False=DOWN
REPLICA_LAST_FAIL = {}       # Timestamp of last failure

_state_lock = threading.Lock()

# ─────────────────────────────────────────
# Registration
# ─────────────────────────────────────────

def register_replica(url: str):
    """Register a new replica or refresh an existing one."""
    with _state_lock:
        if url not in REPLICAS:
            REPLICAS.append(url)
            REPLICA_STATUS[url] = True
            print(f"[REPLICATION] New replica registered: {url}")
        else:
            REPLICA_STATUS[url] = True
            REPLICA_LAST_FAIL.pop(url, None)
            print(f"[REPLICATION] Replica {url} already registered (refreshed)")

def mark_replica_recovered(url: str, down_since: str = None):
    """Mark a replica as recovered."""
    with _state_lock:
        if url in REPLICAS:
            REPLICA_STATUS[url] = True
            REPLICA_LAST_FAIL.pop(url, None)
            print(f"[REPLICATION] ✓ {url} recovered")

def mark_replica_down(url: str):
    """Mark a replica as down."""
    with _state_lock:
        if url in REPLICAS:
            REPLICA_STATUS[url] = False
            REPLICA_LAST_FAIL[url] = datetime.utcnow().isoformat()
            print(f"[REPLICATION] ✗ {url} marked DOWN")

# ─────────────────────────────────────────
# Push Logic
# ─────────────────────────────────────────

def _push_to_replica(replica_url: str, message: dict) -> bool:
    """Attempt to push a message to a single replica with retries."""
    delay = RETRY_BASE_DELAY
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            with httpx.Client() as client:
                r = client.post(
                    f"{replica_url}/replicate",
                    json=message,
                    timeout=REPLICATE_TIMEOUT
                )
                r.raise_for_status()
            print(f"[REPLICATION] ✓ {replica_url}")
            return True
        except Exception:
            print(f"[REPLICATION] ✗ {replica_url} attempt {attempt}")
            if attempt < MAX_RETRY_ATTEMPTS:
                time.sleep(delay)
                delay *= RETRY_BACKOFF
    # Mark as down after failed attempts
    with _state_lock:
        REPLICA_STATUS[replica_url] = False
        REPLICA_LAST_FAIL[replica_url] = datetime.utcnow().isoformat()
    return False

# ─────────────────────────────────────────
# Main Replication
# ─────────────────────────────────────────

def replicate_to_all(message: dict) -> bool:
    """
    Synchronous replication (best-effort).
    Always attempts to send to all replicas, even if some are DOWN.
    """
    with _state_lock:
        targets = list(REPLICAS)  # attempt all

    if not targets:
        print("[REPLICATION] No replicas configured — OK")
        return True

    success_count = 0
    for replica_url in targets:
        ok = _push_to_replica(replica_url, message)
        if ok:
            success_count += 1

    print(f"[REPLICATION] Success {success_count}/{len(targets)}")
    # Best-effort: primary always continues even if some replicas fail
    return True

# ─────────────────────────────────────────
# Register with Primary
# ─────────────────────────────────────────

def register_with_primary(primary_url: str, own_url: str):
    """Non-primary nodes register themselves with the current primary."""
    try:
        with httpx.Client() as client:
            client.post(
                f"{primary_url}/register",
                params={"url": own_url},
                timeout=REPLICATE_TIMEOUT
            )
        print(f"[REPLICATION] Registered with primary {primary_url}")
    except Exception as e:
        print(f"[REPLICATION] Could not register with primary: {e}")

# ─────────────────────────────────────────
# Status
# ─────────────────────────────────────────

def get_replica_status():
    """Return current replica status for monitoring."""
    with _state_lock:
        return {
            url: {
                "up": REPLICA_STATUS.get(url, False),
                "last_failure": REPLICA_LAST_FAIL.get(url)
            }
            for url in REPLICAS
        }