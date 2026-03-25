import httpx
import os
from datetime import datetime
import time
import threading

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────

MY_URL = os.environ.get("MY_URL", "http://localhost:8000")

ALL_SERVERS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]

HEARTBEAT_INTERVAL = 5
PROBE_TIMEOUT = 2

# ─────────────────────────────────────────
# Shared State
# ─────────────────────────────────────────

alive_servers: set = set()
node_down_since: dict = {}

_on_node_down_callbacks = []
_on_node_recover_callbacks = []

_lock = threading.Lock()

# ─────────────────────────────────────────
# Callback Registration
# ─────────────────────────────────────────

def on_node_down(fn):
    _on_node_down_callbacks.append(fn)

def on_node_recover(fn):
    _on_node_recover_callbacks.append(fn)

# ─────────────────────────────────────────
# Core Heartbeat Logic
# ─────────────────────────────────────────

def _probe(url: str) -> bool:
    try:
        with httpx.Client() as client:
            r = client.get(f"{url}/health", timeout=PROBE_TIMEOUT)
            return r.status_code == 200
    except Exception:
        return False


def check_alive_servers():
    # ── Phase 1: probe all remote nodes WITHOUT holding the lock ──────────────
    # _probe makes a live HTTP request (up to PROBE_TIMEOUT seconds per node).
    # Holding _lock during these calls would block get_primary() and
    # get_alive_servers() — and therefore /send — for up to 2s × n nodes.
    probe_results: dict[str, bool] = {}
    for url in ALL_SERVERS:
        if url == MY_URL:
            continue
        probe_results[url] = _probe(url)

    # ── Phase 2: update shared state under the lock (fast, no I/O) ───────────
    with _lock:
        alive_servers.add(MY_URL)   # self is always alive

        for url, is_alive in probe_results.items():
            was_alive = url in alive_servers

            if is_alive:
                if not was_alive:
                    # Transition: DOWN → UP
                    down_since = node_down_since.pop(url, None)
                    print(f"[NODE MANAGER] ✓ {url} is back ONLINE (was down since {down_since})")
                    for fn in _on_node_recover_callbacks:
                        try:
                            fn(url, down_since)
                        except Exception as e:
                            print(f"[NODE MANAGER] recover-callback error: {e}")
                alive_servers.add(url)
            else:
                if was_alive:
                    # Transition: UP → DOWN
                    node_down_since[url] = datetime.utcnow().isoformat()
                    print(f"[NODE MANAGER] ✗ {url} went DOWN at {node_down_since[url]}")
                    for fn in _on_node_down_callbacks:
                        try:
                            fn(url)
                        except Exception as e:
                            print(f"[NODE MANAGER] down-callback error: {e}")
                else:
                    print(f"[NODE MANAGER] ✗ {url} still DOWN")
                alive_servers.discard(url)

        print(f"[NODE MANAGER] Alive: {sorted(alive_servers)}")

# ─────────────────────────────────────────
# Primary Election
# ─────────────────────────────────────────

def get_primary() -> str | None:
    """
    Deterministic primary election:
    The alive node with the lowest port number is primary.
    Self is always included as a candidate (we are always alive).
    """
    with _lock:
        candidates = set(alive_servers)
        candidates.add(MY_URL)  # always include self

        if not candidates:
            return None

        try:
            return sorted(candidates, key=lambda url: int(url.split(":")[-1]))[0]
        except Exception:
            return None


def is_primary() -> bool:
    return get_primary() == MY_URL


def get_role() -> str:
    return "primary" if is_primary() else "replica"


def get_alive_servers() -> list:
    with _lock:
        return sorted(alive_servers)


def get_down_since(url: str) -> str | None:
    with _lock:
        return node_down_since.get(url)

# ─────────────────────────────────────────
# Auto-Registration with Primary
# ─────────────────────────────────────────

def try_register_with_primary():
    if is_primary():
        return

    primary = get_primary()
    if not primary:
        print("[NODE MANAGER] No primary found yet — will retry registration")
        return

    try:
        with httpx.Client() as client:
            client.post(
                f"{primary}/register",
                params={"url": MY_URL},
                timeout=PROBE_TIMEOUT
            )
        print(f"[NODE MANAGER] Registered with primary {primary}")
    except Exception as e:
        print(f"[NODE MANAGER] Could not register with primary: {e}")

# ─────────────────────────────────────────
# Heartbeat Loop
# ─────────────────────────────────────────

def _heartbeat_loop():
    while True:
        check_alive_servers()
        try_register_with_primary()
        time.sleep(HEARTBEAT_INTERVAL)


def start_heartbeat():
    print(f"[NODE MANAGER] Starting — I am {MY_URL}")
    check_alive_servers()
    try_register_with_primary()

    thread = threading.Thread(target=_heartbeat_loop, daemon=True)
    thread.start()
    print(f"[NODE MANAGER] Heartbeat started (interval={HEARTBEAT_INTERVAL}s, timeout={PROBE_TIMEOUT}s)")