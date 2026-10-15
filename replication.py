import httpx
import threading
import time
from datetime import datetime


DEFAULT_NODE_URLS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
    "http://localhost:8004",
]
BOOTSTRAP_PRIMARY_URL = DEFAULT_NODE_URLS[0]
REPLICAS: list[str] = []
REPLICA_STATUS = {}
REPLICA_LAST_FAIL = {}
_state_lock = threading.Lock()
MAX_RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 0.5
RETRY_BACKOFF = 2.0
REPLICATE_TIMEOUT = 2.0


def get_port(url: str) -> int:
    return int(url.rstrip("/").rsplit(":", 1)[1])


def sort_nodes(urls: list[str]) -> list[str]:
    return sorted(set(urls), key=get_port)


def set_replicas(urls: list[str], own_url: str):
    with _state_lock:
        REPLICAS[:] = [url for url in sort_nodes(urls) if url != own_url]


def register_replica(url: str):
    with _state_lock:
        if url not in REPLICAS:
            REPLICAS.append(url)
            REPLICAS[:] = sort_nodes(REPLICAS)
        REPLICA_STATUS[url] = True
        REPLICA_LAST_FAIL.pop(url, None)


def mark_replica_recovered(url: str, down_since: str = None):
    with _state_lock:
        REPLICA_STATUS[url] = True
        REPLICA_LAST_FAIL.pop(url, None)


def mark_replica_down(url: str):
    with _state_lock:
        REPLICA_STATUS[url] = False
        REPLICA_LAST_FAIL.setdefault(url, datetime.utcnow().isoformat())


def get_replica_status():
    with _state_lock:
        return {
            url: {"up": REPLICA_STATUS.get(url, False),
                  "last_failure": REPLICA_LAST_FAIL.get(url)}
            for url in REPLICAS
        }


def _push_to_replica(replica_url: str, message: dict) -> bool:
    delay = RETRY_BASE_DELAY
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            with httpx.Client() as client:
                response = client.post(
                    f"{replica_url}/replicate", json=message, timeout=REPLICATE_TIMEOUT,
                )
                response.raise_for_status()
            mark_replica_recovered(replica_url)
            return True
        except Exception:
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(delay)
                delay *= RETRY_BACKOFF
    mark_replica_down(replica_url)
    return False


def replicate_to_all(message: dict, skip_url: str = None):
    with _state_lock:
        targets = list(REPLICAS)
    for replica_url in targets:
        if replica_url == skip_url:
            print(f"[SKIP] {replica_url} already has this message")
            continue

        _push_to_replica(replica_url, message)
    return True


def register_with_primary(primary_url: str, own_url: str):
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{primary_url}/register",
                params={"url": own_url},
                timeout=2.0,
            )
            response.raise_for_status()
        print(f"[REGISTERED] with primary at {primary_url}")
        return response.json()
    except Exception:
        print(f"[WARNING] Could not register with primary")
        return None


def fetch_node_status(node_url: str):
    try:
        with httpx.Client() as client:
            response = client.get(f"{node_url}/heartbeat", timeout=2.0)
            response.raise_for_status()
        return response.json()
    except Exception:
        return None


def fetch_time_sync(node_url: str, client_send_time_ms: int):
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{node_url}/time-sync",
                json={"client_send_time_ms": client_send_time_ms},
                timeout=2.0,
            )
            response.raise_for_status()
        return response.json()
    except Exception:
        return None


def discover_primary(known_nodes: list[str], own_url: str) -> str:
    for node_url in sort_nodes(known_nodes + [own_url]):
        status = fetch_node_status(node_url)
        if status:
            return status.get("current_primary_url") or status.get("own_url") or node_url
    return BOOTSTRAP_PRIMARY_URL


def choose_lowest_port_leader(node_urls: list[str]) -> str:
    return sort_nodes(node_urls)[0]


def announce_new_primary(new_primary_url: str, known_nodes: list[str], own_url: str) -> list[str]:
    surviving_nodes = [own_url]

    for node_url in sort_nodes(known_nodes):
        if node_url == own_url:
            continue

        try:
            with httpx.Client() as client:
                response = client.post(
                    f"{node_url}/announce-primary",
                    json={
                        "new_primary_url": new_primary_url,
                        "known_nodes": sort_nodes(known_nodes),
                    },
                    timeout=2.0,
                )
                response.raise_for_status()
            surviving_nodes.append(node_url)
        except Exception:
            print(f"[ANNOUNCE FAILED] -> {node_url} is down, removing from active nodes")

    return sort_nodes(surviving_nodes)
