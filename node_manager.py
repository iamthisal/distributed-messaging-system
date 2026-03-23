import httpx
import threading
import time

# All known servers in the cluster
ALL_SERVERS = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]

# Tracks which servers are currently alive
alive_servers = set()
HEARTBEAT_INTERVAL = 5  # seconds


def check_alive_servers():
    """Ping all servers and update alive_servers set."""
    global alive_servers
    new_alive = set()
    for url in ALL_SERVERS:
        try:
            with httpx.Client() as client:
                client.get(f"{url}/health", timeout=2.0)
            new_alive.add(url)
        except Exception:
            pass
    alive_servers = new_alive
    print(f"[NODE MANAGER] Alive servers: {list(alive_servers)}")


def get_primary() -> str:
    """Return the alive server with the lowest port number."""
    if not alive_servers:
        return None
    return sorted(alive_servers, key=lambda url: int(url.split(":")[-1]))[0]


def is_primary(my_url: str) -> bool:
    """Check if this server is currently the primary."""
    return get_primary() == my_url


def get_alive_servers() -> list:
    """Return list of currently alive servers."""
    return list(alive_servers)


def _heartbeat_loop():
    """Background thread — checks alive servers every 5 seconds."""
    while True:
        check_alive_servers()
        time.sleep(HEARTBEAT_INTERVAL)


def start_heartbeat():
    """Start the heartbeat monitor in a background thread."""
    check_alive_servers()
    thread = threading.Thread(target=_heartbeat_loop, daemon=True)
    thread.start()
    print("[NODE MANAGER] Heartbeat monitor started")