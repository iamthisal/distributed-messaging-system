import httpx
import threading
import time

REPLICAS: list[str] = []
REPLICA_STATUS: dict[str, bool] = {}
HEARTBEAT_INTERVAL = 5


def register_replica(url: str):
    """Add a backup server to the replica list."""
    if url not in REPLICAS:
        REPLICAS.append(url)
        REPLICA_STATUS[url] = True
        print(f"[REGISTERED] New replica: {url}")


def replicate_to_all(message: dict):
    """Forward message to all registered replicas."""
    for replica_url in REPLICAS:
        if not REPLICA_STATUS.get(replica_url, False):
            print(f"[SKIPPED] {replica_url} is marked as DOWN")
            continue
        try:
            with httpx.Client() as client:
                client.post(
                    f"{replica_url}/replicate",
                    json=message,
                    timeout=2.0
                )
            print(f"[REPLICATION SUCCESS] → {replica_url}")
        except Exception:
            print(f"[REPLICATION FAILED] → {replica_url} is down")
            REPLICA_STATUS[replica_url] = False


async def register_with_primary(primary_url: str, own_url: str):
    """Called when backup server starts — registers with primary."""
    try:
        with httpx.Client() as client:
            client.post(
                f"{primary_url}/register",
                params={"url": own_url},
                timeout=2.0
            )
        print(f"[REGISTERED] with primary at {primary_url}")
    except Exception:
        print(f"[WARNING] Could not register with primary")