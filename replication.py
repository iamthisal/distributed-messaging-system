import httpx

REPLICAS: list[str] = []

def register_replica(url: str):
    """Add a backup server to the replica list."""
    if url not in REPLICAS:
        REPLICAS.append(url)
        print(f"[REGISTERED] New replica: {url}")

def replicate_to_all(message: dict, skip_url: str = None):
    for replica_url in REPLICAS:
        if replica_url == skip_url:
            print(f"[SKIP] {replica_url} already has this message")
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
            print(f"[REPLICATION FAILED] → {replica_url} is down, skipping")

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

