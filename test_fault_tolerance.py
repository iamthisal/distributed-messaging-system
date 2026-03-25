import requests
import subprocess
import time
import sys
import os
from datetime import datetime, timezone

SERVERS = {
    "node_8000": "http://localhost:8000",
    "node_8001": "http://localhost:8001",
    "node_8002": "http://localhost:8002",
}

TIMEOUT = 5
results = []

# Always run launched servers from the same folder as this test file,
# so uvicorn can find server.py and all its imports.
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────

def section(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


def record(name, passed, detail=""):
    results.append((name, passed, detail))
    symbol = "\u2713" if passed else "\u2717"
    color  = "\033[92m" if passed else "\033[91m"
    reset  = "\033[0m"
    print(f"  {color}{symbol}{reset}  {name}" + (f" \u2014 {detail}" if detail else ""))


def get(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        return r.json() if r.ok else None
    except Exception:
        return None


def post(url, json=None, params=None):
    try:
        r = requests.post(url, json=json, params=params, timeout=TIMEOUT)
        return r.json() if r.ok else None
    except Exception:
        return None


def wait_for(condition_fn, timeout=20, interval=1, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition_fn():
            return True
        print(f"    ... waiting for {label}")
        time.sleep(interval)
    return False


def get_primary_url():
    """
    Pass 1: ask each node directly if it considers itself primary via /role.
    Pass 2: ask any alive node's /status for current_primary and verify it.
    """
    for url in SERVERS.values():
        try:
            data = get(f"{url}/role")
            if data and data.get("role") == "primary":
                return url
        except Exception:
            continue

    for url in SERVERS.values():
        data = get(f"{url}/status")
        if data and data.get("current_primary"):
            candidate = data["current_primary"]
            if get(f"{candidate}/health") is not None:
                return candidate

    return None


def get_any_alive_primary():
    """
    More robust version used after node kills, when some nodes are dead.
    Tries /role on each server, then falls back to /status cross-check.
    """
    for url in SERVERS.values():
        data = get(f"{url}/role")
        if data and data.get("role") == "primary":
            return url

    for url in SERVERS.values():
        data = get(f"{url}/status")
        if data:
            candidate = data.get("current_primary")
            if candidate and get(f"{candidate}/health") is not None:
                return candidate

    return None


def send_message(sender, receiver, text, server=None):
    """Send a message with one automatic retry on failure."""
    target = server or get_primary_url()
    if not target:
        return None

    result = post(f"{target}/send", json={
        "sender": sender,
        "receiver": receiver,
        "content": text
    })

    # Retry once after a short pause if the first attempt failed
    if result is None or "id" not in result:
        time.sleep(1)
        target = server or get_primary_url()
        if not target:
            return None
        result = post(f"{target}/send", json={
            "sender": sender,
            "receiver": receiver,
            "content": text
        })

    return result


def message_count_on(server_url):
    data = get(f"{server_url}/messages")
    return len(data) if data is not None else -1


def kill_server(port):
    print(f"\n  [ACTION] Killing server on port {port}...")
    result = subprocess.run(
        f'netstat -aon | findstr ":{port}"',
        shell=True, capture_output=True, text=True
    )
    killed = False
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5:
            pid = parts[-1]
            try:
                pid_int = int(pid)
                if pid_int > 0:
                    r = subprocess.run(
                        f"taskkill /F /PID {pid_int}",
                        shell=True, capture_output=True
                    )
                    if r.returncode == 0:
                        print(f"  [ACTION] Killed PID {pid_int} on port {port}")
                        killed = True
            except ValueError:
                continue
    time.sleep(3)
    if not killed:
        print(f"  [ACTION] Port {port} process not found (may already be down)")


def start_server(port):
    """
    Start a uvicorn node in a new console window (CREATE_NEW_CONSOLE).

    Why CREATE_NEW_CONSOLE and not DETACHED_PROCESS:
        DETACHED_PROCESS removes the process's console handles entirely.
        Uvicorn writes to stdout/stderr during startup (logging setup).
        With no console handles it crashes before binding the port, so
        the /health wait always times out. CREATE_NEW_CONSOLE gives it
        a real console — exactly like your 3 manual starts.

    Why cwd=SERVER_DIR:
        Without this, uvicorn inherits whatever directory the test was
        launched from. If that isn't the project folder, Python can't
        import server.py and the process exits immediately.
    """
    url = f"http://localhost:{port}"
    print(f"\n  [ACTION] Starting server on port {port}...")

    env = os.environ.copy()
    env["MY_URL"] = url

    flags = 0
    if sys.platform == "win32":
        flags = subprocess.CREATE_NEW_CONSOLE   # gives uvicorn a real console

    subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "server:app",
         "--port", str(port), "--log-level", "warning"],
        env=env,
        cwd=SERVER_DIR,       # always run from the project directory
        creationflags=flags,
    )

    alive = wait_for(
        lambda: get(f"{url}/health") is not None,
        timeout=30, label=f"port {port} to come up"
    )
    if alive:
        print(f"  [ACTION] Port {port} is UP")
    else:
        print(f"  [ACTION] WARNING — port {port} did not respond in time")
    return alive


def re_register_replicas(primary_url):
    """Tell the primary to register all other nodes as replicas."""
    for url in SERVERS.values():
        if url != primary_url:
            post(f"{primary_url}/register", params={"url": url})
    time.sleep(1)


# ─────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────

def test_1_all_nodes_healthy():
    section("TEST 1 — All Nodes Healthy")
    for name, url in SERVERS.items():
        data = get(f"{url}/health")
        ok = data is not None and data.get("status") == "healthy"
        record(f"{name} responds to /health", ok, url)


def test_2_primary_election():
    section("TEST 2 — Primary Election")
    primary = get_primary_url()
    record("A primary is elected", primary is not None, primary or "none found")
    record("Primary is node 8000 (lowest port)",
           primary == "http://localhost:8000",
           f"primary={primary}")


def test_3_replication():
    section("TEST 3 — Message Replication")

    for url in SERVERS.values():
        try:
            requests.delete(f"{url}/messages", timeout=TIMEOUT)
        except Exception:
            pass
    time.sleep(2)   # extra settle time after clearing

    # Discover primary ONCE and reuse — avoids a re-discovery race
    # on the very first send right after a DELETE operation
    primary = get_primary_url()
    if primary:
        re_register_replicas(primary)
    time.sleep(1)

    for i in range(1, 4):
        result = send_message("Alice", "Bob", f"Replication test message {i}", primary)
        ok = result is not None and "id" in result
        record(f"Message {i} accepted by primary", ok,
               result.get("id", "")[:8] + "..." if result and "id" in result else "failed")

    time.sleep(3)

    for name, url in SERVERS.items():
        count = message_count_on(url)
        record(f"{name} has 3 messages after replication", count >= 3,
               f"found {count}")


def test_4_primary_failover():
    section("TEST 4 — Primary Failover (kill port 8000)")

    kill_server(8000)

    print("    INFO  Waiting for new primary election (up to 20s)...")
    elected = wait_for(
        lambda: get_primary_url() not in (None, "http://localhost:8000"),
        timeout=20, label="new primary"
    )

    new_primary = get_primary_url()
    record("New primary elected after 8000 goes down",
           elected and new_primary is not None,
           new_primary or "none")
    record("New primary is port 8001",
           new_primary == "http://localhost:8001",
           f"primary={new_primary}")

    if new_primary:
        re_register_replicas(new_primary)

    # Wait until 8001 consistently self-reports as primary, then add
    # extra buffer so its heartbeat cycle fully completes before we send.
    # Without this, messages sent too early get 403 during the transition.
    print("    INFO  Waiting for new primary to stabilise...")
    wait_for(
        lambda: get(f"{new_primary}/role") is not None
                and get(f"{new_primary}/role").get("role") == "primary",
        timeout=15, label="new primary to stabilise"
    )
    time.sleep(3)   # buffer after confirmed stabilisation

    msgs = []
    for i in range(1, 4):
        r = send_message("Charlie", "Dave", f"Failover message {i}", new_primary)
        msgs.append(r)
        time.sleep(0.5)

    record("Messages accepted by new primary during failover",
           all(r is not None and "id" in r for r in msgs),
           f"{sum(1 for r in msgs if r and 'id' in r)}/{len(msgs)} sent")


def test_5_node_recovery():
    section("TEST 5 — Node Recovery (restart port 8000)")

    count_before = message_count_on("http://localhost:8001")
    print(f"    INFO  Messages on 8001 before recovery: {count_before}")

    started = start_server(8000)
    record("Port 8000 restarts successfully", started)

    if not started:
        record("Recovery sync skipped — node did not start", False)
        return

    print("    INFO  Waiting for delta sync to complete (up to 30s)...")
    time.sleep(5)

    synced = wait_for(
        lambda: message_count_on("http://localhost:8000") >= count_before,
        timeout=30, label="8000 to sync messages"
    )

    count_after = message_count_on("http://localhost:8000")
    record("Recovered node (8000) synced all missed messages", synced,
           f"has {count_after}, expected >={count_before}")

    all_msgs = get("http://localhost:8000/messages")
    if all_msgs:
        ids = [m["id"] for m in all_msgs]
        no_dupes = len(ids) == len(set(ids))
        record("No duplicate messages after recovery", no_dupes,
               f"total={len(ids)}, unique={len(set(ids))}")


def test_6_replica_failure():
    section("TEST 6 — Replica Failure (kill port 8002)")

    kill_server(8002)
    time.sleep(2)

    # Use get_any_alive_primary — robust when some nodes are dead.
    # get_primary_url() can return None here because 8000 may not have
    # completed a heartbeat cycle yet after its restart in test 5.
    primary = get_any_alive_primary()
    if primary:
        r = send_message("Eve", "Frank", "Message during replica failure", primary)
        record("Messages still accepted when one replica is down",
               r is not None and "id" in r,
               r.get("id", "")[:8] + "..." if r and "id" in r else "failed")

        status = get(f"{primary}/status")
        record("Cluster status endpoint still responds",
               status is not None,
               f"alive={status.get('alive_servers', [])} " if status else "no status")
    else:
        record("Messages still accepted when one replica is down", False,
               "no primary found")


def test_7_wal_status():
    section("TEST 7 — WAL Status")

    # Only check nodes currently alive (8002 was killed in test 6)
    alive_nodes = {
        name: url for name, url in SERVERS.items()
        if get(f"{url}/health") is not None
    }

    for name, url in alive_nodes.items():
        data = get(f"{url}/ft-status")
        if data is None:
            record(f"{name} /ft-status responds", False, "no response")
            continue

        record(f"{name} /ft-status responds", True)

        wal = data.get("wal", {})
        total     = wal.get("total", 0)
        committed = wal.get("COMMITTED", 0)
        pending   = wal.get("PENDING", 0)
        failed    = wal.get("FAILED", 0)

        print(f"    INFO  {name} WAL \u2192 total={total}, "
              f"committed={committed}, pending={pending}, failed={failed}")

        record(f"{name} WAL has no stuck PENDING entries", pending == 0,
               f"pending={pending}")


def test_8_deduplication():
    section("TEST 8 — Deduplication")

    primary = get_any_alive_primary()
    if not primary:
        record("Deduplication test skipped — no primary", False)
        return

    fake_msg = {
        "id": "dedup-test-id-12345",
        "sender": "Tester",
        "receiver": "all",
        "content": "Duplicate test message",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    r1 = post(f"{primary}/replicate", json=fake_msg)
    r2 = post(f"{primary}/replicate", json=fake_msg)

    record("First insert accepted",
           r1 is not None and r1.get("status") == "replicated",
           str(r1))
    record("Second insert (duplicate) correctly rejected",
           r2 is not None and r2.get("status") == "duplicate skipped",
           str(r2))


def test_9_crash_recovery():
    section("TEST 9 — WAL Crash Recovery")

    primary = get_any_alive_primary()
    if not primary:
        record("Message sent before crash", False, "no primary")
        return

    result = send_message("Crash", "Test", "Message before crash", primary)
    ok = result is not None and "id" in result
    record("Message sent before crash", ok,
           result.get("id", "")[:8] + "..." if result and "id" in result else "failed")

    if not ok:
        return

    msg_id = result["id"]
    port = int(primary.split(":")[-1])

    kill_server(port)
    restarted = start_server(port)
    record("Node restarted after crash", restarted)

    if not restarted:
        return

    time.sleep(3)

    recovered = wait_for(
        lambda: any(
            msg["id"] == msg_id
            for msg in (get(f"http://localhost:{port}/messages") or [])
        ),
        timeout=15, label="WAL recovery of message"
    )
    record("Message recovered from WAL after crash", recovered,
           f"looking for id={msg_id[:8]}...")


# ─────────────────────────────────────────
# Final Report
# ─────────────────────────────────────────

def print_report():
    section("FINAL REPORT")
    passed = sum(1 for _, ok, _ in results if ok)
    total  = len(results)
    failed_tests = [(n, d) for n, ok, d in results if not ok]

    print(f"\n  Result: {passed}/{total} tests passed\n")

    if failed_tests:
        print("  Failed tests:")
        for name, detail in failed_tests:
            print(f"    \u2717  {name}")
            if detail:
                print(f"       {detail}")

    pct = (passed / total * 100) if total else 0
    if pct == 100:
        print("\n  \033[92m ALL TESTS PASSED \u2014 Fault tolerance fully verified \u2713\033[0m")
    elif pct >= 75:
        print(f"\n  \033[93m {pct:.0f}% passed \u2014 Minor issues detected\033[0m")
    else:
        print(f"\n  \033[91m {pct:.0f}% passed \u2014 Check server logs\033[0m")

    print()


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  Fault Tolerance Test Suite")
    print("  Distributed Chat System")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)
    print("\n  NOTE: All 3 servers must be running before starting.")
    print("  Start them with (3 separate PowerShell terminals):")
    print('    $env:MY_URL="http://localhost:8000"; uvicorn server:app --port 8000')
    print('    $env:MY_URL="http://localhost:8001"; uvicorn server:app --port 8001')
    print('    $env:MY_URL="http://localhost:8002"; uvicorn server:app --port 8002\n')

    input("  Press ENTER when all 3 servers are running...")

    test_1_all_nodes_healthy()
    test_2_primary_election()
    test_3_replication()
    test_4_primary_failover()
    test_5_node_recovery()
    test_6_replica_failure()
    test_7_wal_status()
    test_8_deduplication()
    test_9_crash_recovery()

    print_report()