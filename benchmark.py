import time
import requests
from statistics import mean

PRIMARY = "http://localhost:8000"
REPLICA_1 = "http://localhost:8001"
REPLICA_2 = "http://localhost:8002"

NUM_MESSAGES = 50


# ─────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────

def send_message(i):
    data = {
        "sender": "bench",
        "receiver": "test",
        "content": f"message {i}"
    }
    start = time.time()
    r = requests.post(f"{PRIMARY}/send", json=data)
    end = time.time()

    if r.status_code == 200:
        return end - start
    return None


def get_message_count(url):
    try:
        r = requests.get(f"{url}/messages")
        return len(r.json())
    except:
        return -1


def wait_for_replication(expected_count, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        c1 = get_message_count(REPLICA_1)
        c2 = get_message_count(REPLICA_2)

        if c1 >= expected_count and c2 >= expected_count:
            return time.time() - start
        time.sleep(0.2)
    return None


# ─────────────────────────────────────────
# Benchmark Tests
# ─────────────────────────────────────────

def benchmark_latency():
    print("\n=== LATENCY TEST ===")
    times = []

    for i in range(NUM_MESSAGES):
        t = send_message(i)
        if t:
            times.append(t)

    print(f"Messages sent: {len(times)}")
    print(f"Average latency: {mean(times):.4f} sec")
    print(f"Min latency: {min(times):.4f} sec")
    print(f"Max latency: {max(times):.4f} sec")


def benchmark_replication():
    print("\n=== REPLICATION TIME TEST ===")

    start_count = get_message_count(PRIMARY)

    send_message("replication_test")

    replication_time = wait_for_replication(start_count + 1)

    if replication_time:
        print(f"Replication completed in: {replication_time:.4f} sec")
    else:
        print("Replication timeout!")


def benchmark_throughput():
    print("\n=== THROUGHPUT TEST ===")

    start = time.time()

    for i in range(NUM_MESSAGES):
        send_message(i)

    end = time.time()

    total_time = end - start
    throughput = NUM_MESSAGES / total_time

    print(f"Total time: {total_time:.2f} sec")
    print(f"Throughput: {throughput:.2f} messages/sec")


def benchmark_recovery():
    print("\n=== RECOVERY TEST ===")

    print("Kill primary (8000) manually NOW!")
    input("Press ENTER after killing 8000...")

    print("Sending messages to new primary...")
    for i in range(10):
        requests.post("http://localhost:8001/send", json={
            "sender": "fail",
            "receiver": "test",
            "content": f"failover msg {i}"
        })

    print("Restart 8000 NOW!")
    input("Press ENTER after restarting 8000...")

    start = time.time()

    expected = get_message_count(REPLICA_1)

    while True:
        recovered = get_message_count(PRIMARY)
        if recovered >= expected:
            break
        time.sleep(0.5)

    end = time.time()

    print(f"Recovery time: {end - start:.2f} sec")


def benchmark_wal():
    print("\n=== WAL STATUS ===")

    r = requests.get(f"{PRIMARY}/ft-status")
    data = r.json()

    wal = data.get("wal", {})

    print(f"Total WAL entries: {wal.get('total', 0)}")
    print(f"Committed: {wal.get('COMMITTED', 0)}")
    print(f"Pending: {wal.get('PENDING', 0)}")
    print(f"Failed: {wal.get('FAILED', 0)}")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

if __name__ == "__main__":
    print("=== DISTRIBUTED SYSTEM BENCHMARK ===")

    input("Make sure all 3 servers are running. Press ENTER to start...")

    benchmark_latency()
    benchmark_replication()
    benchmark_throughput()
    benchmark_wal()
    benchmark_recovery()

    print("\n=== BENCHMARK COMPLETE ===")