# Distributed Chat System

A fault-tolerant, distributed messaging system built with Python and FastAPI. Messages are replicated across a cluster of nodes, with automatic primary election, crash recovery via a Write-Ahead Log (WAL), and delta sync for rejoining nodes.

## Team

| Name | Registration No. | Email | Role |
|------|-----------------|-------|------|
|      |                 |       | Fault Tolerance |
|      |                 |       | Data Replication & Consistency |
|      |                 |       | Time Synchronization |
|      |                 |       | Consensus & Agreement |

## Project Structure

```
.
├── server.py            # FastAPI server — all HTTP endpoints
├── client.py            # CLI client for sending and receiving messages
├── database.py          # In-memory message store
├── models.py            # Pydantic request/response models
├── node_manager.py      # Heartbeat, failure detection, primary election
├── replication.py       # Replica registration and message push logic
├── fault_tolerance.py   # WAL, crash recovery, automatic recovery manager
├── benchmark.py         # Performance benchmarks (latency, throughput, WAL)
├── test_fault_tolerance.py  # Automated fault tolerance test suite (9 tests)
└── requirements.txt
```

## Architecture Overview

The system runs as a cluster of 3 nodes (ports 8000, 8001, 8002). At any time, one node is the **primary** and the others are **replicas**.

- **Primary election** is deterministic: the alive node with the lowest port number becomes primary. All nodes run the same election logic independently — no coordination required.
- **Only the primary accepts** `/send` requests. Replicas store messages pushed to them via `/replicate`.
- **Heartbeats** run every 5 seconds. If a node goes down, the next lowest-port alive node automatically becomes primary.
- **WAL (Write-Ahead Log)**: before storing any message, the primary writes a `PENDING` entry to a per-node log file. On restart, any `PENDING` entries are recovered before the node accepts traffic.
- **Delta sync**: when a node rejoins after a crash, only the messages it missed (since `down_since`) are pushed to it — not the full history.

## Requirements

```bash
pip install -r requirements.txt
```

Contents of `requirements.txt`:
```
fastapi
uvicorn
requests
pydantic
httpx
```

## Running the Cluster

Each node needs its own terminal and must know its own URL via the `MY_URL` environment variable.

**Linux / macOS:**
```bash
MY_URL=http://localhost:8000 uvicorn server:app --port 8000
MY_URL=http://localhost:8001 uvicorn server:app --port 8001
MY_URL=http://localhost:8002 uvicorn server:app --port 8002
```

**Windows (PowerShell):**
```powershell
$env:MY_URL="http://localhost:8000"; uvicorn server:app --port 8000
$env:MY_URL="http://localhost:8001"; uvicorn server:app --port 8001
$env:MY_URL="http://localhost:8002"; uvicorn server:app --port 8002
```

Node 8000 will be elected primary on startup (lowest port).

## Running the Client

In a separate terminal:
```bash
python client.py
```

Enter your name when prompted. The client auto-discovers the current primary and polls for new messages every 3 seconds.

**Commands:**
- `@Bob Hello there` — send a message to Bob
- `@all Hey everyone` — broadcast to all users
- `quit` — exit

If the primary goes down, the client will automatically find the new primary on the next send.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Node liveness check |
| GET | `/role` | Returns `primary` or `replica`, and who the current primary is |
| GET | `/status` | Full cluster status: alive nodes, replica states, message count |
| GET | `/ft-status` | Fault tolerance snapshot: WAL stats, pending recoveries |
| POST | `/send` | Send a new message (primary only) |
| GET | `/messages` | Retrieve all messages, or filter by `?receiver=Name` |
| POST | `/replicate` | Accept a replicated message from the primary (internal) |
| POST | `/register` | Register a node as a replica (internal) |
| GET | `/sync` | Delta sync: messages after `?since=<timestamp>` |
| POST | `/rejoin` | Trigger recovery for a rejoining node: `?url=...&since=...` |
| DELETE | `/messages` | Clear all messages (testing only) |

## Running the Tests

With all 3 nodes running, execute the automated test suite:

```bash
python test_fault_tolerance.py
```

The suite runs 9 tests automatically, including killing and restarting nodes:

| Test | What it checks |
|------|---------------|
| 1 | All 3 nodes respond to `/health` |
| 2 | Primary election selects port 8000 |
| 3 | Messages replicate to all nodes |
| 4 | Killing 8000 triggers failover to 8001 |
| 5 | Restarting 8000 syncs all missed messages |
| 6 | System accepts messages when one replica is down |
| 7 | WAL has no stuck PENDING entries |
| 8 | Duplicate messages are correctly rejected |
| 9 | Messages are recovered from WAL after a crash |

## Running the Benchmark

With all 3 nodes running:

```bash
python benchmark.py
```

Measures latency, throughput, replication time, WAL status, and manual recovery time.

## WAL Log Files

Each node writes its own WAL file at startup:
- `wal_node_8000.log`
- `wal_node_8001.log`
- `wal_node_8002.log`

Each line is a newline-delimited JSON entry with state `PENDING`, `COMMITTED`, or `FAILED`. Committed entries are cleaned up automatically every ~60 seconds by the background recovery manager.
