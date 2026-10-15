# Distributed Messaging System

A Python-based distributed messaging system built with FastAPI. The prototype supports message replication, leader-based coordination, failure detection, automatic failover, node rejoin recovery, time synchronization, and a built-in web UI for demonstration.

## Team Members

| Registration Number | Name | Responsibility | Email |
|---|---|---|---|
| IT24610327 | W.V.N.S. Vithanage | Fault Tolerance | IT24610327@my.sliit.lk |
| IT24101868 | Wickramathanthri T.D. | Data Replication and Consistency | IT24101868@my.sliit.lk |
| IT24103737 | D.P.S. Wickramasinghe | Time Synchronization | IT24103737@my.sliit.lk |
| IT24101519 | Hettiarachchi S.D.D. | Consensus and Agreement Algorithms | IT24101519@my.sliit.lk |

## Project Structure

- `server.py` - Main FastAPI server. Handles chat APIs, replication, failover, time sync, Raft state, and the built-in web UI.
- `replication.py` - Cluster communication helpers for replication, registration, node discovery, and leader announcement.
- `database.py` - In-memory message storage with deduplication, sorting, and thread-safe access.
- `models.py` - Pydantic request/response models used by the API.
- `client.py` - Command-line chat client for testing.
- `static/` - Browser UI assets (`index.html`, `styles.css`, `app.js`).
- `requirements.txt` - Python dependencies.

## System Overview

The system runs multiple server nodes on predefined ports:

- `8000`
- `8001`
- `8002`
- `8003`
- `8004`

Main features:

- leader-based message replication
- heartbeat-based failure detection
- automatic failover and re-election
- node rejoin and message recovery
- hybrid time synchronization
- built-in web UI for chat and cluster monitoring

## How to Run the Prototype

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start server nodes

Open a separate terminal for each node you want to run:

```bash
uvicorn server:app --port 8000
uvicorn server:app --port 8001
uvicorn server:app --port 8002
uvicorn server:app --port 8003
uvicorn server:app --port 8004
```

You may run all five nodes for the full demo, or start a smaller subset for testing.

### 3. Open the frontend

After starting at least one server, open a browser and go to:

```text
http://localhost:8000/
```

You can also open the UI from any active node:

- `http://localhost:8001/`
- `http://localhost:8002/`
- `http://localhost:8003/`
- `http://localhost:8004/`

In the web UI you can:

- enter your username
- choose the server you want to connect to
- send direct or broadcast messages
- monitor current leader, node state, time sync, and Raft status

### 4. Optional: Run the command-line client

If you want to test using the terminal client instead of the browser:

```bash
python client.py
```

Then enter the server port and your username when prompted.

## Typical Demo Flow

1. Start nodes on `8000` to `8004`.
2. Open the UI in the browser from one of the running nodes.
3. Choose a server from the UI dropdown and log in with a username.
4. Send messages between users.
5. Stop the current leader to observe failover.
6. Restart the stopped node to observe rejoin and message recovery.

## Useful Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Root endpoint and browser UI entry |
| POST | `/send` | Send a new message |
| GET | `/messages` | Retrieve messages |
| POST | `/replicate` | Receive replicated messages from leader |
| POST | `/register` | Register or re-register a node |
| GET | `/heartbeat` | Node liveness and cluster state |
| GET | `/leader` | Current leader information |
| GET | `/time-status` | Time synchronization status |
| GET | `/raft/state` | Raft consensus state |
| GET | `/ui/status` | Aggregated UI cluster status |
| DELETE | `/messages` | Clear messages for testing |

## Notes

- Messages are stored in memory, so a full cluster shutdown clears all history.
- Rejoining nodes can recover older messages from the current leader as long as at least one active node still has the data.
- The cluster uses a fixed predefined node list rather than fully dynamic membership discovery.
- Do not use `--reload` when starting nodes, because port-based node identification depends on the actual process port.
