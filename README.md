# Distributed Chat System

A Python-based distributed messaging system built with FastAPI. Multiple servers run simultaneously, replicating messages between them to ensure high availability. Clients can connect to any server and send/receive messages in real-time.

## Project Structure

- `server.py` — FastAPI backend server. Handles sending, receiving, replicating, and registering messages.
- `replication.py` — Replication logic. Manages replica registration and message forwarding between servers.
- `client.py` — Command-line client. Sends messages and polls server for new messages in the background.
- `database.py` — In-memory message storage.
- `models.py` — Pydantic models for data validation.
- `requirements.txt` — Project dependencies.

## Architecture
```
Client → POST /send → Primary Server (port 8000)
                            ↓
                     replicates to
                            ↓
                   Backup Server (port 8001)
                   Backup Server (port 8002)

Client → GET /messages → any server → sees all messages ✅
```

### Replication Strategy
- **Primary-backup replication** — port 8000 is always the primary
- Backup servers automatically register with the primary on startup
- When primary receives a message it forwards a copy to all registered backups
- If a backup is down, replication is skipped for that server and continues for others
- Newly joined backup servers receive all existing messages on registration

## How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Primary Server (port 8000)
```bash
uvicorn server:app --port 8000
```

### 3. Start Backup Servers

Open new terminals for each backup:
```bash
uvicorn server:app --port 8001
uvicorn server:app --port 8002
```

Each backup automatically registers with the primary on startup.

### 4. Start a Client

Open a new terminal and run:
```bash
python client.py
```

Enter your name and the server port you want to connect to. You can open multiple client terminals to simulate different users.

### Client Commands

- `@<name> <message>` — Send a message to a specific user (e.g., `@Bob Hello there!`)
- `@all <message>` — Broadcast a message to all users
- `quit` — Exit the client

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/send` | Send a new message |
| GET | `/messages` | Retrieve messages |
| POST | `/replicate` | Receive replicated message from primary |
| POST | `/register` | Register a backup server with primary |
| DELETE | `/messages` | Clear all messages |
| GET | `/` | Server health check |

## Notes

- Messages are stored in memory — they are lost when the server restarts
- Always start the primary server (port 8000) before starting backup servers
- Do not use `--reload` flag when starting servers — it breaks port detection