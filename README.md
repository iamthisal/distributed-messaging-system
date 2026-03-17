# Distributed Chat System

A simple, Python-based distributed messaging system. It allows users to send and receive messages in real-time using a central FastAPI server and a command-line client that polls for updates.

## Project Structure

- `server.py`: The FastAPI backend server. Provides endpoints to send, retrieve, and clear messages.
- `cllient.py`: The command-line client. Allows users to send messages and runs a background thread to poll the server for new messages.
- `database.py`: In-memory storage for messages.
- `models.py`: Pydantic models for data validation.
- `requirements.txt`: Project dependencies.

## How to Run

### 1. Start the Server

Install the requirements and run the FastAPI server using Uvicorn:

```bash
pip install -r requirements.txt
uvicorn server:app --reload --port 8000
```
The server will start on `http://localhost:8000`.

### 2. Start a Client

Open a new terminal and run the client script:

```bash
python cllient.py
```

Enter your name to connect. You can open multiple client terminals to simulate different users communicating.

### Client Commands

- `@<name> <message>`: Send a message to a specific user (e.g., `@Bob Hello there!`)
- `@all <message>`: Broadcast a message to all users
- `quit`: Exit the client
