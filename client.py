import requests
import time
import threading

# List of all servers — client tries each one if primary fails
SERVER_LIST = [
    "http://localhost:8000",
    "http://localhost:8001",
    "http://localhost:8002"
]

POLL_INTERVAL = 3
seen_message_ids = set()


def get_working_server():
    """Try each server and return the first one that responds."""
    for server in SERVER_LIST:
        try:
            response = requests.get(f"{server}/health", timeout=2)
            if response.status_code == 200:
                return server
        except Exception:
            continue
    return None


def get_primary_server():
    """Find the current primary server."""
    for server in SERVER_LIST:
        try:
            response = requests.get(f"{server}/role", timeout=2)
            if response.status_code == 200:
                data = response.json()
                if data.get("role") == "primary":
                    return server
        except Exception:
            continue
    return get_working_server()


def send_message(sender: str, recipient: str, text: str):
    payload = {
        "sender": sender,
        "receiver": recipient,
        "content": text
    }
    # Try primary first, then fallback to any working server
    server = get_primary_server()
    if not server:
        print("  ✗ No servers available!")
        return
    try:
        response = requests.post(f"{server}/send", json=payload)
        response.raise_for_status()
        msg = response.json()
        print(f"  ✓ Message sent via {server} (ID: {msg['id'][:8]}...)")
    except requests.exceptions.ConnectionError:
        print(f"  ✗ Could not connect to {server}, trying another...")
        # Try other servers
        for fallback in SERVER_LIST:
            if fallback == server:
                continue
            try:
                response = requests.post(f"{fallback}/send", json=payload)
                response.raise_for_status()
                msg = response.json()
                print(f"  ✓ Message sent via fallback {fallback}")
                return
            except Exception:
                continue
        print("  ✗ All servers unavailable!")
    except Exception as e:
        print(f"  ✗ Error: {e}")


def poll_for_messages(my_name: str):
    print(f"  [Polling for messages every {POLL_INTERVAL}s...]\n")
    while True:
        try:
            server = get_working_server()
            if not server:
                print("\n  ✗ No servers available, retrying...")
                time.sleep(POLL_INTERVAL)
                continue

            response = requests.get(
                f"{server}/messages",
                params={"receiver": my_name}
            )
            response.raise_for_status()
            all_messages = response.json()

            for msg in all_messages:
                if msg["id"] not in seen_message_ids:
                    seen_message_ids.add(msg["id"])
                    if msg["sender"] != my_name:
                        print(f"\n  📨 [{msg['timestamp']}] {msg['sender']} → you: {msg['content']}")
                        print(f"  [via {server}]")
                        print("  > ", end="", flush=True)

        except Exception as e:
            print(f"\n  ✗ Polling error: {e}")

        time.sleep(POLL_INTERVAL)


def main():
    print("=" * 45)
    print("   Distributed Messaging Client")
    print("=" * 45)

    # Find working server on startup
    print("[CLIENT] Looking for available server...")
    server = get_working_server()
    if server:
        print(f"[CLIENT] Connected to: {server}")
        primary = get_primary_server()
        print(f"[CLIENT] Current primary: {primary}")
    else:
        print("[CLIENT] No servers found! Start a server first.")
        return

    my_name = input("Enter your name: ").strip()
    if not my_name:
        print("Name cannot be empty.")
        return

    print(f"\nWelcome, {my_name}!")
    print("Commands:")
    print("  @Bob Hello there   → send to Bob")
    print("  @all Hey everyone  → broadcast")
    print("  quit               → exit\n")

    poll_thread = threading.Thread(
        target=poll_for_messages,
        args=(my_name,),
        daemon=True
    )
    poll_thread.start()

    while True:
        try:
            user_input = input("> ").strip()

            if user_input.lower() == "quit":
                print("Goodbye!")
                break

            if not user_input:
                continue

            if user_input.startswith("@"):
                parts = user_input.split(" ", 1)
                if len(parts) < 2:
                    print("  Usage: @RecipientName message")
                    continue
                recipient = parts[0][1:]
                text = parts[1]
                send_message(sender=my_name, recipient=recipient, text=text)
            else:
                print("  Tip: Start with @Name — e.g. @Alice hi!")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break


if __name__ == "__main__":
    main()