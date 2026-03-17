

import requests
import time
import threading

SERVER_URL = "http://localhost:8000"
POLL_INTERVAL = 3
seen_message_ids = set()


def send_message(sender: str, recipient: str, text: str):
    payload = {
        "sender": sender,
        "receiver": recipient,
        "content": text
    }
    try:
        response = requests.post(f"{SERVER_URL}/send", json=payload)
        response.raise_for_status()
        msg = response.json()
        print(f"  ✓ Message sent (ID: {msg['id'][:8]}...)")
    except requests.exceptions.ConnectionError:
        print("  ✗ Could not connect to server. Is it running?")
    except Exception as e:
        print(f"  ✗ Error sending message: {e}")


def poll_for_messages(my_name: str):
    print(f"  [Polling for messages every {POLL_INTERVAL}s...]\n")
    while True:
        try:
            response = requests.get(
                f"{SERVER_URL}/messages",
                params={"receiver": my_name}
            )
            response.raise_for_status()
            all_messages = response.json()

            for msg in all_messages:
                if msg["id"] not in seen_message_ids:
                    seen_message_ids.add(msg["id"])
                    if msg["sender"] != my_name:
                        print(f"\n  📨 [{msg['timestamp']}] {msg['sender']} → you: {msg['content']}")
                        print("  > ", end="", flush=True)

        except requests.exceptions.ConnectionError:
            print("\n  ✗ Lost connection to server...")
        except Exception as e:
            print(f"\n  ✗ Polling error: {e}")

        time.sleep(POLL_INTERVAL)


def main():
    print("=" * 45)
    print("   Distributed Messaging Client")
    print("=" * 45)

    my_name = input("Enter your name: ").strip()
    if not my_name:
        print("Name cannot be empty.")
        return

    print(f"\nWelcome, {my_name}!")
    print("Commands:")
    print("  @Bob Hello there   → send 'Hello there' to Bob")
    print("  @all Hey everyone  → broadcast to all users")
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
                    print("  Usage: @RecipientName Your message here")
                    continue
                recipient = parts[0][1:]
                text = parts[1]
                send_message(sender=my_name, recipient=recipient, text=text)
            else:
                print("  Tip: Start with @Name to address someone. E.g. @Alice hi!")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break


if __name__ == "__main__":
    main()