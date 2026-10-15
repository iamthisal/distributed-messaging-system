from typing import List
import threading


# In-memory message store protected by a lock.
messages: List[dict] = []
message_ids: set = set()
messages_lock = threading.Lock()


def _message_sort_key(message: dict):
    return (
        int(message.get("logical_timestamp", 0)),
        message.get("corrected_timestamp") or message.get("timestamp") or "",
        message.get("id") or "",
    )


def add_message(message: dict) -> bool:
    """
    Add a message if its id hasn't been seen. Returns True if stored, False if duplicate.
    """
    with messages_lock:
        message_id = message.get("id")
        if message_id and message_id in message_ids:
            return False
        stored_message = dict(message)
        messages.append(stored_message)
        if message_id:
            message_ids.add(message_id)
        return True


def get_all_messages() -> List[dict]:
    with messages_lock:
        return [dict(message) for message in sorted(messages, key=_message_sort_key)]


def get_messages_for(receiver: str) -> List[dict]:
    with messages_lock:
        return [
            dict(message)
            for message in sorted(messages, key=_message_sort_key)
            if message["receiver"] == receiver or message["receiver"] == "all"
        ]


def get_highest_logical_timestamp() -> int:
    with messages_lock:
        if not messages:
            return 0
        return max(int(message.get("logical_timestamp", 0)) for message in messages)


def clear_all() -> None:
    """Clear all messages and reset seen IDs (useful for testing)."""
    with messages_lock:
        messages.clear()
        message_ids.clear()
