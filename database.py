from typing import List



messages : List[dict] = []


<<<<<<< Updated upstream
def add_message(message : dict):
    messages.append(message)

def get_all_messages() -> List[dict]:
    return messages

def get_messages_for(receiver: str) -> List[dict]:
    """
    Return all messages for a specific recipient.
    Also includes broadcast messages sent to 'all'.
    """
    return [
        m for m in messages
        if m["receiver"] == receiver or m["receiver"] == "all"
    ]
=======
def _message_sort_key(message: dict):
    return (
        int(message.get("logical_timestamp", 0)),
        message.get("corrected_timestamp") or message.get("timestamp") or "",
        message.get("id") or "",
    )


def add_message(message: dict) -> bool:
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

>>>>>>> Stashed changes

def get_highest_logical_timestamp() -> int:
    with messages_lock:
        if not messages:
            return 0
        return max(int(message.get("logical_timestamp", 0)) for message in messages)


def clear_all() -> None:
    """Clear all messages """
    messages.clear()


