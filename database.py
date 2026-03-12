from typing import List



messages : List[dict] = []


def add_message(message : dict):
    messages.append(message)

def get_all_messages() -> List[dict]:
    return messages

def get_messages_for(recipient: str) -> List[dict]:
    """
    Return all messages for a specific recipient.
    Also includes broadcast messages sent to 'all'.
    """
    return [
        m for m in messages
        if m["recipient"] == recipient or m["recipient"] == "all"
    ]

def clear_all() -> None:
    """Clear all messages """
    messages.clear()


