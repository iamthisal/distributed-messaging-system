from typing import List



messages : List[dict] = []


def add_message(message : dict):
    messages.append(message)

def get_all_messages() -> List[dict]:
    return messages


# time sync - sakindu
#
# consensus - dinil
#
# fault - nimthara
#
# replication -

def get_messages_for(receiver: str) -> List[dict]:
    """
    Return all messages for a specific recipient.
    Also includes broadcast messages sent to 'all'.
    """
    return [
        m for m in messages
        if m["receiver"] == receiver or m["receiver"] == "all"
    ]

def clear_all() -> None:
    """Clear all messages """
    messages.clear()


