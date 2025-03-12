from datetime import datetime


class Message:
    def __init__(self, sender, receiver, message, timestamp=None):
        self.sender = sender
        self.receiver = receiver
        self.message = message
        self.timestamp = timestamp if timestamp else datetime.now()

    def __lt__(self, other):
        """Compare messages based on their timestamp."""
        return self.timestamp < other.timestamp

    def to_dict(self):
        """Convert message to a dictionary for serialization."""
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }
