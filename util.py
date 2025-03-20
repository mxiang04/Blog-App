from datetime import datetime
import hashlib
import pickle
import base64


def hash_password(password):
    password_obj = password.encode("utf-8")
    return hashlib.sha256(password_obj).hexdigest()


def serialize_data(user_db, active_user):
    """
    Serializes the user database and active users data for transmission.
    Returns a serialized representation of the data.
    """
    # create a tuple of the data to serialize
    data = (user_db, active_user)

    # serialize for safe and easy transmission
    serialized = pickle.dumps(data)
    encoded = base64.b64encode(serialized).decode("utf-8")

    return encoded


def deserialize_data(encoded_data):
    """
    Deserializes received data back into user database and active users.
    Returns a tuple (user_login_database, active_users)
    """
    try:
        # deserialize the data
        serialized = base64.b64decode(encoded_data)
        data = pickle.loads(serialized)

        return data
    except Exception as e:
        print(f"Error deserializing data: {e}")
        return None


def get_normalized_msg_id(msg):
    # Extract just the date, hour, minute, and second from timestamp
    # This ignores millisecond differences which can cause duplication
    timestamp_str = str(msg.timestamp)

    # Extract the first 19 chars which include up to seconds (YYYY-MM-DD HH:MM:SS)
    # This ignores the microseconds which can differ between replicas
    normalized_timestamp = (
        timestamp_str[:19] if len(timestamp_str) >= 19 else timestamp_str
    )
    return (msg.sender, msg.receiver, msg.message, normalized_timestamp)
