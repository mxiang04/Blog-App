from datetime import datetime
import hashlib


def hash_password(password):
    password_obj = password.encode("utf-8")
    return hashlib.sha256(password_obj).hexdigest()
