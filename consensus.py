import json
import os
import grpc
from protos import blog_pb2, blog_pb2_grpc
from typing import List

class Replica:
    def __init__(self, replica_id, host, port, messages_store, users_store):
        # a local log for each replica for specific messages
        self.log = []
        self.host = host
        self.port = port
        self.id = replica_id
        self.users_store = users_store
        self.messages_store = messages_store
        self.active = False

def get_replicas_config():
    """
    Reads replicas.json and returns a list of dictionaries with the config for each replica.
    """
    try:
        with open("replicas.json", "r") as f:
            data = json.load(f)
            return data.get("replicas", [])
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

def get_replica_by_id(replica_id):
    """
    Return the config dict for the given replica_id, or None.
    """
    replicas = get_replicas_config()
    for r in replicas:
        if r["id"] == replica_id:
            return r
    return None

def build_stub(host, port):
    """
    Create a gRPC stub for the given host/port.
    """
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = blog_pb2_grpc.BlogStub(channel)
    return stub

def get_total_stubs(id_limit=None):
    total_stubs = {}
    with open("replicas.json", "r") as file:
        data = json.load(file)
        replicas = data["replicas"]
        for r in replicas:
            if not id_limit or r["id"] != id_limit:
                print(f"Starting stub at {r['host']}:{r['port']}")
                channel = grpc.insecure_channel(f"{r['host']}:{r['port']}")
                stub = app_pb2_grpc.AppStub(channel)
                total_stubs[r["id"]] = stub
    return total_stubs


REPLICAS = get_replicas_config()
