import json
import grpc
from protos import app_pb2_grpc


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


def get_total_replicas(id_limit=None):
    total_replicas = {}
    with open("replicas.json", "r") as file:
        data = json.load(file)
        replicas = data["replicas"]
        for r in replicas:
            if not id_limit or r["id"] != id_limit:
                total_replicas[r["id"]] = Replica(
                    r["id"], r["host"], r["port"], r["messages_store"], r["users_store"]
                )
    return total_replicas


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


REPLICAS = get_total_replicas()
