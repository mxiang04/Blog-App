from server import Server
from consensus import Replica
import json 
import grpc 
from protos import app_pb2_grpc

def get_total_replicas(id_limit=None): 
    total_replicas = {}
    with open('replicas.json', 'r') as file:
        data = json.load(file)
        replicas = data["replicas"]
        for r in replicas: 
            if r["id"] != id_limit: 
                channel = grpc.insecure_channel(f"{r["host"]}:{r["port"]}")
                stub = app_pb2_grpc.AppStub(channel)
                total_replicas[r["id"]] = Replica(r["id"], stub, r["persistent_store"])
    return total_replicas 




