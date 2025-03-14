# from consensus import Replica
# import json 
# import grpc 
# from protos import app_pb2_grpc

# def get_total_replicas(id_limit=None): 
#     total_replicas = {}
#     with open('replicas.json', 'r') as file:
#         data = json.load(file)
#         replicas = data["replicas"]
#         for r in replicas: 
#             if not id_limit or r["id"] != id_limit: 
#                 total_replicas[r["id"]] = Replica(r['id'], r['host'], r['port'], r['persistent_store'])
#     return total_replicas

# def get_total_stubs(id_limit=None): 
#     total_stubs = {} 
#     with open('replicas.json', 'r') as file:
#         data = json.load(file)
#         replicas = data["replicas"]
#         for r in replicas: 
#             if not id_limit or r["id"] != id_limit: 
#                 print(f"Starting stub at {r['host']}:{r['port']}")
#                 channel = grpc.insecure_channel(f"{r['host']}:{r['port']}")
#                 stub = app_pb2_grpc.AppStub(channel)
#                 total_stubs[r["id"]] = stub
#     return total_stubs 





