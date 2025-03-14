import grpc
from protos import app_pb2_grpc, app_pb2


def test_heartbeat(host, port):
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = app_pb2_grpc.AppStub(channel)
    try:
        response = stub.RPCHeartbeat(app_pb2.Request())
        print(f"Heartbeat successful: {response}")
    except grpc.RpcError as e:
        print(f"Heartbeat failed: {e}")

# Test connectivity to the leader
test_heartbeat("localhost", 5001)