import socket
import os
from protos import app_pb2, app_pb2_grpc
from util import hash_password
import threading
import logging
import grpc
import time
from consensus import REPLICAS

# Configure logging
logging.basicConfig(level=logging.INFO)


class Client:
    # global variables consistent across all instances of the Client class
    FORMAT = "utf-8"
    HEADER = 64

    # polling thread to handle incoming messages from the server
    CLIENT_LOCK = threading.Lock()

    def __init__(self):
        self.stub = None

        self.username = ""
        # last time we tried to connect to any replica
        self.last_connection_attempt = 0
        # connection cooldown to prevent constant retries
        self.connection_cooldown = 3  # seconds
        # track working replicas to prioritize them
        self.working_replicas = set(REPLICAS.keys())

        # initial connection attempt
        self.connect_to_any_replica()

    def connect_to_any_replica(self):
        """
        Attempt to connect directly to any available replica.
        Try known working replicas first before trying others.
        """
        current_time = time.time()

        # we enforce a cooldown period 
        if current_time - self.last_connection_attempt < self.connection_cooldown:
            return False

        self.last_connection_attempt = current_time
        for replica_id in list(self.working_replicas):
            if self.try_connect_to_replica(replica_id):
                return True

        for replica_id in REPLICAS:
            if replica_id not in self.working_replicas:
                if self.try_connect_to_replica(replica_id):
                    return True

        print("Failed to connect to any replica")
        return False

    def try_connect_to_replica(self, replica_id):
        """Try to connect to a specific replica directly"""
        if replica_id not in REPLICAS:
            return False

        replica = REPLICAS[replica_id]
        host, port = replica.host, replica.port

        try:
            logging.info(
                f"Attempting to connect to replica {replica_id} at {host}:{port}"
            )
            channel = grpc.insecure_channel(f"{host}:{port}")
            # Set a short deadline for the connection attempt
            channel_ready = grpc.channel_ready_future(channel)
            channel_ready.result(timeout=2)  # 2 second timeout

            # Create stub and test with a simple call
            stub = app_pb2_grpc.AppStub(channel)

            # Try to get the leader info
            res = stub.RPCGetLeaderInfo(app_pb2.Request(), timeout=2)

            if res.operation == app_pb2.SUCCESS:
                leader_id = "".join(res.info)

                # If this replica is the leader, use it directly
                if leader_id == replica_id:
                    self.stub = stub
                    logging.info(f"Connected to leader replica {replica_id}")
                    self.working_replicas.add(replica_id)
                    return True

                # Otherwise, try to connect to the leader
                leader = REPLICAS.get(leader_id)
                if leader:
                    leader_host, leader_port = leader.host, leader.port
                    logging.info(
                        f"Connecting to leader {leader_id} at {leader_host}:{leader_port}"
                    )

                    try:
                        leader_channel = grpc.insecure_channel(
                            f"{leader_host}:{leader_port}"
                        )
                        channel_ready = grpc.channel_ready_future(leader_channel)
                        channel_ready.result(timeout=2)  # 2 second timeout

                        self.stub = app_pb2_grpc.AppStub(leader_channel)
                        logging.info(f"Successfully connected to leader {leader_id}")
                        self.working_replicas.add(leader_id)
                        self.working_replicas.add(
                            replica_id
                        )  # The queried replica works too
                        return True
                    except Exception as e:
                        logging.warning(f"Failed to connect to leader {leader_id}: {e}")
                        # If leader connection fails, use the working replica instead
                        self.stub = stub
                        self.working_replicas.add(replica_id)
                        logging.info(f"Using replica {replica_id} as fallback")
                        return True

            # If we get here, the replica responded but isn't a leader
            # Still mark it as working for future attempts
            self.working_replicas.add(replica_id)
            self.stub = stub  # Use this replica anyway
            logging.info(f"Connected to replica {replica_id} (non-leader)")
            return True

        except Exception as e:
            logging.warning(f"Failed to connect to replica {replica_id}: {e}")
            # Remove from working replicas set if connection failed
            if replica_id in self.working_replicas:
                self.working_replicas.remove(replica_id)
            return False

    def ensure_connection(self):
        """Ensure we have a connection before making a request"""
        if self.stub is None:
            return self.connect_to_any_replica()
        return True

    def handle_rpc_error(self, e, operation_name):
        """Common error handling for RPC errors"""
        logging.warning(f"RPC error during {operation_name}: {e}")
        self.stub = None  # Reset stub on error

        # Try to reconnect immediately
        if not self.connect_to_any_replica():
            logging.error(f"Failed to reconnect after RPC error in {operation_name}")
            return False
        return True

    def login(self, username, password):
        """
        Handles the login process for the client application.
        """
        if not self.ensure_connection():
            return False, 0

        try:
            # hash password
            password_hash = hash_password(password)
            request = app_pb2.Request(info=[username, password_hash])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGIN")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")

            # Use a timeout to avoid hanging
            res = self.stub.RPCLogin(request, timeout=5)
            status = res.operation

            print(f"STATUS {status}")

            if status == app_pb2.SUCCESS:
                self.username = username
                unread_messages = int(res.info[0])
                return True, int(unread_messages)
            else:
                return False, 0

        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "LOGIN"):
                # Try once more if reconnection was successful
                ret = self.login(username, password)
                print(ret)
                return ret
            return False, 0

    def logout(self):
        if not self.username:
            return True

        if not self.ensure_connection():
            return False

        try:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGOUT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCLogout(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                self.username = ""
                return True
            else:
                return False
        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "LOGOUT"):
                return self.logout()
            return False

    def create_account(self, username, password):
        """
        Handles the account creation process for the client application.
        """
        if not self.ensure_connection():
            return False

        try:
            # hash password
            password = hash_password(password)
            # create the data object to send to the server, specifying the version number, operation type, and info
            request = app_pb2.Request(info=[username, password])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: CREATE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCCreateAccount(request, timeout=5)
            status = res.operation

            if status == app_pb2.SUCCESS:
                return True
            else:
                return False

        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "CREATE_ACCOUNT"):
                return self.create_account(username, password)
            return False

    def list_accounts(self, search_string):
        """
        Handles the account listing process for the client application.
        """
        if not self.ensure_connection():
            return []

        try:
            request = app_pb2.Request(info=[search_string])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LIST ACCOUNTS")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCListAccount(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return res.info
            else:
                logging.error("Listing accounts failed!")
                return []
        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "LIST_ACCOUNTS"):
                return self.list_accounts(search_string)
            return []

    def send_message(self, receiver, msg):
        """
        Handles the message sending process for the client application.
        """
        if not self.ensure_connection():
            return False

        try:
            request = app_pb2.Request(info=[self.username, receiver, msg])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: SEND MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCSendMessage(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True
            else:
                logging.error("Sending message unexpectedly failed")
                return False

        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "SEND_MESSAGE"):
                return self.send_message(receiver, msg)
            return False

    def read_message(self):
        """
        Handles the message reading process for the client application.
        """
        if not self.ensure_connection():
            return []

        try:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: READ MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCReadMessage(request, timeout=5)
            status = res.operation

            if status == app_pb2.SUCCESS:
                messages = res.messages
                return messages
            else:
                logging.error("Reading message failed")
                return []

        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "READ_MESSAGE"):
                return self.read_message()
            return []

    def delete_messages(self, messages):
        """
        Deletes a list of messages from the server.
        """
        # iterates through the list of messages and deletes each message
        if not messages:
            return True

        success = True
        for message in messages:
            try:
                sender = message.sender
                receiver = message.receiver
                timestamp = message.timestamp
                msg = message.message
                if not self.delete_message(sender, receiver, msg, timestamp):
                    logging.error(
                        f"message from {sender} to {receiver} on {timestamp} could not be deleted"
                    )
                    success = False
            except KeyError as e:
                logging.error(f"Message is missing required field: {e}")
                success = False

        return success

    def delete_message(self, sender, receiver, msg, timestamp):
        """
        Deletes a single message from the server.
        """
        if not self.ensure_connection():
            return False

        try:
            request = app_pb2.Request(info=[sender, receiver, msg, timestamp])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCDeleteMessage(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True
            else:
                return False
        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "DELETE_MESSAGE"):
                return self.delete_message(sender, receiver, msg, timestamp)
            return False

    def get_instant_messages(self):
        """
        Gets instant messages for the current user.
        """
        if not self.username:
            return []

        if not self.ensure_connection():
            return []

        try:
            request = app_pb2.Request(info=[self.username])
            res = self.stub.RPCGetInstantMessages(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return res.messages
            else:
                return []
        except grpc.RpcError as e:
            # For instant messages, don't retry to avoid blocking the UI
            print("ERRRORRRRR WITH INSTANT")
            self.connect_to_any_replica()
            logging.debug(f"Error getting instant messages: {e}")
            return []

    def delete_account(self):
        """
        Handles the account deletion process for the client application.
        """
        if not self.ensure_connection():
            return False

        try:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCDeleteAccount(request, timeout=5)
            status = res.operation
            if status == app_pb2.SUCCESS:
                self.username = ""
                return True
            else:
                return False
        except grpc.RpcError as e:
            if self.handle_rpc_error(e, "DELETE_ACCOUNT"):
                return self.delete_account()
            return False

    # Legacy methods kept for compatibility
    def create_data_object(self, version, operation, info):
        return {"version": version, "type": operation, "info": [info]}

    def unwrap_data_object(self, data):
        if data and len(data["info"]) == 1:
            data["info"] = data["info"][0]
        return data
