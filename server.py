from user import User
from message import Message
from datetime import datetime
import csv
import os
import threading
from protos import app_pb2_grpc, app_pb2
from util import hash_password
from consensus import REPLICAS, get_total_stubs
import time
import grpc
from filelock import FileLock


class Server(app_pb2_grpc.AppServicer):
    HEADER = 64
    FORMAT = "utf-8"
    HEART_BEAT_FREQUENCY = 1

    def __init__(self, replica):
        # all users and their associated data stored in the User object
        self.user_login_database = {}
        self.active_users = {}
        self.replica = replica

        self.leader_id = min(REPLICAS.keys())
        self.leader_port = REPLICAS[self.leader_id].port
        self.leader_host = REPLICAS[self.leader_id].host

        print(f"LEADER {self.leader_id}")

        self.replica_keys = list(REPLICAS.keys())
        self.election_lock = FileLock("election.lock")

        channel = grpc.insecure_channel(f"{self.replica.host}:{self.replica.port}")
        self.replica_stub = app_pb2_grpc.AppStub(channel)

        self.running = False
        self.REPLICA_STUBS = None

        time.sleep(3)

        self.start_heartbeat()
        self.load_data()

    def load_data(self):
        """
        Load users and messages from CSV files if they exist
        """
        # Load users
        if os.path.exists(self.replica.users_store):
            try:
                with open(self.replica.users_store, "r", newline="") as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip header
                    for row in reader:
                        username, password = row
                        self.user_login_database[username] = User(username, password)
                print(
                    f"Loaded {len(self.user_login_database)} users from {self.replica.users_store}"
                )
            except Exception as e:
                print(f"Error loading users: {e}")

        # Load messages
        if os.path.exists(self.replica.messages_store):
            try:
                with open(self.replica.messages_store, "r", newline="") as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip header
                    for row in reader:
                        sender, receiver, msg, timestamp_str, is_read = row
                        timestamp = datetime.strptime(
                            timestamp_str, "%Y-%m-%d %H:%M:%S.%f"
                        )
                        message = Message(sender, receiver, msg)
                        message.timestamp = timestamp

                        # add to sender's messages
                        if sender in self.user_login_database:
                            self.user_login_database[sender].messages.append(message)

                        # add to receiver's messages or unread_messages
                        if receiver in self.user_login_database:
                            if is_read == "True":
                                self.user_login_database[receiver].messages.append(
                                    message
                                )
                            else:
                                self.user_login_database[
                                    receiver
                                ].unread_messages.append(message)
                print(f"Loaded messages from {self.replica.messages_store}")
            except Exception as e:
                print(f"Error loading messages: {e}")

    def save_data(self):
        """
        Save all user and message data to CSV files
        """
        # save users
        try:
            with open(self.replica.users_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["username", "password"])
                for username, user in self.user_login_database.items():
                    writer.writerow([username, user.password])
        except Exception as e:
            print(f"Error saving users: {e}")

        # save messages
        try:
            with open(self.replica.messages_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    ["sender", "receiver", "message", "timestamp", "is_read"]
                )

                # Create a set to track which messages have been saved
                saved_messages = set()

                # process all users
                for username, user in self.user_login_database.items():
                    # save read messages where this user is the sender
                    for msg in user.messages:
                        if msg.sender == username:
                            # Create a unique identifier for the message
                            msg_id = f"{msg.sender}|{msg.receiver}|{msg.message}|{msg.timestamp}"

                            # Only save if we haven't seen this message before
                            if msg_id not in saved_messages:
                                writer.writerow(
                                    [
                                        msg.sender,
                                        msg.receiver,
                                        msg.message,
                                        msg.timestamp,
                                        "True",
                                    ]
                                )
                                saved_messages.add(msg_id)

                    # Save unread messages where this user is the receiver
                    for msg in user.unread_messages:
                        # Create a unique identifier for the message
                        msg_id = (
                            f"{msg.sender}|{msg.receiver}|{msg.message}|{msg.timestamp}"
                        )

                        # Only save if we haven't seen this message before
                        if msg_id not in saved_messages:
                            writer.writerow(
                                [
                                    msg.sender,
                                    msg.receiver,
                                    msg.message,
                                    msg.timestamp,
                                    "False",
                                ]
                            )
                            saved_messages.add(msg_id)
        except Exception as e:
            print(f"Error saving messages: {e}")

    def elect_leader(self):
        print(f"inside electing leader? with {self.leader_id} and {self.replica_keys}")
        if self.leader_id in self.replica_keys:
            print("inside electing leader?3")
            old_id = self.leader_id
            self.replica_keys.remove(old_id)

            self.leader_id = min(self.replica_keys)

            self.leader_port = REPLICAS[self.leader_id].port
            self.leader_host = REPLICAS[self.leader_id].host

            print(f"New leader values {self.leader_id}")
            self.notify_replicas_new_leader()

    def start_heartbeat(self):
        threading.Thread(target=self.send_beats, daemon=True).start()

    def servers_running(self):
        request = app_pb2.HeartbeatRequest()
        while True:
            success = 0
            print("redoing")
            for replica_id in self.replica_keys:
                if replica_id == self.replica.id:
                    sucess += 1
                else:
                    try:
                        res = self.replica_stub.RPCHeartbeat(request)
                        if res == app_pb2.SUCCESS:
                            success += 1
                    except grpc.RpcError as e:
                        print(f"Not all servers running - please wait {success}")
        # return success == len(REPLICAS)

    def notify_replicas_new_leader(self):
        print(f"notify replicas {self.leader_id} from {self.replica.id}")
        request = app_pb2.Request(
            info=[self.leader_id, self.leader_host, str(self.leader_port)]
        )
        for replica_id in self.replica_keys:
            print(f"Notifying these replicas {replica_id}")
            if replica_id == self.replica.id:
                continue
            try:
                # TEST
                if not self.REPLICA_STUBS:
                    self.REPLICA_STUBS = get_total_stubs()
                res = self.REPLICA_STUBS[replica_id].RPCUpdateLeader(request)
                print(f"Notified {replica_id} of new leader {self.leader_id}")
            except grpc.RpcError as e:
                print(f"Failed to notify {replica_id}: {e}")

    def send_beats(self):
        time.sleep(5)  # Wait for servers to initialize
        while True:
            if self.leader_id == self.replica.id:
                time.sleep(self.HEART_BEAT_FREQUENCY)
                continue
            if self.leader_port and self.leader_host and self.leader_id:
                try:
                    with grpc.insecure_channel(
                        f"{self.leader_host}:{self.leader_port}"
                    ) as channel:
                        stub = app_pb2_grpc.AppStub(channel)
                        response = stub.RPCHeartbeat(app_pb2.Request())
                except grpc.RpcError as e:
                    # print(f"Before replica id {self.replica.id}")
                    if self.replica.id == max(self.replica_keys):
                        print(f"Highest replica id {self.replica.id}")
                        # we implement a bully algorithm such that only the highest replica id gets to elect the leader 
                        # with self.election_lock:
                        print(f"Heartbeat failed: {e}")
                        print(
                            f"Triggered by {self.replica.id} with leader {self.leader_host}:{self.leader_port}"
                        )
                        self.elect_leader()
                        print("Elected new leader")
            time.sleep(self.HEART_BEAT_FREQUENCY)

    def RPCHeartbeat(self, request, context):
        return app_pb2.Response(operation=app_pb2.SUCCESS)

    def RPCUpdateLeader(self, request, context):
        print(f"updating leader {request.info} on {self.replica.id}")
        if len(request.info) != 3:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Update Leader Request Failed"
            )
        leader_id, leader_host, leader_port = (
            request.info[0],
            request.info[1],
            request.info[2],
        )
        print(f"new leader lection {leader_id}")
        
        if leader_id != self.leader_id:
            self.replica_keys.remove(self.leader_id)

            self.leader_id = leader_id
            self.leader_host = leader_host
            self.leader_port = leader_port

        return app_pb2.Response(operation=app_pb2.SUCCESS, info="")

    def check_valid_user(self, username):
        """
        Checks if the user is in the login database and active users.

        username: the username of the user

        Returns:
            bool: True if the user is in the login database and active users, False otherwise
        """
        # check if the user is in the login database and active users
        return username in self.user_login_database and username in self.active_users

    def RPCLogin(self, request, context):
        """
        Logs in the user if the username and password are correct.

        Args:
            username: The username of the user
            password: The password of the user

        Returns:
            dict: A dictionary representing the data object
        """
        # check if the username and password are correct
        print("RPC Login")
        if self.leader_id == self.replica.id:
            print("Broadcasting")
            self.BroadcastUpdate(request, "RPCLogin")
        if len(request.info) != 2:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Login Request Invalid"
            )

        username, password = request.info
        print(username, password)

        print(f"Failing here username should be in {self.user_login_database}, \n and not in {self.active_users}")
        print(username in self.user_login_database)
        print(self.user_login_database[username].password, password)
        print(username not in self.active_users)
        if (
            username in self.user_login_database
            and self.user_login_database[username].password == password
            and username not in self.active_users
        ):
            print("INSIDE HERE")
            unread_messages = len(self.user_login_database[username].unread_messages)
            self.active_users[username] = []
            response = app_pb2.Response(
                operation=app_pb2.SUCCESS, info=f"{unread_messages}"
            )
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGIN")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

        else:
            return app_pb2.Response(operation=app_pb2.FAILURE, info="Login Failed")

    def RPCCreateAccount(self, request, context):
        """
        Creates an account if the username and password are not taken.

        Args:
            username: The username of the user
            password: The password of the user

        Returns:
            dict: A dictionary representing the data object
        """
        print(f"RPC Create Account {self.replica.id}")
        if self.leader_id == self.replica.id:
            print("RPC LEADER")
            ret = self.BroadcastUpdate(request, "RPCCreateAccount")
            if not ret:
                print("here -1")
                return app_pb2.Response(
                    operation=app_pb2.FAILURE, info="Broadcast Not Successful"
                )
        print("here 0")
        if len(request.info) != 2:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Create Account Request Invalid"
            )
        print("here 1")
        username, password = request.info
        print(username, password)
        # check if the username is taken
        if username in self.user_login_database:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Create Account Failed"
            )
        # check if the username and password are not empty
        elif not username or not password:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Create Account Failed"
            )
        # create the account
        else:
            self.user_login_database[username] = User(username, password)
            # Save changes to CSV
            print(self.replica, "HELLO", "SAVING DATA")
            self.save_data()

            response = app_pb2.Response(operation=app_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: CREATE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

    def RPCListAccount(self, request, context):
        """
        Lists all accounts that start with the search string.

        search_string: The string to search for

        Returns:
            dict: A dictionary representing the data object
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCListAccount")
        try:
            if len(request.info) != 1:
                return app_pb2.Response(
                    operation=app_pb2.FAILURE, info="List Account Request Invalid"
                )
            search_string = request.info[0]
            accounts = [
                username
                for username in self.user_login_database.keys()
                if username.startswith(search_string)
            ]
            response = app_pb2.Response(operation=app_pb2.SUCCESS, info=accounts)
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LIST ACCOUNTS")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

        except:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="List Account Failed"
            )

    def RPCSendMessage(self, request, context):
        """
        Sends a message to the receiver if the sender and receiver are valid users.

        Args:
            sender: The username of the sender
            receiver: The username of the receiver
            msg: The message to send

        Returns:
            dict: A dictionary representing the data object
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCSendMessage")
        if len(request.info) != 3:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Send Message Request Invalid"
            )

        sender, receiver, msg = request.info
        # check if the sender is a valid user
        if sender not in self.user_login_database:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Send Message Failed"
            )

        # check if the receiver is a valid user
        if receiver not in self.user_login_database:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Send Message Failed"
            )

        # check if the sender and receiver are the same
        if sender == receiver:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Send Message Failed"
            )
        # check if the message is empty
        if not msg:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Send Message Failed"
            )

        message = Message(sender, receiver, msg)

        # check if the receiver is active and appends to unread messages if not active and regular messages otherwise
        if receiver not in self.active_users:
            self.user_login_database[receiver].unread_messages.append(message)
        else:
            self.active_users[receiver].append(message)
            self.user_login_database[receiver].messages.append(message)

        # append the message to the sender's messages
        self.user_login_database[sender].messages.append(message)

        # Save changes to CSV
        self.save_data()

        response = app_pb2.Response(operation=app_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: SEND MESSAGE")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        return response

    def RPCGetInstantMessages(self, request, context):
        """
        Gets the instant messages of the user.
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCGetInstantMessages")
        if len(request.info) != 1:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Get Instant Messages Request Invalid"
            )

        username = request.info[0]

        if (
            username not in self.user_login_database
            or username not in self.active_users
        ):
            return app_pb2.Response(operation=app_pb2.FAILURE, info="User not found")

        incoming_messages = self.active_users[username]

        incoming_messages = [
            app_pb2.Message(
                sender=msg.sender,
                receiver=msg.receiver,
                timestamp=str(msg.timestamp),
                message=msg.message,
            )
            for msg in incoming_messages
        ]
        response = app_pb2.Response(
            operation=app_pb2.SUCCESS, info="", messages=incoming_messages
        )
        return response

    def RPCReadMessage(self, request, context):
        """
        Reads the messages of the user.

        username: The username of the user

        Returns:
            dict: A dictionary representing the data object
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCReadMessage")
        if len(request.info) != 1:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Read Message Request Invalid"
            )
        username = request.info[0]

        # check if the user is a valid user
        if username not in self.user_login_database:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Read Message Failed"
            )

        try:
            user = self.user_login_database[username]
            # check if the user has unread messages and appends to messages if they do
            if user.unread_messages:
                user.messages += user.unread_messages
                user.unread_messages = []

            # sort the messages by timestamp
            messages = sorted(user.messages, key=lambda x: x.timestamp)

            # create the data object as a list of dictionaries that represent the messages
            message_list = [
                app_pb2.Message(
                    sender=msg.sender,
                    receiver=msg.receiver,
                    timestamp=str(msg.timestamp),
                    message=msg.message,
                )
                for msg in messages
            ]
            self.active_users[username] = []
            response = app_pb2.Response(
                operation=app_pb2.SUCCESS, info="", messages=message_list
            )
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: READ MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

        except:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Read Message Failed"
            )

    def delete_message_from_user(
        self, user, sender, receiver, msg, timestamp, unread=False
    ):
        """
        Deletes a message from the user's messages and unread messages.

        Args:
            user: The user object
            sender: The username of the sender
            receiver: The username of the receiver
            msg: The message to delete
            timestamp: The timestamp of the message

        Returns:
            list: A list of messages filtered out of the deleted message
        """
        # checks to see if unread messages should also be deleted from the receiver side
        if unread:
            user.unread_messages = [
                message
                for message in user.unread_messages
                if not (
                    message.receiver == receiver
                    and message.sender == sender
                    and message.message == msg
                    and message.timestamp
                    == datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
                )
            ]
        user.messages = [
            message
            for message in user.messages
            if not (
                message.receiver == receiver
                and message.sender == sender
                and message.message == msg
                and message.timestamp
                == datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
            )
        ]

    def RPCDeleteMessage(self, request, context):
        """
        Deletes a message from the user's messages and unread messages.

        Args:
            sender: The username of the sender
            receiver: The username of the receiver
            msg: The message to delete
            timestamp: The timestamp of the message

        Returns:
            dict: A dictionary representing the data object
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCDeleteMessage")
        if len(request.info) != 4:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Delete Message Request Invalid"
            )

        sender, receiver, msg, timestamp = request.info
        try:
            # check if the sender is a valid user
            if sender in self.user_login_database:
                user = self.user_login_database[sender]
                # gets the user and searches for the message to delete
                self.delete_message_from_user(user, sender, receiver, msg, timestamp)

            # check if the receiver is a valid user and deletes the message from their messages
            if receiver in self.user_login_database:
                user = self.user_login_database[receiver]
                self.delete_message_from_user(
                    user, sender, receiver, msg, timestamp, unread=True
                )

            # Save changes to CSV
            self.save_data()

            response = app_pb2.Response(operation=app_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

        except:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Delete Message Failed"
            )

    def RPCDeleteAccount(self, request, context):
        """
        Deletes an account from the user login database and active users.

        Args:
            username: The username of the user

        Returns:
            dict: A dictionary representing the data object
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCDeleteAccount")
        if len(request.info) != 1:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Delete Account Request Invalid"
            )

        username = request.info[0]
        try:
            # check if the user is a valid user
            if username not in self.user_login_database:
                return app_pb2.Response(
                    operation=app_pb2.FAILURE, info="Delete Account Failed"
                )

            self.user_login_database.pop(username)
            if username in self.active_users:
                self.active_users.pop(username)

            # Save changes to CSV
            self.save_data()

            response = app_pb2.Response(operation=app_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response

        except:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Delete Account Failed"
            )

    def RPCLogout(self, request, context):
        """
        Logs out the user.
        """
        if self.leader_id == self.replica.id:
            self.BroadcastUpdate(request, "RPCLogout")
        if len(request.info) != 1:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Logout Request Invalid"
            )

        username = request.info[0]
        try:
            self.active_users.pop(username)
            response = app_pb2.Response(operation=app_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGOUT")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            return response
        except:
            return app_pb2.Response(operation=app_pb2.FAILURE, info="Logout Failed")

    def RPCGetLeaderInfo(self, request, context):
        return app_pb2.Response(operation=app_pb2.SUCCESS, info=f"{self.leader_id}")

    def BroadcastUpdate(self, request, method):
        print("inside broadcast")
        if not self.REPLICA_STUBS:
            self.REPLICA_STUBS = get_total_stubs()
            print("in here?")
        if not self.leader_id == self.replica.id:
            return
        success_count = 0
        for backup_replica_id in self.replica_keys:
            if backup_replica_id == self.replica.id:
                continue
            backup_stub = self.REPLICA_STUBS[backup_replica_id]
            rpc_method = getattr(backup_stub, method, None)
            if rpc_method:
                try:
                    res = rpc_method(request)
                    status = res.operation
                    if status == app_pb2.SUCCESS:
                        success_count += 1
                except grpc.RpcError as e:
                    self.replica_keys.remove(backup_replica_id)
                    print(f"{backup_replica_id} removed from list of replicas")
        if success_count >= len(self.replica_keys) - 1:
            print("broadcast success")
            return True
        return False
