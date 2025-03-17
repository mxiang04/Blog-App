from user import User
from message import Message
from datetime import datetime
import csv
import os
import threading
from protos import app_pb2_grpc, app_pb2
from util import deserialize_data, hash_password, serialize_data
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
        if replica.id == self.leader_id:
            self.load_data()
            self.sync_data_to_followers()

    def load_data(self):
        """
        Load users and messages from CSV files if they exist, otherwise create empty CSV files.
        """
        # ensure file exists
        if not os.path.exists(self.replica.users_store):
            with open(self.replica.users_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["username", "password"])

        # load users
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

        # same thing but for messages
        if not os.path.exists(self.replica.messages_store):
            with open(self.replica.messages_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["sender", "receiver", "msg", "timestamp", "is_read"])
        try:
            with open(self.replica.messages_store, "r", newline="") as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    sender, receiver, msg, timestamp_str, is_read = row
                    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
                    message = Message(sender, receiver, msg)
                    message.timestamp = timestamp

                    # add to sender's messages
                    if sender in self.user_login_database:
                        self.user_login_database[sender].messages.append(message)

                    # add to receiver's messages or unread_messages
                    if receiver in self.user_login_database:
                        if is_read == "True":
                            self.user_login_database[receiver].messages.append(message)
                        else:
                            self.user_login_database[receiver].unread_messages.append(
                                message
                            )
            print(f"Loaded messages from {self.replica.messages_store}")
        except Exception as e:
            print(f"Error loading messages: {e}")

    def save_data(self, user_login_db):
        """
        Save all user and message data to CSV files
        """
        # save users
        try:
            with open(self.replica.users_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["username", "password"])
                for username, user in user_login_db.items():
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

                # process all users
                for username, user in self.user_login_database.items():
                    # save read messages
                    for msg in user.messages:
                        # we only save messages where this user is the sender to avoid duplicates
                        if msg.sender == username:
                            writer.writerow(
                                [
                                    msg.sender,
                                    msg.receiver,
                                    msg.message,
                                    msg.timestamp,
                                    "True",
                                ]
                            )

                    # Save unread messages
                    for msg in user.unread_messages:
                        writer.writerow(
                            [
                                msg.sender,
                                msg.receiver,
                                msg.message,
                                msg.timestamp,
                                "False",
                            ]
                        )
        except Exception as e:
            print(f"Error saving messages: {e}")

    def sync_data_to_followers(self):
        """
        Synchronizes user database and active users data from leader to all follower replicas.
        Called whenever the leader makes changes to the data.

        Returns:
            bool: True if synchronization was successful for at least one follower, False otherwise
        """
        # leader only
        if self.leader_id != self.replica.id:
            print(
                f"Warning: Non-leader replica {self.replica.id} attempted to sync to followers"
            )
            return False

        # initialize replica stubs if not already done
        if not self.REPLICA_STUBS:
            self.REPLICA_STUBS = get_total_stubs()

        # serialize data
        serialized_data = serialize_data(self.user_login_database, self.active_users)

        # sync request
        sync_request = app_pb2.SyncDataRequest(
            source_id=self.replica.id, data=serialized_data, is_leader=True
        )

        # track successful syncs
        success_count = 0
        total_followers = len(self.replica_keys) - 1  # Exclude self

        # send to followers
        for follower_id in self.replica_keys:
            if follower_id == self.replica.id:
                continue

            try:
                follower_stub = self.REPLICA_STUBS[follower_id]
                response = follower_stub.RPCSyncData(sync_request)

                if response.operation == app_pb2.SUCCESS:
                    success_count += 1
                    print(f"Successfully synced data to follower {follower_id}")
                else:
                    print(
                        f"Failed to sync data to follower {follower_id}: {response.info}"
                    )
            except grpc.RpcError as e:
                print(f"Error syncing data to follower {follower_id}: {e}")

        print(f"Synced data to {success_count}/{total_followers} followers")
        return success_count > 0

    def sync_data_to_leader(self):
        """
        Synchronizes user database and active users data from a follower to the leader.
        Called whenever a follower makes local changes that need to be propagated.

        Returns:
            bool: True if synchronization was successful, False otherwise
        """
        # followers only
        if self.leader_id == self.replica.id:
            print(
                f"Warning: Leader replica {self.replica.id} attempted to sync to leader"
            )
            return True

        try:
            # create a channel to the leader
            with grpc.insecure_channel(
                f"{self.leader_host}:{self.leader_port}"
            ) as channel:
                leader_stub = app_pb2_grpc.AppStub(channel)

                # serialize
                serialized_data = serialize_data(
                    self.user_login_database, self.active_users
                )

                # sync request
                sync_request = app_pb2.SyncDataRequest(
                    source_id=self.replica.id, data=serialized_data, is_leader=False
                )
                response = leader_stub.RPCSyncData(sync_request)

                if response.operation == app_pb2.SUCCESS:
                    print(f"Successfully sent data update to leader ({self.leader_id})")
                    return True
                else:
                    print(f"Failed to send data update to leader: {response.info}")
                    return False

        except grpc.RpcError as e:
            print(f"Error sending data to leader: {e}")
            # this might indicate leader is down
            with self.election_lock:
                print("Leader may be down, triggering election")
                self.elect_leader()
            return False

    def RPCSyncData(self, request, context):
        """
        Handles incoming data synchronization requests from both leaders and followers.
        This is the RPC endpoint that receives sync requests.

        Parameters:
            request: The SyncDataRequest containing the serialized data
            context: The gRPC context

        Returns:
            Response: A gRPC Response indicating success or failure
        """
        try:
            # extract information from request
            source_id = request.source_id
            is_from_leader = request.is_leader
            serialized_data = request.data

            print(
                f"Received sync data from {'leader' if is_from_leader else 'follower'} {source_id}"
            )

            # deserialize
            deserialized_data = deserialize_data(serialized_data)

            if not deserialized_data or len(deserialized_data) != 2:
                return app_pb2.Response(
                    operation=app_pb2.FAILURE, info="Invalid sync data format"
                )

            received_user_db, received_active_users = deserialized_data

            if is_from_leader:
                print(
                    f"Replica {self.replica.id} updating data from leader {source_id}"
                )
                self.active_users = received_active_users
                self.save_data(received_user_db)

            else:
                if self.leader_id == self.replica.id:
                    print(
                        f"Leader {self.replica.id} merging data from follower {source_id}"
                    )
                    # merge data from follower
                    self.merge_data(received_user_db, received_active_users)
                    # propagate changes to other followers
                    self.sync_data_to_followers()
                else:
                    print(
                        f"Warning: Follower {self.replica.id} received update from follower {source_id}"
                    )
                    return app_pb2.Response(
                        operation=app_pb2.FAILURE,
                        info="Follower-to-follower sync not allowed",
                    )

            return app_pb2.Response(
                operation=app_pb2.SUCCESS,
                info=f"Data synchronized from {'leader' if is_from_leader else 'follower'} {source_id}",
            )

        except Exception as e:
            print(f"Error in RPCSyncData: {str(e)}")
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info=f"Sync data failed: {str(e)}"
            )

    def merge_data(self, incoming_user_db, incoming_active_users):
        """
        Merges incoming data with the existing data, resolving conflicts.

        Parameters:
            incoming_user_db: User database from another replica
            incoming_active_users: Active users from another replica
        """
        # merge user database
        merge_db = {}
        for username, incoming_user in incoming_user_db.items():
            if username not in self.user_login_database:
                # new user, add to database
                merge_db[username] = incoming_user
            else:
                merge_db[username] = self.user_login_database[username]

        # merge active users
        for username, incoming_messages in incoming_active_users.items():
            if username not in self.active_users:
                self.active_users[username] = incoming_messages
        self.save_data(merge_db)

    def sync_after_operation(self):
        """
        Helper function that should be called after operations that modify data.
        Determines whether to sync to leader or followers based on replica role.
        """
        if self.leader_id == self.replica.id:
            # sync to followers
            self.sync_data_to_followers()
        else:
            # sync to leader
            self.sync_data_to_leader()

    def elect_leader(self):
        print("inside electing leader?2")
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
            if replica_id == self.replica.id:
                continue
            try:
                res = self.replica_stub.RPCUpdateLeader(request)
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
                    with self.election_lock:
                        print(f"Heartbeat failed: {e}")
                        print(
                            f"Triggered by {self.replica.id} with leader {self.leader_host}:{self.leader_port}"
                        )
                        self.elect_leader()
                        print("Electing new leader")
            time.sleep(self.HEART_BEAT_FREQUENCY)

    def RPCHeartbeat(self, request, context):
        return app_pb2.Response(operation=app_pb2.SUCCESS)

    def RPCUpdateLeader(self, request, context):
        print(f"updating leader {request.info}")
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

        if self.leader_id in self.replica_keys:
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
        if (
            username in self.user_login_database
            and self.user_login_database[username].password == password
            and username not in self.active_users
        ):

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
            print(self.replica.id, "HELLO", "SAVING DATA")
            self.sync_after_operation()

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
        self.sync_after_operation()

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
            self.sync_after_operation()

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
            self.sync_after_operation()

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
                res = rpc_method(request)
                status = res.operation
                if status == app_pb2.SUCCESS:
                    success_count += 1
        if success_count > 0:
            print("broadcast success")
            return True
        return False
