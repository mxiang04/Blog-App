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

        # Initialize with self as leader by default
        self.leader_id = replica.id
        self.leader_port = replica.port
        self.leader_host = replica.host

        self.replica_keys = list(REPLICAS.keys())
        self.election_lock = FileLock("election.lock")

        # Initialize stub to this replica first (will connect to other replicas later)
        channel = grpc.insecure_channel(f"{self.replica.host}:{self.replica.port}")
        self.replica_stub = app_pb2_grpc.AppStub(channel)

        self.running = False
        self.REPLICA_STUBS = {}  # Initialize as empty dict instead of None
        self.discovered_replicas = set([replica.id])  # Start with self-awareness

        # Start discovery and coordination threads
        threading.Thread(target=self.discover_replicas, daemon=True).start()
        time.sleep(1)  # Short delay to initialize

        # Start heartbeat after discovery has a chance to run
        self.start_heartbeat()

        # Load data initially - will be overwritten if not actually leader
        self.load_data()

        print(
            f"Replica {self.replica.id} started. Currently believes it is the leader."
        )
        print(f"Discovering other replicas and determining true leader...")

    def discover_replicas(self):
        """Continuously tries to discover other replicas and determine the leader"""
        while True:
            self.update_active_replicas()
            self.determine_true_leader()
            time.sleep(5)  # Check for new replicas every 5 seconds

    def update_active_replicas(self):
        """Try to connect to all configured replicas to see which ones are active"""
        for replica_id, replica_info in REPLICAS.items():
            if replica_id in self.discovered_replicas:
                continue  # Already discovered

            try:
                # Try to establish connection
                with grpc.insecure_channel(
                    f"{replica_info.host}:{replica_info.port}"
                ) as channel:
                    stub = app_pb2_grpc.AppStub(channel)
                    # Simple heartbeat to check if alive
                    response = stub.RPCHeartbeat(app_pb2.Request())
                    if response.operation == app_pb2.SUCCESS:
                        print(f"Discovered new replica: {replica_id}")
                        self.discovered_replicas.add(replica_id)

                        # Create a persistent stub for this replica
                        if replica_id not in self.REPLICA_STUBS:
                            persistent_channel = grpc.insecure_channel(
                                f"{replica_info.host}:{replica_info.port}"
                            )
                            self.REPLICA_STUBS[replica_id] = app_pb2_grpc.AppStub(
                                persistent_channel
                            )
            except Exception as e:
                # Can't connect, replica not available yet
                pass

    def determine_true_leader(self):
        """
        Determine the true leader among all discovered replicas
        and synchronize data if needed
        """
        if not self.discovered_replicas:
            return

        # The leader should be the lowest ID among discovered replicas
        potential_leader_id = min(self.discovered_replicas)

        # If we're not currently using this leader, update
        if self.leader_id != potential_leader_id:
            old_leader = self.leader_id
            self.leader_id = potential_leader_id
            self.leader_host = REPLICAS[potential_leader_id].host
            self.leader_port = REPLICAS[potential_leader_id].port

            print(f"Updated leader from {old_leader} to {potential_leader_id}")

            # If we just became the leader, sync data to followers
            if self.leader_id == self.replica.id and old_leader != self.replica.id:
                print(
                    f"This replica ({self.replica.id}) is now the leader. Syncing data to followers."
                )
                self.sync_data_to_followers()
            # If we just stopped being the leader, request latest data
            elif old_leader == self.replica.id and self.leader_id != self.replica.id:
                print(
                    f"This replica ({self.replica.id}) is no longer the leader. Requesting data from new leader."
                )
                self.request_data_from_leader()

    def request_data_from_leader(self):
        """Request the latest data from the current leader"""
        if self.leader_id == self.replica.id:
            return  # We are the leader, no need to request

        try:
            with grpc.insecure_channel(
                f"{self.leader_host}:{self.leader_port}"
            ) as channel:
                leader_stub = app_pb2_grpc.AppStub(channel)
                request = app_pb2.Request(info=["initial_sync"])
                response = leader_stub.RPCRequestLeaderData(request)

                if response.operation == app_pb2.SUCCESS and response.info:
                    print(f"Received initial data from leader {self.leader_id}")
                    # Deserialize and update our data
                    deserialized_data = deserialize_data(response.info[0])
                    if deserialized_data and len(deserialized_data) == 2:
                        self.user_login_database, self.active_users = deserialized_data
                        self.save_data(self.user_login_database)
                        print(
                            f"Data synchronized successfully from leader {self.leader_id}"
                        )
                        return True
        except Exception as e:
            print(f"Failed to request data from leader: {e}")
            # Leader might be down, reevaluate
            with self.election_lock:
                if self.leader_id in self.discovered_replicas:
                    self.discovered_replicas.remove(self.leader_id)
                self.determine_true_leader()

        return False

    def RPCRequestLeaderData(self, request, context):
        """Handle requests for initial data from followers"""
        if self.leader_id != self.replica.id:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Not the leader, cannot provide data"
            )

        # Serialize current data state
        serialized_data = serialize_data(self.user_login_database, self.active_users)

        return app_pb2.Response(operation=app_pb2.SUCCESS, info=[serialized_data])

    def load_data(self):
        """
        Load users and messages from CSV files if they exist, otherwise create empty CSV files.
        """
        # ensure file exists
        if not os.path.exists(self.replica.users_store):
            os.makedirs(os.path.dirname(self.replica.users_store), exist_ok=True)
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
            os.makedirs(os.path.dirname(self.replica.messages_store), exist_ok=True)
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
            directory = os.path.dirname(self.replica.users_store)
            if directory:  # Only attempt to create if there's a directory path
                os.makedirs(directory, exist_ok=True)
            with open(self.replica.users_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["username", "password"])
                for username, user in user_login_db.items():
                    writer.writerow([username, user.password])
        except Exception as e:
            print(f"Error saving users: {e}")

        # save messages
        try:
            directory = os.path.dirname(self.replica.messages_store)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(self.replica.messages_store, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(
                    ["sender", "receiver", "message", "timestamp", "is_read"]
                )

                # process all users
                for username, user in user_login_db.items():
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

        total_followers = len(self.discovered_replicas) - 1  # Exclude self

        if total_followers == 0:
            # No followers to sync to, we're the only replica
            print("No followers detected. Data has been saved locally only.")
            return True

        # Make sure we have stubs for all discovered replicas
        for replica_id in self.discovered_replicas:
            if replica_id != self.replica.id and replica_id not in self.REPLICA_STUBS:
                try:
                    replica_info = REPLICAS[replica_id]
                    channel = grpc.insecure_channel(
                        f"{replica_info.host}:{replica_info.port}"
                    )
                    self.REPLICA_STUBS[replica_id] = app_pb2_grpc.AppStub(channel)
                except Exception as e:
                    print(f"Error creating stub for replica {replica_id}: {e}")

        # serialize data
        serialized_data = serialize_data(self.user_login_database, self.active_users)

        # sync request
        sync_request = app_pb2.SyncDataRequest(
            source_id=self.replica.id, data=serialized_data, is_leader=True
        )

        # track successful syncs
        success_count = 0

        if total_followers == 0:
            # No followers to sync to, we're the only replica
            return True

        # send to followers
        for follower_id in list(
            self.discovered_replicas
        ):  # Use a copy to avoid modification during iteration
            if follower_id == self.replica.id:
                continue

            try:
                follower_stub = self.REPLICA_STUBS.get(follower_id)
                if follower_stub:
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
                # Remove from discovered replicas if not reachable
                if follower_id in self.discovered_replicas:
                    self.discovered_replicas.remove(follower_id)
                if follower_id in self.REPLICA_STUBS:
                    del self.REPLICA_STUBS[follower_id]

        if total_followers > 0:
            print(f"Synced data to {success_count}/{total_followers} followers")

        # Even if we couldn't sync to all followers, consider it a success if at least one sync worked
        # or if we're the only replica
        return success_count > 0 or total_followers == 0

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
            # This might indicate leader is down
            with self.election_lock:
                print("Leader may be down, triggering reevaluation")
                if self.leader_id in self.discovered_replicas:
                    self.discovered_replicas.remove(self.leader_id)
                self.determine_true_leader()
            return False

    def RPCGetLeaderInfo(self, request, context):
        return app_pb2.Response(operation=app_pb2.SUCCESS, info=f"{self.leader_id}")

    def BroadcastUpdate(self, request, method):
        """
        Broadcast an update to all backup replicas.
        Modified to handle missing replicas and be fault-tolerant.
        """
        print("Inside broadcast")

        # If not the leader, don't broadcast
        if not self.leader_id == self.replica.id:
            return False

        # Refresh stubs if needed
        if not self.REPLICA_STUBS:
            try:
                self.REPLICA_STUBS = get_total_stubs()
                print("Initialized replica stubs")
            except Exception as e:
                print(f"Error initializing stubs: {e}")
                # Continue with empty stubs, we'll handle errors per replica
                self.REPLICA_STUBS = {}

        success_count = 0
        active_replicas = 0

        # Use discovered_replicas to know which replicas are actually available
        for backup_replica_id in list(self.discovered_replicas):
            if backup_replica_id == self.replica.id:
                continue

            active_replicas += 1

            # Safely get or create the stub
            backup_stub = None
            try:
                if backup_replica_id in self.REPLICA_STUBS:
                    backup_stub = self.REPLICA_STUBS[backup_replica_id]
                else:
                    # Create a new stub if missing
                    replica_info = REPLICAS.get(backup_replica_id)
                    if replica_info:
                        channel = grpc.insecure_channel(
                            f"{replica_info.host}:{replica_info.port}"
                        )
                        backup_stub = app_pb2_grpc.AppStub(channel)
                        self.REPLICA_STUBS[backup_replica_id] = backup_stub
            except Exception as e:
                print(f"Error creating stub for replica {backup_replica_id}: {e}")
                # Remove from discovered if we can't connect
                if backup_replica_id in self.discovered_replicas:
                    self.discovered_replicas.remove(backup_replica_id)
                continue

            # Skip if we couldn't get a valid stub
            if not backup_stub:
                print(f"No valid stub for replica {backup_replica_id}, skipping")
                continue

            # Try to call the method
            try:
                rpc_method = getattr(backup_stub, method, None)
                if rpc_method:
                    res = rpc_method(request)
                    status = res.operation
                    if status == app_pb2.SUCCESS:
                        success_count += 1
                        print(f"Successfully broadcast to replica {backup_replica_id}")
                    else:
                        print(
                            f"Broadcast to replica {backup_replica_id} failed: {res.info}"
                        )
                else:
                    print(
                        f"Method {method} not found on stub for replica {backup_replica_id}"
                    )
            except grpc.RpcError as e:
                print(f"RPC error broadcasting to replica {backup_replica_id}: {e}")
                # Remove from discovered replicas if communication fails
                if backup_replica_id in self.discovered_replicas:
                    self.discovered_replicas.remove(backup_replica_id)
                if backup_replica_id in self.REPLICA_STUBS:
                    del self.REPLICA_STUBS[backup_replica_id]

        # Consider broadcast successful if:
        # 1. We have no active replicas (single replica system)
        # 2. We reached at least one replica successfully
        if active_replicas == 0:
            print("No active replicas to broadcast to")
            return True
        elif success_count > 0:
            print(f"Broadcast successful to {success_count}/{active_replicas} replicas")
            return True
        else:
            print(f"Broadcast failed to all {active_replicas} active replicas")
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

            # Add to discovered replicas
            if source_id not in self.discovered_replicas:
                self.discovered_replicas.add(source_id)
                print(f"Added {source_id} to discovered replicas via sync")

                # Create a persistent stub for this replica if we don't have one
                if source_id not in self.REPLICA_STUBS:
                    replica_info = REPLICAS.get(source_id)
                    if replica_info:
                        channel = grpc.insecure_channel(
                            f"{replica_info.host}:{replica_info.port}"
                        )
                        self.REPLICA_STUBS[source_id] = app_pb2_grpc.AppStub(channel)

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

                # Update in-memory data
                self.active_users = received_active_users
                # Then save to disk
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
        # merge user database - keeping both databases and preserving local users
        merge_db = self.user_login_database.copy()
        for username, incoming_user in incoming_user_db.items():
            if username not in merge_db:
                # new user, add to database
                merge_db[username] = incoming_user
            else:
                # Existing user, merge messages
                existing_user = merge_db[username]

                # Create sets of message identifiers to avoid duplicates
                existing_message_ids = {
                    (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    for msg in existing_user.messages
                }
                existing_unread_ids = {
                    (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    for msg in existing_user.unread_messages
                }

                # Add non-duplicate messages
                for msg in incoming_user.messages:
                    msg_id = (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    if msg_id not in existing_message_ids:
                        existing_user.messages.append(msg)
                        existing_message_ids.add(msg_id)

                for msg in incoming_user.unread_messages:
                    msg_id = (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    if (
                        msg_id not in existing_unread_ids
                        and msg_id not in existing_message_ids
                    ):
                        existing_user.unread_messages.append(msg)
                        existing_unread_ids.add(msg_id)

        # merge active users
        for username, incoming_messages in incoming_active_users.items():
            if username not in self.active_users:
                self.active_users[username] = incoming_messages
            else:
                # Add any messages not already in active_users
                existing_active_ids = {
                    (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    for msg in self.active_users[username]
                }

                for msg in incoming_messages:
                    msg_id = (msg.sender, msg.receiver, msg.message, str(msg.timestamp))
                    if msg_id not in existing_active_ids:
                        self.active_users[username].append(msg)

        # Update our database with the merged result
        self.save_data(merge_db)

    def sync_after_operation(self):
        """
        Helper function that should be called after operations that modify data.
        Determines whether to sync to leader or followers based on replica role.
        """
        # Always save locally first
        print("REPLICA FILES", self.replica.messages_store, self.replica.users_store)
        self.save_data(self.user_login_database)

        if self.leader_id == self.replica.id:
            # sync to followers
            return self.sync_data_to_followers()
        else:
            # sync to leader
            return self.sync_data_to_leader()

    def elect_leader(self):
        """Handle leader election when a leader failure is detected"""
        print("Initiating leader election")

        # Get currently active replicas
        active_replicas = list(self.discovered_replicas)

        if self.leader_id in active_replicas:
            active_replicas.remove(self.leader_id)

        if not active_replicas:
            # If no other replicas are known, become the leader
            new_leader_id = self.replica.id
        else:
            # Choose the replica with the lowest ID
            new_leader_id = min(active_replicas)

        if new_leader_id == self.leader_id:
            # No change needed
            return

        old_leader_id = self.leader_id
        self.leader_id = new_leader_id
        self.leader_host = REPLICAS[new_leader_id].host
        self.leader_port = REPLICAS[new_leader_id].port

        print(f"Leader changed from {old_leader_id} to {new_leader_id}")

        # Notify other replicas
        self.notify_replicas_new_leader()

        # If we're the new leader, start leader duties
        if self.leader_id == self.replica.id:
            print(f"This replica ({self.replica.id}) is now the leader")
            self.sync_data_to_followers()

    def start_heartbeat(self):
        threading.Thread(target=self.send_beats, daemon=True).start()

    def notify_replicas_new_leader(self):
        """Notify all discovered replicas about the new leader"""
        print(f"Notifying replicas about new leader: {self.leader_id}")

        request = app_pb2.Request(
            info=[str(self.leader_id), self.leader_host, str(self.leader_port)]
        )

        for replica_id in list(
            self.discovered_replicas
        ):  # Use a copy for safe iteration
            if replica_id == self.replica.id:
                continue

            try:
                if replica_id not in self.REPLICA_STUBS:
                    replica_info = REPLICAS[replica_id]
                    channel = grpc.insecure_channel(
                        f"{replica_info.host}:{replica_info.port}"
                    )
                    self.REPLICA_STUBS[replica_id] = app_pb2_grpc.AppStub(channel)

                replica_stub = self.REPLICA_STUBS[replica_id]
                response = replica_stub.RPCUpdateLeader(request)

                if response.operation == app_pb2.SUCCESS:
                    print(
                        f"Successfully notified replica {replica_id} about new leader"
                    )
                else:
                    print(f"Failed to notify replica {replica_id}: {response.info}")
            except Exception as e:
                print(f"Error notifying replica {replica_id}: {e}")
                # Remove from discovered replicas if not reachable
                if replica_id in self.discovered_replicas:
                    self.discovered_replicas.remove(replica_id)
                if replica_id in self.REPLICA_STUBS:
                    del self.REPLICA_STUBS[replica_id]

    def send_beats(self):
        """Send heartbeats to the leader to detect failures"""
        # Wait for initial startup to complete
        time.sleep(3)

        while True:
            # Don't send heartbeats if we're the leader
            if self.leader_id == self.replica.id:
                time.sleep(self.HEART_BEAT_FREQUENCY)
                continue

            if self.leader_id and self.leader_host and self.leader_port:
                try:
                    with grpc.insecure_channel(
                        f"{self.leader_host}:{self.leader_port}"
                    ) as channel:
                        stub = app_pb2_grpc.AppStub(channel)
                        response = stub.RPCHeartbeat(app_pb2.Request())
                        # Leader is alive, no need to do anything
                except grpc.RpcError as e:
                    with self.election_lock:
                        print(f"Heartbeat to leader failed: {e}")
                        print(
                            f"Triggered by {self.replica.id} with leader {self.leader_id}"
                        )
                        # Leader is down, initiate election
                        if self.leader_id in self.discovered_replicas:
                            self.discovered_replicas.remove(self.leader_id)
                        if self.leader_id in self.REPLICA_STUBS:
                            del self.REPLICA_STUBS[self.leader_id]
                        self.elect_leader()

            time.sleep(self.HEART_BEAT_FREQUENCY)

    def RPCHeartbeat(self, request, context):
        """Handle heartbeat requests from other replicas"""
        # Update discovered replicas if this is a new client
        client_info = context.peer()
        if client_info:
            # Extract client ID if possible (implementation-specific)
            # Add logic here if you want to track which replicas are sending heartbeats
            pass

        return app_pb2.Response(operation=app_pb2.SUCCESS)

    def RPCUpdateLeader(self, request, context):
        """Handle leader update notifications from other replicas"""
        if len(request.info) != 3:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info="Update Leader Request Invalid"
            )

        try:
            new_leader_id = int(request.info[0])
            new_leader_host = request.info[1]
            new_leader_port = request.info[2]

            print(f"Received leader update: new leader is {new_leader_id}")

            old_leader_id = self.leader_id
            self.leader_id = new_leader_id
            self.leader_host = new_leader_host
            self.leader_port = new_leader_port

            # If we're the new leader, start leader duties
            if self.leader_id == self.replica.id and old_leader_id != self.replica.id:
                print(f"This replica ({self.replica.id}) is now the leader")
                self.sync_data_to_followers()

            return app_pb2.Response(
                operation=app_pb2.SUCCESS, info=f"Leader updated to {new_leader_id}"
            )
        except Exception as e:
            return app_pb2.Response(
                operation=app_pb2.FAILURE, info=f"Leader update failed: {e}"
            )

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
