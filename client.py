import socket
import os
from protos import blog_pb2, blog_pb2_grpc
from util import hash_password
import threading
import logging
import grpc
import time
from consensus import REPLICAS

# Configure logging
logging.basicConfig(level=logging.INFO)
SUCCESS = 0
FAILURE = 1

class PostData:
    def __init__(self, post_id, author, title, content, timestamp, likes, comments):
        self.post_id = post_id
        self.author = author
        self.title = title
        self.content = content
        self.timestamp = timestamp
        self.likes = likes
        self.comments = comments

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
        self.working_replicas = set(replica["id"] for replica in REPLICAS)

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
            if replica_id not in list(self.working_replicas):
                if self.try_connect_to_replica(replica_id):
                    return True

        print("Failed to connect to any replica")
        return False

    def try_connect_to_replica(self, replica_id):
        """Try to connect to a specific replica directly"""
        replicas = [replica for replica in REPLICAS if replica["id"] == replica_id]
        if len(replicas) == 0:
            return False
        
        replica = replicas[0]
        host, port = replica["host"], replica["port"]

        try:
            logging.info(
                f"Attempting to connect to replica {replica_id} at {host}:{port}"
            )
            channel = grpc.insecure_channel(f"{host}:{port}")
            channel_ready = grpc.channel_ready_future(channel)
            channel_ready.result(timeout=2)  # 2 second timeout

            stub = blog_pb2_grpc.BlogStub(channel)

            # try to get the leader
            res = stub.RPCGetLeaderInfo(blog_pb2.Request(), timeout=2)

            if res.operation == blog_pb2.SUCCESS:
                leader_id = "".join(res.info)

                # use if replica is leader
                if leader_id == replica_id:
                    self.stub = stub
                    logging.info(f"Connected to leader replica {replica_id}")
                    self.working_replicas.add(replica_id)
                    return True

                # else, we try to get the leader and connect
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

                        self.stub = blog_pb2_grpc.AppStub(leader_channel)
                        logging.info(f"Successfully connected to leader {leader_id}")
                        self.working_replicas.add(leader_id)
                        self.working_replicas.add(
                            replica_id
                        )  # The queried replica works too
                        return True
                    except Exception as e:
                        logging.warning(f"Failed to connect to leader {leader_id}: {e}")
                        # use working replica if leader connection fails
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

    def _rpc(self, rpc_name, req, timeout=3.0):
        """
        Generic RPC caller that retries once on "Not leader" or connection error.
        """
        for _ in range(2):
            if not self.ensure_leader():
                return None
            try:
                method = getattr(self.leader_stub, rpc_name)
                resp = method(req, timeout=timeout)
            except grpc.RpcError as e:
                logging.debug(f"RPC error on {rpc_name}: {e}")
                self.leader_stub = None
                continue
            # Handle follower "Not leader" replies
            if resp.operation == FAILURE and getattr(resp, 'info', None) and any('Not leader' in i for i in resp.info):
                logging.debug(f"{rpc_name} returned Not leader, clearing stub and retrying.")
                self.leader_stub = None
                continue
            return resp
        return None

    def login(self, username, password):
        try:
            h = hash_password(password)
            req = blog_pb2.Request(info=[username, h])
            resp = self._rpc('RPCLogin', req, timeout=3.0)
            if resp and resp.operation == SUCCESS:
                self.username = username
                unread = len(resp.notifications) if resp.notifications else 0
                return True, unread
        except Exception as e:
            logging.debug(f"login error: {e}")
        return False, 0

    def create_account(self, username, password):
        try:
            hpw = hash_password(password)
            req = blog_pb2.Request(info=[username, hpw])
            resp = self._rpc('RPCCreateAccount', req, timeout=3.0)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"create_account error: {e}")
        return False

    def logout(self):
        try:
            if not self.username:
                return True
            req = blog_pb2.Request(info=[self.username])
            resp = self._rpc('RPCLogout', req)
            if resp and resp.operation == SUCCESS:
                self.username = None
                return True
        except Exception as e:
            logging.debug(f"logout error: {e}")
        return False

    def delete_account(self):
        try:
            if not self.username:
                return False
            req = blog_pb2.Request(info=[self.username])
            resp = self._rpc('RPCDeleteAccount', req)
            if resp and resp.operation == SUCCESS:
                self.username = None
                return True
        except Exception as e:
            logging.debug(f"delete_account error: {e}")
        return False

    def search_users(self, query):
        try:
            req = blog_pb2.Request(info=[query])
            resp = self._rpc('RPCSearchUsers', req)
            return resp.info if resp and resp.operation == SUCCESS else []
        except Exception as e:
            logging.debug(f"search_users error: {e}")
        return []

    def create_post(self, title, content):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, title, content])
            resp = self._rpc('RPCCreatePost', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"create_post error: {e}")
        return False

    def like_post(self, post_id):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[post_id, self.username])
            resp = self._rpc('RPCLikePost', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"like_post error: {e}")
        return False
    def unlike_post(self, post_id):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[post_id, self.username])
            resp = self._rpc('RPCUnlikePost', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"unlike_post error: {e}")
        return False

    def has_liked_post(self, post_id):
        """Check if the current user has liked a post"""
        if not self.username:
            return False
        try:
            post = self.get_post(post_id)
            if post:
                return self.username in post.likes
        except Exception as e:
            logging.debug(f"has_liked_post error: {e}")
        return False

    def delete_post(self, post_id):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[post_id, self.username])
            resp = self._rpc('RPCDeletePost', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"delete_post error: {e}")
        return False

    def get_post(self, post_id):
        try:
            req = blog_pb2.Request(info=[post_id])
            resp = self._rpc('RPCGetPost', req)
            if resp and resp.operation == SUCCESS and resp.posts:
                post = resp.posts[0]
                return PostData(post.post_id, post.author, post.title, post.content, post.timestamp, post.likes)
        except Exception as e:
            logging.debug(f"get_post error: {e}")
        return None

    def get_user_posts(self, username=None):
        target = username or self.username
        if not target:
            return []
        try:
            req = blog_pb2.Request(info=[target])
            resp = self._rpc('RPCGetUserPosts', req)
            return resp.posts if resp and resp.operation == SUCCESS else []
        except Exception as e:
            logging.debug(f"get_user_posts error: {e}")
        return []

    def subscribe(self, followed):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, followed])
            resp = self._rpc('RPCSubscribe', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"subscribe error: {e}")
        return False

    def unsubscribe(self, followed):
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, followed])
            resp = self._rpc('RPCUnsubscribe', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"unsubscribe error: {e}")
        return False

    def get_subscriptions(self):
        if not self.username:
            return []
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self._rpc('RPCGetSubscriptions', req)
            return resp.info if resp and resp.operation == SUCCESS else []
        except Exception as e:
            logging.debug(f"get_subscriptions error: {e}")
        return []

    def get_followers(self):
        if not self.username:
            return []
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self._rpc('RPCGetUserFollowers', req)
            return resp.info if resp and resp.operation == SUCCESS else []
        except Exception as e:
            logging.debug(f"get_followers error: {e}")
        return []

    def get_notifications(self):
        if not self.username:
            return []
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self._rpc('RPCGetNotifications', req)
            return resp.notifications if resp and resp.operation == SUCCESS else []
        except Exception as e:
            logging.debug(f"get_notifications error: {e}")
        return []

    def add_replica(self, rid, host, port, raft_store, posts_store, users_store, subscriptions_store):
        try:
            cfg = json.dumps({
                'id': rid, 'host': host, 'port': port,
                'raft_store': raft_store, 'posts_store': posts_store, 'users_store': users_store, 'subscriptions_store':subscriptions_store
            })
            req = blog_pb2.Request(info=[cfg])
            resp = self._rpc('RPCAddReplica', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"add_replica error: {e}")
        return False

    def remove_replica(self, rid):
        try:
            req = blog_pb2.Request(info=[rid])
            resp = self._rpc('RPCRemoveReplica', req)
            return bool(resp and resp.operation == SUCCESS)
        except Exception as e:
            logging.debug(f"remove_replica error: {e}")
        return False

    def sync_membership(self):
        try:
            req = blog_pb2.Request(info=[])
            resp = self._rpc('RPCGetClusterMembership', req)
            if resp and resp.operation == SUCCESS and resp.info:
                return json.loads(resp.info[0])
        except Exception as e:
            logging.debug(f"sync_membership error: {e}")
        return None