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

    # --------------------------------------------------------------------------
    # User Authentication
    # --------------------------------------------------------------------------
    def login(self, username, password):
        if not self.ensure_leader():
            return (False, 0)
        try:
            h = hash_password(password)
            req = blog_pb2.Request(info=[username, h])
            resp = self.leader_stub.RPCLogin(req, timeout=3.0)
            if resp.operation == SUCCESS:
                self.username = username
                unread = len(resp.notifications) if resp.notifications else 0
                return (True, unread)
            return (False, 0)
        except:
            self.leader_stub = None
            return (False, 0)

    def create_account(self, username, password):
        if not self.ensure_leader():
            return False
        try:
            hpw = hash_password(password)
            req = blog_pb2.Request(info=[username, hpw])
            resp = self.leader_stub.RPCCreateAccount(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False

    def delete_account(self):
        if not self.ensure_leader():
            return False
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self.leader_stub.RPCDeleteAccount(req, timeout=3.0)
            if resp.operation == SUCCESS:
                self.username = None
                return True
            return False
        except:
            self.leader_stub = None
            return False

    def logout(self):
        if not self.ensure_leader():
            return False
        if not self.username:
            return True
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self.leader_stub.RPCLogout(req, timeout=3.0)
            if resp.operation == SUCCESS:
                self.username = None
                return True
            return False
        except:
            self.leader_stub = None
            return False

    def search_users(self, query):
        if not self.ensure_leader():
            return []
        try:
            req = blog_pb2.Request(info=[query])
            resp = self.leader_stub.RPCSearchUsers(req, timeout=3.0)
            if resp.operation == SUCCESS:
                return resp.info
            return []
        except:
            self.leader_stub = None
            return []

    # --------------------------------------------------------------------------
    # Blog Post Operations
    # --------------------------------------------------------------------------
    def create_post(self, title, content):
        if not self.ensure_leader():
            return False
        if not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, title, content])
            resp = self.leader_stub.RPCCreatePost(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False

    def like_post(self, post_id):
        if not self.ensure_leader() or not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, post_id])
            resp = self.leader_stub.RPCLikePost(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False

    def delete_post(self, post_id):
        if not self.ensure_leader() or not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, post_id])
            resp = self.leader_stub.RPCDeletePost(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False
    
    def get_post(self, post_id):
        if not self.ensure_leader():
            return None
        try:
            req = blog_pb2.Request(info=[post_id])
            resp = self.leader_stub.RPCGetPost(req, timeout=3.0)
            if resp.operation == SUCCESS and resp.posts:
                pid, author, title, content, ts, likes, comments_json = resp.posts
                comments = json.loads(comments_json) if comments_json else []
                return PostData(pid, author, title, content, ts, int(likes), comments)
        except Exception:
            self.leader_stub = None
        return None

    # --------------------------------------------------------------------------
    # Fetch all posts for a user, returning full PostData objects
    def get_user_posts(self, username=None):
        if not self.ensure_leader():
            return []
        target = username if username else self.username
        if not target:
            return []
        try:
            req = blog_pb2.Request(info=[target])
            resp = self.leader_stub.RPCGetUserPosts(req, timeout=3.0)
            if resp.operation != SUCCESS:
                return []
            posts = []
            for summary in resp.posts:
                # summary format: post_id|title|timestamp|likes
                parts = summary.split("|", 1)
                pid = parts[0]
                post_obj = self.get_post(pid)
                if post_obj:
                    posts.append(post_obj)
            return posts
        except Exception:
            self.leader_stub = None
            return []

    # --------------------------------------------------------------------------
    # Subscription Operations
    # --------------------------------------------------------------------------
    def subscribe(self, target_user):
        if not self.ensure_leader() or not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, target_user])
            resp = self.leader_stub.RPCSubscribe(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False

    def unsubscribe(self, target_user):
        if not self.ensure_leader() or not self.username:
            return False
        try:
            req = blog_pb2.Request(info=[self.username, target_user])
            resp = self.leader_stub.RPCUnsubscribe(req, timeout=3.0)
            return (resp.operation == SUCCESS)
        except:
            self.leader_stub = None
            return False

    def get_subscriptions(self):
        if not self.ensure_leader() or not self.username:
            return []
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self.leader_stub.RPCGetSubscriptions(req, timeout=3.0)
            return resp.info if resp.operation == SUCCESS else []
        except:
            self.leader_stub = None
            return []

    def get_followers(self, username=None):
        if not self.ensure_leader():
            return []
        try:
            target_user = username if username else self.username
            if not target_user:
                return []
            req = blog_pb2.Request(info=[target_user])
            resp = self.leader_stub.RPCGetUserFollowers(req, timeout=3.0)
            return resp.info if resp.operation == SUCCESS else []
        except:
            self.leader_stub = None
            return []

    # --------------------------------------------------------------------------
    # Notification Operations
    # --------------------------------------------------------------------------
    def get_notifications(self):
        if not self.ensure_leader() or not self.username:
            return []
        try:
            req = blog_pb2.Request(info=[self.username])
            resp = self.leader_stub.RPCGetNotifications(req, timeout=3.0)
            return resp.notifications if resp.operation == SUCCESS else []
        except:
            self.leader_stub = None
            return []