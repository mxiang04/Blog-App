import grpc
import time
import logging
import json
import os
from protos import blog_pb2, blog_pb2_grpc
from util import hash_password
from consensus import get_replicas_config

SUCCESS = 0
FAILURE = 1

class PostData:
    def __init__(self, post_id, author, title, content, timestamp, likes):
        self.post_id = post_id
        self.author = author
        self.title = title
        self.content = content
        self.timestamp = timestamp
        self.likes = likes

class Client:
    def __init__(self):
        self.leader_stub = None
        self.replicas = get_replicas_config()
        self.username = None
        self.find_leader()

    def load_replicas(self):
        if os.path.exists("replicas.json"):
            with open("replicas.json", "r") as f:
                data = json.load(f)
                self.replicas = data.get("replicas", [])

    def find_leader(self):
        """
        Discover the current leader by querying all replicas.
        """
        self.load_replicas()
        backoff, max_backoff = 1.0, 10.0
        while True:
            for r in self.replicas:
                try:
                    channel = grpc.insecure_channel(f"{r['host']}:{r['port']}")
                    stub = blog_pb2_grpc.BlogStub(channel)
                    resp = stub.RPCGetLeaderInfo(blog_pb2.Request(), timeout=2.0)
                    if resp.operation == SUCCESS and resp.info:
                        leader_id = resp.info[0]
                        cfg = next((c for c in self.replicas if c['id'] == leader_id), None)
                        if cfg:
                            lch = grpc.insecure_channel(f"{cfg['host']}:{cfg['port']}")
                            self.leader_stub = blog_pb2_grpc.BlogStub(lch)
                            logging.info(f"Found leader: {leader_id}")
                            return True
                except Exception as e:
                    logging.debug(f"Error finding leader at {r['host']}:{r['port']}: {e}")
            logging.warning("Leader not found, retrying...")
            time.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

    def ensure_leader(self):
        """
        Ensure we have a valid leader_stub, re-discover if needed.
        """
        if not self.leader_stub:
            return self.find_leader()
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