import csv
import os
import random
import time
import threading
import grpc
import json
import logging
from datetime import datetime
import uuid
from email_validator import validate_email, EmailNotValidError
from email_queue import email_worker
from dotenv import load_dotenv

from protos import blog_pb2, blog_pb2_grpc
from user import User
from post import Post
from util import hash_password
from consensus import (
    RaftNode,
    build_stub,
    get_replicas_config,
    RaftLogEntry
)

SUCCESS = 0
FAILURE = 1
load_dotenv()

class Server(blog_pb2_grpc.BlogServicer):
    ELECTION_TIMEOUT = random.uniform(3.0, 5.0)
    HEARTBEAT_INTERVAL = 1.5

    def __init__(self, replica_config):
        self.replica_id = replica_config["id"]
        self.host = replica_config["host"]
        self.port = replica_config["port"]
        self.raft_store = replica_config["raft_store"]
        self.posts_store = replica_config["posts_store"]
        self.users_store = replica_config["users_store"]
        self.subscriptions_store = replica_config["subscriptions_store"]

        # Blog data
        self.user_database = {}
        self.posts_database = {}  # post_id -> Post

        # Build Raft
        self.raft_node = RaftNode(self.replica_id, self.raft_store)

        self._stubs_cache = {}
        self.replicas_config = get_replicas_config()
        for r in self.replicas_config:
            rid = r["id"]
            if rid != self.replica_id:
                self.raft_node.nextIndex[rid] = len(self.raft_node.log) + 1
                self.raft_node.matchIndex[rid] = 0

        # Load data
        self.load_data()
        self.raft_node.lastApplied = self.raft_node.commitIndex
        
        # Reset votedFor to break deadlock
        self.raft_node.votedFor = None
        self.raft_node.save_raft_state()
        
        # Start background
        self.stop_flag = False
        self.election_timer = None
        self.heartbeat_timer = None
        self.reset_election_timer()

        # Start email worker
        email_worker.start()

    def get_cluster_stubs(self):
        # Refresh stubs for replicas that might have restarted
        for cfg in self.replicas_config:
            rid = cfg["id"]
            if rid != self.replica_id:
                # Check if we need to refresh this connection
                need_refresh = False
            
                if rid not in self._stubs_cache:
                    need_refresh = True
                else:
                    # Try a lightweight health check
                    try:
                        # Quick non-blocking check using gRPC channel state
                        channel = self._stubs_cache[rid]._channel
                        if channel.check_connectivity_state(True) in (
                            grpc.ChannelConnectivity.TRANSIENT_FAILURE,
                            grpc.ChannelConnectivity.SHUTDOWN
                        ):
                            need_refresh = True
                    except:
                        need_refresh = True
                
                if need_refresh:
                    try:
                        # Create a fresh connection with a short timeout
                        channel = grpc.insecure_channel(
                            f"{cfg['host']}:{cfg['port']}",
                            options=[
                                ('grpc.enable_retries', 0),
                                ('grpc.keepalive_time_ms', 5000),
                                ('grpc.keepalive_timeout_ms', 1000)
                            ]
                        )
                        self._stubs_cache[rid] = blog_pb2_grpc.BlogStub(channel)
                        logging.info(f"Refreshed connection to replica {rid}")
                    except Exception as e:
                        logging.error(f"Failed to refresh connection to replica {rid}: {e}")
        
        return self._stubs_cache

    def reset_election_timer(self):
        # cancel any existing timer
        if self.election_timer:
            self.election_timer.cancel()

        # pick a uniform random timeout between 3 and 5 seconds
        timeout = random.uniform(3.0, 5.0)
        self.ELECTION_TIMEOUT = timeout
        logging.info(f"Election timeout set to {timeout:.2f}s")

        # when it fires, try to become candidate
        self.election_timer = threading.Timer(timeout, self.become_candidate)
        self.election_timer.start()

    def reset_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(self.HEARTBEAT_INTERVAL, self.leader_heartbeat)
        self.heartbeat_timer.start()

    def stop(self):
        self.stop_flag = True
        if self.election_timer:
            self.election_timer.cancel()
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            
        email_worker.stop()

    # --------------------------------------------------------------------------
    # Raft roles - unchanged from original implementation
    # --------------------------------------------------------------------------
    def become_candidate(self):
        if self.raft_node.role == "leader":
            return
        self.raft_node.role = "candidate"
        self.raft_node.currentTerm += 1
        self.raft_node.votedFor = self.replica_id
        self.raft_node.save_raft_state()

        votes_granted = 1
        cluster_stubs = self.get_cluster_stubs()
        self.reset_election_timer()

        def request_vote_async(stub):
            try:
                req = blog_pb2.Request(
                    term=self.raft_node.currentTerm,
                    candidateId=self.replica_id,
                    lastLogIndex=self.raft_node.last_log_index(),
                    lastLogTerm=self.raft_node.last_log_term()
                )
                return stub.RequestVote(req, timeout=2.0)
            except:
                return None

        threads = []
        results = []
        for rid, stub in cluster_stubs.items():
            t = threading.Thread(target=lambda stub=stub: results.append(request_vote_async(stub)))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        currentTerm = self.raft_node.currentTerm
        for r in results:
            if not r:
                continue
            if r.term > currentTerm:
                self.raft_node.role = "follower"
                self.raft_node.currentTerm = r.term
                self.raft_node.votedFor = None
                self.raft_node.save_raft_state()
                return
            if r.voteGranted:
                votes_granted += 1

        majority = (len(self.replicas_config)//2)+1
        if votes_granted >= majority:
            self.become_leader()
        
        else:
            self.reset_election_timer()

    def become_leader(self):
        self.raft_node.role = "leader"
        for r in self.replicas_config:
            rid = r["id"]
            if rid != self.replica_id:
                self.raft_node.nextIndex[rid] = len(self.raft_node.log) + 1
                self.raft_node.matchIndex[rid] = 0
        self.reset_heartbeat_timer()

    def leader_heartbeat(self):
        if self.raft_node.role != "leader":
            return

        self.check_leader_status()
        if self.raft_node.role == "leader":
            self.send_append_entries_to_all()
            self.reset_heartbeat_timer()
         
    def check_leader_status(self):
        if self.raft_node.role != "leader":
            return

        # Count ourselves
        reachable = 1

    # Get fresh stubs for all followers
        stubs = self.get_cluster_stubs()

        # Build an empty AppendEntries (heartbeat) request
        hb_req = blog_pb2.Request(
            term=self.raft_node.currentTerm,
            leaderId=self.replica_id,
            prevLogIndex=self.raft_node.last_log_index(),
            prevLogTerm=self.raft_node.last_log_term(),
            leaderCommit=self.raft_node.commitIndex,
            entries=[],
        )

        # Ping each follower
        for rid, stub in stubs.items():
            try:
                resp = stub.AppendEntries(hb_req, timeout=0.5)
                # If they recognize our term, they’re alive
                if resp.term == self.raft_node.currentTerm:
                    reachable += 1
            except Exception:
                # RPC failed or stub unreachable → skip
                pass

        # Demote if we’ve lost majority
        majority = (len(self.replicas_config) // 2) + 1
        if reachable < majority:
            logging.warn(f"Leader stepping down: only {reachable}/{len(self.replicas_config)} live")
            self.raft_node.role = "follower"
        self.reset_election_timer()


    def send_append_entries_to_all(self):
        cstubs = self.get_cluster_stubs()
        term = self.raft_node.currentTerm
        
        # Use a thread pool to send AppendEntries in parallel
        threads = []
        for rid, stub in cstubs.items():
            nxt = self.raft_node.nextIndex[rid]
            prevLogIndex = nxt - 1
            prevLogTerm = 0
            if prevLogIndex > 0 and prevLogIndex <= len(self.raft_node.log):
                prevLogTerm = self.raft_node.log[prevLogIndex-1].term
            entries = []
            if nxt <= len(self.raft_node.log):
                for e in self.raft_node.log[nxt-1:]:
                    entries.append(
                        blog_pb2.RaftLogEntry(term=e.term, operation=e.operation, params=e.params)
                    )
            req = blog_pb2.Request(
                term=term,
                leaderId=self.replica_id,
                prevLogIndex=prevLogIndex,
                prevLogTerm=prevLogTerm,
                leaderCommit=self.raft_node.commitIndex,
                entries=entries
            )
            t = threading.Thread(target=self.append_entries_async, args=(stub, req, rid))
            t.start()
            threads.append(t)
        
        # Wait for all threads to complete
        for t in threads:
            t.join(timeout=1.0)  # Add timeout to prevent blocking indefinitely

    def append_entries_async(self, stub, req, followerId):
        try:
            resp = stub.AppendEntries(req, timeout=2.0)
        except Exception as e:
            logging.error(f"AppendEntries RPC to {followerId} failed: {e}")
            return
        self.handle_append_entries_response(resp, followerId, req)


    def handle_append_entries_response(self, resp, followerId, req):
        if not resp:
            return
        if resp.term > self.raft_node.currentTerm:
            self.raft_node.role = "follower"
            self.raft_node.currentTerm = resp.term
            self.raft_node.votedFor = None
            self.raft_node.save_raft_state()
            return
        if self.raft_node.role != "leader":
            return
        if resp.success:
            appended_count = len(req.entries)
            if appended_count > 0:
                self.raft_node.nextIndex[followerId] += appended_count
                self.raft_node.matchIndex[followerId] = self.raft_node.nextIndex[followerId] - 1
            
            # Update commitIndex based on matchIndex values
            for n in range(self.raft_node.commitIndex + 1, len(self.raft_node.log) + 1):
                # Only consider entries from current term
                if n > 0 and self.raft_node.log[n-1].term == self.raft_node.currentTerm:
                    count = 1  # Count ourselves
                    for rid in self.raft_node.matchIndex:
                        if self.raft_node.matchIndex[rid] >= n:
                            count += 1
                    if count >= ((len(self.replicas_config)//2) + 1):
                        self.raft_node.commitIndex = n
                        break
            self.apply_committed_entries()
        else:
            # On failure, decrement nextIndex and retry
            if self.raft_node.nextIndex[followerId] > 1:
                self.raft_node.nextIndex[followerId] -= 1
            # Don't reset to 1 immediately - backtrack gradually
            return

    def apply_committed_entries(self):
        while self.raft_node.lastApplied<self.raft_node.commitIndex:
            self.raft_node.lastApplied += 1
            entry = self.raft_node.log[self.raft_node.lastApplied-1]
            self.apply_blog_operation(entry)
        self.save_data()

    # --------------------------------------------------------------------------
    # Data Loading and Saving
    # --------------------------------------------------------------------------
    def load_data(self):
        # Load users
        if os.path.exists(self.users_store):
            try:
                with open(self.users_store, "r", newline="") as f:
                    rd = csv.reader(f)
                    next(rd)  # Skip header
                    for row in rd:
                        uname, pw, email = row
                        self.user_database[uname] = User(uname, pw)
                        self.user_database[uname].email = email
            except Exception as e:
                logging.error(f"Error loading users: {e}")
        
        # Load subscriptions
        if os.path.exists(self.subscriptions_store):
            try:
                with open(self.subscriptions_store, "r", newline="") as f:
                    rd = csv.reader(f)
                    next(rd)  # Skip header
                    for row in rd:
                        follower, followed = row
                        if follower in self.user_database:
                            if followed not in self.user_database[follower].subscriptions:
                                self.user_database[follower].subscriptions.append(followed)
                        if followed in self.user_database:
                            if follower not in self.user_database[followed].followers:
                                self.user_database[followed].followers.append(follower)
            except Exception as e:
                logging.error(f"Error loading subscriptions: {e}")
        
        # Load posts
        if os.path.exists(self.posts_store):
            try:
                with open(self.posts_store, "r") as f:
                    posts_data = json.load(f)
                    for post_id, post_data in posts_data.items():
                        post = Post.from_dict(post_data)
                        post.post_id = post_id
                        self.posts_database[post_id] = post
                        # Add to author's posts
                        author = post.author
                        if author in self.user_database:
                            self.user_database[author].posts.append(post_id)
            except Exception as e:
                logging.error(f"Error loading posts: {e}")

    def save_data(self):
        # Save users
        try:
            with open(self.users_store, "w", newline="") as f:
                wr = csv.writer(f)
                wr.writerow(["username", "password", "email"])
                for username, user_obj in self.user_database.items():
                    wr.writerow([username, user_obj.password, user_obj.email or ""])
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error(f"Error saving users data: {e}")

        # Save subscriptions
        try:
            with open(self.subscriptions_store, "w", newline="") as f:
                wr = csv.writer(f)
                wr.writerow(["follower", "followed"])
                for username, user_obj in self.user_database.items():
                    for subscribed_to in user_obj.subscriptions:
                        wr.writerow([username, subscribed_to])
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error(f"Error saving subscriptions data: {e}")
        
        # Save posts
        try:
            posts_data = {}
            for post_id, post in self.posts_database.items():
                posts_data[post_id] = post.to_dict()
            
            with open(self.posts_store, "w") as f:
                json.dump(posts_data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error(f"Error saving posts data: {e}")

    # --------------------------------------------------------------------------
    # Blog operations application
    # --------------------------------------------------------------------------
    def apply_blog_operation(self, entry: RaftLogEntry):
        op = entry.operation
        params = entry.params
        
        if op == "CREATE_ACCOUNT":
            if len(params) != 2:
                return
            email, pw = params
            if email not in self.user_database:
                self.user_database[email] = User(email, pw)
                self.user_database[email].email = email
                
        elif op == "CREATE_POST":
            if len(params) < 5:
                return
            author, title, content, timestamp_str, post_id = params
            if author not in self.user_database:
                return
                
            post = Post(author, title, content)
            post.timestamp = datetime.fromisoformat(timestamp_str)
            post.post_id = post_id
            
            self.posts_database[post_id] = post
            
            if post_id not in self.user_database[author].posts:
                self.user_database[author].posts.append(post_id)
                
        elif op == "LIKE_POST":
            if len(params) < 2:
                return
            post_id, username = params
            if post_id in self.posts_database and username in self.user_database:
                self.posts_database[post_id].like(username)
                
        elif op == "UNLIKE_POST":
            if len(params) < 2:
                return
            post_id, username = params
            if post_id in self.posts_database and username in self.user_database:
                self.posts_database[post_id].unlike(username)
                
        elif op == "SUBSCRIBE":
            if len(params) < 2:
                return
            follower, followed = params
            if follower in self.user_database and followed in self.user_database:
                if followed not in self.user_database[follower].subscriptions:
                    self.user_database[follower].subscriptions.append(followed)
                if follower not in self.user_database[followed].followers:
                    self.user_database[followed].followers.append(follower)
                    
        elif op == "UNSUBSCRIBE":
            if len(params) < 2:
                return
            follower, followed = params
            if follower in self.user_database and followed in self.user_database:
                if followed in self.user_database[follower].subscriptions:
                    self.user_database[follower].subscriptions.remove(followed)
                if follower in self.user_database[followed].followers:
                    self.user_database[followed].followers.remove(follower)
                    
        elif op == "ADD_COMMENT":
            if len(params) < 4:
                return
            post_id, author, comment, timestamp_str = params
            if post_id in self.posts_database and author in self.user_database:
                comment_obj = {
                    "author": author,
                    "content": comment,
                    "timestamp": timestamp_str
                }
                self.posts_database[post_id].comments.append(comment_obj)
                
        elif op == "DELETE_POST":
            if len(params) < 2:
                return
            post_id, author = params
            if post_id in self.posts_database and author == self.posts_database[post_id].author:
                if post_id in self.user_database[author].posts:
                    self.user_database[author].posts.remove(post_id)
                del self.posts_database[post_id]
                
        elif op == "DELETE_ACCOUNT":
            if len(params) < 1:
                return
            username = params[0]
            if username in self.user_database:
                # Remove user from followers/following lists
                for user in self.user_database.values():
                    if username in user.followers:
                        user.followers.remove(username)
                    if username in user.subscriptions:
                        user.subscriptions.remove(username)
                
                # Remove user's posts
                for post_id in list(self.user_database[username].posts):
                    if post_id in self.posts_database:
                        del self.posts_database[post_id]
                
                # Remove user
                del self.user_database[username]
        elif op == "ADD_REPLICA":
            cfg_str = params[0]
            new_cfg = json.loads(cfg_str)
            self.add_replica_local(new_cfg)
        elif op == "REMOVE_REPLICA":
            rid = params[0]
            self.remove_replica_local(rid)

    def notify_followers_of_new_post(self, author, post):
        if self.raft_node.role != "leader":
            return
        
        followers = self.user_database[author].followers
        for follower in followers:
            if follower in self.user_database:
                subject = f"New Post from {author}: {post.title}"
                content = f"""
                {author} has published a new post:
                
                {post.title}
                
                {post.content[:200]}{'...' if len(post.content) > 200 else ''}
                
                View the full post on our platform.
                """
                # Queue the email
                email_worker.queue_email(
                    author,
                    follower,
                    subject,
                    content
                )

    def add_replica_local(self, new_cfg):
        arr = get_replicas_config()
        found = any(r["id"] == new_cfg["id"] for r in arr)
        if not found:
            arr.append(new_cfg)
            with open("replicas.json", "w") as f:
                json.dump({"replicas": arr}, f, indent=2)
        self._stubs_cache = {}
        self.replicas_config = arr
        self.raft_node.nextIndex[new_cfg["id"]] = len(self.raft_node.log) + 1
        self.raft_node.matchIndex[new_cfg["id"]] = 0

    def remove_replica_local(self, rid):
        arr = get_replicas_config()
        updated = [r for r in arr if r["id"] != rid]
        with open("replicas.json", "w") as f:
            json.dump({"replicas": updated}, f, indent=2)

        # Remove from stubs
        if rid in self._stubs_cache:
            del self._stubs_cache[rid]
        # Remove from nextIndex, matchIndex
        if rid in self.raft_node.nextIndex:
            del self.raft_node.nextIndex[rid]
        if rid in self.raft_node.matchIndex:
            del self.raft_node.matchIndex[rid]

        self.replicas_config = updated

    # --------------------------------------------------------------------------
    # Replication
    # --------------------------------------------------------------------------
    def replicate_command(self, op, params):
        if self.raft_node.role != "leader":
            return FAILURE
        e = RaftLogEntry(self.raft_node.currentTerm, op, params)
        self.raft_node.log.append(e)
        self.raft_node.save_raft_state()

        # --- IMMEDIATELY COMMIT ON THE LEADER ---
        self.raft_node.commitIndex = len(self.raft_node.log)
        self.apply_committed_entries()

        # Then push out AppendEntries (including the new commitIndex)
        self.send_append_entries_to_all()
        return SUCCESS


    # --------------------------------------------------------------------------
    # Raft RPC
    # --------------------------------------------------------------------------
    # RequestVote RPC - Unchanged from original
    def RequestVote(self, request, context):
        term = request.term
        candId = request.candidateId
        lastLogIndex = request.lastLogIndex
        lastLogTerm = request.lastLogTerm

        # If our term is higher, reject immediately
        if term < self.raft_node.currentTerm:
            return blog_pb2.Response(term=self.raft_node.currentTerm, voteGranted=False)

        # Update term if needed
        if term > self.raft_node.currentTerm:
            self.raft_node.currentTerm = term
            self.raft_node.votedFor = None
            self.raft_node.role = "follower"
            self.raft_node.save_raft_state()

        # Check if we've already voted in this term
        already_voted = self.raft_node.votedFor is not None and self.raft_node.votedFor != candId
        
        # Grant vote if we haven't voted yet and candidate's log is at least as up-to-date as ours
        log_is_current = (lastLogTerm > self.raft_node.last_log_term() or 
                        (lastLogTerm == self.raft_node.last_log_term() and 
                        lastLogIndex >= self.raft_node.last_log_index()))
        
        if not already_voted and log_is_current:
            self.raft_node.votedFor = candId
            self.raft_node.save_raft_state()
            self.reset_election_timer()  # Reset timer when granting vote
            return blog_pb2.Response(term=self.raft_node.currentTerm, voteGranted=True)
        
        return blog_pb2.Response(term=self.raft_node.currentTerm, voteGranted=False)

    # AppendEntries RPC - Unchanged from original
    def AppendEntries(self, request, context):
        if request.term < self.raft_node.currentTerm:
            return blog_pb2.Response(term=self.raft_node.currentTerm, success=False)

        # If the leader's term is higher, update and persist the new term information.
        if request.term > self.raft_node.currentTerm:
            self.raft_node.role = "follower"
            self.raft_node.currentTerm = request.term
            self.raft_node.votedFor = None
            self.raft_node.save_raft_state()  # Force write new term/vote info to disk

        self.reset_election_timer()
        
        prevLogIndex = request.prevLogIndex
        prevLogTerm = request.prevLogTerm

        # Convert incoming entries
        new_entries = [RaftLogEntry(e.term, e.operation, list(e.params))
                        for e in request.entries]

        log_updated = False
        
        # If a follower is completely empty (prevLogIndex == 0),
        # just overwrite its log in one shot.
        if prevLogIndex == 0:
            self.raft_node.log = list(new_entries)
            log_updated = True
            success = True
        else:
            success = self.raft_node.append_entries_to_log(prevLogIndex, prevLogTerm, new_entries)
            if success and new_entries:  # If we actually appended something
                log_updated = True

        if not success:
            return blog_pb2.Response(term=self.raft_node.currentTerm, success=False)

        # Update commit index based on leaderCommit.
        commit_index_changed = False
        if request.leaderCommit > self.raft_node.commitIndex:
            lastNew = len(self.raft_node.log)
            old_commit_index = self.raft_node.commitIndex
            self.raft_node.commitIndex = min(request.leaderCommit, lastNew)
            commit_index_changed = (self.raft_node.commitIndex > old_commit_index)
            
        # Apply any new committed entries and update lastApplied
        if commit_index_changed:
            self.apply_committed_entries()
        
        # After making in-memory updates, write the new Raft state to the JSON file.
        self.raft_node.save_raft_state()
        
        # If the log was updated (either by complete replacement or append), 
        # ensure we save to persistent storage even if no entries were applied yet
        if log_updated and not commit_index_changed and new_entries:
            # This saves data even if the commitIndex hasn't changed yet
            # but we received new log entries that will eventually be committed
            self.save_data()

        return blog_pb2.Response(term=self.raft_node.currentTerm, success=True)

    def RPCGetLeaderInfo(self, request, context):
        if self.raft_node.role == "leader":
            return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=[self.replica_id])
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=[])

    # --------------------------------------------------------------------------
    # Blog RPCs
    # --------------------------------------------------------------------------
    def RPCLogin(self, request, context):
        if len(request.info) != 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing user/pw"])
        uname, pw = request.info
        user = self.user_database.get(uname)
        if user and user.password == pw:
            notifications_count = len(user.unread_notifications)
            return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=[str(notifications_count)])
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Login failed"])
    
    def RPCLogout(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing user"])
        username = request.info[0]
        if username in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not an account"])

    def RPCCreateAccount(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        
        if len(request.info) != 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing user/pw/email"])
        email, pwhash = request.info
        try:
            validate_email(email)
            # Email is valid
        except EmailNotValidError:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Username must be a valid email"])

        if email in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Username already taken"])
        
        op = "CREATE_ACCOUNT"
        params = [email, pwhash]
        res = self.replicate_command(op, params)
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCSearchUsers(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing prefix"])
        prefix = request.info[0]
        
        matching_users = []
        for username in self.user_database:
            if username.startswith(prefix):
                matching_users.append(username)
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=matching_users)

    def RPCCreatePost(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])

        if len(request.info) < 3:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing author/title/content"])
        
        author, title, content = request.info
        if author not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User does not exist"])
        
        timestamp = datetime.now().isoformat()
        post_id = str(uuid.uuid4())  # Assuming uuid is imported
        
        op = "CREATE_POST"
        params = [author, title, content, timestamp, post_id]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            post = Post(author, title, content)
            post.timestamp = datetime.fromisoformat(timestamp)
            post.post_id = post_id
            # Notify followers via email
            self.notify_followers_of_new_post(author, post)
            return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=[post_id])
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCLikePost(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing post_id/username"])
        
        post_id, username = request.info
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post does not exist"])
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User does not exist"])
        
        if username in self.posts_database[post_id].likes:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post already liked"])
        
        op = "LIKE_POST"
        params = [post_id, username]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    
    def RPCUnlikePost(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing post_id/username"])
        
        post_id, username = request.info
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post does not exist"])
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User does not exist"])
        
        # Check if user has liked the post
        if username not in self.posts_database[post_id].likes:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post not liked"])
        
        op = "UNLIKE_POST"
        params = [post_id, username]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCSubscribe(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing follower/followed"])
        
        follower, followed = request.info
        if follower not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Follower does not exist"])
        if followed not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Followed user does not exist"])
        
        op = "SUBSCRIBE"
        params = [follower, followed]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCUnsubscribe(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing follower/followed"])
        
        follower, followed = request.info
        if follower not in self.user_database or followed not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User does not exist"])
        
        op = "UNSUBSCRIBE"
        params = [follower, followed]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCDeletePost(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing post_id/author"])
        
        post_id, author = request.info
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post does not exist"])
        if author != self.posts_database[post_id].author:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not post owner"])
        
        op = "DELETE_POST"
        params = [post_id, author]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCDeleteAccount(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User does not exist"])
        
        op = "DELETE_ACCOUNT"
        params = [username]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCGetPost(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing post_id"])
        
        post_id = request.info[0]
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Post not found"])
        
        post = self.posts_database[post_id]
        # Convert post to serializable format
        post_info = [blog_pb2.Post(post_id=post.post_id, author=post.author, title=post.title, content=post.content, timestamp=str(post.timestamp), likes=post.likes)]
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, posts=post_info)

    def RPCGetUserPosts(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User not found"])
        
        user_posts = []
        for post_id in self.user_database[username].posts:
            if post_id in self.posts_database:
                post = self.posts_database[post_id]
                rpc_post = blog_pb2.Post(post_id=post_id, author=username, title=post.title, content=post.content, timestamp=str(post.timestamp), likes=post.likes)
                user_posts.append(rpc_post)
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, posts=user_posts)

    def RPCGetNotifications(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User not found"])
        
        notifications = self.user_database[username].unread_notifications
        # Convert notifications to strings
        notification_strings = [json.dumps(n) for n in notifications]
        
        # Clear unread notifications
        self.user_database[username].unread_notifications = []
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, notifications=notification_strings)

    def RPCGetSubscriptions(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User not found"])
        
        subscriptions = self.user_database[username].subscriptions
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=subscriptions)

    def RPCGetUserFollowers(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User not found"])
        
        followers = self.user_database[username].followers
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=followers)

    def RPCAddReplica(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing replica config"])
        
        cfg_str = request.info[0]
        op = "ADD_REPLICA"
        params = [cfg_str]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])

    def RPCRemoveReplica(self, request, context):
        if self.raft_node.role != "leader":
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Not leader"])
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing replica ID"])
        
        replica_id = request.info[0]
        op = "REMOVE_REPLICA"
        params = [replica_id]
        res = self.replicate_command(op, params)
        
        if res == SUCCESS:
            return blog_pb2.Response(operation=blog_pb2.SUCCESS)
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Could not replicate"])