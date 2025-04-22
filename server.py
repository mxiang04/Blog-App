from user import User
from message import Message
from datetime import datetime
import csv
import os
import threading
from protos import blog_pb2_grpc, blog_pb2
from util import hash_password
from consensus import REPLICAS, get_total_stubs
import time
import grpc
from filelock import FileLock
import uuid
import json


class Server(blog_pb2_grpc.BlogServicer):
    HEADER = 64
    FORMAT = "utf-8"
    HEART_BEAT_FREQUENCY = 1

    def __init__(self, replica):
        # all users and their associated data stored in the User object
        self.user_database = {}
        self.posts_database = {}
        self.replica = replica
        self.leader_id = min(replica["id"] for replica in REPLICAS)
        self.leader_port = [replica["port"] for replica in REPLICAS if replica["id"] == self.leader_id][0]
        self.leader_host = [replica["host"] for replica in REPLICAS if replica["id"] == self.leader_id][0]

        print(f"LEADER {self.leader_id}")

        self.replica_keys = [replica["id"] for replica in REPLICAS]
        self.election_lock = FileLock("election.lock")

        channel = grpc.insecure_channel(f"{self.replica['host']}:{self.replica['port']}")
        self.replica_stub = blog_pb2_grpc.BlogStub(channel)

        self.running = False
        self.REPLICA_STUBS = None

        time.sleep(3)

        self.start_heartbeat()
        self.load_data()

    def load_data(self):
        """
        Load users and posts from CSV files if they exist
        """
        # Load users
        if os.path.exists(self.replica["users_store"]):
            try:
                with open(self.replica["users_store"], "r", newline="") as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip header
                    for row in reader:
                        username, password = row
                        self.user_database[username] = User(username, password)
                print(
                    f"Loaded {len(self.user_database)} users from {self.replica['users_store']}"
                )
            except Exception as e:
                print(f"Error loading users: {e}")

        # Load posts
        if os.path.exists(self.replica["posts_store"]):
            try:
                with open(self.replica["posts_store"], "r", newline="") as file:
                    reader = csv.reader(file)
                    next(reader)  # Skip header
                    for row in reader:
                        post_id, author, title, content, timestamp_str, likes_str, comments_json = row
                        post = {
                            "post_id": post_id,
                            "author": author,
                            "title": title,
                            "content": content,
                            "timestamp": datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f"),
                            "likes": int(likes_str),
                            "comments": json.loads(comments_json)
                        }
                        self.posts_database[post_id] = post
                        # Add post to author's posts list
                        if author in self.user_database:
                            if not hasattr(self.user_database[author], 'posts'):
                                self.user_database[author].posts = []
                            self.user_database[author].posts.append(post_id)
                print(f"Loaded posts from {self.replica['posts_store']}")
            except Exception as e:
                print(f"Error loading posts: {e}")

    def save_data(self):
        """
        Save all user and post data to CSV files
        """
        # save users
        try:
            with open(self.replica["users_store"], "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["username", "password", "subscriptions", "followers"])
                for username, user in self.user_database.items():
                    subscriptions = json.dumps(getattr(user, 'subscriptions', []))
                    followers = json.dumps(getattr(user, 'followers', []))
                    writer.writerow([username, user.password, subscriptions, followers])
        except Exception as e:
            print(f"Error saving users: {e}")

        # save posts
        try:
            with open(self.replica["posts_store"], "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["post_id", "author", "title", "content", "timestamp", "likes", "comments"])
                for post_id, post in self.posts_database.items():
                    writer.writerow([
                        post_id,
                        post["author"],
                        post["title"],
                        post["content"],
                        post["timestamp"].isoformat(),
                        str(post["likes"]),
                        json.dumps(post["comments"])
                    ])
        except Exception as e:
            print(f"Error saving posts: {e}")

    def elect_leader(self):
        if self.leader_id in self.replica_keys:
            old_id = self.leader_id
            self.replica_keys.remove(old_id)

            self.leader_id = min(self.replica_keys)

            self.leader_port = [replica["port"] for replica in REPLICAS if replica["id"] == self.leader_id][0]
            self.leader_host = [replica["host"] for replica in REPLICAS if replica["id"] == self.leader_id][0]

            print(f"New leader values {self.leader_id}")
            self.notify_replicas_new_leader()

    def start_heartbeat(self):
        threading.Thread(target=self.send_beats, daemon=True).start()

    def servers_running(self):
        request = blog_pb2.HeartbeatRequest()
        while True:
            success = 0
            for replica_id in self.replica_keys:
                if replica_id == self.replica["id"]:
                    success += 1
                else:
                    try:
                        res = self.replica_stub.RPCHeartbeat(request)
                        if res == blog_pb2.SUCCESS:
                            success += 1
                    except grpc.RpcError as e:
                        print(f"Not all servers running - please wait {success}")
        # return success == len(REPLICAS)

    def notify_replicas_new_leader(self):
        request = blog_pb2.Request(
            info=[self.leader_id, self.leader_host, str(self.leader_port)]
        )
        for replica_id in self.replica_keys:
            print(f"Notifying these replicas {replica_id}")
            if replica_id == self.replica["id"]:
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
            if self.leader_id == self.replica["id"]:
                time.sleep(self.HEART_BEAT_FREQUENCY)
                continue
            if self.leader_port and self.leader_host and self.leader_id:
                try:
                    with grpc.insecure_channel(
                        f"{self.leader_host}:{self.leader_port}"
                    ) as channel:
                        stub = blog_pb2_grpc.BlogStub(channel)
                        response = stub.RPCHeartbeat(blog_pb2.Request())
                except grpc.RpcError as e:
                    # print(f"Before replica id {self.replica["id"]}")
                    if self.replica["id"] == max(self.replica_keys):
                        print(f"Highest replica id {self.replica['id']}")
                        # we implement a bully algorithm such that only the highest replica id gets to elect the leader
                        # with self.election_lock:
                        print(f"Heartbeat failed: {e}")
                        print(
                            f"Triggered by {self.replica['id']} with leader {self.leader_host}:{self.leader_port}"
                        )
                        self.elect_leader()
                        print("Elected new leader")
            time.sleep(self.HEART_BEAT_FREQUENCY)

    def RPCHeartbeat(self, request, context):
        return blog_pb2.Response(operation=blog_pb2.SUCCESS)

    def RPCUpdateLeader(self, request, context):
        print(f"updating leader {request.info} on {self.replica['id']}")
        if len(request.info) != 3:
            return blog_pb2.Response(
                operation=blog_pb2.FAILURE, info="Update Leader Request Failed"
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

        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")

    # BLOG SERVICE FUNCTIONS

    def RPCLogin(self, request, context):
        """
        Logs in the user if the username and password are correct.
        """
        if len(request.info) != 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing user/pw")
        
        username, password = request.info
        user = self.user_database.get(username)
        
        if user and user.password == password:
            notifications_count = len(getattr(user, 'unread_notifications', []))
            
            response = blog_pb2.Response(
                operation=blog_pb2.SUCCESS, info=f"{notifications_count}"
            )
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGIN")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            
            if self.leader_id == self.replica["id"]:
                self.BroadcastUpdate(request, "RPCLogin")
                
            return response
        else:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Login Failed")

    def RPCCreateAccount(self, request, context):
        """
        Creates an account if the username and password are not taken.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) != 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Create Account Request Invalid")
        
        email, password = request.info
        
        # Check if the username is taken
        if email in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Create Account Failed")
        # Check if the username and password are not empty
        elif not email or not password:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Create Account Failed")
        # Create the account
        else:
            user = User(email, password)
            user.posts = []
            user.subscriptions = []
            user.followers = []
            user.unread_notifications = []
            self.user_database[email] = user
            
            # Save changes to CSV
            self.save_data()

            response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: CREATE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            
            if self.leader_id == self.replica["id"]:
                ret = self.BroadcastUpdate(request, "RPCCreateAccount")
                if not ret:
                    return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Broadcast Not Successful")
            
            return response

    def RPCLogout(self, request, context):
        """
        Logs out the user.
        """
        if len(request.info) != 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing user")
        
        username = request.info[0]
        if username in self.user_database:
            response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
            response_size = response.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGOUT")
            print(f"SERIALIZED DATA LENGTH: {response_size}")
            print("--------------------------------")
            
            if self.leader_id == self.replica["id"]:
                self.BroadcastUpdate(request, "RPCLogout")
                
            return response
        return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not an account")

    def RPCSearchUsers(self, request, context):
        """
        Lists all accounts that start with the search string.
        """
        if len(request.info) != 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing prefix")
        
        prefix = request.info[0]
        matching_users = [
            username
            for username in self.user_database.keys()
            if username.startswith(prefix)
        ]
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=matching_users)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: SEARCH USERS")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCSearchUsers")
            
        return response

    def RPCCreatePost(self, request, context):
        """
        Creates a new blog post.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) < 3:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing author/title/content")
        
        author, title, content = request.info
        if author not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User does not exist")
        
        timestamp = datetime.now()
        post_id = str(uuid.uuid4())
        
        post = {
            "post_id": post_id,
            "author": author,
            "title": title,
            "content": content,
            "timestamp": timestamp,
            "likes": 0,
            "comments": []
        }
        
        self.posts_database[post_id] = post
        
        if not hasattr(self.user_database[author], 'posts'):
            self.user_database[author].posts = []
        self.user_database[author].posts.append(post_id)
        
        # Notify followers
        for username, user in self.user_database.items():
            if hasattr(user, 'subscriptions') and author in user.subscriptions:
                if not hasattr(user, 'unread_notifications'):
                    user.unread_notifications = []
                notification = {
                    "type": "new_post",
                    "from": author,
                    "post_id": post_id,
                    "title": title,
                    "timestamp": timestamp.isoformat()
                }
                user.unread_notifications.append(notification)
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=post_id)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: CREATE POST")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCCreatePost")
            
        return response

    def RPCLikePost(self, request, context):
        """
        Adds a like to a post.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing post_id/username")
        
        post_id, username = request.info
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Post does not exist")
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User does not exist")
        
        # Increment like count
        self.posts_database[post_id]["likes"] += 1
        
        # Notify post author
        post_author = self.posts_database[post_id]["author"]
        if post_author != username:  # Don't notify if liking own post
            if not hasattr(self.user_database[post_author], 'unread_notifications'):
                self.user_database[post_author].unread_notifications = []
            notification = {
                "type": "like",
                "from": username,
                "post_id": post_id,
                "title": self.posts_database[post_id]["title"],
                "timestamp": datetime.now().isoformat()
            }
            self.user_database[post_author].unread_notifications.append(notification)
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: LIKE POST")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCLikePost")
            
        return response

    def RPCSubscribe(self, request, context):
        """
        Subscribes a user to another user's posts.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing follower/followed")
        
        follower, followed = request.info
        if follower not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Follower does not exist")
        if followed not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Followed user does not exist")
        
        # Add to follower's subscriptions
        if not hasattr(self.user_database[follower], 'subscriptions'):
            self.user_database[follower].subscriptions = []
        if followed not in self.user_database[follower].subscriptions:
            self.user_database[follower].subscriptions.append(followed)
        
        # Add to followed's followers
        if not hasattr(self.user_database[followed], 'followers'):
            self.user_database[followed].followers = []
        if follower not in self.user_database[followed].followers:
            self.user_database[followed].followers.append(follower)
        
        # Notify followed user
        if not hasattr(self.user_database[followed], 'unread_notifications'):
            self.user_database[followed].unread_notifications = []
        notification = {
            "type": "new_follower",
            "from": follower,
            "timestamp": datetime.now().isoformat()
        }
        self.user_database[followed].unread_notifications.append(notification)
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: SUBSCRIBE")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCSubscribe")
            
        return response

    def RPCUnsubscribe(self, request, context):
        """
        Unsubscribes a user from another user's posts.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing follower/followed")
        
        follower, followed = request.info
        if follower not in self.user_database or followed not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User does not exist")
        
        # Remove from follower's subscriptions
        if hasattr(self.user_database[follower], 'subscriptions') and followed in self.user_database[follower].subscriptions:
            self.user_database[follower].subscriptions.remove(followed)
        
        # Remove from followed's followers
        if hasattr(self.user_database[followed], 'followers') and follower in self.user_database[followed].followers:
            self.user_database[followed].followers.remove(follower)
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: UNSUBSCRIBE")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCUnsubscribe")
            
        return response

    def RPCDeletePost(self, request, context):
        """
        Deletes a post if the user is the author.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) < 2:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing post_id/author")
        
        post_id, author = request.info
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Post does not exist")
        if author != self.posts_database[post_id]["author"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not post owner")
        
        # Remove from author's posts
        if hasattr(self.user_database[author], 'posts') and post_id in self.user_database[author].posts:
            self.user_database[author].posts.remove(post_id)
        
        # Remove the post
        del self.posts_database[post_id]
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: DELETE POST")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCDeletePost")
            
        return response

    def RPCDeleteAccount(self, request, context):
        """
        Deletes an account from the user database.
        """
        if self.leader_id != self.replica["id"]:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Not leader")
            
        if len(request.info) != 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing username")
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User does not exist")
        
        # Remove all posts by this user
        if hasattr(self.user_database[username], 'posts'):
            for post_id in list(self.user_database[username].posts):
                if post_id in self.posts_database:
                    del self.posts_database[post_id]
        
        # Remove from subscriptions and followers lists
        for user in self.user_database.values():
            if hasattr(user, 'subscriptions') and username in user.subscriptions:
                user.subscriptions.remove(username)
            if hasattr(user, 'followers') and username in user.followers:
                user.followers.remove(username)
        
        # Delete the user
        del self.user_database[username]
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info="")
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: DELETE ACCOUNT")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        if self.leader_id == self.replica["id"]:
            self.BroadcastUpdate(request, "RPCDeleteAccount")
            
        return response

    def RPCGetPost(self, request, context):
        """
        Gets a specific post by ID.
        """
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing post_id")
        
        post_id = request.info[0]
        if post_id not in self.posts_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Post not found")
        
        post = self.posts_database[post_id]
        
        post_info = [
            post["post_id"],
            post["author"],
            post["title"],
            post["content"],
            post["timestamp"].isoformat(),
            str(post["likes"]),
            json.dumps(post["comments"])
        ]
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=post_info)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: GET POST")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        return response

    def RPCGetUserPosts(self, request, context):
        """
        Gets all posts by a specific user.
        """
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing username")
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User not found")
        
        user_posts = []
        if hasattr(self.user_database[username], 'posts'):
            for post_id in self.user_database[username].posts:
                if post_id in self.posts_database:
                    post = self.posts_database[post_id]
                    post_summary = f"{post_id}|{post['title']}|{post['timestamp'].isoformat()}|{post['likes']}"
                    user_posts.append(post_summary)
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=user_posts)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: GET USER POSTS")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        return response

    def RPCGetNotifications(self, request, context):
        """
        Gets the unread notifications for a user.
        """
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing username")
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User not found")
        
        if not hasattr(self.user_database[username], 'unread_notifications'):
            self.user_database[username].unread_notifications = []
        
        notifications = self.user_database[username].unread_notifications
        notification_strings = [json.dumps(n) for n in notifications]
        
        # Clear unread notifications
        self.user_database[username].unread_notifications = []
        
        # Save changes to CSV
        self.save_data()
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=notification_strings)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: GET NOTIFICATIONS")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        return response

    def RPCGetSubscriptions(self, request, context):
        """
        Gets the subscriptions for a user.
        """
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="Missing username")
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info="User not found")
        
        if not hasattr(self.user_database[username], 'subscriptions'):
            self.user_database[username].subscriptions = []
        
        subscriptions = self.user_database[username].subscriptions
        
        response = blog_pb2.Response(operation=blog_pb2.SUCCESS, info=subscriptions)
        response_size = response.ByteSize()
        print("--------------------------------")
        print(f"OPERATION: GET SUBSCRIPTIONS")
        print(f"SERIALIZED DATA LENGTH: {response_size}")
        print("--------------------------------")
        
        return response

    def RPCGetUserFollowers(self, request, context):
        if len(request.info) < 1:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["Missing username"])
        
        username = request.info[0]
        if username not in self.user_database:
            return blog_pb2.Response(operation=blog_pb2.FAILURE, info=["User not found"])
        
        followers = self.user_database[username].followers
        
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=followers)

    def RPCGetLeaderInfo(self, request, context):
        return blog_pb2.Response(operation=blog_pb2.SUCCESS, info=f"{self.leader_id}")

    def BroadcastUpdate(self, request, method):
        if not self.leader_id == self.replica["id"]:
            return
        success_count = 0
        for backup_replica_id in self.replica_keys:
            if not self.REPLICA_STUBS:
                self.REPLICA_STUBS = get_total_stubs()
            if backup_replica_id == self.replica["id"]:
                success_count += 1
                continue
            backup_stub = self.REPLICA_STUBS[backup_replica_id]
            rpc_method = getattr(backup_stub, method, None)
            print(f"RPC METHOD {rpc_method}")
            if rpc_method:
                try:
                    print(f"Broadcasting {request}")
                    res = rpc_method(request)
                    status = res.operation
                    if status == blog_pb2.SUCCESS:
                        success_count += 1
                except grpc.RpcError as e:
                    self.replica_keys.remove(backup_replica_id)
                    print(f"{backup_replica_id} removed from list of replicas")
        if success_count > len(self.replica_keys) - 1:
            print("broadcast success")
            return True
        return False
