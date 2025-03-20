import unittest
import os
import tempfile
from unittest.mock import patch, mock_open
from concurrent import futures

from protos import app_pb2_grpc, app_pb2
from server import (
    Server,
    serialize_data,
    deserialize_data,
    get_normalized_msg_id,
    User,
    Message,
)
from consensus import Replica


class TestChatAppReplication(unittest.TestCase):
    def setUp(self):
        # Create temporary directories for test data
        self.test_dir = tempfile.mkdtemp()
        self.users_file = os.path.join(self.test_dir, "users.csv")
        self.messages_file = os.path.join(self.test_dir, "messages.csv")

        # Create mock replica info
        self.replica1 = Replica(
            replica_id=1,
            host="localhost",
            port=50051,
            users_store=self.users_file,
            messages_store=self.messages_file,
        )

        self.replica2 = Replica(
            replica_id=2,
            host="localhost",
            port=50052,
            users_store=os.path.join(self.test_dir, "users2.csv"),
            messages_store=os.path.join(self.test_dir, "messages2.csv"),
        )

        # Set up global REPLICAS dict for testing
        global REPLICAS
        REPLICAS = {1: self.replica1, 2: self.replica2}

        # Create server instance without starting discovery threads
        with patch("threading.Thread"):
            with patch.object(Server, "start_heartbeat"):
                self.server = Server(self.replica1)
                # Manually set as leader to avoid discovery
                self.server.leader_id = self.replica1.id
                self.server.discovered_replicas = {1}

        # Setup test users and messages
        self.test_username = "testuser"
        self.test_password = "testpass"
        self.test_message = "Hello, world!"

    def tearDown(self):
        # Clean up test files
        for file in [self.users_file, self.messages_file]:
            if os.path.exists(file):
                os.remove(file)

        # Remove temp directory
        os.rmdir(self.test_dir)

    def test_create_account(self):
        """Test creating a new user account"""
        request = app_pb2.Request(info=[self.test_username, self.test_password])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                response = self.server.RPCCreateAccount(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertIn(self.test_username, self.server.user_login_database)
        self.assertEqual(
            self.server.user_login_database[self.test_username].password,
            self.test_password,
        )

    def test_create_duplicate_account(self):
        """Test that creating a duplicate account fails"""
        # Create the account first
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )

        # Try to create the same account again
        request = app_pb2.Request(info=[self.test_username, self.test_password])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCCreateAccount(request, None)

        self.assertEqual(response.operation, app_pb2.FAILURE)
        self.assertEqual("".join(response.info), "Create Account Failed")

    def test_login(self):
        """Test user login"""
        # Create user first
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )

        # Try to login
        request = app_pb2.Request(info=[self.test_username, self.test_password])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCLogin(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertIn(self.test_username, self.server.active_users)

    def test_login_failure_wrong_password(self):
        """Test login with wrong password"""
        # Create user first
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )

        # Try to login with wrong password
        request = app_pb2.Request(info=[self.test_username, "wrongpass"])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCLogin(request, None)

        self.assertEqual(response.operation, app_pb2.FAILURE)
        self.assertEqual("".join(response.info), "Login Failed")

    def test_send_message(self):
        """Test sending a message between users"""
        # Create users
        sender = "sender"
        receiver = "receiver"
        self.server.user_login_database[sender] = User(sender, "pass1")
        self.server.user_login_database[receiver] = User(receiver, "pass2")

        # Send message
        request = app_pb2.Request(info=[sender, receiver, self.test_message])

        # Mock broadcast and sync to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                response = self.server.RPCSendMessage(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)

        # Verify message was added to sender's messages
        self.assertEqual(len(self.server.user_login_database[sender].messages), 1)
        self.assertEqual(
            self.server.user_login_database[sender].messages[0].message,
            self.test_message,
        )

        # Verify message was added to receiver's unread messages (since not active)
        self.assertEqual(
            len(self.server.user_login_database[receiver].unread_messages), 1
        )
        self.assertEqual(
            self.server.user_login_database[receiver].unread_messages[0].message,
            self.test_message,
        )

    def test_list_accounts(self):
        """Test listing accounts with a search string"""
        # Create some test users
        users = ["user1", "user2", "admin1", "admin2"]
        for user in users:
            self.server.user_login_database[user] = User(user, "pass")

        # Search for users starting with "user"
        request = app_pb2.Request(info=["user"])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCListAccount(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertEqual(set(response.info), {"user1", "user2"})

    def test_save_data(self):
        """Test saving data to CSV files"""
        # Create a mock user and message
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )
        test_message = Message("sender", self.test_username, "Hello!")
        self.server.user_login_database[self.test_username].messages.append(
            test_message
        )

        # Test with mocked open to avoid actual file operations
        with patch("builtins.open", mock_open()) as m:
            self.server.save_data(self.server.user_login_database)

        # Check that open was called for both files
        self.assertEqual(m.call_count, 2)

        # For a more comprehensive test, we could check the actual contents written

    def test_load_data(self):
        """Test loading data from CSV files"""
        # Create test CSV data
        user_data = "username,password\nuser1,pass1\nuser2,pass2\n"
        message_data = "sender,receiver,message,timestamp,is_read\nuser1,user2,Hello!,2025-03-20 12:00:00.000000,True\n"

        # Mock file operations
        with patch("builtins.open") as m:
            # Configure mock to return different content for different files
            m.return_value.__enter__.return_value.read.side_effect = [
                user_data,
                message_data,
            ]

            # Configure csv.reader to parse our test data
            with patch("csv.reader") as mock_reader:
                mock_reader.return_value.__iter__.return_value = [
                    ["username", "password"],
                    ["user1", "pass1"],
                    ["user2", "pass2"],
                ]

                self.server.load_data()

        # Verify data was loaded
        self.assertIn("user1", self.server.user_login_database)
        self.assertIn("user2", self.server.user_login_database)

    def test_merge_data(self):
        """Test merging data from multiple replicas"""
        # Setup original data
        self.server.user_login_database = {
            "user1": User("user1", "pass1"),
            "user2": User("user2", "pass2"),
        }

        # Setup incoming data with a new user
        incoming_db = {
            "user1": User("user1", "pass1"),  # Same user
            "user3": User("user3", "pass3"),  # New user
        }

        # Add some test messages
        incoming_active_users = {"user1": [Message("user2", "user1", "Hey from user2")]}

        # Mock save_data to avoid file operations
        with patch.object(Server, "save_data"):
            merged_db = self.server.merge_data(incoming_db, incoming_active_users)

        # Verify merged results
        self.assertIn("user1", merged_db)
        self.assertIn("user2", merged_db)
        self.assertIn("user3", merged_db)  # New user should be added

    def test_serialize_deserialize_data(self):
        """Test serialization and deserialization of data"""
        # Create test data
        test_user_db = {"user1": User("user1", "pass1")}
        test_user_db["user1"].messages.append(Message("user2", "user1", "Hello"))

        test_active_users = {"user1": [Message("user2", "user1", "Hello")]}

        # Serialize
        serialized = serialize_data(test_user_db, test_active_users)

        # Deserialize
        deserialized_user_db, deserialized_active_users = deserialize_data(serialized)

        # Verify results
        self.assertIn("user1", deserialized_user_db)
        self.assertEqual(deserialized_user_db["user1"].password, "pass1")
        self.assertEqual(len(deserialized_user_db["user1"].messages), 1)

    def test_integration_create_send_message(self):
        """Integration test for account creation and message sending"""
        # Mock network operations
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                # Create accounts
                self.server.RPCCreateAccount(
                    app_pb2.Request(info=["user1", "pass1"]), None
                )
                self.server.RPCCreateAccount(
                    app_pb2.Request(info=["user2", "pass2"]), None
                )

                # Login
                self.server.RPCLogin(app_pb2.Request(info=["user1", "pass1"]), None)

                # Send message
                response = self.server.RPCSendMessage(
                    app_pb2.Request(info=["user1", "user2", "Hello!"]), None
                )

        # Verify message was sent successfully
        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertEqual(
            len(self.server.user_login_database["user2"].unread_messages), 1
        )
        self.assertEqual(
            self.server.user_login_database["user2"].unread_messages[0].message,
            "Hello!",
        )

    def test_delete_account(self):
        """Test deleting a user account"""
        # Create user first
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )

        # Ensure user exists before deletion
        self.assertIn(self.test_username, self.server.user_login_database)

        # Delete the account
        request = app_pb2.Request(info=[self.test_username])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                response = self.server.RPCDeleteAccount(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertNotIn(self.test_username, self.server.user_login_database)

    def test_delete_nonexistent_account(self):
        """Test deleting a non-existent account"""
        # Ensure user doesn't exist
        self.assertNotIn("nonexistent", self.server.user_login_database)

        # Try to delete non-existent account
        request = app_pb2.Request(info=["nonexistent"])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCDeleteAccount(request, None)

        self.assertEqual(response.operation, app_pb2.FAILURE)
        self.assertEqual("".join(response.info), "Delete Account Failed")

    def test_delete_nonexistent_message(self):
        """Test deleting a non-existent message"""
        # Create user
        sender = "sender"
        self.server.user_login_database[sender] = User(sender, "pass1")

        # Try to delete non-existent message
        request = app_pb2.Request(info=[sender, "nonexistent_id"])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCDeleteMessage(request, None)

        self.assertEqual(response.operation, app_pb2.FAILURE)
        self.assertEqual("".join(response.info), "Delete Message Request Invalid")

    def test_list_messages(self):
        """Test listing messages for a user"""
        # Create users
        sender = "sender"
        receiver = "receiver"
        self.server.user_login_database[sender] = User(sender, "pass1")
        self.server.user_login_database[receiver] = User(receiver, "pass2")

        # Send multiple messages
        messages = ["First message", "Second message", "Third message"]

        for msg_text in messages:
            message = Message(sender, receiver, msg_text)
            self.server.user_login_database[sender].messages.append(message)
            self.server.user_login_database[receiver].unread_messages.append(message)

        # List messages for receiver
        request = app_pb2.Request(info=[receiver])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCReadMessage(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)

    def test_read_message(self):
        """Test reading a message"""
        # Create users
        sender = "sender"
        receiver = "receiver"
        self.server.user_login_database[sender] = User(sender, "pass1")
        self.server.user_login_database[receiver] = User(receiver, "pass2")

        # Send a message
        test_message = "Read me please"
        message = Message(sender, receiver, test_message)
        message_id = get_normalized_msg_id(message)

        # Add to receiver's unread messages
        self.server.user_login_database[receiver].unread_messages.append(message)
        # Read the message
        request = app_pb2.Request(info=[receiver, str(message_id)])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                response = self.server.RPCReadMessage(request, None)

    def test_read_nonexistent_message(self):
        """Test reading a non-existent message"""
        # Create user
        receiver = "receiver"
        self.server.user_login_database[receiver] = User(receiver, "pass1")

        # Try to read non-existent message
        request = app_pb2.Request(info=[receiver, "nonexistent_id"])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            response = self.server.RPCReadMessage(request, None)

        self.assertEqual(response.operation, app_pb2.FAILURE)
        self.assertEqual("".join(response.info), "Read Message Request Invalid")

    def test_logout(self):
        """Test user logout"""
        # Create user and set as active
        self.server.user_login_database[self.test_username] = User(
            self.test_username, self.test_password
        )
        self.server.active_users[self.test_username] = []

        # Ensure user is active before logout
        self.assertIn(self.test_username, self.server.active_users)

        # Logout
        request = app_pb2.Request(info=[self.test_username])

        # Mock broadcast to avoid actual network calls
        with patch.object(Server, "BroadcastUpdate", return_value=True):
            with patch.object(Server, "sync_after_operation"):
                response = self.server.RPCLogout(request, None)

        self.assertEqual(response.operation, app_pb2.SUCCESS)
        self.assertNotIn(self.test_username, self.server.active_users)


if __name__ == "__main__":
    unittest.main()
