from concurrent import futures
import os
import unittest
import time
import tkinter as tk
import threading
import socket
from unittest.mock import patch
from app import ChatAppGUI  # Assuming you have this import
from protos import app_pb2_grpc
from server import Server
import grpc


class TestChatAppGUI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up server and required components once for all tests"""
        cls.root = tk.Tk()
        cls.root.withdraw()  # Hide the main window

        # Start server in a separate thread
        cls.server_thread = threading.Thread(target=cls.start_server)
        cls.server_thread.daemon = True
        cls.server_thread.start()

        # Allow server time to start
        time.sleep(1)

        # Initialize the app GUI
        cls.app = ChatAppGUI(cls.root)
        cls.app.start_client()

        # Test account details
        cls.test_username = "test_user"
        cls.test_password = "test_pass"

        # Create test account
        cls.app.create_account_menu()
        cls.app.new_username_entry.insert(0, cls.test_username)
        cls.app.new_password_entry.insert(0, cls.test_password)
        cls.app.attempt_create_account()

        cls.app2 = ChatAppGUI(cls.root)  # Use same root

        cls.test_username2 = "test_user3"
        cls.test_password2 = "test_pass3"

        # Start second client and create an account
        cls.app2.start_client()
        time.sleep(1)  # Give time for client to connect

        cls.app2.create_account_menu()
        cls.app2.new_username_entry.insert(0, cls.test_username2)
        cls.app2.new_password_entry.insert(0, cls.test_password2)
        cls.app2.attempt_create_account()

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests are done"""
        cls.root.quit()
        cls.root.destroy()

    @classmethod
    def start_server(cls):
        """Start the server in the background"""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        app_pb2_grpc.add_AppServicer_to_server(Server(), server)
        server.add_insecure_port("[::]:" + os.getenv("PORT"))
        server.start()
        print("Server started, listening on " + os.getenv("PORT"))
        server.wait_for_termination()

    def setUp(self):
        """Set up before each test - log in with test account"""
        self.app.login_menu()
        self.app.username_entry.insert(0, self.test_username)
        self.app.password_entry.insert(0, self.test_password)
        self.app.attempt_login()
        time.sleep(0.5)  # Give time for login to complete

    def tearDown(self):
        """Clean up after each test"""
        self.app.clear_frame()

    def test_01_account_creation_and_login(self):
        """Test account creation and login flow"""

        # Create new test account
        test_user = "test_user2"
        test_pass = "test_pass2"

        self.app.create_account_menu()
        self.app.new_username_entry.insert(0, test_user)
        self.app.new_password_entry.insert(0, test_pass)
        result = self.app.attempt_create_account()

        # Try logging in with new account
        self.app.login_menu()
        self.app.username_entry.insert(0, test_user)
        self.app.password_entry.insert(0, test_pass)
        login_result = self.app.attempt_login()

        # Verify login was successful by checking if we're in user menu
        children = self.app.main_frame.winfo_children()
        labels = [w for w in children if isinstance(w, tk.Label)]
        self.assertTrue(
            any(f"Welcome, {test_user}" in label.cget("text") for label in labels)
        )

    def test_02_message_sending_and_receiving(self):
        """Test sending and receiving messages between clients"""

        self.app2.login_menu()
        self.app2.username_entry.insert(0, "test_user3")
        self.app2.password_entry.insert(0, "test_pass3")
        self.app2.attempt_login()
        test_message = "Hello from test!"
        self.app.send_message_menu()
        self.app.receiver_entry.insert(0, "test_user3")
        self.app.message_entry.insert(0, test_message)
        self.app.attempt_send_message()
        time.sleep(3)
        messages = self.app2.client.read_message()
        self.assertTrue(any(msg.message == test_message for msg in messages))

    def test_03_message_deletion(self):
        """Test message deletion functionality"""
        # Send a test message to self
        self.app.send_message_menu()
        self.app.receiver_entry.insert(0, "test_user3")
        self.app.message_entry.insert(0, "Test message for deletion")
        self.app.attempt_send_message()

        time.sleep(1)  # Give time for message to be delivered

        # Read messages and try to delete the test message
        self.app2.login_menu()
        self.app2.username_entry.insert(0, "test_user3")
        self.app2.password_entry.insert(0, "test_pass3")
        self.app2.attempt_login()
        self.app2.read_messages()
        time.sleep(0.5)  # Give time for messages to load

        # Select the last message (our test message)
        self.app2.listbox.select_set("end")
        self.app2.delete_selected()

        # Verify message was deleted
        messages = self.app2.client.read_message()
        self.assertFalse(
            any(msg.message == "Test message for deletion" for msg in messages)
        )

    def test_04_account_listing(self):
        """Test account listing functionality"""
        # Verify that our test accounts are listed
        accounts = self.app.client.list_accounts("test_user")
        self.assertTrue(any(self.test_username == acc for acc in accounts))

    def test_05_notification_system(self):
        """Test that notifications appear when messages are received"""

        self.app2.login_menu()
        self.app2.username_entry.insert(0, "test_user3")
        self.app2.username_entry.insert(0, "test_pass3")
        self.app2.send_message_menu()
        self.app2.receiver_entry.insert(0, "test_user")
        self.app2.message_entry.insert(0, "Hello from test!")
        self.app2.attempt_send_message()
        messages = self.app2.client.read_message()
        self.assertTrue(any(msg.message == "Hello from test!" for msg in messages))

    def test_06_account_deletion(self):
        """Test account deletion functionality"""
        # Create account to delete
        self.app.create_account_menu()
        self.app.new_username_entry.insert(0, "delete_test")
        self.app.new_password_entry.insert(0, "delete_pass")
        self.app.attempt_create_account()

        # Login with new account
        self.app.login_menu()
        self.app.username_entry.insert(0, "delete_test")
        self.app.password_entry.insert(0, "delete_pass")
        self.app.attempt_login()

        # Delete account
        self.app.delete_account()

        # Try to login again - should fail
        self.app.login_menu()
        self.app.username_entry.insert(0, "delete_test")
        self.app.password_entry.insert(0, "delete_pass")
        login_result = self.app.attempt_login()

        # Verify login failed because account was deleted
        self.assertFalse(login_result)


if __name__ == "__main__":
    unittest.main()
