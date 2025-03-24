import unittest
import grpc
import tempfile
import os
import time
import threading
import shutil
from unittest.mock import patch
from copy import deepcopy

from protos import app_pb2, app_pb2_grpc
from server import Server
from consensus import Replica, REPLICAS
from client import Client
from concurrent import futures


class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # create temp directory
        cls.test_dir = tempfile.mkdtemp()

        # test replicas
        cls.test_replicas = deepcopy(REPLICAS)

        # update file paths
        for replica_id, replica in cls.test_replicas.items():
            replica.users_store = os.path.join(cls.test_dir, f"users_{replica_id}.json")
            replica.messages_store = os.path.join(
                cls.test_dir, f"messages_{replica_id}.json"
            )

    @classmethod
    def tearDownClass(cls):
        # clean up the temp directory
        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def setUp(self):
        # starting servers
        self.servers = []

        for replica_id, replica in self.test_replicas.items():
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            server_servicer = Server(replica)
            app_pb2_grpc.add_AppServicer_to_server(server_servicer, server)
            server.add_insecure_port(f"{replica.host}:{replica.port}")
            server.start()
            self.servers.append(server)

        # test client
        self.client = Client()

        # waiting for servers to be fully up
        time.sleep(3)

    def tearDown(self):
        # stop all servers with a grace period
        for server in self.servers:
            server.stop(1)

        # wait for servers to fully shutdown
        time.sleep(2)

        # clean up any files
        for replica_id, replica in self.test_replicas.items():
            if os.path.isfile(replica.users_store):
                try:
                    os.remove(replica.users_store)
                except (PermissionError, FileNotFoundError):
                    pass

            if os.path.isfile(replica.messages_store):
                try:
                    os.remove(replica.messages_store)
                except (PermissionError, FileNotFoundError):
                    pass

    def test_account_creation_and_login(self):
        """Test creating accounts and logging in"""
        result = self.client.create_account("user1", "pass1")
        self.assertTrue(result)

        result = self.client.create_account("user2", "pass2")
        self.assertTrue(result)

        # try to create existing account
        result = self.client.create_account("user1", "pass1")
        self.assertFalse(result)

        result = self.client.login("user1", "pass1")
        self.assertTrue(result[0])

        # login with wrong password
        result = self.client.login("user1", "wrongpass")
        self.assertFalse(result[0])

        # logout
        result = self.client.logout()
        self.assertTrue(result)

    def test_messaging(self):
        """Test sending and receiving messages"""
        # create accounts and login
        self.client.create_account("sender", "pass")
        self.client.create_account("receiver", "pass")
        self.client.login("sender", "pass")

        # send message
        result = self.client.send_message("receiver", "Hello, this is a test message!")
        self.assertTrue(result)

        self.client.logout()
        self.client.login("receiver", "pass")

        # read messages
        messages = self.client.read_message()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].sender, "sender")
        self.assertEqual(messages[0].message, "Hello, this is a test message!")

        # delete message
        result = self.client.delete_message(
            messages[0].sender,
            messages[0].receiver,
            messages[0].message,
            messages[0].timestamp,
        )
        self.assertTrue(result)

        messages = self.client.read_message()
        self.assertEqual(len(messages), 0)

    def test_list_accounts(self):
        """Test listing accounts with prefix"""
        self.client.create_account("user1", "pass")
        self.client.create_account("user2", "pass")
        self.client.create_account("test1", "pass")
        self.client.login("user1", "pass")

        # list accounts starting with "user"
        accounts = self.client.list_accounts("user")
        self.assertEqual(len(accounts), 2)
        self.assertIn("user1", accounts)
        self.assertIn("user2", accounts)

        accounts = self.client.list_accounts("test")
        self.assertEqual(len(accounts), 1)
        self.assertIn("test1", accounts)

    def test_persistence(self):
        """Test data persistence across server restarts"""
        self.client.create_account("persist1", "pass")
        self.client.create_account("persist2", "pass")
        self.client.login("persist1", "pass")
        self.client.send_message("persist2", "Persistence test message")
        self.client.logout()

        # stop all servers
        for server in self.servers:
            server.stop(1)

        time.sleep(2)

        # restart
        self.servers = []
        for replica_id, replica in self.test_replicas.items():
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            server_servicer = Server(replica)
            app_pb2_grpc.add_AppServicer_to_server(server_servicer, server)
            server.add_insecure_port(f"{replica.host}:{replica.port}")
            server.start()
            self.servers.append(server)

        time.sleep(3)

        # verify accounts still exist
        self.assertTrue(self.client.login("persist1", "pass")[0])
        accounts = self.client.list_accounts("")
        self.assertIn("persist1", accounts)
        self.assertIn("persist2", accounts)

        # verify messages are preserved
        self.client.login("persist2", "pass")
        messages = self.client.read_message()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].sender, "persist1")
        self.assertEqual(messages[0].message, "Persistence test message")

    def test_fault_tolerance(self):
        """Test 2-fault tolerance"""
        self.client.create_account("fault1", "pass")
        self.client.create_account("fault2", "pass")
        self.client.login("fault1", "pass")
        self.client.send_message("fault2", "Testing fault tolerance")

        stopped_servers = [self.servers[0], self.servers[1]]

        # stop two replicas (including leader)
        stopped_servers[0].stop(1)
        stopped_servers[1].stop(1)

        self.servers = self.servers[2:]

        time.sleep(5)

        # verify we can still login and access data
        self.client.login("fault2", "pass")
        self.client.read_message()


if __name__ == "__main__":
    unittest.main()
