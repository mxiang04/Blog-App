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
        new_client = Client()

        # verify we can still login and access data
        result = new_client.login("fault2", "pass")
        self.assertTrue(result)

        messages = new_client.read_message()
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].sender, "fault1")
        self.assertEqual(messages[0].message, "Testing fault tolerance")

        result = new_client.create_account("fault3", "pass")
        self.assertTrue(result)

        accounts = new_client.list_accounts("fault")
        self.assertEqual(len(accounts), 3)
        self.assertIn("fault3", accounts)


if __name__ == "__main__":
    unittest.main()
