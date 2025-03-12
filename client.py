import socket
import os
from protos import app_pb2
from util import hash_password
import threading
import logging
import json
from dotenv import load_dotenv


class Client:
    # global variables consistent across all instances of the Client class
    FORMAT = "utf-8"
    HEADER = 64

    # polling thread to handle incoming messages from the server
    CLIENT_LOCK = threading.Lock()

    def __init__(self, stub):
        load_dotenv()
        self.host = os.getenv("HOST")
        self.port = int(os.getenv("PORT"))
        self.stub = stub

        # username of the current client
        self.username = ""

    def create_data_object(self, version, operation, info):
        """
        Creates a data object with the given version, operation, and info.

        Args:
            version: The version of the data object
            operation: The operation to be performed
            info: information for the data object to pass

        Returns:
            dict: A dictionary representing the data object
        """
        return {"version": version, "type": operation, "info": [info]}

    def unwrap_data_object(self, data):
        """
        Unwraps the data object to return the info field if it is a single element list.
        Specific to the case where the operation is not reading messages or listing accounts.

        Args:
            data: The data object to unwrap

        Returns:
            dict: The info field from the data object
        """
        if data and len(data["info"]) == 1:
            data["info"] = data["info"][0]
        return data

    def logout(self):
        if self.username:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGOUT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCLogout(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True
        return False

    def login(self, username, password):
        """
        Handles the login process for the client application.
        Prompts the user for their username and password, hashes the password,
        and sends the login request to the server.

        Args:
            username: The username of the client
            password: The password of the client
        Returns:
            bool: True if login is successful, False otherwise
        """
        try:
            # hash password
            password = hash_password(password)
            request = app_pb2.Request(info=[username, password])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LOGIN")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCLogin(request)
            status = res.operation

            if status == app_pb2.SUCCESS:
                self.username = username
                unread_messages = int(res.info[0])
                return True, int(unread_messages)

            return False, 0

        except:
            return False, 0

    def create_account(self, username, password):
        """
        Handles the account creation process for the client application.
        Prompts the user for a unique username and password, hashes the password,
        and sends the account creation request to the server.

        Args:
            username: The username of the client
            password: The password of the client

        Returns:
            bool: True if account creation is successful, False otherwise
        """
        try:
            # hash password
            password = hash_password(password)
            # create the data object to send to the server, specifying the version number, operation type, and info
            request = app_pb2.Request(info=[username, password])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: CREAT ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCCreateAccount(request)
            status = res.operation

            if status == app_pb2.SUCCESS:
                return True

            return False

        except:
            return False

    def list_accounts(self, search_string):
        """
        Handles the account listing process for the client application.
        Prompts the user for a search string and sends the account listing request to the server.

        Args:
            search_string: The search string to search for in the accounts

        Returns:
            list: The list of accounts that match the search string
        """
        try:
            request = app_pb2.Request(info=[search_string])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: LIST ACCOUNTS")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCListAccount(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return res.info

        except:
            logging.error("Listing accounts failed!")
            return

    def send_message(self, receiver, msg):
        """
        Handles the message sending process for the client application.
        Prompts the user for the receiver's username and the message content,
        and sends the message request to the server.

        Args:
            receiver: The receiver of the message
            msg: The message content

        Returns:
            bool: True if message sending is successful, False otherwise
        """
        try:
            request = app_pb2.Request(info=[self.username, receiver, msg])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: SEND MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCSendMessage(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True

            return False

        except:
            logging.error("Sending message unexpectedly failed")
            return False

    def read_message(self):
        """s
        Handles the message reading process for the client application.
        Sends a request to the server to read all messages for the current user.

        Returns:
            list: The list of messages for the current user
        """
        try:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: READ MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCReadMessage(request)
            status = res.operation

            if status == app_pb2.SUCCESS:
                messages = res.messages
                return messages

            else:
                logging.error("Reading message failed")

        except:
            logging.error("Unexpected error in read_message")
            return

    def delete_messages(self, messages):
        """
        Deletes a list of messages from the server.

        Args:
            messages: List of messages to delete

        Returns:
            int: True if all messages are deleted successfully, False otherwise
        """
        # iterates through the list of messages and deletes each message
        for message in messages:
            try:
                sender = message.sender
                receiver = message.receiver
                timestamp = message.timestamp
                msg = message.message
                if not self.delete_message(sender, receiver, msg, timestamp):
                    logging.error(
                        f"message from {sender} to {receiver} on {timestamp} could not be deleted"
                    )
                    return False
            except KeyError as e:
                logging.error(f"Message is missing required field: {e}")
                return False

        return True

    def delete_message(self, sender, receiver, msg, timestamp):
        """
        Deletes a single message from the server.

        Args:
            sender: The sender of the message
            receiver: The receiver of the message
            msg: The message content
            timestamp: The timestamp of the message

        Returns:
            bool: True if the message is deleted successfully, False otherwise
        """
        try:
            request = app_pb2.Request(info=[sender, receiver, msg, timestamp])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE MESSAGE")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCDeleteMessage(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True

            else:
                return False

        except:
            return False

    def get_instant_messages(self):
        try:
            request = app_pb2.Request(info=[self.username])
            res = self.stub.RPCGetInstantMessages(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return res.messages
            else:
                return []
        except:
            return []

    def delete_account(self):
        """
        Handles the account deletion process for the client application.
        Prompts the user for their username and sends the account deletion request to the server.

        Returns:
            bool: True if account deletion is successful, False otherwise
        """
        try:
            request = app_pb2.Request(info=[self.username])
            request_size = request.ByteSize()
            print("--------------------------------")
            print(f"OPERATION: DELETE ACCOUNT")
            print(f"SERIALIZED DATA LENGTH: {request_size} ")
            print("--------------------------------")
            res = self.stub.RPCDeleteAccount(request)
            status = res.operation
            if status == app_pb2.SUCCESS:
                return True

            else:
                return False

        except:
            return False
