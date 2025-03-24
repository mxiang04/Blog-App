# CS 2620 - Chat App

This repository contains the code for Design Exercise 3: Replication in a Chat App Harvard's CS 2620: Distributed Programming class. This builds upon the previous design exercises by adding 2-fault tolerance and persistence. The chat app can be loaded with a GUI. You can access the design document and engineering notebook [here](https://docs.google.com/document/d/1vJeS7PuXCz1lkp-FrzXvrbb7IFf1vcbZgZWthI5IdKU/edit?usp=sharing). The application supports the following features:

### Features

- Creating and logging in to an account. We use a secure hashing algorithm to keep the password safe, and upon login, we allow the user to see how many unread messages they have. We do not allow for the same user to log in to multiple different devices.

- Listing accounts. At the home page before logging in, we allow a user to search for a particular account(s). If the search phrase is a prefix of any usernames in our database, we return a list of such usernames. The usage of this feature is to allow a user to find their login username if they have forgotten parts of it.
- Sending a message to a recipient. If the recipient is logged in, the app delivers immediately and notifies the recipient that they have a message through a pop up as well as with a notification on their home screen. Otherwise, the message is enqueued and the recipient receives it later when they are logged in.
- Reading messages. The user will be able to view how many messages they have currently, and they can enter how many messages they wish to view (starting from messages sent more recently).
- Deleting messages. We allow a user to delete messages, which will delete the messages permanently between sender and recipient. The recipient will also have the message deleted from their account.
- Deleting an account. We allow the user to confirm deletion of their account. Deleting account keeps all messages already sent in the database. If the user is logged in on two different devices, deletion of the account prevents the user on the other device from making any changes.
- Our app is 2-fault tolerant and persistent. It can handle 2 servers failing and still working, and after the servers shut down, it is able to handle the servers launching back up with the same data.

### Setup

To setup, we first require people to clone our repository via

```
git clone https://github.com/nchen55555/Persistent-Chat.git
```

After, we require the user to have Python3 and pip installed on their system. To install the necessary packages, run

```
pip3 install -r requirements.txt
```

Now, we can finally use the chat app. Depending on the server and how you choose to run your system, you will need to double check your network setting to find the IP address of the server. Create our edit your `.env` file to include the IP address of the server. We've specified port 65432 as the default port, but feel free to change it to whatever you want.The server handles all client/user interactions, so we need to set the server up first. Run

```
python app.py
```

to activate the chat app and then initialize the server. Run the same command again and initialize a client.

### Architecture

#### Files

##### app.py

This file starts up the app for either the client or the server. We use the `tkinter` library to create the GUI.

##### client.py

This contains the code for the client/user side of the app.

##### server.py

This contains the server code, which handles multiple client connections. This also accounts for multiple replicas, which we need to ensure our app is 2-fault tolerant.

##### consensus.py

This creates the replica class and loads them up from a json file configuring our servers. Each replica is created with a different post and hort configuration, and they are used in our other files to make sure our servers can communicate with each other.

##### message.py

This contains the class for the messages, which is structured so that every message has a sender, recipient, message itself, and the time it was sent

##### user.py

This contains the class for the users, structured around the username and hashed password, and also including the user's messages and unread messages.

##### test\_{x}.py

This contains unit tests that we use to test the effectiveness of our app. Simply run these unit tests via

```
python test_{x}.py
```

where x represents what you want to test (functions, fault tolerance, or persistence).

##### util.py

This contains helper functions related to hashing.

##### requirements.txt

This contains the list of packages we need for the app.

##### app.proto

This contains the structures and functions needed for our gRPC architecture. We use this file to autogenerate python files for server/client communication.

#### Protocol

Protocol

We use gRPC to handle communication between the server and clients. The protocol is defined in app.proto, which specifies the messages, operations, and services available. This file is used to autogenerate Python files for server-client interaction.

Each client sends a Request to the server, which processes it and returns a Response. The request and response formats depend on the operation being performed.

A Request contains a list of strings under info, which holds the parameters required for different operations. A Response includes an operation field that indicates the status of the request (such as SUCCESS, FAILURE, or a specific operation like READ_MESSAGE), along with optional info or messages fields.

Messages exchanged between users are structured using the Message type, which includes a sender, receiver, timestamp, and the message content itself.

The RPC methods define how clients interact with the server. Users can log in, create accounts, list available accounts, send and read messages, delete messages, retrieve real-time messages, and log out. The server processes these requests and responds with the appropriate status and data.

To generate the required gRPC Python files from app.proto, run the following command:

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. app.proto
```

This will generate app_pb2.py and app_pb2_grpc.py, which are used in the server and client implementations.

For example, when a client logs in, it sends a request with the username and password. The server checks the credentials and returns a response indicating success or failure. Similarly, when a user sends a message, the client provides the sender, receiver, and message content, and the server handles delivery. Messages can be retrieved later through RPCReadMessage, or users can fetch real-time messages using RPCGetInstantMessages.

This protocol ensures a structured and efficient way for clients and the server to communicate in the chat application.
