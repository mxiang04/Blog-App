# Engineering Notebook Documentation 
## gRPC and Protos Design 
We create a `protos/app.proto` file which is our Protocol Buffer definition file. It defines the data structures (message) and the methods (services) that can be used to communicate in our system. Because Protobufs are language agnostic (allows us to scale for different clients that use different languages and specifications), we need to generate python-specific proto code via the command: 

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --mypy_out=. protos/app.proto
```
Our auto generated python code is housed in the protos file. 

We now enumerate the structure of our Protocol Buffer definition file. We have two message structures which define how data will be exchanged between systems. We call the two `Request` and `Response` where in the Request, we have the specific info we would like to send over (in a list) and in the Response, we offer the Operational enum and the specific info we would like to send over (such as a message). The Operational enum indicates whether the request was processed successfully or unsuccessfully with more detailed specification. 

```
message Request {
    repeated string info = 1;
}

message Response {
    Operation operation = 1;
    repeated string info = 2;
    repeated Message messages = 3;
}
```

Further we define 7 different methods under the app service which specify remote procedure calls (RPCs). Each rpc takes in a Request and outputs a Response. Each method specifies each action that our client/server application enables. 

```
service App {
    rpc RPCLogin(Request) returns (Response) {}
    rpc RPCCreateAccount(Request) returns (Response) {}
    rpc RPCListAccount(Request) returns (Response) {}
    rpc RPCSendMessage(Request) returns (Response) {}
    rpc RPCReadMessage(Request) returns (Response) {}
    rpc RPCDeleteMessage(Request) returns (Response) {}
    rpc RPCDeleteAccount(Request) returns (Response) {}
    rpc RPCGetInstantMessages(Request) returns (Response) {}
    rpc RPCLogout(Request) returns (Response) {}
}
```

#### Does the use of this tool make the application easier or more difficult? 

The use of gRPC makes our chat application disproportionately easier to implement. First, gRPC abstracts the difficulties of serializing and deserializing data. In our wire protocol implementation, not only did we have to design how to format the data we’d send over from the client to the server and vice versa, but we also specified the constraints for how the data would be serialized and deserialized via our wire protocol implementation. The most difficult part was figuring out how to robustly delineate different pieces of data sent over the wire and in cases where the length of the data was indeterminate (ie. with lists), we would need to recursively implement our wire protocol. gRPC abstracts the deserialization and serialization of data over the wire. Indeed, all high-level information about the structure of data sent over the wire between different commands is simply listed in the a .proto file. Developers define the structure of the data sent over via the messages keyword. For example, to send over a list of messages, our chat app now simply creates a gRPC service with an RPC called RPCReadMessage which takes in a Request and outputs a Response. Our Response, which ought to include the list of messages to read includes a repeated Message messages variable that is simply a list of Message objects defined by our protos file under the “message” keyword. By offering simple instructions for how to define data structures within the protos file, gRPC offers a standardized and robust way to send information over the wire, abstracting the serialization and deserialization component. 

Indeed, not only do we now not have to serialize and deserialize, we also do not have to worry about setting up multiple socket connections in the server (one socket for each client). gRPC abstracts those processes for us so that to start a multi-client + server application, we simply need to create a gRPC server and a gRPC stub for each client we start! This abstraction is possible because in gRPC, the stub acts as the client-side object that proxies for the remote service. Because the stub is created from the auto-generated gRPC protocol buffer file (app.protos), when you call a method on the stub, the request is automatically sent to the server which is also created and connected to the auto-generated gRPC protocol buffer file. Accordingly the network communication is entirely abstracted making the entire build-out for the application much easier to scale and implement. 

#### What does it do to the size of the data passed? 

### Client-side

| Operation          | Custom Wire Protocol | JSON | gRPC |
|--------------------|----------------------|------|------|
| Create Account     | 113                  | 145  | 74   |
| Login              | 113                  | 145  | 74   |
| Send Message       | 71                   | 103  | 23   |
| Read Message       | 33                   | 65   | 8    |
| Delete Message     | 114                  | 146  | 51   |
| List Accounts      | 33                   | 65   | 8    |
| Delete Account     | 33                   | 65   | 8    |

### Server-side

| Operation          | Custom Wire Protocol | JSON | gRPC |
|--------------------|----------------------|------|------|
| Create Account     | 41                   | 73   | 0    |
| Login              | 27                   | 59   | 3    |
| Send Message       | 70                   | 102  | 0    |
| Read Message       | 114                  | 146  | 53   |
| Delete Message     | 54                   | 86   | 0    |
| List Accounts      | 33                   | 65   | 8    |
| Delete Account     | 45                   | 77   | 0    |

We can see from the above that the size (in bytes) of the data being passed to and from the client and the server severely diminishes when we use gRPC versus both our custom wire protocol and JSON. Indeed, in some instances, gRPC reduces the size of the data being passed through by more than half. 

The significantly smaller size of the data being passed is because protocol buffers use binary encodings and therefore removes unnecessary metadata such as the field names in JSON. On the other hand, to maintain a certain degree of readability, JSON delineates with field names as well as delimiters such as commas, semicolons, and quotations. Additionally, the protocol buffer file mandates that data passed to and from the wire follow the strict proto structure, using keywords such as “repeated” for lists and “message” for specific fields and data structures. These strict mandates allow the protocol buffer to pack data efficiently as compared to JSON and our wire protocol which allows more flexibility in how data is passed through and therefore is not able to assure as packed of a structure. 

#### How does it change the structure of the client? The server? 

Within our `app.py` file which specifies the chat app’s UI and starting the server and client connections, we simply change how we initialize and connect our server and client. Specifically, when “server” is chosen, we begin our `run_server` method which creates a new grpc server instance. We use a ThreadPoolExecutor with 10 working threads to handle concurrent requests so that we can process incoming requests in parallel. 

We then register our `Server()` class instance (where our rpc methods are implemented) with the grpc server instance we just instantiated. Lastly, we set up our server to listen to our port. 

On the client side, when “client” is selected, we use grpc to set up a channel with the host:port address that the server is listening on before creating a stub which provides the methods that match the server definitions to our proto file. Because the stub is created from the auto-generated gRPC protocol buffer file (app.protos), when you call a method on the stub, the request is automatically sent to the server which is also created and connected to the auto-generated gRPC protocol buffer file. 

Beyond the initialization of the client and server within the `app.py` file, gRPC abstracts away the serialization and the socket polling connections, making the structure of our code far easier to read. Specifically, within the server-side, each method matches the rpc service methods delineated in the protocol buffer file (app.protos). This ensures that when our client stub calls a specific method, the stub is able to 1:1 match the protos code to the server code which houses the actual implementation. From there, the server decouples the information sent over the wire. If, for example, the client sends a message, specifying in the info section of the Request that the information is a list with the sender’s username, receiver’s username, and the actual message, then to access that information, the Server simply has to check that the provided requests’s info tag has a length greater than 3 and then decouples the info tag (`request.info`) into the sender, receiver, and message. It then performs the necessary operations to successfully send the message before returning a Response with the specified parameters. The same flow is applied to the client-side where instead of sending over a Response, the client sends over a Request, waiting to receive the Response from the server-side. 

Implementing the instant messages feature of our chat app initially seemed daunting as the sockets of each of our clients were not individually exposed. However, it quickly turned out that implementing the instant messages was more of an `app.py` feature than anything different on the client and server side. Indeed, the client and server side code remains similar to other rpc methods in that there is a request and a response provided by the server. However, the bulk of the change came from `app.py` continously sending requests on the client side to the server to get instant messages in a separate thread. Indeed, this polling method is documented below: 

```
while self.polling_active:
  try:
    self.messages = self.client.get_instant_messages()
    if len(self.messages) > 0:
      self.root.after(0, self.show_notification)
      time.sleep(0.1)  # short sleep to prevent CPU spinning
    except Exception as e:
      logging.error(f"Error in background poll: {e}")
      break
```

#### How does this change the testing of the application?

We maintained most of the same types of methods when testing the application (ie. we tested the same processes and methods in the gRPC version as the Wire Protocol version). However, for our Wire Protocol, our first test checked to see if the client server connection was fulfilled by directly checking whether the client socket had been initialized or not. The gRPC version does not touch the specific client socket and therefore, we do not test that aspect of the code. 

Other than that, our tests did not change for this application because the majority of our tests concentrated on the front-end/functionalities of our application. Therefore, underlying changes in the infrastructure of our wire would not impact our tests unless they impacted our application functionality. 

### Testing
Our unit tests are housed in tests.py and can be run via the command python tests.py. In this file, we test every functionality of our app. The below specifies in more detail what each test assesses: 
Test 1 - creates an account and attempts to login 
Test 2 - creates a new account, sends a message from the first account to the second account; has the second account read the message 
Test 3 - first account sends message to second account; first account reads all messages and deletes message
Test 4 - lists accounts 
Test 5 - creates a new account, sends a message, checks if the message just sent appears in the read messages list of the new account 
Test 6 - creates a new account, logs in, deletes account, tries to log back in 

## Entries 
#### February 16, 2025 
Duplicated the Wire Protocol and re-implemented it as the gRPC 
Researched how to generate protos and what necessary packages need to be installed and included in `requirements.txt` to run with protos 
Using grpcio and grpcio-tools to set up our grpc on the client and server side 
Using protobuf and mypy-protobuf to generate the protobuf files in python 

#### February 18, 2025 
Defined the structures in app.proto
For simplicity, choosing an easy “info” field for Response and Request message; this way, limited customization is necessary for each rpc method we provide 
Found necessary command to auto generate the protos for the methods and the data structures mandated 

#### February 19, 2025
Difficulty figuring out how to connect server and client together without the sockets and selectors 
Researched and discovered that the autogenerated protocol buffer python code with app_pb2_grpc could connect server and client with the stub
Needed to add server to the autogenerated code 
Needed to add client to the autogenerated code 
Replaced the necessary start_client and start_server code in app.py with instead the packages from the auto generated code including the stubs an the channels
Changed the info field to repeating to accommodate for nested information (list accounts and read messages) 

#### February 20, 2025
Login, Create Account, and List Accounts baseline features implemented 
All followed a very similar structure where we would define the Request in the client-side and then receive the request on the Server side, packaging a Response on the server side to the Client 
Instant Messages 
This was difficult - wasn’t sure how to proceed since gRPC abstracted all the sockets of different clients
Wasn’t sure how to trigger the instant messages response on the server side immediately after sending the message on the client side 
Discovered that a consistent background polling where the client sends a get instant messages request can do the job 
Majority of implementation for instant messaging was in the `app.py` file with the client server request response code emulating other methods 

#### February 21, 2025 
Implemented Delete Messages, Read Messages, and Delete Account, and Logout 
All these methodologies had very similar approaches as before 
Updated gRPC tests 
Got rid of Test 1 with sockets; maintained the rest of the tests 

#### February 22-23, 2025 
Polished up code including adding logging statements, documentation, and began experimentation 
Found that gRPC byte size is considerably smaller than JSON or our custom wire protocol 


