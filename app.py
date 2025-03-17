from datetime import datetime
import json
import os
from dotenv import load_dotenv
import tkinter as tk
from tkinter import messagebox, simpledialog
import threading
from tkinter import scrolledtext
from client import Client
from server import Server
import time
import logging
import sys
import grpc
from protos import app_pb2_grpc
from concurrent import futures
import signal
import multiprocessing
from consensus import REPLICAS, Replica


class ChatAppGUI:
    # Global connection ID counter
    connection_id = 0

    def __init__(self, root, protocol_version=None):
        self.root = root
        self.root.title("Chat App")
        self.main_frame = tk.Frame(root)
        self.main_frame.pack(padx=20, pady=20)

        # assigned when Client is initialized
        self.client = None
        self.notification_windows = []
        self.unread_messages = []

        # assigned when client and server are initialized
        self.protocol_version = protocol_version

        # create a frame for notifications
        self.notification_frame = tk.Frame(root)
        self.notification_frame.pack(side="bottom", fill="x", padx=5, pady=5)

        # create a label for notifications and new messages
        self.messages_header = tk.Label(
            self.notification_frame,
            text="New Messages",
            font=("Arial", 10, "bold"),
            fg="white",
        )
        self.messages_header.pack(side="top", anchor="w", padx=5)

        # allow notifications to be scrolled
        self.notification_text = scrolledtext.ScrolledText(
            self.notification_frame,
            height=3,
            width=50,
            font=("Arial", 10),
            wrap=tk.WORD,
        )
        self.notification_text.pack(side="left", fill="x", expand=True)

        self.messages = None

        self.start_menu()
        self.root.protocol("WM_DELETE_WINDOW", self.on_exit)
        self.root.after(100, self.check_interrupt)  # check for interrupts periodically

    def check_interrupt(self):
        try:
            sys.stdout.flush()
        except KeyboardInterrupt:
            print("Keyboard interrupt detected. Exiting...")
            self.on_exit()
            self.root.quit()
            sys.exit(0)
        self.root.after(100, self.check_interrupt)

    def clear_frame(self):
        """Clears all widgets from the main frame before switching screens."""
        for widget in self.main_frame.winfo_children():
            widget.destroy()

    def show_notification(self):
        """Display a popup notification for new messages"""

        # update notification widget
        self.notification_text.delete(1.0, tk.END)
        for message in self.messages:
            timestamp = message.timestamp
            sender = message.sender
            msg = message.message
            self.notification_text.insert(
                tk.END, f"[{timestamp}] From {sender}: {msg}\n"
            )

        # auto-scroll
        self.notification_text.see(tk.END)

    def start_menu(self):
        """Initial menu to choose between Client or Server."""
        self.notification_frame.pack_forget()
        self.clear_frame()

        # create a label for the start menu
        tk.Label(self.main_frame, text="Select an Option", font=("Arial", 14)).pack(
            pady=10
        )

        # create a button for the client
        tk.Button(
            self.main_frame, text="Client", command=self.start_client, width=20
        ).pack(pady=5)

        # create a button for the server
        tk.Button(
            self.main_frame, text="Server", command=self.start_server, width=20
        ).pack(pady=5)

    def start_client(self):
        """Starts the client instance with Tkinter GUI."""
        self.clear_frame()
        # lead_server_id = min(REPLICAS.keys())
        # print(f"CLIENT SIDE {REPLICAS[lead_server_id].host}:{REPLICAS[lead_server_id].port}")
        # self.channel = grpc.insecure_channel(f"{REPLICAS[lead_server_id].host}:{REPLICAS[lead_server_id].port}")
        # self.stub = app_pb2_grpc.AppStub(self.channel)
        self.client = Client()

        # start polling in a background thread
        self.polling_active = True
        self.polling_thread = threading.Thread(target=self.background_poll, daemon=True)
        self.polling_thread.start()

        self.login_menu()

    def background_poll(self):
        """Continuously polls for messages in background thread"""
        if self.messages is None:
            self.messages = self.client.get_instant_messages()
            self.root.after(0, self.show_notification)
        while self.polling_active:
            try:
                self.messages = self.client.get_instant_messages()
                if len(self.messages) > 0:
                    self.root.after(0, self.show_notification)
                time.sleep(0.1)  # short sleep to prevent CPU spinning
            except Exception as e:
                logging.error(f"Error in background poll: {e}")
                break

    def cleanup(self):
        """Clean up resources before closing"""
        self.polling_active = False
        if hasattr(self, "polling_thread"):
            self.polling_thread.join(timeout=1.0)
        if self.client:
            self.client.logout()

    def login_menu(self):
        """Login screen with username and password input fields."""
        self.clear_frame()

        # create a label for the login screen
        tk.Label(self.main_frame, text="Login", font=("Arial", 14)).pack(pady=10)

        # create a label for the username
        tk.Label(self.main_frame, text="Username:").pack()
        self.username_entry = tk.Entry(self.main_frame)
        self.username_entry.pack()

        # create a label for the password
        tk.Label(self.main_frame, text="Password:").pack()
        self.password_entry = tk.Entry(self.main_frame, show="*")
        self.password_entry.pack()

        # create a button for the login
        tk.Button(
            self.main_frame, text="Login", command=self.attempt_login, width=20
        ).pack(pady=5)

        # create a button for the create account
        tk.Button(
            self.main_frame,
            text="Create Account",
            command=self.create_account_menu,
            width=20,
        ).pack(pady=5)

        # create a button for the list accounts
        tk.Button(
            self.main_frame,
            text="List Accounts",
            command=self.list_accounts_menu,
            width=20,
        ).pack(pady=5)

        # create a button for the back
        tk.Button(self.main_frame, text="Back", command=self.start_menu, width=20).pack(
            pady=5
        )

    def attempt_login(self):
        """Gets username and password from entries and calls client.login()."""
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()

        # check if both fields are filled
        if not username or not password:
            messagebox.showerror("Error", "Both fields are required!")
            return

        success, unread_messages = self.client.login(username, password)

        # check if login was successful
        if success:
            messagebox.showinfo(
                "Success",
                f"Login successful! You have {unread_messages} unread messages.",
            )
            self.notification_frame.pack(side="bottom", fill="x", padx=5, pady=5)
            self.messages_header.pack(side="top", anchor="w", padx=5)
            self.user_menu()
        else:
            messagebox.showerror("Error", "Login failed. Try again.")

    def list_accounts_menu(self):
        """Gets search string to list accounts"""
        self.clear_frame()

        # create a label for the list accounts screen
        tk.Label(
            self.main_frame, text="Search for an account", font=("Arial", 14)
        ).pack(pady=10)

        # create a label for the username
        tk.Label(self.main_frame, text="Username:").pack()

        # create a label for the username search entry
        self.username_search_entry = tk.Entry(self.main_frame)
        self.username_search_entry.pack()

        # create a button for the search account
        tk.Button(
            self.main_frame,
            text="Search Account",
            command=self.attempt_list_accounts,
            width=20,
        ).pack(pady=5)

        # create a button for the back
        tk.Button(self.main_frame, text="Back", command=self.login_menu, width=20).pack(
            pady=5
        )

    def attempt_list_accounts(self):
        """Lists accounts associated under a given search string"""
        username = self.username_search_entry.get().strip()

        # check if the search string is filled
        if not username:
            messagebox.showerror("Error", "Search string is required!")
            return

        # get accounts
        accounts = self.client.list_accounts(username)

        # check if accounts were returned
        if accounts is not None:
            if len(accounts) == 0:
                messagebox.showinfo("Success", "No accounts found")

            else:
                messagebox.showinfo("Success", "Searched accounts were returned")
                self.display_accounts(accounts)
        else:
            messagebox.showerror("Error", "Account search failed.")

    def display_accounts(self, accounts):
        """Lists accounts under the GUI"""
        msg_window = tk.Toplevel(self.root)
        msg_window.title("Accounts")
        msg_window.geometry("450x400")

        # Scrollable Listbox
        listbox_frame = tk.Frame(msg_window)
        listbox_frame.pack(pady=10, fill="both", expand=True)

        scrollbar = tk.Scrollbar(listbox_frame, orient="vertical")
        self.listbox = tk.Listbox(
            listbox_frame,
            selectmode=tk.MULTIPLE,
            width=60,
            height=15,
            yscrollcommand=scrollbar.set,
        )

        scrollbar.config(command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.listbox.pack(side="left", fill="both", expand=True)

        for acc in accounts:
            display_text = f"{acc}"  # Show preview
            self.listbox.insert("end", display_text)

    def create_account_menu(self):
        """Account creation screen."""
        self.clear_frame()

        # create a label for the create account screen
        tk.Label(self.main_frame, text="Create Account", font=("Arial", 14)).pack(
            pady=10
        )

        # create a label for the username
        tk.Label(self.main_frame, text="Username:").pack()
        self.new_username_entry = tk.Entry(self.main_frame)
        self.new_username_entry.pack()

        # create a label for the password
        tk.Label(self.main_frame, text="Password:").pack()
        self.new_password_entry = tk.Entry(self.main_frame, show="*")
        self.new_password_entry.pack()

        # create a button for the create account
        tk.Button(
            self.main_frame,
            text="Create Account",
            command=self.attempt_create_account,
            width=20,
        ).pack(pady=5)

        # create a button for the back
        tk.Button(self.main_frame, text="Back", command=self.login_menu, width=20).pack(
            pady=5
        )

    def attempt_create_account(self):
        """Gets username and password for account creation and calls client.create_account()."""
        username = self.new_username_entry.get().strip()
        password = self.new_password_entry.get().strip()

        if not username or not password:
            messagebox.showerror("Error", "Both fields are required!")
            return

        success = self.client.create_account(username, password)

        if success:
            messagebox.showinfo("Success", "Account created successfully!")
            self.login_menu()
        else:
            messagebox.showerror("Error", "Account creation failed.")

    def user_menu(self):
        """User menu after login."""
        self.clear_frame()

        # create a label for the user menu
        tk.Label(
            self.main_frame,
            text=f"Welcome, {self.client.username}!",
            font=("Arial", 14),
        ).pack(pady=10)

        # create a button for the send message
        tk.Button(
            self.main_frame,
            text="Send Message",
            command=self.send_message_menu,
            width=20,
        ).pack(pady=5)

        # create a button for the read messages
        tk.Button(
            self.main_frame, text="Read Messages", command=self.read_messages, width=20
        ).pack(pady=5)

        # create a button for the delete account
        tk.Button(
            self.main_frame,
            text="Delete Account",
            command=self.delete_account,
            width=20,
        ).pack(pady=5)

        # create a button for logout
        tk.Button(
            self.main_frame,
            text="Logout",
            command=self.logout,
            width=20,
        ).pack(pady=5)

    def send_message_menu(self):
        """Message sending screen."""
        self.clear_frame()

        # create a label for the send message screen
        tk.Label(self.main_frame, text="Send Message", font=("Arial", 14)).pack(pady=10)

        # create a label for the receiver
        tk.Label(self.main_frame, text="Receiver:").pack()
        self.receiver_entry = tk.Entry(self.main_frame)
        self.receiver_entry.pack()

        tk.Label(self.main_frame, text="Message:").pack()
        self.message_entry = tk.Entry(self.main_frame)
        self.message_entry.pack()

        tk.Button(
            self.main_frame, text="Send", command=self.attempt_send_message, width=20
        ).pack(pady=5)
        tk.Button(self.main_frame, text="Back", command=self.user_menu, width=20).pack(
            pady=5
        )

    def attempt_send_message(self):
        """Gets receiver and message and calls client.send_message()."""
        receiver = self.receiver_entry.get().strip()
        message = self.message_entry.get().strip()

        if not receiver or not message:
            messagebox.showerror("Error", "Both fields are required!")
            return

        success = self.client.send_message(receiver, message)

        if success:
            messagebox.showinfo("Success", "Message sent successfully!")
        else:
            messagebox.showerror("Error", "Failed to send message.")

        self.user_menu()

    def read_messages(self):
        """Fetch messages and display options for reading and deleting."""
        self.unread_messages.clear()
        self.notification_text.delete(1.0, tk.END)

        messages = self.client.read_message()

        if messages is None:
            messagebox.showerror("Error", "Failed to retrieve messages.")
            return

        total_messages = len(messages)
        if total_messages == 0:
            messagebox.showinfo("Messages", "No messages.")
            return

        # Ask how many messages to read
        num_to_read = simpledialog.askinteger(
            "Messages",
            f"You have {total_messages} messages.\nHow many do you want to read?",
            minvalue=0,
            maxvalue=total_messages,
        )

        if num_to_read is None or num_to_read == 0:
            return  # User canceled input
        # Create a new window for messages
        self.display_messages(messages[-num_to_read:])

    def display_messages(self, messages):
        """Display messages in a Listbox with multi-select for deletion."""
        self.msg_window = tk.Toplevel(self.root)
        self.msg_window.title("Your Messages")
        self.msg_window.geometry("450x400")

        tk.Label(
            self.msg_window,
            text="Select messages to delete:",
            font=("Arial", 12, "bold"),
        ).pack(pady=5)

        # Scrollable Listbox
        listbox_frame = tk.Frame(self.msg_window)
        listbox_frame.pack(pady=10, fill="both", expand=True)

        scrollbar = tk.Scrollbar(listbox_frame, orient="vertical")
        self.listbox = tk.Listbox(
            listbox_frame,
            selectmode=tk.MULTIPLE,
            width=60,
            height=15,
            yscrollcommand=scrollbar.set,
        )

        scrollbar.config(command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.listbox.pack(side="left", fill="both", expand=True)

        # populate listbox with messages
        self.message_map = {}  # maps listbox index to message object
        for idx, msg in enumerate(messages):
            sender, receiver, content, timestamp = (
                msg.sender,
                msg.receiver,
                msg.message,
                msg.timestamp,
            )
            displayed_timestamp = datetime.strptime(
                timestamp, "%Y-%m-%d %H:%M:%S.%f"
            ).strftime("%Y-%m-%d")
            display_text = f"From {sender} to {receiver} on {displayed_timestamp}: {content[:50]}"  # Show preview
            self.listbox.insert("end", display_text)
            self.message_map[idx] = msg  # Store full message

        delete_button = tk.Button(
            self.msg_window, text="Delete Selected", command=self.delete_selected
        )
        delete_button.pack(pady=10)

    def delete_selected(self):
        """Deletes selected messages from the listbox."""
        selected_indices = self.listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Info", "No messages selected for deletion.")
            return

        to_delete = [self.message_map[idx] for idx in selected_indices]
        result = self.client.delete_messages(to_delete)

        if result:
            messagebox.showinfo("Success", "Selected messages deleted successfully!")
            self.msg_window.destroy()
        else:
            messagebox.showerror("Error", "Failed to delete some messages.")

    def delete_account(self):
        """Deletes the user's account."""
        confirmation = messagebox.askyesno(
            "Delete Account", "Are you sure you want to delete your account?"
        )
        if confirmation:
            success = self.client.delete_account()
            if success:
                messagebox.showinfo("Success", "Account deleted successfully.")
                self.start_menu()
            else:
                messagebox.showerror("Error", "Account deletion failed.")

    def logout(self):
        """Logs out the current user and returns to login menu."""
        if self.client and self.client.logout():
            messagebox.showinfo("Success", "Logged out successfully!")
            self.client.username = None  # Clear the current user
            self.messages = None
            self.notification_frame.pack_forget()  # Hide notifications
            self.login_menu()
        else:
            messagebox.showerror("Error", "Log out unsuccessful!")

    def start_server(self):
        """Starts the server in a separate thread and displays replica management interface."""
        self.clear_frame()

        # Create header
        tk.Label(
            self.main_frame, text="Server Running", font=("Arial", 14, "bold")
        ).pack(pady=10)

        # Create frame for replica list
        replicas_frame = tk.Frame(self.main_frame)
        replicas_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Create scrollable list view for replicas
        list_frame = tk.Frame(replicas_frame)
        list_frame.pack(fill="both", expand=True)

        # Headers for the replica list
        headers_frame = tk.Frame(list_frame)
        headers_frame.pack(fill="x")

        header_fields = [
            "ID",
            "Host",
            "Port",
            "Messages Store",
            "Users Store",
            "Actions",
        ]
        col_widths = [10, 15, 8, 20, 20, 15]

        for i, header in enumerate(header_fields):
            tk.Label(
                headers_frame,
                text=header,
                font=("Arial", 10, "bold"),
                width=col_widths[i],
            ).grid(row=0, column=i, padx=5, pady=5, sticky="w")

        # Create proper scrollable frame with both vertical and horizontal scrollbars
        container_frame = tk.Frame(list_frame)
        container_frame.pack(fill="both", expand=True)

        # Add both scrollbars
        v_scrollbar = tk.Scrollbar(container_frame, orient="vertical")
        h_scrollbar = tk.Scrollbar(container_frame, orient="horizontal")

        # Place scrollbars
        v_scrollbar.pack(side="right", fill="y")
        h_scrollbar.pack(side="bottom", fill="x")

        # Create canvas with proper scrolling
        canvas = tk.Canvas(container_frame)
        canvas.pack(side="left", fill="both", expand=True)

        # Connect scrollbars to canvas
        v_scrollbar.config(command=canvas.yview)
        h_scrollbar.config(command=canvas.xview)
        canvas.config(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)

        # Create inner frame for content
        replica_list_frame = tk.Frame(canvas)

        # Add the frame to the canvas
        canvas_window = canvas.create_window(
            (0, 0), window=replica_list_frame, anchor="nw"
        )

        # Configure canvas scrolling
        def configure_canvas(event):
            # Update the scrollregion to encompass the inner frame
            canvas.configure(scrollregion=canvas.bbox("all"))

            # Make sure the window expands properly
            required_width = replica_list_frame.winfo_reqwidth()
            required_height = replica_list_frame.winfo_reqheight()
            canvas.config(
                width=min(required_width, 800), height=min(required_height, 400)
            )
            canvas.itemconfig(canvas_window, width=required_width)

        replica_list_frame.bind("<Configure>", configure_canvas)

        # Allow mousewheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        # Function to create unique commands for each button
        def create_start_command(replica_id, replica_data):
            return lambda: self.start_specific_replica(replica_data)

        def create_remove_command(replica_id, replica_data):
            return lambda: self.remove_replica(replica_data)

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # Load replicas from JSON file
        try:
            with open("replicas.json", "r") as file:
                replicas_data = json.load(file)
                replicas = replicas_data.get("replicas", [])
        except (FileNotFoundError, json.JSONDecodeError):
            replicas = []

        # Store button functions to prevent garbage collection
        self.button_commands = []

        # Display each replica
        for i, replica in enumerate(replicas):
            replica_id = replica.get("id", f"replica{i+1}")

            for j, key in enumerate(
                ["id", "host", "port", "messages_store", "users_store"]
            ):
                tk.Label(
                    replica_list_frame,
                    text=str(replica.get(key, "")),
                    width=col_widths[j],
                ).grid(row=i, column=j, padx=5, pady=2, sticky="w")

            # Add action buttons for each replica
            actions_frame = tk.Frame(replica_list_frame)
            actions_frame.grid(row=i, column=5, padx=5, pady=2)

            # Create specific command functions for this replica
            start_cmd = create_start_command(replica_id, replica)
            remove_cmd = create_remove_command(replica_id, replica)

            # Store commands to prevent garbage collection
            self.button_commands.append(start_cmd)
            self.button_commands.append(remove_cmd)

            # Create buttons with the specific commands
            start_button = tk.Button(actions_frame, text="Start", command=start_cmd)
            start_button.pack(side="left", padx=2)

            remove_button = tk.Button(actions_frame, text="Remove", command=remove_cmd)
            remove_button.pack(side="left", padx=2)

        # Create form to add new replicas
        add_frame = tk.LabelFrame(self.main_frame, text="Add New Replica")
        add_frame.pack(fill="x", padx=10, pady=10)

        form_fields = [
            ("ID:", "id"),
            ("Host:", "host"),
            ("Port:", "port"),
            ("Messages Store:", "messages_store"),
            ("Users Store:", "users_store"),
        ]

        entry_widgets = {}

        for i, (label_text, field_name) in enumerate(form_fields):
            tk.Label(add_frame, text=label_text).grid(
                row=i, column=0, padx=5, pady=5, sticky="e"
            )
            entry = tk.Entry(add_frame)
            entry.grid(row=i, column=1, padx=5, pady=5, sticky="ew")
            entry_widgets[field_name] = entry

        # Add default values
        if not replicas:
            entry_widgets["id"].insert(0, "replica1")
            entry_widgets["host"].insert(0, "localhost")
            entry_widgets["port"].insert(0, "5001")
            entry_widgets["messages_store"].insert(0, "replica1_messages.csv")
            entry_widgets["users_store"].insert(0, "replica1_users.csv")
        else:
            # Suggest next replica number
            next_num = len(replicas) + 1
            entry_widgets["id"].insert(0, f"replica{next_num}")
            entry_widgets["host"].insert(0, "localhost")
            entry_widgets["port"].insert(0, f"{5000 + next_num}")
            entry_widgets["messages_store"].insert(0, f"replica{next_num}_messages.csv")
            entry_widgets["users_store"].insert(0, f"replica{next_num}_users.csv")

        # Create a specific function for the add replica button
        add_cmd = lambda: self.add_new_replica(entry_widgets)
        self.button_commands.append(add_cmd)

        # Add button to create new replica
        tk.Button(add_frame, text="Add Replica", command=add_cmd).grid(
            row=len(form_fields), column=0, columnspan=2, pady=10
        )

        # Add button to start all replicas
        tk.Button(
            self.main_frame,
            text="Start All Replicas",
            font=("Arial", 12),
            bg="#4CAF50",
            fg="black",
            command=self.start_server_processes,
        ).pack(pady=10)

        self.root.update_idletasks()

    def add_new_replica(self, entry_widgets):
        """Adds a new replica to the JSON file and updates the UI."""
        # Get values from entry widgets
        new_replica = {field: widget.get() for field, widget in entry_widgets.items()}

        # Convert port to integer
        try:
            new_replica["port"] = int(new_replica["port"])
        except ValueError:
            messagebox.showerror("Invalid Port", "Port must be a number")
            return

        # Load existing replicas
        try:
            with open("replicas.json", "r") as file:
                replicas_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            replicas_data = {"replicas": []}

        # Add new replica
        replicas_data["replicas"].append(new_replica)

        # Save updated replicas
        with open("replicas.json", "w") as file:
            json.dump(replicas_data, file, indent=2)

        # Update the global REPLICAS dictionary to include the new replica
        replica_obj = Replica(
            new_replica["id"],
            new_replica["host"],
            new_replica["port"],
            new_replica["messages_store"],
            new_replica["users_store"],
        )
        REPLICAS[new_replica["id"]] = replica_obj

        # Restart server view to refresh the list
        self.start_server()

        messagebox.showinfo(
            "Success", f"Replica {new_replica['id']} added successfully"
        )

    def remove_replica(self, replica):
        """Removes a replica from the JSON file and updates the UI."""
        # Confirm deletion
        confirm = messagebox.askyesno(
            "Confirm Deletion",
            f"Are you sure you want to remove replica {replica['id']}?",
        )

        if not confirm:
            return

        # Load existing replicas
        with open("replicas.json", "r") as file:
            replicas_data = json.load(file)

        # Remove the selected replica
        replicas_data["replicas"] = [
            r for r in replicas_data["replicas"] if r["id"] != replica["id"]
        ]

        # Save updated replicas
        with open("replicas.json", "w") as file:
            json.dump(replicas_data, file, indent=2)

        # Remove from the global REPLICAS dictionary if it exists
        if replica["id"] in REPLICAS:
            del REPLICAS[replica["id"]]

        # Restart server view to refresh the list
        self.start_server()

        messagebox.showinfo("Success", f"Replica {replica['id']} removed successfully")

    def start_specific_replica(self, replica):
        """Starts a specific replica."""
        # Create a replica config object
        replica_obj = Replica(
            replica["id"],
            replica["host"],
            replica["port"],
            replica["messages_store"],
            replica["users_store"],
        )

        # Launch a single replica process
        try:
            p = multiprocessing.Process(
                target=run_server,  # Use standalone function
                args=(replica_obj,),
            )
            p.start()

            # Clear the frame and show "Server Running" message
            self.clear_frame()
            tk.Label(
                self.main_frame,
                text=f"Server Running\nReplica {replica['id']} started on {replica['host']}:{replica['port']}",
                font=("Arial", 14, "bold"),
            ).pack(pady=10)

            # Add a button to go back to the server management interface
            tk.Button(
                self.main_frame,
                text="Back to Server Management",
                font=("Arial", 12),
                command=self.start_server,
            ).pack(pady=10)

        except Exception as e:
            messagebox.showerror("Server Error", str(e))

    def start_server_processes(self):
        """Handles launching server processes in a separate thread."""
        # First, load replicas from JSON
        try:
            with open("replicas.json", "r") as file:
                replicas_data = json.load(file)
                replicas = replicas_data.get("replicas", [])
        except (FileNotFoundError, json.JSONDecodeError):
            replicas = []
            messagebox.showwarning("Warning", "No replicas found in replicas.json")
            return

        # Create replica config objects and update REPLICAS dictionary
        for replica in replicas:
            replica_obj = Replica(
                replica["id"],
                replica["host"],
                replica["port"],
                replica["messages_store"],
                replica["users_store"],
            )
            REPLICAS[replica["id"]] = replica_obj

        # Start processes for each replica
        processes = []
        started_replicas = []
        try:
            for replica_id in REPLICAS:
                p = multiprocessing.Process(
                    target=run_server,  # Use standalone function
                    args=(REPLICAS[replica_id],),
                )
                p.start()
                processes.append(p)
                started_replicas.append(replica_id)

            # Clear the frame and show "Servers Running" message
            self.clear_frame()

            tk.Label(
                self.main_frame, text="Servers Running", font=("Arial", 14, "bold")
            ).pack(pady=10)

            # Show list of started replicas
            replicas_text = "\n".join(
                [f"Replica {r_id} started" for r_id in started_replicas]
            )
            tk.Label(
                self.main_frame,
                text=f"Started {len(processes)} replica(s):\n{replicas_text}",
                font=("Arial", 12),
                justify="left",
            ).pack(pady=10)

            # Add a button to go back to the server management interface
            tk.Button(
                self.main_frame,
                text="Back to Server Management",
                font=("Arial", 12),
                command=self.start_server,
            ).pack(pady=10)

        except Exception as e:
            messagebox.showerror("Server Error", str(e))

    def on_exit(self):
        self.cleanup()
        self.root.destroy()


def run_server(replica):
    """Runs the server."""
    try:
        print("run server")
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        server_object = Server(replica)
        app_pb2_grpc.add_AppServicer_to_server(
            server_object, server
        )  # Correct registration
        server.add_insecure_port(f"{replica.host}:{replica.port}")
        server.start()
        print(f"Server started, listening on {replica.host}:{replica.port}")
        server.wait_for_termination()

    except Exception as e:
        logging.error("Server Error")


if __name__ == "__main__":
    root = tk.Tk()
    app = ChatAppGUI(root)

    def handle_exit_signal(signum, frame):
        print("Received exit signal. Exiting gracefully...")
        app.on_exit()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)
    root.mainloop()
