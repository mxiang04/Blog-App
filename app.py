import tkinter as tk
from tkinter import messagebox, simpledialog, scrolledtext
import sys
import time
import grpc
import threading
import multiprocessing
import signal
import json
from datetime import datetime

from client import Client
from server import Server
from consensus import get_replicas_config
from util import hash_password

class BlogAppGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Raft Blog App")
        self.main_frame = tk.Frame(root)
        self.main_frame.pack(padx=20, pady=20)

        self.client = None
        self.polling_thread = None
        self.polling_active = False

        self.start_menu()
        self.root.protocol("WM_DELETE_WINDOW", self.on_exit)
        self.root.after(100, self.check_interrupt)

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
        for widget in self.main_frame.winfo_children():
            widget.destroy()

    def start_menu(self):
        self.clear_frame()

        tk.Label(self.main_frame, text="Select an Option", font=("Arial", 14)).pack(
            pady=10
        )
        tk.Button(
            self.main_frame, text="Client", command=self.start_client, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Server", command=self.start_server, width=20
        ).pack(pady=5)

    # --------------------------------------------------------------------------
    # CLIENT UI
    # --------------------------------------------------------------------------
    def start_client(self):
        self.clear_frame()
        self.client = Client()

        self.login_menu()

    def login_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Login", font=("Arial", 14)).pack(pady=10)

        tk.Label(self.main_frame, text="Username:").pack()
        self.username_entry = tk.Entry(self.main_frame)
        self.username_entry.pack()

        tk.Label(self.main_frame, text="Password:").pack()
        self.password_entry = tk.Entry(self.main_frame, show="*")
        self.password_entry.pack()

        tk.Button(
            self.main_frame, text="Login", command=self.attempt_login, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Create Account", command=self.create_account_menu, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Search Users", command=self.search_users_menu, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Back", command=self.start_menu, width=20
        ).pack(pady=5)

    def attempt_login(self):
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()
        if not username or not password:
            messagebox.showerror("Error", "Both fields are required!")
            return

        ok, unread = self.client.login(username, password)
        if ok:
            messagebox.showinfo("Success", f"Login successful!")
            self.user_menu()
        else:
            messagebox.showerror("Error", "Login failed. Try again.")

    def create_account_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Create Account", font=("Arial", 14)).pack(
            pady=10
        )

        tk.Label(self.main_frame, text="Username:").pack()
        self.new_username_entry = tk.Entry(self.main_frame)
        self.new_username_entry.pack()

        tk.Label(self.main_frame, text="Password:").pack()
        self.new_password_entry = tk.Entry(self.main_frame, show="*")
        self.new_password_entry.pack()

        tk.Button(
            self.main_frame,
            text="Create Account",
            command=self.attempt_create_account,
            width=20,
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Back", command=self.login_menu, width=20
        ).pack(pady=5)

    def attempt_create_account(self):
        username = self.new_username_entry.get().strip()
        password = self.new_password_entry.get().strip()
        if not username or not password:
            messagebox.showerror("Error", "Both fields are required!")
            return
        ok = self.client.create_account(username, password)
        if ok:
            messagebox.showinfo("Success", "Account created successfully!")
            self.login_menu()
        else:
            messagebox.showerror("Error", "Account creation failed.")

    def search_users_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Search for Users", font=("Arial", 14)).pack(
            pady=10
        )

        tk.Label(self.main_frame, text="Search Query:").pack()
        self.search_users_entry = tk.Entry(self.main_frame)
        self.search_users_entry.pack()

        tk.Button(
            self.main_frame,
            text="Search",
            command=self.attempt_search_users,
            width=20,
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Back", command=self.login_menu, width=20
        ).pack(pady=5)

    def attempt_search_users(self):
        query = self.search_users_entry.get().strip()
        if not query:
            messagebox.showerror("Error", "Search query is required!")
            return
        users = self.client.search_users(query)
        if users is None:
            messagebox.showerror("Error", "User search failed.")
            return
        if len(users) == 0:
            messagebox.showinfo("Results", "No users found.")
            return
        self.display_users(users)

    def display_users(self, users):
        user_window = tk.Toplevel(self.root)
        user_window.title("Users")
        user_window.geometry("450x400")

        listbox_frame = tk.Frame(user_window)
        listbox_frame.pack(pady=10, fill="both", expand=True)

        scrollbar = tk.Scrollbar(listbox_frame, orient="vertical")
        lb = tk.Listbox(listbox_frame, width=60, height=15, yscrollcommand=scrollbar.set)
        scrollbar.config(command=lb.yview)
        scrollbar.pack(side="right", fill="y")
        lb.pack(side="left", fill="both", expand=True)

        for user in users:
            lb.insert("end", user)

        def on_subscribe():
            if not self.client.username:
                messagebox.showinfo("Error", "You must be logged in to subscribe.")
                return
                
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("Info", "No user selected.")
                return
            selected_user = users[sel[0]]
            ok = self.client.subscribe(selected_user)
            if ok:
                messagebox.showinfo("Success", f"Subscribed to {selected_user}!")
            else:
                messagebox.showerror("Error", "Failed to subscribe.")

        tk.Button(user_window, text="Subscribe to Selected", command=on_subscribe).pack(pady=10)

    def user_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text=f"Welcome, {self.client.username}!", font=("Arial", 14)).pack(pady=10)

        tk.Button(
            self.main_frame, text="Create Post", command=self.create_post_menu, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="View Your Posts", command=self.view_own_posts, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="View User Posts", command=self.view_user_posts_menu, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Manage Subscriptions", command=self.manage_subscriptions_menu, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Delete Account", command=self.delete_account, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Logout", command=self.logout, width=20
        ).pack(pady=5)

    def create_post_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Create New Post", font=("Arial", 14)).pack(pady=10)

        tk.Label(self.main_frame, text="Title:").pack()
        self.post_title_entry = tk.Entry(self.main_frame, width=50)
        self.post_title_entry.pack(pady=5)

        tk.Label(self.main_frame, text="Content:").pack()
        self.post_content_text = scrolledtext.ScrolledText(
            self.main_frame, width=50, height=10
        )
        self.post_content_text.pack(pady=5)

        tk.Button(
            self.main_frame, text="Create Post", command=self.attempt_create_post, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Back", command=self.user_menu, width=20
        ).pack(pady=5)

    def attempt_create_post(self):
        title = self.post_title_entry.get().strip()
        content = self.post_content_text.get("1.0", tk.END).strip()
        
        if not title or not content:
            messagebox.showerror("Error", "Both title and content are required!")
            return
            
        ok = self.client.create_post(title, content)
        if ok:
            messagebox.showinfo("Success", "Post created successfully!")
            self.user_menu()
        else:
            messagebox.showerror("Error", "Failed to create post.")

    def view_own_posts(self):
        posts = self.client.get_user_posts()
        if not posts:
            messagebox.showinfo("Posts", "You have no posts.")
            return
        self.display_posts(posts)

    def view_user_posts_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="View User's Posts", font=("Arial", 14)).pack(pady=10)

        tk.Label(self.main_frame, text="Username:").pack()
        self.user_posts_entry = tk.Entry(self.main_frame)
        self.user_posts_entry.pack(pady=5)

        tk.Button(
            self.main_frame, text="View Posts", command=self.attempt_view_user_posts, width=20
        ).pack(pady=5)
        tk.Button(
            self.main_frame, text="Back", command=self.user_menu, width=20
        ).pack(pady=5)

    def attempt_view_user_posts(self):
        username = self.user_posts_entry.get().strip()
        if not username:
            messagebox.showerror("Error", "Username is required!")
            return
            
        posts = self.client.get_user_posts(username)
        if not posts:
            messagebox.showinfo("Posts", f"{username} has no posts.")
            return
        self.display_posts(posts)

    def display_posts(self, posts):
        posts_window = tk.Toplevel(self.root)
        posts_window.title("Posts")
        posts_window.geometry("600x500")

        main_frame = tk.Frame(posts_window)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Create a canvas with scrollbar
        canvas = tk.Canvas(main_frame)
        scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Add posts to the scrollable frame
        for post in posts:
            post_frame = tk.Frame(scrollable_frame, relief=tk.RAISED, borderwidth=1)
            post_frame.pack(fill="x", padx=5, pady=5, expand=True)
            
            # Post header
            header_frame = tk.Frame(post_frame)
            header_frame.pack(fill="x", padx=5, pady=2)
            
            tk.Label(
                header_frame, 
                text=post.title,
                font=("Arial", 12, "bold")
            ).pack(side="left")
            
            likes_label = tk.Label(
                header_frame,
                text=f"❤️ {len(post.likes)}",
                font=("Arial", 10)
            )
            likes_label.pack(side="right")
            
            # Author and date
            info_frame = tk.Frame(post_frame)
            info_frame.pack(fill="x", padx=5, pady=2)
            
            tk.Label(
                info_frame,
                text=f"By {post.author} on {post.timestamp[:19]}",
                font=("Arial", 8, "italic")
            ).pack(side="left")
            
            # Post content
            content_text = scrolledtext.ScrolledText(
                post_frame, 
                height=4, 
                wrap=tk.WORD,
                font=("Arial", 10)
            )
            content_text.pack(fill="x", padx=5, pady=2)
            content_text.insert(tk.END, post.content)
            content_text.config(state=tk.DISABLED)
            
            # Button frame
            button_frame = tk.Frame(post_frame)
            button_frame.pack(fill="x", padx=5, pady=2)
            
            # Like button
            def like_post_callback(post_id, label, button):
                if self.client.has_liked_post(post_id):
                    if self.client.unlike_post(post_id):
                        current_likes = int(label.cget("text").split(" ")[1]) - 1
                        label.config(text=f"❤️ {current_likes}")
                        button.config(text="Like")  # Update button text to "Like"
                    else:
                        messagebox.showerror("Error", "Failed to unlike post.")
                else:
                    if self.client.like_post(post_id):
                        current_likes = int(label.cget("text").split(" ")[1]) + 1
                        label.config(text=f"❤️ {current_likes}")
                        button.config(text="Unlike")  # Update button text to "Unlike"
                    else:
                        messagebox.showerror("Error", "Failed to like post.")

            # Create the button with correct variable capture
            like_button = tk.Button(
                button_frame,
                text="Unlike" if self.client.has_liked_post(post.post_id) else "Like"
            )
            like_button.pack(side="left", padx=2)

            # Set the command with proper value capture - this is the key fix
            like_button.config(command=lambda pid=post.post_id, lbl=likes_label, btn=like_button: 
                                like_post_callback(pid, lbl, btn))
              
            # Delete button (only for own posts)
            if post.author == self.client.username:
                def delete_post_callback(post_id=post.post_id, frame=post_frame):
                    if messagebox.askyesno("Delete Post", "Are you sure you want to delete this post?"):
                        if self.client.delete_post(post_id):
                            frame.destroy()
                        else:
                            messagebox.showerror("Error", "Failed to delete post.")
                
                tk.Button(
                    button_frame,
                    text="Delete",
                    command=delete_post_callback
                ).pack(side="right", padx=2)
                
    
    def open_post_window(self, post_id):
        post = self.client.get_post(post_id)
        if not post:
            messagebox.showerror("Error", "Could not fetch post.")
            return
        w = tk.Toplevel(self.root)
        w.title(post.title)
        tk.Label(w, text=post.title, font=("Arial", 14, "bold")).pack(pady=5)
        tk.Label(w, text=f"By {post.author} on {post.timestamp}", 
                font=("Arial", 10, "italic")).pack(pady=5)
        txt = scrolledtext.ScrolledText(w, wrap=tk.WORD, width=60, height=20)
        txt.insert(tk.END, post.content)
        txt.config(state=tk.DISABLED)
        txt.pack(padx=10, pady=10)

    def manage_subscriptions_menu(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Manage Subscriptions", font=("Arial", 14)).pack(pady=10)
         
         # existing tabs
        tab_frame = tk.Frame(self.main_frame)
        tab_frame.pack(fill="both", expand=True)
        subscriptions_button = tk.Button(
             tab_frame, 
             text="Your Subscriptions", 
             command=self.view_subscriptions,
             width=20
        )
        subscriptions_button.pack(side="left", padx=5, pady=5)
        followers_button = tk.Button(
             tab_frame,
             text="Your Followers",
             command=self.view_followers,
             width=20
        )
        followers_button.pack(side="left", padx=5, pady=5)

        # new subscription flow
        tk.Button(
        self.main_frame,
        text="Subscribe to User",
        command=self.subscribe_ui,
        width=20
        ).pack(pady=5)
         
        tk.Button(
             self.main_frame, text="Back", command=self.user_menu, width=20
         ).pack(pady=10)


    def view_subscriptions(self):
        subscriptions = self.client.get_subscriptions()
        if not subscriptions:
            messagebox.showinfo("Subscriptions", "You are not subscribed to any users.")
            return
        
        sub_window = tk.Toplevel(self.root)
        sub_window.title("Your Subscriptions")
        sub_window.geometry("400x300")
        
        tk.Label(sub_window, text="Users you follow:", font=("Arial", 12, "bold")).pack(pady=5)
        
        listbox_frame = tk.Frame(sub_window)
        listbox_frame.pack(pady=10, fill="both", expand=True)
        
        scrollbar = tk.Scrollbar(listbox_frame, orient="vertical")
        lb = tk.Listbox(listbox_frame, width=40, height=10, yscrollcommand=scrollbar.set)
        scrollbar.config(command=lb.yview)
        scrollbar.pack(side="right", fill="y")
        lb.pack(side="left", fill="both", expand=True)
        
        for sub in subscriptions:
            lb.insert("end", sub)
            
        def on_unsubscribe():
            sel = lb.curselection()
            if not sel:
                messagebox.showinfo("Info", "No user selected.")
                return
            selected_user = subscriptions[sel[0]]
            ok = self.client.unsubscribe(selected_user)
            if ok:
                messagebox.showinfo("Success", f"Unsubscribed from {selected_user}!")
                lb.delete(sel)
            else:
                messagebox.showerror("Error", "Failed to unsubscribe.")
                
        tk.Button(sub_window, text="Unsubscribe from Selected", command=on_unsubscribe).pack(pady=10)

    def view_followers(self):
        followers = self.client.get_followers()
        if not followers:
            messagebox.showinfo("Followers", "You have no followers.")
            return
        
        followers_window = tk.Toplevel(self.root)
        followers_window.title("Your Followers")
        followers_window.geometry("400x300")
        
        tk.Label(followers_window, text="Users following you:", font=("Arial", 12, "bold")).pack(pady=5)
        
        listbox_frame = tk.Frame(followers_window)
        listbox_frame.pack(pady=10, fill="both", expand=True)
        
        scrollbar = tk.Scrollbar(listbox_frame, orient="vertical")
        lb = tk.Listbox(listbox_frame, width=40, height=10, yscrollcommand=scrollbar.set)
        scrollbar.config(command=lb.yview)
        scrollbar.pack(side="right", fill="y")
        lb.pack(side="left", fill="both", expand=True)
        
        for follower in followers:
            lb.insert("end", follower)

    def delete_account(self):
        confirm = messagebox.askyesno("Delete Account", "Are you sure you want to delete your account? This action cannot be undone.")
        if confirm:
            ok = self.client.delete_account()
            if ok:
                messagebox.showinfo("Success", "Account deleted successfully.")
                self.start_menu()
            else:
                messagebox.showerror("Error", "Account deletion failed.")

    def logout(self):
        ok = self.client.logout()
        if ok:
            messagebox.showinfo("Success", "Logged out successfully!")
            self.client.username = None
            self.login_menu()
        else:
            messagebox.showerror("Error", "Log out unsuccessful!")
    
    def subscribe_ui(self):
        # Prompt for a search prefix
        prefix = simpledialog.askstring("Subscribe", "Enter name prefix to search for users:")
        if not prefix:
            return
        users = self.client.search_users(prefix)
        if users is None:
            messagebox.showerror("Error", "Search failed.")
            return
        if not users:
            messagebox.showinfo("No results", "No users match that prefix.")
            return
        # Reuse your existing display_users window (it has the Subscribe button built in)
        self.display_users(users)


    # --------------------------------------------------------------------------
    # SERVER UI
    # --------------------------------------------------------------------------
    def start_server(self):
        self.clear_frame()

        reps = get_replicas_config()
        self.selected_replica_var = tk.StringVar()
        if reps:
            ids = [r["id"] for r in reps]
            self.selected_replica_var.set(ids[0])
        else:
            ids = ["No config found"]

        tk.Label(self.main_frame, text="Start a Raft Replica", font=("Arial",14)).pack(pady=10)
        tk.OptionMenu(self.main_frame, self.selected_replica_var, *ids).pack(pady=5)

        tk.Button(self.main_frame, text="Start", command=self.start_replica_process).pack(pady=5)
        tk.Button(self.main_frame, text="Server Management", command=self.server_management_screen).pack(pady=5)
        tk.Button(self.main_frame, text="Back", command=self.start_menu).pack(pady=5)

    def start_replica_process(self):
        rid = self.selected_replica_var.get()
        reps = get_replicas_config()
        config = None
        for c in reps:
            if c["id"] == rid:
                config = c
                break
        if not config:
            messagebox.showerror("Error", f"No config found for {rid}")
            return

        p = multiprocessing.Process(target=run_server, args=(config,))
        p.start()
        messagebox.showinfo("Success", f"Started replica {rid} (PID {p.pid})")

    def server_management_screen(self):
        self.clear_frame()
        tk.Label(self.main_frame, text="Server Management", font=("Arial",14)).pack(pady=10)

        # Option: Add new server
        tk.Button(self.main_frame, text="Add New Server", command=self.add_new_server_ui).pack(pady=5)
        # Option: Remove server
        tk.Button(self.main_frame, text="Remove Server", command=self.remove_server_ui).pack(pady=5)
        # Option: Sync/Refresh replicas.json from the leader
        tk.Button(self.main_frame, text="Sync Replicas from Leader", command=self.sync_replicas).pack(pady=5)
        # Go back
        tk.Button(self.main_frame, text="Back", command=self.start_menu).pack(pady=5)

    def add_new_server_ui(self):
        def on_submit():
            rid = entry_id.get().strip()
            h = entry_host.get().strip()
            prt = entry_port.get().strip()
            raftp = entry_raft.get().strip()
            postp = entry_post.get().strip()
            userp = entry_user.get().strip()
            subp = entry_subscriptions.get().strip()
            if not (rid and h and prt and raftp):
                messagebox.showerror("Error", "ID, Host, Port, and raft_store required!")
                return
            try:
                port_int = int(prt)
            except:
                messagebox.showerror("Error", "Port must be an integer.")
                return

            if not self.client:
                self.client = Client()

            ok = self.client.add_replica(rid, h, port_int, raftp, postp, userp, subp)
            if ok:
                messagebox.showinfo("Success", f"Replica {rid} added via leader replication.")
                w.destroy()
            else:
                messagebox.showerror("Error", "Failed to add replica (not leader or other error).")

        w = tk.Toplevel(self.root)
        w.title("Add New Replica")

        tk.Label(w, text="Replica ID:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        entry_id = tk.Entry(w)
        entry_id.insert(0, "replicaX")
        entry_id.grid(row=0, column=1, padx=5, pady=5)

        tk.Label(w, text="Host:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        entry_host = tk.Entry(w)
        entry_host.insert(0, "127.0.0.1")
        entry_host.grid(row=1, column=1, padx=5, pady=5)

        tk.Label(w, text="Port:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        entry_port = tk.Entry(w)
        entry_port.insert(0, "500X")
        entry_port.grid(row=2, column=1, padx=5, pady=5)

        tk.Label(w, text="raft_store:").grid(row=3, column=0, padx=5, pady=5, sticky="e")
        entry_raft = tk.Entry(w)
        entry_raft.insert(0, "replicaX_raft.json")
        entry_raft.grid(row=3, column=1, padx=5, pady=5)

        tk.Label(w, text="posts_store:").grid(row=4, column=0, padx=5, pady=5, sticky="e")
        entry_post = tk.Entry(w)
        entry_post.insert(0, "replicaX_posts.csv")
        entry_post.grid(row=4, column=1, padx=5, pady=5)

        tk.Label(w, text="users_store:").grid(row=5, column=0, padx=5, pady=5, sticky="e")
        entry_user = tk.Entry(w)
        entry_user.insert(0, "replicaX_users.csv")
        entry_user.grid(row=5, column=1, padx=5, pady=5)
        
        tk.Label(w, text="subscription_store:").grid(row=6, column=0, padx=5, pady=5, sticky="e")
        entry_subscriptions = tk.Entry(w)
        entry_subscriptions.insert(0, "replicaX_subscriptions.csv")
        entry_subscriptions.grid(row=6, column=1, padx=5, pady=5)

        tk.Button(w, text="Submit", command=on_submit).grid(row=7, column=0, columnspan=2, pady=10)

    def remove_server_ui(self):
        """
        UI to remove an existing server from the cluster by ID.
        """
        def on_submit():
            rid = entry_id.get().strip()
            if not rid:
                messagebox.showerror("Error", "Replica ID is required!")
                return
            if not self.client:
                self.client = Client()

            ok = self.client.remove_replica(rid)
            if ok:
                messagebox.showinfo("Success", f"Replica {rid} removed.")
                w.destroy()
            else:
                messagebox.showerror("Error", "Failed to remove replica (not leader or other error).")

        w = tk.Toplevel(self.root)
        w.title("Remove Replica")

        tk.Label(w, text="Replica ID to remove:").pack(padx=5, pady=5)
        entry_id = tk.Entry(w)
        entry_id.pack(padx=5, pady=5)

        tk.Button(w, text="Submit", command=on_submit).pack(pady=10)

    def sync_replicas(self):
        if not self.client:
            self.client = Client()
        updated_list = self.client.sync_membership()
        if updated_list is None:
            messagebox.showerror("Error", "Failed to sync membership (no leader?).")
            return
        with open("replicas.json", "w") as f:
            json.dump({"replicas": updated_list}, f, indent=2)
        messagebox.showinfo("Success", f"Replicas.json updated with {len(updated_list)} servers.")

    def on_exit(self):
        self.cleanup()
        self.root.destroy()

    def cleanup(self):
        self.polling_active = False
        if self.polling_thread and self.polling_thread.is_alive():
            self.polling_thread.join(timeout=1.0)

def run_server(replica_config):
    from concurrent import futures
    import grpc
    import protos.blog_pb2_grpc as pb2_grpc

    server_obj = Server(replica_config)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_BlogServicer_to_server(server_obj, grpc_server)
    grpc_server.add_insecure_port(f"{replica_config['host']}:{replica_config['port']}")
    grpc_server.start()
    print(f"Replica {replica_config['id']} started on port {replica_config['port']}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Server stopping...")
        server_obj.stop()
        grpc_server.stop(0)

if __name__ == "__main__":
    root = tk.Tk()
    app = BlogAppGUI(root)

    def handle_exit_signal(signum, frame):
        print("Received exit signal. Exiting gracefully...")
        app.on_exit()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_exit_signal)
    signal.signal(signal.SIGTERM, handle_exit_signal)
    root.mainloop()