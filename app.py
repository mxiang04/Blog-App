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
        self.notification_frame = tk.Frame(root)
        self.notification_frame.pack(side="bottom", fill="x", padx=5, pady=5)

        self.notification_header = tk.Label(
            self.notification_frame,
            text="Notifications",
            font=("Arial", 10, "bold"),
            fg="white",
        )
        self.notification_header.pack(side="top", anchor="w", padx=5)

        self.notification_text = scrolledtext.ScrolledText(
            self.notification_frame,
            height=3,
            width=50,
            font=("Arial", 10),
            wrap=tk.WORD,
        )
        self.notification_text.pack(side="left", fill="x", expand=True)
        self.notification_frame.pack_forget()

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
        self.notification_frame.pack_forget()
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

        # Start polling for notifications in background
        self.polling_active = True
        self.polling_thread = threading.Thread(target=self.background_poll, daemon=True)
        self.polling_thread.start()

        self.login_menu()

    def background_poll(self):
        while self.polling_active:
            try:
                if self.client and self.client.username:
                    # Get notifications
                    notifications = self.client.get_notifications()
                    if notifications:
                        self.root.after(0, lambda: self.update_notifications(notifications))
                time.sleep(1.0)
            except:
                time.sleep(1.0)

    def update_notifications(self, notifications):
        self.notification_text.delete(1.0, tk.END)
        for notif in notifications:
            t = notif.timestamp
            from_user = getattr(notif, 'from')
            notif_type = notif.type
            post_title = notif.title
            self.notification_text.insert(tk.END, f"[{t}] {from_user} {notif_type}: {post_title}\n")
        self.notification_text.see(tk.END)

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
            messagebox.showinfo("Success", f"Login successful! {unread} unread notifications.")
            self.notification_frame.pack(side="bottom", fill="x", padx=5, pady=5)
            self.notification_header.pack(side="top", anchor="w", padx=5)
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
            self.main_frame, text="View Notifications", command=self.view_notifications, width=20
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
                text=f"❤️ {post.likes}",
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
            def like_post_callback(post_id=post.post_id, label=likes_label):
                if self.client.like_post(post_id):
                    current_likes = int(label.cget("text").split(" ")[1]) + 1
                    label.config(text=f"❤️ {current_likes}")
                else:
                    messagebox.showerror("Error", "Failed to like post.")
            
            tk.Button(
                button_frame,
                text="Like",
                command=like_post_callback
            ).pack(side="left", padx=2)
            
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
                
            # Comments section
            if post.comments:
                tk.Label(
                    post_frame,
                    text=f"Comments ({len(post.comments)})",
                    font=("Arial", 10, "bold")
                ).pack(anchor="w", padx=5, pady=2)
                
                for comment in post.comments:
                    comment_frame = tk.Frame(post_frame, relief=tk.SUNKEN, borderwidth=1)
                    comment_frame.pack(fill="x", padx=15, pady=2)
                    
                    tk.Label(
                        comment_frame,
                        text=f"{comment.author} ({comment.timestamp[:19]}): {comment.content}",
                        font=("Arial", 9),
                        wraplength=500,
                        anchor="w",
                        justify=tk.LEFT
                    ).pack(fill="x", padx=5, pady=2)
    
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
        
        # Create tabs for subscriptions and followers
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

    def view_notifications(self):
        notifications = self.client.get_notifications()
        if not notifications:
            messagebox.showinfo("Notifications", "You have no notifications.")
            return
        
        notif_window = tk.Toplevel(self.root)
        notif_window.title("Your Notifications")
        notif_window.geometry("500x400")
        
        tk.Label(notif_window, text="Notifications:", font=("Arial", 12, "bold")).pack(pady=5)
        
        listbox = scrolledtext.ScrolledText(
            notif_window,
            width=60,
            height=15,
            font=("Arial", 10),
            wrap=tk.WORD
        )
        listbox.pack(pady=10, fill="both", expand=True)
        
        for notif in notifications:
            post_id = notif.post_id
            from_user = getattr(notif, 'from')
            notif_type = notif.type
            title = notif.title
            ts = notif.timestamp
            
            listbox.insert(tk.END, f"[{ts}] {from_user} {notif_type}: {title} (Post ID: {post_id})\n")
            listbox.insert(tk.END, "-" * 50 + "\n")
        
        listbox.config(state=tk.DISABLED)

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
            self.notification_frame.pack_forget()
            self.login_menu()
        else:
            messagebox.showerror("Error", "Log out unsuccessful!")

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
        # Go back
        tk.Button(self.main_frame, text="Back", command=self.start_menu).pack(pady=5)

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