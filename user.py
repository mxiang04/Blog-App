class User:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.posts = []
        self.subscriptions = []  # users this user follows
        self.followers = []      # users following this user
        self.email = None
        self.unread_notifications = []  # Unread notifications