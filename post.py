from datetime import datetime

class Post:
    def __init__(self, author, title, content, created_at=None):
        self.author = author
        self.title = title
        self.content = content
        self.timestamp = created_at or datetime.now()
        self.likes = []
        self.post_id = None  # Will be assigned when stored
        self.comments = []
        
    def to_dict(self):
        return {
            "author": self.author,
            "title": self.title,
            "content": self.content,
            "timestamp": str(self.timestamp),
            "likes": self.likes,
            "post_id": self.post_id,
            "comments": self.comments
        }
    
    @classmethod
    def from_dict(cls, data):
        post = cls(
            author=data["author"],
            title=data["title"],
            content=data["content"],
            created_at=datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        )
        post.likes = data["likes"]
        post.post_id = data["post_id"]
        post.comments = data["comments"]
        return post

    def like(self, username):
        if username not in self.likes:
            self.likes.append(username)
            return True
        return False
        
    def unlike(self, username):
        if username in self.likes:
            self.likes.remove(username)
            return True
        return False