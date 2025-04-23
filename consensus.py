import json
import os
import grpc
from protos import blog_pb2, blog_pb2_grpc
from typing import List

def get_replicas_config():
    """
    Reads replicas.json and returns a list of dictionaries with the config for each replica.
    """
    try:
        with open("replicas.json", "r") as f:
            data = json.load(f)
            return data.get("replicas", [])
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

def get_replica_by_id(replica_id):
    """
    Return the config dict for the given replica_id, or None.
    """
    replicas = get_replicas_config()
    for r in replicas:
        if r["id"] == replica_id:
            return r
    return None

def build_stub(host, port):
    """
    Create a gRPC stub for the given host/port.
    """
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = blog_pb2_grpc.BlogStub(channel)
    return stub

class RaftLogEntry:
    """
    A single Raft log entry in memory. 
    This parallels blog_pb2.RaftLogEntry but stored in Python object form.
    """
    def __init__(self, term, operation, params):
        self.term = term
        self.operation = operation
        self.params = params  # e.g. [username, password], etc.

    def to_dict(self):
        return {
            "term": self.term,
            "operation": self.operation,
            "params": self.params
        }

    @staticmethod
    def from_dict(d):
        return RaftLogEntry(d["term"], d["operation"], d["params"])

class RaftNode:
    """
    This class holds the persistent and in-memory Raft state for one replica.

    * Persistent state on all servers:
      - currentTerm
      - votedFor
      - log[]

    * Volatile state on all servers:
      - commitIndex
      - lastApplied

    * Volatile state on leaders:
      - nextIndex[] (for each follower)
      - matchIndex[] (for each follower)
    """
    def __init__(self, replica_id, raft_store_path):
        self.replica_id = replica_id
        self.raft_store_path = raft_store_path

        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log: List[RaftLogEntry] = []

        # Volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # Leader state
        self.nextIndex = {}
        self.matchIndex = {}

        # Role: follower, candidate, or leader
        self.role = "follower"

        # Load persistent state from file if it exists
        self.load_raft_state()

    def load_raft_state(self):
        """
        Loads persistent Raft state (term, vote, log) from a JSON file.
        """
        if not os.path.exists(self.raft_store_path):
            return

        try:
            with open(self.raft_store_path, "r") as f:
                data = json.load(f)
                self.currentTerm = data.get("currentTerm", 0)
                self.votedFor = data.get("votedFor", None)
                log_data = data.get("log", [])
                self.log = [RaftLogEntry.from_dict(e) for e in log_data]
        except:
            pass

    def save_raft_state(self):
        """
        Persists currentTerm, votedFor, and the log to the JSON store.
        """
        data = {
            "currentTerm": self.currentTerm,
            "votedFor": self.votedFor,
            "log": [e.to_dict() for e in self.log],
        }
        
        # Write to a temporary file first, then rename for atomic operation
        temp_path = f"{self.raft_store_path}.tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            
            # Atomic rename operation
            os.replace(temp_path, self.raft_store_path)
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            raise e

    def last_log_index(self):
        return len(self.log)

    def last_log_term(self):
        if len(self.log) == 0:
            return 0
        return self.log[-1].term

    def append_entries_to_log(self, prevLogIndex, prevLogTerm, entries: List[RaftLogEntry]):
        """
        Attempt to append new entries, after verifying the existing log at prevLogIndex matches prevLogTerm.
        Returns True if successful, False if there's a mismatch.
        """
        # Special case for empty log (prevLogIndex=0)
        if prevLogIndex == 0:
            # If we're starting from the beginning, just accept the entries
            if len(entries) > 0:
                # But first, check if we need to truncate our log
                if len(self.log) > 0:
                    # Check if there's a term mismatch at the first position
                    if len(entries) > 0 and self.log[0].term != entries[0].term:
                        self.log = []  # Clear log if conflict at the first entry
                
                # Append all new entries
                self.log.extend(entries)
                self.save_raft_state()
            return True
        
        # If the leader's log is ahead of ours
        if prevLogIndex > len(self.log):
            return False
        
        # Check for term match at prevLogIndex
        if prevLogIndex > 0 and self.log[prevLogIndex - 1].term != prevLogTerm:
            return False
        
        # Process new entries
        new_index = 0
        for i in range(prevLogIndex, prevLogIndex + len(entries)):
            new_entry = entries[new_index]
            new_index += 1
            
            if i < len(self.log):
                # Check for term conflict
                if self.log[i].term != new_entry.term:
                    # Conflict: truncate log and append new entry
                    self.log = self.log[:i]
                    self.log.append(new_entry)
            else:
                # Beyond current log length: just append
                self.log.append(new_entry)
        
        self.save_raft_state()
        return True