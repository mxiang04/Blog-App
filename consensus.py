import json
import os
import grpc
from protos import app_pb2, app_pb2_grpc
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
    stub = app_pb2_grpc.AppStub(channel)
    return stub

class RaftLogEntry:
    """
    A single Raft log entry in memory. 
    This parallels app_pb2.RaftLogEntry but stored in Python object form.
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

        # persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log: List[RaftLogEntry] = []

        # volatile state
        self.commitIndex = 0
        self.lastApplied = 0

        # leader state
        self.nextIndex = {}
        self.matchIndex = {}

        self.role = "follower"

        # load persistent store
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
        with open(self.raft_store_path, "w") as f:
            json.dump(data, f, indent=2)

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
        # fail if our log is too short
        if prevLogIndex > len(self.log):
            return False

        # check if logs match
        if prevLogIndex > 0:
            if self.log[prevLogIndex - 1].term != prevLogTerm:
                return False

        # If an existing entry conflicts with a new one (same index but different terms),
        # we delete the existing entry and all that follow it. we then append new entries
        idx = prevLogIndex
        for new_entry in entries:
            # if there's a conflict
            if idx < len(self.log):
                if self.log[idx].term != new_entry.term:
                    # delete everything from idx onward
                    self.log = self.log[:idx]
                    self.log.append(new_entry)
                else:
                    # same term, same index => do nothing
                    pass
            else:
                # append new entry
                self.log.append(new_entry)
            idx += 1

        self.save_raft_state()
        return True