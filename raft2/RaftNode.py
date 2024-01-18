from typing import Optional, List, Dict
import asyncio
import random

from raft2.components import LogEntry

class RaftNode:
    def __init__(self, node_id: int, state_machine):
        self.node_id = node_id
        self.state_machine = state_machine
        self.log = []  # List of LogEntry, assuming LogEntry is defined elsewhere
        self.node_state = "Stopped"
        self.leader_id = None
        self.voted_for = None
        self.commit_index = -1
        self.last_applied = -1
        self.current_term = 0
        self.next_index = {}  # Dictionary[uint, int]
        self.match_index = {}  # Dictionary[uint, int]
        self.election_timeout_ms = None
        self.election_timer = None
        self.heartbeat_timer = None

    def configure(self, cluster, election_timeout_ms):
        self.cluster = cluster
        self.election_timeout_ms = election_timeout_ms
        self.node_state = "Follower"

    async def run(self):
        if self.cluster is None:
            raise Exception("Cluster must be configured before running.")
        if self.node_state == "Candidate":
            await self.start_election()
        elif self.node_state == "Leader":
            self.reset_heartbeat_timer()
        elif self.node_state == "Follower":
            self.reset_election_timer()

    async def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if self.node_state == "Stopped":
            return (False, self.current_term)
        if term < self.current_term:
            self.log_message("Received AppendEntries with outdated term. Declining.")
            return (False, self.current_term)
        if entries and self.log and self.log[prev_log_index].term_number != prev_log_term:
            return (False, self.current_term)
        self.stop_heartbeat_timer()
        self.reset_election_timer()
        self.current_term = term
        self.node_state = "Follower"
        self.leader_id = leader_id
        if entries:
            self.log = self.log[:prev_log_index + 1] + entries
            self.log_message(f"Node {self.node_id} appending new entry {entries[0].command}")
        else:
            self.log_message(f"Node {self.node_id} received heartbeat from {leader_id}")
        if leader_commit > self.commit_index:
            self.apply_entries(leader_commit)
        return (True, self.current_term)

    async def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if self.node_state == "Stopped":
            return (False, self.current_term)
        self.log_message(f"Node {candidate_id} is requesting vote from node {self.node_id}")
        vote_granted = False
        if term >= self.current_term:
            self.stop_heartbeat_timer()
            self.reset_election_timer()
            self.current_term = term
            if (self.voted_for is None or self.voted_for == candidate_id) and self.is_candidate_up_to_date(last_log_index, last_log_term):
                vote_granted = True
                self.voted_for = candidate_id
        return (vote_granted, self.current_term)

    def apply_entries(self, leader_commit):
        to_apply = self.log[self.commit_index + 1:leader_commit]
        for entry in to_apply:
            self.state_machine.apply(entry.command)
        self.commit_index = min(leader_commit, len(self.log) - 1)
        self.last_applied = self.commit_index

    def is_candidate_up_to_date(self, last_log_index, last_log_term):
        return last_log_index >= len(self.log) - 1 and last_log_term >= self.get_last_log_term()

    def get_last_log_term(self):
        return self.log[-1].term_number if self.log else 0

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = asyncio.get_event_loop().call_later(self.election_timeout_ms / 1000, self.trigger_election)

    def reset_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = asyncio.get_event_loop().call_later(self.election_timeout_ms / 2000, self.send_heartbeats)

    async def trigger_election(self):
        self.node_state = "Candidate"
        # Election logic to be implemented


    def log_message(self, msg):
        print(msg)  # Simplified logging for demonstration

    # Other methods like Stop, Restart, MakeRequest, etc. are to be implemented similarly
    async def start_election(self):
        self.current_term += 1
        self.voted_for = self.node_id
        vote_count = 1
        nodes = self.cluster.get_node_ids_except(self.node_id)
        votes = await self.request_votes_from_nodes(nodes)
        vote_count += votes
        if vote_count >= self.get_majority():
            self.log_message(f"New leader!! : {self.node_id} with {vote_count} votes")
            self.leader_id = self.node_id
            self.node_state = "Leader"
            await self.run()

    async def request_votes_from_nodes(self, nodes):
        votes = 0
        for node_id in nodes:
            res = await self.cluster.request_vote_from(node_id, self.current_term, self.node_id, len(self.log) - 1, self.get_last_log_term())
            self.current_term = max(self.current_term, res.term)
            if res.value:
                votes += 1
        return votes

    def get_majority(self):
        return (len(self.cluster.nodes) + 1) // 2

    async def send_heartbeats(self):
        nodes = self.cluster.get_node_ids_except(self.node_id)
        for node_id in nodes:
            prev_log_index = max(0, self.next_index.get(node_id, len(self.log)) - 1)
            prev_log_term = self.log[prev_log_index].term_number if self.log else 0
            entries = self.log[self.next_index.get(node_id, len(self.log)):] if self.log else None
            res = await self.cluster.send_append_entries_to(node_id, self.current_term, self.node_id, prev_log_index, prev_log_term, entries, self.commit_index)
            self.current_term = max(self.current_term, res.term)
            if res.value and entries:
                self.next_index[node_id] = len(self.log)
                self.match_index[node_id] = len(self.log) - 1
            elif not res.value and self.next_index[node_id] > 0:
                self.next_index[node_id] -= 1

        await self.check_commit_entries()

    async def check_commit_entries(self):
        for i in range(self.commit_index + 1, len(self.log)):
            replicated_in = sum(1 for x in self.match_index.values() if x >= i) + 1
            if self.log[i].term_number == self.current_term and replicated_in >= self.get_majority():
                self.commit_index = i
                self.state_machine.apply(self.log[i].command)
                self.last_applied = i

    def restart(self):
        if self.node_state == "Stopped":
            self.log_message(f"Restarting node {self.node_id}")
            self.node_state = "Follower"
            asyncio.ensure_future(self.run())

    def stop(self):
        if self.node_state != "Stopped":
            self.log_message(f"Bringing node {self.node_id} down")
            self.node_state = "Stopped"
            asyncio.ensure_future(self.run())

    def make_request(self, command):
        if self.node_state == "Leader":
            self.log_message("This node is the leader")
            entry = LogEntry(self.current_term, len(self.log), command)
            self.log.append(entry)
        elif self.node_state == "Follower" and self.leader_id is not None:
            self.log_message(f"Redirecting to leader {self.leader_id} by {self.node_id}")
            self.cluster.redirect_request_to_node(command, self.leader_id)
        else:
            self.log_message("Couldn't find a leader. Dropping request.")

    def get_committed_entries(self):
        return self.log[:self.commit_index + 1]

    def test_connection(self):
        self.state_machine.test_connection()

    def __str__(self):
        state = f"Follower (of {self.leader_id})" if self.node_state == "Follower" else self.node_state
        return f"Node ({self.node_id}) -- {state}"