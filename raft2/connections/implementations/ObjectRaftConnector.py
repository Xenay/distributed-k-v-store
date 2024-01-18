

from raft2.RaftNode import RaftNode
from raft2.components.Result import Result
from raft2.connections.implementations.APIRaftConnector import APIRaftConnector


class ObjectRaftConnector(APIRaftConnector):
    def __init__(self, node_id: int, node: RaftNode):
        self.node_id = node_id
        self.node = node

    async def make_request(self, command: str):
        self.node.make_request(command)

    async def request_vote(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> Result[bool]:
        return self.node.request_vote(term, candidate_id, last_log_index, last_log_term)

    async def append_entries(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int, 
                             entries, leader_commit: int) -> Result[bool]:
        return self.node.append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)

    async def test_connection(self):
        self.node.test_connection()
