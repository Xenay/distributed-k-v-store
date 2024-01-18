import asyncio
import random
from typing import List

from raft2.components.LogEntry import LogEntry
from raft2.components.Result import Result



class RaftCluster:
    def __init__(self):
        self.nodes = []  # List of objects implementing IRaftConnector

    @property
    def size(self) -> int:
        return len(self.nodes)

    def add_node(self, node):
        self.nodes.append(node)

    async def redirect_request_to_node(self, command: str, leader_id: int):
        node = next((x for x in self.nodes if x.node_id == leader_id), None)
        if node:
            await node.make_request(command)

    def calculate_election_timeout_ms(self) -> int:
        broadcast_time = self.calculate_broadcast_time_ms()
        return random.randint(broadcast_time * 12, broadcast_time * 24)

    async def calculate_broadcast_time_ms(self) -> int:
        start_time = asyncio.get_event_loop().time()
        await asyncio.gather(*(node.test_connection() for node in self.nodes))
        elapsed_ms = (asyncio.get_event_loop().time() - start_time) * 1000
        return max(25, int(elapsed_ms))

    def get_node_ids_except(self, node_id: int) -> List[int]:
        return [x.node_id for x in self.nodes if x.node_id != node_id]

    async def send_append_entries_to(self, node_id: int, term: int, leader_id: int, prev_log_index: int, 
                                     prev_log_term: int, entries: List[LogEntry], leader_commit: int) -> Result[bool]:
        node = next((x for x in self.nodes if x.node_id == node_id), None)
        if node:
            return await node.append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
        return Result(False, 0)

    async def request_vote_from(self, node_id: int, term: int, candidate_id: int, last_log_index: int, 
                                last_log_term: int) -> Result[bool]:
        node = next((x for x in self.nodes if x.node_id == node_id), None)
        if node:
            return await node.request_vote(term, candidate_id, last_log_index, last_log_term)
        return Result(False, 0)
