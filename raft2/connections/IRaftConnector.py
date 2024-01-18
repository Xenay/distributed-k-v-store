from abc import ABC, abstractmethod
from typing import List

from raft2.components.LogEntry import LogEntry
from raft2.components.Result import Result
    # Importing Result class

class IRaftConnector(ABC):
    @property
    @abstractmethod
    def node_id(self) -> int:
        pass

    @abstractmethod
    async def make_request(self, command: str):
        pass

    @abstractmethod
    async def request_vote(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> Result[bool]:
        pass

    @abstractmethod
    async def append_entries(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int, 
                             entries: List[LogEntry], leader_commit: int) -> Result[bool]:
        pass

    @abstractmethod
    async def test_connection(self):
        pass