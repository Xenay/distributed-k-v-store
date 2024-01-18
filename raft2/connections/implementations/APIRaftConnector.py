import requests
import json
from typing import List
from raft2.components.LogEntry import LogEntry
from raft2.components.Result import Result

from raft2.connections.IRaftConnector import IRaftConnector


class APIRaftConnector(IRaftConnector):
    def __init__(self, node_id: int, base_url: str):
        self.node_id = node_id
        self.base_url = base_url.rstrip('/') + '/'

    async def make_request(self, command: str):
        data = {'request': command}
        response = requests.post(self.base_url + 'makerequest', data=data)

    async def request_vote(self, term: int, candidate_id: int, last_log_index: int, last_log_term: int) -> Result[bool]:
        data = {
            'term': term,
            'candidateId': candidate_id,
            'lastLogIndex': last_log_index,
            'lastLogTerm': last_log_term
        }
        response = requests.post(self.base_url + 'requestvote', data=data)
        return self.parse_result_from_json(response.text)

    async def append_entries(self, term: int, leader_id: int, prev_log_index: int, prev_log_term: int, 
                             entries: List[LogEntry], leader_commit: int) -> Result[bool]:
        entries_data = [entry.__dict__ for entry in entries]
        data = {
            'term': term,
            'leaderId': leader_id,
            'prevLogIndex': prev_log_index,
            'prevLogTerm': prev_log_term,
            'entries': json.dumps(entries_data),
            'leaderCommit': leader_commit
        }
        response = requests.post(self.base_url + 'appendentries', data=data)
        return self.parse_result_from_json(response.text)

    async def test_connection(self):
        response = requests.get(self.base_url + 'test')

    def parse_result_from_json(self, json_str: str) -> Result[bool]:
        data = json.loads(json_str)
        return Result(value=data['value'], term=data['term'])
