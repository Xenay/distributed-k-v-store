

from raft2.stateMachine.IRaftStateMachine import IRaftStateMachine


class DictionaryStateMachine(IRaftStateMachine):
    def __init__(self):
        self.state = {}

    def apply(self, command: str):
        parts = command.split(" ")
        try:
            if parts[0].upper() == "SET" and len(parts) == 3:
                self.state[parts[1]] = int(parts[2])
            elif parts[0].upper() == "CLEAR" and len(parts) == 2:
                self.state.pop(parts[1], None)  # Remove the key if it exists
        except ValueError:
            pass  # Ignore bad requests

    def request_status(self, param: str) -> str:
        return str(self.state.get(param, ""))

    def test_connection(self):
        test_state = {"X": 0}
        test_state.clear()
