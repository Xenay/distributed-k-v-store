
from raft2.stateMachine.IRaftStateMachine import IRaftStateMachine


class NumeralStateMachine(IRaftStateMachine):
    def __init__(self):
        self.state = 0

    def apply(self, command: str):
        try:
            delta = int(command)
            self.state += delta
        except ValueError:
            # Silently ignore bad requests
            pass
        except OverflowError:
            # Silently ignore bad requests
            pass

    def request_status(self, param: str) -> str:
        return str(self.state)

    def test_connection(self):
        # Dummy method for testing connections
        test_state = 0
        test_state += int("-1")  # Just a dummy operation
