from abc import ABC, abstractmethod

class IRaftStateMachine(ABC):
    @abstractmethod
    def apply(self, command: str):
        """
        Applies a command to the state machine. The State Machine will silently ignore bad commands.
        :param command: Command to apply
        """
        pass

    @abstractmethod
    def request_status(self, param: str) -> str:
        """
        Finds the string representation of the state machine, limited by the given command.
        :param param: Limits, filters or specifies the information to retrieve.
        :return: A string containing the specified representation of the state machine
        """
        pass

    @abstractmethod
    def test_connection(self):
        """
        Used to determine the broadcast time.
        The method must implement a dummy request to the state machine, in order to correctly approximate the broadcast time.
        """
        pass
