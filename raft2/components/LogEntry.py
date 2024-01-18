class LogEntry:
    def __init__(self, term_number: int, index: int, command: str):
        self.term_number = term_number
        self.index = index
        self.command = command