from typing import Generic, TypeVar

T = TypeVar('T')

class Result(Generic[T]):
    def __init__(self, value: T, term: int):
        self.value = value
        self.term = term