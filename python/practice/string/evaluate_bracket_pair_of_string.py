from typing import List


def two_dimensions_to_dict(lst: List[List[str]]):
    return {kv[0]: kv[1] for kv in lst}


class Solution:
    def evaluate(self, s: str, knowledge: List[List[str]]) -> str:
        knowledge = two_dimensions_to_dict(knowledge)
