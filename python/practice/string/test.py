from typing import List


class Solution:
    def countConsistentStrings(self, allowed: str, words: List[str]) -> int:

        result = 0

        for w in words:

            for i in set(w):
                if i not in allowed:
                    break
            else:
                result += 1

        return result


print(Solution().countConsistentStrings("abc",["a","b","c","ab","ac","bc","abc"]))
