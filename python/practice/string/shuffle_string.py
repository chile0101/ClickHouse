from typing import List


class Solution:
    def restoreString(self, s: str, indices: List[int]) -> str:
        l = len(s)
        result = [''] * l

        for i in range(l):
            result[indices[i]] = s[i]

        return ''.join(result)


print(Solution().restoreString("codeleet", [4, 5, 6, 7, 0, 2, 1, 3]))
