from typing import List
from collections import Counter


class Solution:
    def singleNumber(self, nums: List[int]) -> int:
        nums = Counter(nums)

        for num, frequency in nums.items():
            if frequency == 1:
                return num
        return -1

    # def singleNumber(self, nums: List[int]) -> int:
    #     container = {}
    #     for num in nums:
    #         if container.get(num):
    #             container[num] += 1
    #         else:
    #             container[num] = 1
    #     for num, count in container.items():
    #         if count == 1:
    #             return num
    #
    #     return -1


print(Solution().singleNumber([1, 2, 1, 2, 3]))
