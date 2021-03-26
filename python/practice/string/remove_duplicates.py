from typing import List


class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:

        i = 0
        while i < len(nums) - 1:
            j = i + 1
            while j < len(nums):
                if nums[i] == nums[j]:
                    nums.remove(nums[j])
                else:
                    break
            i += 1

        return len(nums)


lst = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4]

print(Solution().removeDuplicates(lst))
print(lst)