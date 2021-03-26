from typing import List


#
#
# class Solution:
#     def smallerNumbersThanCurrent(self, nums: List[int]) -> List[int]:
#         m = {v: k for k, v in enumerate(sorted(nums))}
#         print(m)
#         result = []
#         for i in nums:
#             result.append(m.get(i))
#         return result
#
# print(Solution().smallerNumbersThanCurrent([8,1,2,2,3]))


class Solution:
    def removeDuplicates(self, nums: List[int]) -> int:

        i = 0
        while i < len(nums) - 1:
            j = i + 1
            while j < len(nums):
                if nums[i] == nums[j]:
                    nums.remove(nums[j])
                j += 1
            i += 1

        return len(nums)


lst = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4]

print(Solution().removeDuplicates(lst))
print(lst)
