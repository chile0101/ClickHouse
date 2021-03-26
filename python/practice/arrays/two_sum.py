from typing import List


# solution
class Solution:
    def twoSum(self, nums: List[int], target: int) -> List[int] or None:
        for n, i in enumerate(nums):
            number_to_find = target - n

            for m, j in enumerate(nums[i], start=i + 1):
                if nums[j] == number_to_find:
                    return [i, j]
        return None


lst = [1, 3, 7, 9, 2]
t = 11
expect = [3, 4]

print(Solution().twoSum(lst, t))


def test_happy_case():
    lst = [1, 3, 7, 9, 2]
    t = 11
    expect = [3, 4]


def test_happy_case_1():
    lst = [1, 6]
    t = 7
    expect = [0, 1]


def test_no_solution_exist():
    lst = [1, 3, 7, 9, 2]
    t = 25
    expect = None


def test_lst_is_empty():
    lst = []
    t = 1
    expect = None


def test_result_is_only_sum_a_number():
    lst = [1]
    t = 1
    expect = None
