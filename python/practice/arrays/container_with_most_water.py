from typing import List


class Solution:
    def maxArea(self, height: List[int]) -> int:

        max_area = 0
        for i, y in enumerate(height):

            for j, y_next in enumerate(height[i + 1:], start=i + 1):
                area = min(y, y_next) * (j - i)
                if area > max_area:
                    max_area = area
        return max_area

    def maxArea1(self, height: List[int]) -> int:

        max_area = 0
        for i, y in enumerate(height):

            for j, y_end in reversed(list(enumerate(height))):
                if i >= j:
                    continue

                area = min(y, y_end) * (j - i)
                if area > max_area:
                    max_area = area
        return max_area


lst = [1, 8, 6, 2, 5, 4, 8, 3, 7]

print(Solution().maxArea1(lst))