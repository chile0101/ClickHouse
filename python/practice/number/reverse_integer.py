class Solution:
    def reverse(self, x: int) -> int:

        # [-231, 231 - 1]

        if -10 < x < 10:
            return x

        if x <= -10:
            positive = False
        else:
            positive = True

        result = 0

        n = abs(x)

        while n != 0:
            result = result * 10 + n % 10
            n = n // 10

        result = result if positive else -result

        if result < -2 ** 31 or result > 2 ** 31 - 1:
            return 0



x = 123
x = -123
x = 120
x = 0
x = -10
x = 1534236469
    2147483647

print(Solution().reverse(x))
