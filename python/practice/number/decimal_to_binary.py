class Solution:
    def solve(self, n):
        binary = "{0:b}".format(int(n))

        ones = binary.split('0')

        max_one = max(ones, key=len)
        return len(max_one)


# def decimalToBinary(n):
#     return "{0:b}".format(int(n))
#
#
# def decimal_to_binary(num):
#     if num >= 1:
#         decimal_to_binary(num // 2)
#         print(num % 2, end='')


# print(decimal_to_binary(156))

print(Solution().solve(156))
