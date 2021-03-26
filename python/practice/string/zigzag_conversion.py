class Solution:
    def convert(self, s: str, numRows: int) -> str:

        if numRows == 1 or numRows >= len(s) :
            return s

        s_list = [''] * numRows

        i = 0
        j = 0
        increment = True
        while i < len(s):

            s_list[j] += s[i]

            if j == numRows - 1:
                increment = False

            if j == 0:
                increment = True

            if increment:
                j += 1
            else:
                j -= 1

            i += 1

        return ''.join(s_list)


# t1 = Solution().convert("PAYPALISHIRING", 3) == "PAHNAPLSIIGYIR"

print(Solution().convert("ABCDE", 5))
# "PINALSIGYAHRPI"
# "PINALSIGYAHRPI"
# "PINALIGYAIHRNPI"
