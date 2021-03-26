class Solution:
    def longestPalindrome(self, s: str) -> str:

        max_s = s[0]

        i = 0
        while i < len(s) - 1:

            j = i + 1
            while j < len(s):
                sample = s[i: j + 1]
                if is_palindrome(sample):
                    max_s = sample if len(max_s) < len(sample) else max_s

                j += 1

            i += 1

        return max_s


def is_palindrome(s: str):
    i = 0
    j = len(s) - 1

    while i < j:

        if s[i] != s[j]:
            return False

        i += 1
        j -= 1

    return True

s = 'abccba'

print(Solution().longestPalindrome(s))
