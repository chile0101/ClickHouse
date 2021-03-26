class Solution:
    def lengthOfLongestSubstring(self, s: str) -> int:
        length = len(s)
        if length < 2:
            return length

        longest = 1

        i = 0
        j = i + 1
        while i < length:

            while j < length:

                check = self.in_string(s[j], s[i:j], start=i)
                if check == -1:
                    j += 1
                    longest = max(longest, j - i)
                    continue
                else:
                    j += 1
                    i = check
                    break

            i += 1

        return longest

    def in_string(self, c, string: str, start):
        for i, s in enumerate(string, start=start):
            if c == s:
                return i
        return -1


s = "abcabcbb"
s = "pmnhkwlhwkew"
s = "pwwkew"

print(Solution().lengthOfLongestSubstring(s))




