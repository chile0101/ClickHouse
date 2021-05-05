import re

# tap
s = 'a\tb'
print(s)

raw_s = r'a\tb'
print(raw_s)

# match
pattern = r"\d+"
text = "a43 is 42 a lucky number 42"

match = re.match(pattern, text)
if match:
    print(match.group(0), 'at index', match.start())
else:
    print('not match')


def is_integer(text):
    pattern = r'\d+$'
    match = re.match(pattern, text)

    if match:
        return True
    return False


print(is_integer('a123'))
print(is_integer('123'))
print(is_integer('123a'))



pattern = r'\d+'
text = 'NY Postal Codes are 10001, 1002, 10003, 124'

match = re.findall(pattern, text)
if match:
    print(match)
else:
    print('no match')
