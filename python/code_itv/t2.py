import re


def validate(username):
    if len(username) < 4:
        return False

    if re.search(r"\s", username):
        return False

    regex = "[A-Za-z][a-zA-Z0-9]*_?[^a-zA-Z0-9]+"
    return bool(re.search(regex, username))


print(validate("Mike_Standish"))  # Valid username
print(validate("Mike Standish"))  # Invalid username
