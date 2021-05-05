def find_unique_numbers(numbers):
    counter = {}
    for n in numbers:
        if counter.get(n):
            counter[n] += 1
        else:
            counter[n] = 1

    result = []
    for k, v in counter.items():
        result.append

    return result


if __name__ == "__main__":
    print(find_unique_numbers([1, 2, 1, 3]))
