def binary_search(arr, k, left, right):
    if left <= right:
        mid = (right + left) // 2

        if arr[mid] == k:
            return mid

        if arr[mid] < k:
            return binary_search(arr, k, mid + 1, right)

        return binary_search(arr, k, left, mid - 1)

    return -1


array = [2, 3, 4, 10, 40]
x = 50
print(binary_search(array, x, 0, len(array) - 1))  # -1

array = [2, 3, 4, 10, 40]
x = 10
print(binary_search(array, x, 0, len(array) - 1))  # 3

array = [2, 3, 4, 10, 40]
x = 2
print(binary_search(array, x, 0, len(array) - 1))  # 0

array = [2, 3, 4, 10, 40]
x = 40
print(binary_search(array, x, 0, len(array) - 1))  # 40

array = [2, 3]
x = 3
print(binary_search(array, x, 0, len(array) - 1))  # 1

array = [2, 3]
x = 4
print(binary_search(array, x, 0, len(array) - 1))  # -1
