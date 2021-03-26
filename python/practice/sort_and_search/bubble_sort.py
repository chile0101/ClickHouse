def bubble_sort(arr):
    length = len(arr)

    for i in range(length - 1):
        for j in range(length - 1 - i):

            if arr[j] > arr[j + 1]:
                temp = arr[j]
                arr[j] = arr[j + 1]
                arr[j + 1] = temp


array = [3, 7, 8, 5, 2, 1, 9, 5, 4]
bubble_sort(array)

print(array)

# Độ phức tạp thuật toán
# •Trường hợp tốt: O(n)
# •Trung bình: O(nˆ2)
# •Trường hợp xấu: O(nˆ2)
# Không gian bộ nhớ sử dụng: O(1)
