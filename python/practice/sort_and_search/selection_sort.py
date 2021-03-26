def selection_sort(arr):
    for i in range(len(arr) - 1):
        min_index = i

        for j in range(i + 1, len(arr)):
            if arr[j] < arr[min_index]:
                min_index = j

        temp = arr[i]
        arr[i] = arr[min_index]
        arr[min_index] = temp


array = [3, 7, 8, 5, 2, 1, 9, 5, 4]

selection_sort(array)

print(array)
