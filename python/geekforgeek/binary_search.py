print('imported')

def binary_search(arr, k, left, right):
    mid = (left + right + 1) / 2

    if k == arr[mid]:
        return mid
    elif k < arr[mid]:
        return binary_search(arr[:mid - 1], k, left, mid - 1)
    else:
        return binary_search(arr[mid + 1:], k, mid + 1, right)


