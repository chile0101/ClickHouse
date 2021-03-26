# if len <=2 return 0
# if len == 3: lst[1] < lst[0] and lst[1] < lst[2] return  (min(lst[0], lst[2]) * (2-0-1) ) - 1
#
# i1 = 0
# i2 = None
# # for i, n in enumerate(lst[1:-1], start=1):
# #     if n >= lst[i1]:
# #         i1 = i
# #         continue
# #
# #     for j, m in enumerate(lst[i + 1:], start=i + 1):
# #         if m < n:
# #             break

# i = 1
# i_lower = 0
# result = 0
# while i <= len(lst) - 1:
#     if lst[i_lower] <= lst[i]:
#         i_lower = i
#         i += 1
#         continue
#
#     # lst[i_lower] > lst[i]
#     j = i + 1
#     w = lst[i]
#     i_upper = j
#     while j < len(lst):
#
#         w += lst[j]
#         j += 1
#
#         if lst[i_lower] <= lst[j]:
#             result += lst[i_lower] * (j - i_lower - 1) - w
#             i_lower = j
#             i = j
#             break
#
#         if lst[j] > lst[i_upper]:
#             i_upper = j
#
#
#
#     if j == len(lst) - 1:
#         result += lst[i_upper] * (j - i_lower - 1) - w
#
#
#
#     i += 1
#
# print(result)


# i = 8
# i_upper = i + 1
# i_lower = i - 1
# w = lst[8]
# while i_lower >= 0 and i_upper < len(lst):
#
#     if lst[i_lower] >= lst[i]:
#         i_lower -= 1
#
#     if lst[i_upper + 1] >= lst[i_upper]:
#         i_upper += 1
# ;
#
#

# lst = [0, 3, 2, 0, 0, 2, 8]
# lst = [0, 1, 0, 2, 1, 0, 3, 1, 0, 1, 2]
# lst = [4,2,0,3,2,5]
#
# def one_block(i):
#     if lst[i] > lst[i + 1] and lst[i] > lst[i - 1]:
#         return i, i, 0
#
#     w = lst[i]
#
#     i_max = i
#     while i_max + 1 < len(lst):
#         if lst[i_max + 1] < lst[i_max]:
#             break
#         i_max += 1
#         w += lst[i_max]
#
#     i_min = i
#     while i_min > 0:
#         if lst[i_min - 1] < lst[i_min]:
#             break
#         i_min -= 1
#         w += lst[i_min]
#
#     w = w - lst[i_max] - lst[i_min]
#     water = min(lst[i_min], lst[i_max]) * (i_max - i_min - 1) - w
#     # print(water)
#     return i_max, i_min, water
#
#
# i = 1
# result = 0
# while i < len(lst) - 1:
#     i_max, i_min, w = one_block(i)
#     print(f'i: {i}, i_max: {i_max}, i_min: {i_min}, w: {w}')
#     result += w
#     i += 1
#
# print(result)


def get_trapping_rainwater(heights):
    total_water = 0
    for i, n in enumerate(heights):

        i_left = i
        max_left = 0
        while i_left >= 0:
            max_left = max(max_left, heights[i_left])
            i_left -= 1

        i_right = i
        max_right = 0
        while i_right < len(heights):
            max_right = max(max_right, heights[i_right])
            i_right += 1

        print(f'i: {i}, max_left: {max_left}, max_right: {max_right}')
        current_water = min(max_left, max_right) - heights[i]

        if current_water > 0:
            total_water += current_water

    return total_water


# lst = [0, 1, 0, 2, 1, 0, 3, 1, 0, 1, 2]
lst = [4, 2, 0, 3, 2, 5]
#

print(get_trapping_rainwater(lst))
