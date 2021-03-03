import unittest

from python.geekforgeek.binary_search import binary_search

print('running')


class BinarySearchTest(unittest.TestCase):
    def test_binary_search_arr_empty(self, ):
        lst = []
        value = 1
        print('chile')
        try:
            binary_search(lst, value)
            assert False
        except ValueError:
            assert True


def test_binary_search_value_is_middle_of_list():
    lst = [2, 6, 8]
    value = 6


def test_binary_search_value_is_left_of_list():
    lst = [2, 6, 8]
    value = 1


def test_binary_search_value_is_right_of_list():
    lst = [2, 6, 8]
    value = 8


def test_binary_search_list_have_mutiple_element():
    lst = [2, 6, 8, 11, 13, 15, 16, 20]
    value = 16


def test_binary_search_value_not_in_list_1():
    lst = [2, 6, 8]
    value = 1

    try:
        binary_search(lst, value)
    except ValueError:
        assert True


def test_binary_search_value_not_in_list_2():
    lst = [2, 6, 8]
    value = 10


def test_binary_search_value_not_in_list_3():
    lst = [2, 6, 8]
    value = 7
