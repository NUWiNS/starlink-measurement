import unittest
import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.utilities.list_utils import replace_with_elements, find_consecutive_with_condition


class ListUtilTest(unittest.TestCase):
    def test_replace_with_elements(self):
        list = [1, 2, 3, 4, 5]
        replace_idx = 2
        replace_elements = [6, 7]
        expected_list = [1, 2, 6, 7, 4, 5]
        self.assertEqual(replace_with_elements(list, replace_idx, replace_elements), expected_list)

    def test_find_consecutive_with_condition_basic(self):
        # Test basic consecutive values
        df = pd.DataFrame({
            'value': [1, 2, 2, 2, 1, 2, 2, 1]
        })
        condition = lambda row: row['value'] == 2
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [(1, 3), (5, 2)])

    def test_find_consecutive_with_condition_empty(self):
        # Test with no matching conditions
        df = pd.DataFrame({
            'value': [1, 1, 1, 1]
        })
        condition = lambda row: row['value'] == 2
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [])

    def test_find_consecutive_with_condition_all_true(self):
        # Test when all values match condition
        df = pd.DataFrame({
            'value': [2, 2, 2, 2]
        })
        condition = lambda row: row['value'] == 2
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [(0, 4)])

    def test_find_consecutive_with_condition_single_values(self):
        # Test with isolated single values
        df = pd.DataFrame({
            'value': [1, 2, 1, 2, 1]
        })
        condition = lambda row: row['value'] == 2
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [(1, 1), (3, 1)])

    def test_find_consecutive_with_condition_end_sequence(self):
        # Test with sequence at the end
        df = pd.DataFrame({
            'value': [1, 1, 2, 2, 2]
        })
        condition = lambda row: row['value'] == 2
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [(2, 3)])

    def test_find_consecutive_with_condition_string_operations(self):
        # Test with string operations
        df = pd.DataFrame({
            'tech': ['4G', 'no service', 'no service', '5G', 'no service']
        })
        condition = lambda row: row['tech'].lower() == 'no service'
        result = find_consecutive_with_condition(df, condition)
        self.assertEqual(result, [(1, 2), (4, 1)])


if __name__ == '__main__':
    unittest.main()
