from typing import Callable, List, Tuple, Union

import pandas as pd


def replace_with_elements(list: List, idx: int, elements: List, inplace: bool = False):
    """
    replace the elements in the list starting from the idx with the elements in the elements list
    """
    if inplace:
        list.pop(idx)
        list[idx:idx] = elements
        return list
    else:
        _list = list.copy()
        _list.pop(idx)
        _list[idx:idx] = elements
        return _list


def find_consecutive_with_condition(items: Union[pd.DataFrame, List], condition: Callable) -> List[Tuple[int, int]]:
    """
    Find consecutive rows with the true condition
    
    Args:
        df: pandas DataFrame containing the data
        condition: Callable that takes a row and returns True/False
        
    Returns:
        List of tuples, each containing (start_idx, length)
            start_idx: Index where consecutive True values start
            length: Number of consecutive True values
    """
    consecutive_periods = []
    start_idx = None
    
    for idx, item in enumerate(items):
        # Handle DataFrame specifically
        if isinstance(items, pd.DataFrame):
            item = items.iloc[idx]
            
        if condition(item):
            if start_idx is None:
                start_idx = idx
        elif start_idx is not None:
            # Found the end of a period where condition was True
            length = idx - start_idx
            consecutive_periods.append((start_idx, length))
            start_idx = None
    
    # Handle case where condition extends to the end
    if start_idx is not None:
        length = len(items) - start_idx
        consecutive_periods.append((start_idx, length))
    
    return consecutive_periods