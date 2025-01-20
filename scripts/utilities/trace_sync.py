from datetime import datetime
from typing import List, Tuple, Set

class TimestampMatcher:
    def __init__(self, threshold_seconds: int = 600):
        self.threshold_seconds = threshold_seconds
    
    def convert_to_seconds(self, timestamp_str: str) -> int:
        """Convert timestamp string to seconds since midnight"""
        try:
            dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            return dt.hour * 3600 + dt.minute * 60 + dt.second
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}. Expected format: YYYYMMDD_HHMMSS") from e

    def convert_to_timestamp(self, datetime_str: str) -> int:
        try:
            dt = datetime.strptime(datetime_str, "%Y%m%d_%H%M%S")
            return int(dt.timestamp())
        except ValueError as e:
            raise ValueError(f"Invalid datetime format: {datetime_str}. Expected format: YYYYMMDD_HHMMSS") from e

    def convert_to_datetime_str(self, timestamp: float) -> str:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y%m%d_%H%M%S")

    def greedy_match(self, list_a: List, list_b: List, 
                     threshold: float,
                     key_fn_a=lambda x: x,
                     key_fn_b=lambda x: x,
                     distance_fn=lambda a, b: abs(a - b)) -> Tuple[List[Tuple], List, List]:
        """
        Generic greedy matching function that matches elements from two lists based on a threshold.
        
        Args:
            list_a: First list of elements
            list_b: Second list of elements
            threshold: Maximum allowed distance between matched elements
            key_fn_a: Function to extract comparable value from elements in list_a
            key_fn_b: Function to extract comparable value from elements in list_b
            distance_fn: Function to compute distance between two comparable values
        
        Returns:
            Tuple containing:
            - List of matched pairs (element_a, element_b)
            - List of unmatched elements from list_a
            - List of unmatched elements from list_b
        """
        # Generate all valid pairs with their distances
        pairs = []
        for i, a in enumerate(list_a):
            a_val = key_fn_a(a)
            for j, b in enumerate(list_b):
                b_val = key_fn_b(b)
                dist = distance_fn(a_val, b_val)
                if dist <= threshold:
                    pairs.append((dist, a, b, i, j))
        
        # Sort pairs by distance
        pairs.sort(key=lambda x: x[0])
        
        # Match greedily while maintaining one-to-one constraint
        matched_pairs = []
        used_a = set()
        used_b = set()
        
        for _, a, b, a_idx, b_idx in pairs:
            if a_idx not in used_a and b_idx not in used_b:
                matched_pairs.append((a, b))
                used_a.add(a_idx)
                used_b.add(b_idx)
        
        # Find unmatched elements
        leftover_a = [a for i, a in enumerate(list_a) if i not in used_a]
        leftover_b = [b for i, b in enumerate(list_b) if i not in used_b]
        
        return matched_pairs, leftover_a, leftover_b

    def match_datetimes(self, list_a: List[str], list_b: List[str]) -> Tuple[List[Tuple[str, str]], List[str], List[str]]:
        """
        Match timestamps between two lists based on closest time difference within threshold.
        
        Args:
            list_a: First list of timestamps
            list_b: Second list of timestamps
        
        Returns:
            Tuple containing:
            - List of matched pairs (timestamp_a, timestamp_b)
            - List of unmatched timestamps from list_a
            - List of unmatched timestamps from list_b
        """
        return self.greedy_match(
            list_a=list_a,
            list_b=list_b,
            threshold=self.threshold_seconds,
            key_fn_a=self.convert_to_timestamp,
            key_fn_b=self.convert_to_timestamp
        )



def main():
    # Example usage
    list_a = [
        "20250118_000000",
        "20250118_000900",
        "20250118_002800",
        "20250118_003900",
        "20250118_005100"
    ]
    
    list_b = [
        "20250118_000800",
        "20250118_002900",
        "20250118_000500"
    ]
    
    # Create matcher instance with 10 minutes threshold
    matcher = TimestampMatcher(threshold_minutes=10)
    
    try:
        # Perform matching
        matched_pairs, leftover_a, leftover_b = matcher.match_datetimes(list_a, list_b)
        
        # Print results
        print("Matched pairs:")
        for pair in matched_pairs:
            print(f"{pair[0]} | {pair[1]}")
        
        print("\nUnmatched timestamps from List A:")
        for timestamp in leftover_a:
            print(timestamp)
        
        print("\nUnmatched timestamps from List B:")
        for timestamp in leftover_b:
            print(timestamp)
            
    except ValueError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
