import os
import pandas as pd

class DatasetHelper:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def get_tput_data(self, operator: str, protocol: str = '*', direction: str = '*') -> pd.DataFrame:
        """
        Get the throughput data for the given operator, protocol, and direction from the given base directory.
        :param base_dir: The base directory to search for the throughput data.
        :param operator: The operator to filter the throughput data.
        :param protocol: tcp or udp or *, default is *.
        :param direction: downlink or uplink or *, default is *.
        :return: A pandas DataFrame containing the throughput data.
        """
        if protocol == '*': 
            if direction == '*':
                return pd.concat([
                    self.get_single_tput_data(operator, 'tcp', direction='downlink'),
                    self.get_single_tput_data(operator, 'tcp', direction='uplink'),
                    self.get_single_tput_data(operator, 'udp', direction='downlink'),
                    self.get_single_tput_data(operator, 'udp', direction='uplink'),
                ])
            else:
                return pd.concat([
                    self.get_single_tput_data(operator, 'tcp', direction=direction),
                    self.get_single_tput_data(operator, 'udp', direction=direction),
                ])
        else:
            if direction == '*':
                return pd.concat([
                    self.get_single_tput_data(operator, protocol, direction='downlink'),
                    self.get_single_tput_data(operator, protocol, direction='uplink'),
                ])
            else:
                return self.get_single_tput_data(operator, protocol, direction)
    
    def get_single_tput_data(self, operator: str, protocol: str, direction: str) -> pd.DataFrame:
        if protocol not in ['tcp', 'udp']:
            raise ValueError(f'Invalid protocol: {protocol}')
        if direction not in ['downlink', 'uplink']:
            raise ValueError(f'Invalid direction: {direction}')
        csv_filename = f'{operator}_{protocol}_{direction}.csv'
        file_path = os.path.join(self.base_dir, csv_filename)
        df = pd.read_csv(file_path)
        df['operator'] = operator
        df['protocol'] = protocol
        df['direction'] = direction
        return df

    def get_ping_data(self, operator: str) -> pd.DataFrame:
        csv_filename = f'{operator}_ping.csv'
        file_path = os.path.join(self.base_dir, csv_filename)
        df = pd.read_csv(file_path)
        df['operator'] = operator
        return df
