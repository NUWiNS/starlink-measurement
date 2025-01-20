
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.cell_leo_in_remote_us.cell_tech_distribution.main import main as cell_tech_distribution_main
from scripts.cell_leo_in_remote_us.cell_tcp_dl_with_areas.main import main as cell_tcp_dl_with_areas_main
from scripts.cell_leo_in_remote_us.cell_tcp_ul_with_areas.main import main as cell_tcp_ul_with_areas_main
from scripts.cell_leo_in_remote_us.cell_icmp_latency_with_areas.main import main as cell_icmp_latency_with_areas_main

def main():
  cell_tech_distribution_main()
  cell_tcp_dl_with_areas_main()
  cell_tcp_ul_with_areas_main()
  cell_icmp_latency_with_areas_main()

if __name__ == '__main__':
  main()
