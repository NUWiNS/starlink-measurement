
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from scripts.cell_leo_in_remote_us.cell_downlink_tput.main import main as cell_downlink_tput_main
from scripts.cell_leo_in_remote_us.cell_uplink_tput.main import main as cell_uplink_tput_main
from scripts.cell_leo_in_remote_us.cell_icmp_latency.main import main as cell_icmp_latency_main

from scripts.cell_leo_in_remote_us.cell_tech_distribution.main import main as cell_tech_distribution_main
from scripts.cell_leo_in_remote_us.cell_tcp_dl_with_areas.main import main as cell_tcp_dl_with_areas_main
from scripts.cell_leo_in_remote_us.cell_tcp_ul_with_areas.main import main as cell_tcp_ul_with_areas_main
from scripts.cell_leo_in_remote_us.cell_icmp_latency_with_areas.main import main as cell_icmp_latency_with_areas_main


from scripts.cell_leo_in_remote_us.all_operator_downlink_tput.main import main as all_operator_downlink_tput_main
from scripts.cell_leo_in_remote_us.all_operator_uplink_tput.main import main as all_operator_uplink_tput_main
from scripts.cell_leo_in_remote_us.starlink_downlink_tput.main import main as starlink_downlink_tput_main
# from scripts.cell_leo_in_remote_us.starlink_uplink_tput import main as starlink_uplink_tput_main
from scripts.cell_leo_in_remote_us.starlink_icmp_latency.main import main as starlink_icmp_latency_main



def main():
  # Cellular
  cell_downlink_tput_main()
  cell_uplink_tput_main()
  cell_icmp_latency_main()

  # Cellular with areas and tech breakdown
  # cell_tech_distribution_main()
  # cell_tcp_dl_with_areas_main()
  # cell_tcp_ul_with_areas_main()
  # cell_icmp_latency_with_areas_main()

  # Starlink
  all_operator_downlink_tput_main()
  all_operator_uplink_tput_main()
  starlink_downlink_tput_main()
  starlink_icmp_latency_main()


if __name__ == '__main__':
  main()
