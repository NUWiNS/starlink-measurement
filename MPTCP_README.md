# MPTCP Measurement Data Format

This folder contains MPTCP (Multipath TCP) measurement data comparing different network operators. The data is stored in CSV format with two main types of measurements: throughput and RTT (Round Trip Time).

## File Structure

Files are named following the pattern:
- `fused_trace.tcp_downlink.[operator1]_[operator2].csv` - For throughput measurements
- `fused_trace.tcp_uplink.[operator1]_[operator2].csv` - For throughput measurements
- `fused_trace.ping.[operator1]_[operator2].csv` - For RTT measurements

- For Alaska, we have 3 operators: starlink, att, and verizon.
- For Hawaii, we have 4 operators: starlink, att, verizon, and tmobile.

## Data Format

### Common Columns
Both operators (A and B) share similar column structures with their respective prefixes:

| Column | Description |
|--------|-------------|
| A/B | Operator identifier (e.g., starlink, verizon, att) |
| A_run/B_run | Measurement run identifier in format YYYYMMDD_HHMMSS |
| A_time/B_time | Timestamp of measurement in ISO format with timezone |
| A_actual_tech/B_actual_tech | Network technology (e.g., LTE, 5G-low). May be empty due to mapping mismatches |

### Measurement-Specific Columns

#### Throughput Files
- `A_throughput_mbps/B_throughput_mbps`: Throughput measurement in Megabits per second

#### RTT Files
- `A_rtt_ms/B_rtt_ms`: Round Trip Time measurement in milliseconds

## Notes

1. The `actual_tech` field may contain empty values when there is no successful mapping between application throughput data and xcal throughput data.

2. Each row represents a synchronized measurement between two operators (A and B) at approximately the same time.

3. Run identifiers help group measurements that were taken during the same test session.