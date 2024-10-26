###Client

import logging
import os
import socket
import time
import statistics
from logging_utils import SilentLogger, create_logger

def send_ping(client_socket, message, logger):
    send_time = time.time()
    client_socket.sendall(message.encode())
    # logger.info(f"Sent: {len(message)} bytes at {send_time}")

    ack = client_socket.recv(1024).decode()
    receive_time = time.time()
    # logger.info(f"Received: {len(ack)} bytes at {receive_time}")

    rtt = (receive_time - send_time) * 1000
    # logger.info(f"Round-trip time: {rtt:.2f} ms")
    return rtt

def create_message(packet_size, char="A"):
    return char * packet_size

def start_client(
        host='10.0.0.184', 
        port=65432, 
        packet_size=38,
        packet_count=4,
        interval=1,
        logger: logging.Logger | None = None
    ):
    if logger is None:
        logger = SilentLogger()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        logger.info(f"Connected to {host}:{port}")

        message = create_message(packet_size)
        msg_bytes = message.encode()
        msg_length = len(msg_bytes)
        total_bytes = msg_length + 28  # 28 bytes for IP header (20) and TCP header (8)
        logger.info(f"PING {host} {msg_length}({total_bytes}) bytes of data.")

        rtts = []
        for i in range(packet_count):
            try:
                rtt = send_ping(client_socket, message, logger)
                rtts.append(rtt)
                logger.info(f"{len(message)} bytes from {host}: time={rtt:.2f} ms")
                if i < packet_count - 1:
                    time.sleep(interval)
            except Exception as e:
                logger.error(f"Error sending ping {i+1}: {e}")

        # Calculate statistics
        if rtts:
            min_rtt = min(rtts)
            avg_rtt = statistics.mean(rtts)
            max_rtt = max(rtts)
            stddev_rtt = statistics.stdev(rtts) if len(rtts) > 1 else 0

            logger.info(f"--- {host} ping statistics ---")
            logger.info(f"{packet_count} packets transmitted, {len(rtts)} received, {(packet_count-len(rtts))/packet_count*100:.1f}% packet loss")
            logger.info(f"rtt min/avg/max/mdev = {min_rtt:.3f}/{avg_rtt:.3f}/{max_rtt:.3f}/{stddev_rtt:.3f} ms")

def main():
    host = os.environ.get('SERVER_HOST', '127.0.0.1')
    port = int(os.environ.get('SERVER_PORT', 65432))
    packet_size = int(os.environ.get('PACKET_SIZE', 38))
    packet_count = int(os.environ.get('PACKET_COUNT', 3))
    interval_s = float(os.environ.get('INTERVAL', 0.2))

    if packet_count < 1:
        logger.error("PACKET_COUNT must be greater than 0")
        exit(1)

    log_file_path = os.environ.get('LOG_FILE_PATH', None)
    if log_file_path is None:
        CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
        log_file_path = os.path.join(CURRENT_DIR, 'outputs', 'tcp_rtt_client.log')
    if not os.path.isdir(os.path.dirname(log_file_path)):
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = create_logger('client', filename=log_file_path)
    
    try:
        start_client(host=host, port=port, packet_size=packet_size, packet_count=packet_count, interval=interval_s, logger=logger)
    except Exception as e:
        logger.error(f"Error (server is {host}:{port}): {e}")

if __name__ == "__main__":
    main()
