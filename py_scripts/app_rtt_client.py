###Client

import os
import socket
import time
from logging_utils import create_logger

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = create_logger('client', filename=os.path.join(CURRENT_DIR, 'outputs', 'app_rtt_client.log'))

def start_client(host='54.197.223.49', port=65432, message="Hello, Server"):
    """
    Connects to the server, receives 2 MB of data, sends a message, and waits for ACK.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))

        # Receive the 2 MB of initial data
        received_data = b""
        while len(received_data) < 2 * 1024 * 1024:
            chunk = client_socket.recv(4096)  # Adjust chunk size as needed
            if not chunk:
                break
            received_data += chunk
        logger.info(f"Received {len(received_data)} bytes of initial data")

        # Send a message to the server
        send_time = time.time()
        client_socket.sendall(message.encode())
        logger.info(f"Sent: {message} at {send_time}")

        # Receive ACK from the server
        ack = client_socket.recv(1024).decode()
        receive_time = time.time()
        logger.info(f"Received: {ack} at {receive_time}")

        # Calculate round-trip time
        time_difference = (receive_time - send_time) * 1000
        logger.info(f"Round-trip time: {time_difference:.2f} milliseconds")

if __name__ == "__main__":
    # Run the client 10 times
    host = '127.0.0.1'
    port = 65432
    message = "Hello, Server"
    for _ in range(1):
        try:
            start_client(host=host, port=port, message=message)
        except Exception as e:
            logger.error(f"Error: {e}")
