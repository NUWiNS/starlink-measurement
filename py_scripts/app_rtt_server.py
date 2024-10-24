import os
import socket

from logging_utils import create_logger

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = create_logger('server', filename=os.path.join(CURRENT_DIR, 'outputs', 'app_rtt_server.log'))
        
def start_server(host='0.0.0.0', port=65432, byte_size=2 * 1024 * 1024):
    """
    Starts the server that sends `byte_size` amount of data to the client
    before sending an ACK.
    """
    # Create a TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)  # Allow up to 5 connections
        logger.info(f"Server listening on {host}:{port}")

        while True:
            # Blocking until a new connection is available. 
            # When a client connects, it returns a new socket specific to that client.
            # Each connection will be handled sequentially, one after the other.
            client_socket, client_address = server_socket.accept()
            with client_socket:
                logger.info(f"Connected to {client_address}")

                # Send `byte_size` amount of bytes to the client
                data = b'A' * byte_size  # Example: Send 'A' repeated `byte_size` times
                client_socket.sendall(data)
                logger.info(f"Sent {byte_size} bytes to client")

                # Receive client message
                message = client_socket.recv(1024).decode()
                logger.info(f"Received: {message}")

                # Send acknowledgment (ACK)
                ack_message = "ACK"
                client_socket.sendall(ack_message.encode())
                logger.info("Sent: ACK")

if __name__ == "__main__":
    host = '0.0.0.0'  # This allows connections from any IP address
    port = 65432
    byte_size = 2 * 1024 * 1024
    try:
        start_server(host=host, port=port, byte_size=byte_size)
    except Exception as e:
        logger.error(f"Error (server is {host}:{port}): {e}")
