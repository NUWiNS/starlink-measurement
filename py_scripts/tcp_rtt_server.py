import os
import socket
import threading

from logging_utils import create_logger

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = create_logger('server', filename=os.path.join(CURRENT_DIR, 'outputs', 'app_rtt_server.log'))

def handle_client(client_socket, client_address):
    """
    Handle a single client connection.
    """
    logger.info(f"Connected to {client_address}")
    
    try:
        while True:
            # Receive client message
            message = client_socket.recv(1024).decode()
            if not message:
                break
            logger.info(f"Received {len(message)} bytes from {client_address}: {message}")

            # Send acknowledgment (ACK)
            ack_message = "ACK"
            client_socket.sendall(ack_message.encode())
            logger.info(f"Sent ACK ({len(ack_message)} bytes) to {client_address}")
    except Exception as e:
        logger.error(f"Error handling client {client_address}: {e}")
    finally:
        client_socket.close()
        logger.info(f"Connection closed with {client_address}")

def start_server(host='0.0.0.0', port=65432):
    """
    Starts the server that sends `byte_size` amount of data to the client
    before sending an ACK. Handles multiple clients concurrently.
    """
    # Create a TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(5)  # Allow up to 5 queued connections
        logger.info(f"Server listening on {host}:{port}")

        while True:
            # Blocking until a new connection is available. 
            # When a client connects, it returns a new socket specific to that client.
            client_socket, client_address = server_socket.accept()
            
            # Create a new thread to handle the client
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address)
            )
            client_thread.start()

if __name__ == "__main__":
    host = '0.0.0.0'
    port = int(os.environ.get('SERVER_PORT', 65432))
    try:
        start_server(host=host, port=port)
    except Exception as e:
        logger.error(f"Error (server is {host}:{port}): {e}")
