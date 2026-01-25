"""
TCP Order Handler Server.

Accepts client connections, receives orders, and sends back responses.
Mirrors the C++ tcp_order_handler.hpp implementation.
"""

import socket
import threading
import logging
from typing import Callable, Dict, Optional

logger = logging.getLogger(__name__)


class TCPOrderHandler:
    """
    TCP server for handling order connections.

    Each connected client gets its own handler thread.
    Orders are processed via a callback, with responses sent synchronously.
    Async messages (passive fill notifications) can be sent to specific clients.
    """

    def __init__(
        self,
        port: int,
        order_callback: Callable[[int, str], str],
        disconnect_callback: Optional[Callable[[int], None]] = None
    ):
        """
        Initialize the order handler.

        Args:
            port: TCP port to listen on
            order_callback: Function(connection_id, order_string) -> response_string
            disconnect_callback: Optional function called when a client disconnects
        """
        self.port = port
        self._order_callback = order_callback
        self._disconnect_callback = disconnect_callback

        self._server_socket: Optional[socket.socket] = None
        self._clients: Dict[int, socket.socket] = {}
        self._clients_lock = threading.Lock()
        self._running = False
        self._accept_thread: Optional[threading.Thread] = None
        self._next_conn_id = 0
        self._conn_id_lock = threading.Lock()

    def start(self) -> bool:
        """Start the server. Returns True on success."""
        try:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind(('0.0.0.0', self.port))
            self._server_socket.listen(10)
            self._server_socket.settimeout(1.0)  # For graceful shutdown

            self._running = True
            self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            self._accept_thread.start()

            logger.info(f"Order handler started on port {self.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start order handler: {e}")
            if self._server_socket:
                self._server_socket.close()
                self._server_socket = None
            return False

    def stop(self) -> None:
        """Stop the server and close all connections."""
        self._running = False

        if self._accept_thread:
            self._accept_thread.join(timeout=2.0)
            self._accept_thread = None

        with self._clients_lock:
            for conn_id, sock in self._clients.items():
                try:
                    sock.close()
                except Exception:
                    pass
            self._clients.clear()

        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
            self._server_socket = None

        logger.info("Order handler stopped")

    def _get_next_conn_id(self) -> int:
        """Get the next connection ID."""
        with self._conn_id_lock:
            self._next_conn_id += 1
            return self._next_conn_id

    def _accept_loop(self) -> None:
        """Accept new connections (runs in its own thread)."""
        while self._running:
            try:
                client_socket, addr = self._server_socket.accept()
                conn_id = self._get_next_conn_id()

                with self._clients_lock:
                    self._clients[conn_id] = client_socket

                # Start a handler thread for this client
                handler_thread = threading.Thread(
                    target=self._handle_client,
                    args=(conn_id, client_socket),
                    daemon=True
                )
                handler_thread.start()

                logger.info(f"Client {conn_id} connected from {addr}")

            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    logger.error(f"Accept error: {e}")

    def _handle_client(self, conn_id: int, client_socket: socket.socket) -> None:
        """Handle a single client connection (runs in per-client thread)."""
        buffer = ""
        client_socket.settimeout(1.0)

        try:
            while self._running:
                try:
                    data = client_socket.recv(4096)
                    if not data:
                        # Client disconnected
                        break

                    buffer += data.decode('utf-8')

                    # Process complete lines
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        line = line.strip()

                        if line:
                            # Process the order and get response
                            response = self._order_callback(conn_id, line)

                            # Send response back to client
                            if response:
                                self._send_to_client(conn_id, response)

                except socket.timeout:
                    continue
                except Exception as e:
                    logger.error(f"Client {conn_id} error: {e}")
                    break

        finally:
            # Clean up
            with self._clients_lock:
                if conn_id in self._clients:
                    del self._clients[conn_id]

            try:
                client_socket.close()
            except Exception:
                pass

            if self._disconnect_callback:
                self._disconnect_callback(conn_id)

            logger.info(f"Client {conn_id} disconnected")

    def _send_to_client(self, conn_id: int, message: str) -> bool:
        """Send a message to a specific client."""
        with self._clients_lock:
            if conn_id not in self._clients:
                return False

            client_socket = self._clients[conn_id]

        try:
            # Ensure message ends with newline
            if not message.endswith('\n'):
                message += '\n'
            client_socket.sendall(message.encode('utf-8'))
            return True
        except Exception as e:
            logger.error(f"Failed to send to client {conn_id}: {e}")
            return False

    def send_async_message(self, conn_id: int, message: str) -> bool:
        """
        Send an asynchronous message to a client (for passive fill notifications).

        This is called from outside the client handler thread.
        """
        return self._send_to_client(conn_id, message)

    def get_client_count(self) -> int:
        """Get the number of connected clients."""
        with self._clients_lock:
            return len(self._clients)
