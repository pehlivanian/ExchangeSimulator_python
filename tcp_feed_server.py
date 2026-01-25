"""
TCP Feed Server for STP (Straight-Through Processing) broadcasts.

Accepts client connections and broadcasts trade notifications to all connected clients.
Mirrors the C++ tcp_feed_server.hpp implementation.
"""

import socket
import threading
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class TCPFeedServer:
    """
    TCP server for broadcasting trade notifications.

    All connected clients receive all trade messages.
    Clients are read-only (they just receive, don't send).
    """

    def __init__(self, port: int):
        """
        Initialize the feed server.

        Args:
            port: TCP port to listen on
        """
        self.port = port

        self._server_socket: Optional[socket.socket] = None
        self._clients: List[socket.socket] = []
        self._clients_lock = threading.Lock()
        self._running = False
        self._accept_thread: Optional[threading.Thread] = None

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

            logger.info(f"Feed server started on port {self.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start feed server: {e}")
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
            for sock in self._clients:
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

        logger.info("Feed server stopped")

    def _accept_loop(self) -> None:
        """Accept new connections (runs in its own thread)."""
        while self._running:
            try:
                client_socket, addr = self._server_socket.accept()

                with self._clients_lock:
                    self._clients.append(client_socket)

                logger.info(f"Feed client connected from {addr}")

            except socket.timeout:
                continue
            except Exception as e:
                if self._running:
                    logger.error(f"Feed accept error: {e}")

    def broadcast(self, message: str) -> None:
        """
        Broadcast a message to all connected clients.

        Disconnected clients are automatically removed.
        """
        if not message.endswith('\n'):
            message += '\n'

        data = message.encode('utf-8')
        disconnected = []

        with self._clients_lock:
            for i, sock in enumerate(self._clients):
                try:
                    sock.sendall(data)
                except Exception as e:
                    logger.debug(f"Feed client disconnected during broadcast: {e}")
                    disconnected.append(i)

            # Remove disconnected clients in reverse order to maintain indices
            for i in reversed(disconnected):
                try:
                    self._clients[i].close()
                except Exception:
                    pass
                del self._clients[i]

    def get_client_count(self) -> int:
        """Get the number of connected clients."""
        with self._clients_lock:
            return len(self._clients)
