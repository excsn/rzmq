#!/usr/bin/env python3
"""Raw ZMTP/2.0 *server* peer (no pyzmq).

Listens on a TCP port and performs the ZMTP/2.0 server-side handshake by hand,
proving rzmq downgrades correctly when it *initiates* a connection to a legacy
v2 peer (the harder direction). Receives one data frame and validates it.

Usage: zmtp2_server.py <tcp://host:port> <socket_type_byte>
Prints "READY" once bound (so rzmq may connect) and "SUCCESS"/"FAILURE: ..."
after receiving the data frame.
"""
import socket
import sys
import time


def parse_endpoint(ep):
    assert ep.startswith("tcp://"), "only tcp:// endpoints supported"
    host, port = ep[len("tcp://"):].rsplit(":", 1)
    return host, int(port)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("peer closed connection early")
        buf += chunk
    return buf


def send_v2_greeting(sock, socket_type_byte):
    signature = b"\xff" + b"\x00" * 8 + b"\x7f"  # 10 bytes
    sock.sendall(signature + bytes([0x01, socket_type_byte]))


def send_frame(sock, payload, more=False):
    assert len(payload) < 256, "this raw peer only emits short frames"
    flags = 0x01 if more else 0x00
    sock.sendall(bytes([flags, len(payload)]) + payload)


def read_frame(sock):
    flags, length = recv_exact(sock, 2)
    body = recv_exact(sock, length) if length else b""
    return flags, body


def drain_peer_greeting_and_identity(sock):
    greeting = recv_exact(sock, 12)
    assert greeting[0] == 0xFF and greeting[9] == 0x7F, "bad signature from rzmq"
    read_frame(sock)  # rzmq's identity frame


def main():
    endpoint = sys.argv[1]
    socket_type_byte = int(sys.argv[2])
    host, port = parse_endpoint(endpoint)

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen(1)

    print("READY")
    sys.stdout.flush()

    conn, _ = listener.accept()
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # Both sides send their greeting simultaneously (the ZMTP model).
    send_v2_greeting(conn, socket_type_byte)
    send_frame(conn, b"")                     # our (anonymous) identity
    drain_peer_greeting_and_identity(conn)    # rzmq greeting + identity

    _flags, body = read_frame(conn)           # the data frame rzmq sends
    if body == b"Hello":
        print("SUCCESS")
    else:
        print("FAILURE: %r" % body)
    sys.stdout.flush()
    time.sleep(0.5)


if __name__ == "__main__":
    main()
