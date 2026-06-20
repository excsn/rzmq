#!/usr/bin/env python3
"""Raw ZMTP/2.0 *client* peer (no pyzmq).

Emits exactly the wire bytes a libzmq 3.0-3.2 (ZMTP/2.0) peer produces so we can
prove rzmq's ZMTP/2.0 downgrade/handshake interoperates over a real TCP socket.
Modern libzmq cannot be told to speak v2 (it only downgrades when it *detects* a
v2 peer), so hand-emitting the v2 byte stream is the faithful way to test this.

Usage: zmtp2_client.py <tcp://host:port> <socket_type_byte> [identity]

ZMTP/2.0 wire layout produced here:
  greeting  = signature(10) + revision(0x01) + socket-type(1)
  identity  = one bare frame (flags=0x00, len, body)  -- empty for anonymous
  data      = bare frames (flags bit0 = MORE)
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
        try:
            chunk = sock.recv(n - len(buf))
        except socket.timeout:
            sys.stderr.write("recv_exact timed out: wanted %d, got %d: %r\n" % (n, len(buf), buf))
            sys.stderr.flush()
            raise
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
    read_frame(sock)  # rzmq's identity frame (empty for anonymous sockets)


def main():
    endpoint = sys.argv[1]
    socket_type_byte = int(sys.argv[2])
    identity = sys.argv[3].encode() if len(sys.argv) > 3 else b""
    host, port = parse_endpoint(endpoint)

    sock = socket.create_connection((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.settimeout(8.0)

    send_v2_greeting(sock, socket_type_byte)
    send_frame(sock, identity)               # v2 identity exchange
    drain_peer_greeting_and_identity(sock)   # let rzmq complete its side

    print("READY")
    sys.stdout.flush()

    send_frame(sock, b"Hello")
    print("SENT")
    sys.stdout.flush()

    # Keep the connection open long enough for rzmq to deliver the message.
    time.sleep(2.0)


if __name__ == "__main__":
    main()
