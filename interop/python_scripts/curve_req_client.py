"""CURVE REQ client. Sends a two-frame request with the given server key and reports
the handshake outcome as libzmq saw it, then whether the reply came back."""
import sys
import zmq
from zmq.utils.monitor import recv_monitor_message


def report_handshake(monitor):
    while monitor.poll(3000):
        event = recv_monitor_message(monitor)
        if event["event"] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
            print("PY-CURVE-REQ: HANDSHAKE OK", flush=True)
            return
        if event["event"] in (
            zmq.EVENT_HANDSHAKE_FAILED_AUTH,
            zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL,
            zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL,
        ):
            print(f"PY-CURVE-REQ: HANDSHAKE FAILED {event['event']} {event['value']}", flush=True)
            return
    print("PY-CURVE-REQ: NO HANDSHAKE EVENT", flush=True)


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <endpoint> <server_public_z85>", file=sys.stderr)
        sys.exit(1)
    endpoint, server_key = sys.argv[1], sys.argv[2].encode()

    public, secret = zmq.curve_keypair()
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 3000)
    socket.setsockopt(zmq.CURVE_SERVERKEY, server_key)
    socket.setsockopt(zmq.CURVE_PUBLICKEY, public)
    socket.setsockopt(zmq.CURVE_SECRETKEY, secret)
    monitor = socket.get_monitor_socket()
    socket.connect(endpoint)
    print("PY-CURVE-REQ: READY", flush=True)

    try:
        report_handshake(monitor)
        payload = bytes(range(256)) * 20
        socket.send_multipart([b"hello", payload])
        try:
            reply = socket.recv_multipart()
        except zmq.error.Again:
            print("PY-CURVE-REQ: NO REPLY", flush=True)
            sys.exit(0)
        if reply == [b"hello-reply", payload + b"-reply"]:
            print("PY-CURVE-REQ: SUCCESS", flush=True)
            sys.exit(0)
        print(f"PY-CURVE-REQ: FAILURE - got {len(reply)} frames", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        monitor.close(linger=0)
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
