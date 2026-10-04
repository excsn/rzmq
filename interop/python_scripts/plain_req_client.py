"""PLAIN REQ client. Sends "hello" with the given credentials and reports whether
"hello-reply" came back within 2 s. A monitor reports the handshake outcome as
libzmq saw it: "HANDSHAKE OK" or "AUTH FAILED <status>"."""
import sys
import zmq
from zmq.utils.monitor import recv_monitor_message


def report_handshake(monitor):
    """Returns True when the handshake succeeded."""
    while monitor.poll(2000):
        event = recv_monitor_message(monitor)
        if event["event"] == zmq.EVENT_HANDSHAKE_SUCCEEDED:
            print("PY-PLAIN-REQ: HANDSHAKE OK", flush=True)
            return True
        if event["event"] == zmq.EVENT_HANDSHAKE_FAILED_AUTH:
            print(f"PY-PLAIN-REQ: AUTH FAILED {event['value']}", flush=True)
            return False
        if event["event"] in (zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL, zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL):
            print(f"PY-PLAIN-REQ: HANDSHAKE FAILED {event['event']} {event['value']}", flush=True)
            return False
    print("PY-PLAIN-REQ: NO HANDSHAKE EVENT", flush=True)
    return False


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <endpoint> <username> <password>", file=sys.stderr)
        sys.exit(1)
    endpoint, username, password = sys.argv[1:4]

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, 2000)
    socket.setsockopt_string(zmq.PLAIN_USERNAME, username)
    socket.setsockopt_string(zmq.PLAIN_PASSWORD, password)
    monitor = socket.get_monitor_socket()
    socket.connect(endpoint)
    print("PY-PLAIN-REQ: READY", flush=True)

    try:
        if not report_handshake(monitor):
            print("PY-PLAIN-REQ: NO REPLY", flush=True)
            sys.exit(0)
        socket.send(b"hello")
        try:
            reply = socket.recv()
        except zmq.error.Again:
            print("PY-PLAIN-REQ: NO REPLY", flush=True)
            sys.exit(0)
        if reply == b"hello-reply":
            print("PY-PLAIN-REQ: SUCCESS", flush=True)
            sys.exit(0)
        print(f"PY-PLAIN-REQ: FAILURE - got {reply!r}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        monitor.close(linger=0)
        socket.close(linger=0)
        context.term()


if __name__ == "__main__":
    main()
