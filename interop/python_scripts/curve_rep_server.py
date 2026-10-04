"""CURVE REP server with no ZAP handler, so any client holding the server's public
key is accepted (the Stonehouse pattern). Prints its public key, then replies
"<request>-reply" and exits on "shutdown"."""
import sys
import time
import zmq


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)
    endpoint = sys.argv[1]

    public, secret = zmq.curve_keypair()
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.CURVE_SERVER, 1)
    socket.setsockopt(zmq.CURVE_SECRETKEY, secret)
    socket.setsockopt(zmq.LINGER, 500)
    socket.bind(endpoint)
    print(f"PY-CURVE-REP: PUBLIC {public.decode()}", flush=True)
    print("PY-CURVE-REP: READY", flush=True)

    try:
        while True:
            frames = socket.recv_multipart()
            print(f"PY-CURVE-REP: Received {len(frames)} frames, {sum(len(f) for f in frames)} bytes", flush=True)
            socket.send_multipart([f + b"-reply" for f in frames])
            if frames == [b"shutdown"]:
                break
    finally:
        socket.close()
        context.term()
        time.sleep(0.1)


if __name__ == "__main__":
    main()
