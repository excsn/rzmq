"""REQ client for the rzmq REP turn-taking interop tests.

Modes:
  correlate  Sets ZMQ_REQ_RELAXED and ZMQ_REQ_CORRELATE, sends "slow", gives up
             after 250 ms, sends "next" and expects "re:next". The late "re:slow"
             must be discarded by libzmq on its request id.
  giveup     Sends "slow", gives up after 250 ms, prints "Gave up." and closes.
"""
import sys
import zmq


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <endpoint> <correlate|giveup>", file=sys.stderr)
        sys.exit(1)
    endpoint, mode = sys.argv[1], sys.argv[2]

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 500)
    socket.setsockopt(zmq.RCVTIMEO, 250)
    if mode == "correlate":
        socket.setsockopt(zmq.REQ_RELAXED, 1)
        socket.setsockopt(zmq.REQ_CORRELATE, 1)
    socket.connect(endpoint)
    print("PY-REQ: READY", flush=True)

    try:
        socket.send(b"slow")
        try:
            socket.recv()
            print("PY-REQ: FAILURE - slow reply arrived within RCVTIMEO.", file=sys.stderr, flush=True)
            sys.exit(1)
        except zmq.error.Again:
            print("PY-REQ: Gave up.", flush=True)

        if mode == "giveup":
            sys.exit(0)

        socket.setsockopt(zmq.RCVTIMEO, 3000)
        socket.send(b"next")
        reply = socket.recv()
        if reply == b"re:next":
            print("PY-REQ: SUCCESS - re:next", flush=True)
            sys.exit(0)
        print(f"PY-REQ: FAILURE - got {reply!r}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
