import zmq
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: router_raw.py <endpoint>")
        sys.exit(1)

    endpoint = sys.argv[1]
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    router.bind(endpoint)

    print("READY")
    sys.stdout.flush()

    # Recv. Expect [Identity, Payload]
    # rzmq DEALER (Manual) sends b"Hello".
    # pyzmq ROUTER receives [Identity, b"Hello"]
    parts = router.recv_multipart()
    print(f"Received {len(parts)} parts: {parts}")

    if len(parts) != 2:
        print(f"FAILURE: Expected 2 parts, got {len(parts)}")
        return

    identity = parts[0]
    payload = parts[1]

    if payload != b"Hello":
        print(f"FAILURE: Expected b'Hello', got {payload}")
        return

    # Send reply [Identity, b"Reply"]
    # pyzmq ROUTER will send this as [Identity, b"Reply"] on the wire (Identity used for routing).
    router.send_multipart([identity, b"Reply"])
    print("SUCCESS")
    sys.stdout.flush()

if __name__ == "__main__":
    main()