import zmq
import sys
import time

def main():
    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)

    try:
        socket.bind(endpoint)
        print(f"ROUTER Server bound to {endpoint}. READY.", flush=True)

        # Loop to echo requests
        while True:
            # ROUTER receives [identity, empty_frame, payload] from a DEALER
            frames = socket.recv_multipart()
            print(f"Received frames: {frames}", flush=True)

            if len(frames) != 3:
                print("Error: Expected 3 frames from DEALER", file=sys.stderr)
                continue

            identity, delimiter, payload = frames

            if payload == b"shutdown":
                break

            # Send a reply back to the same client
            reply_payload = payload + b"-reply"
            print(f"Sending reply: {[identity, delimiter, reply_payload]}", flush=True)
            socket.send_multipart([identity, delimiter, reply_payload])

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        print("ROUTER Server shutting down.", flush=True)
        socket.close()
        context.term()

if __name__ == "__main__":
    main()