import zmq
import sys
import time

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)

    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)

    try:
        socket.bind(endpoint)
        print(f"PUSH Server bound to {endpoint}. READY.", flush=True)

        # Give the client a moment to connect
        time.sleep(0.2)

        # Send a burst of messages
        for i in range(5):
            message = f"message-{i}"
            print(f"Sending: {message}", flush=True)
            socket.send_string(message)
            time.sleep(0.05) # Small delay between sends

        print("All messages sent. Server is idle.", flush=True)
        # Keep running until the test orchestrator kills the process
        while True:
            time.sleep(1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()