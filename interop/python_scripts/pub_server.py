import zmq
import sys
import time

def main():
    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.PUB)

    try:
        socket.bind(endpoint)
        print(f"PUB Server bound to {endpoint}. READY.", flush=True)

        # IMPORTANT: Wait for the subscriber to connect and send its subscription
        time.sleep(0.5)

        print("Sending message on TOPIC_A...", flush=True)
        socket.send_multipart([b"TOPIC_A", b"hello from topic A"])

        print("Sending message on TOPIC_B...", flush=True)
        socket.send_multipart([b"TOPIC_B", b"this should be filtered"])

        # Send a final message that the client can use to know it's done
        print("Sending shutdown message...", flush=True)
        socket.send_multipart([b"TOPIC_A", b"shutdown"])
        
        # Give a moment for the last message to be sent
        time.sleep(0.1)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()