import zmq
import sys
import time

def main():
    if len(sys.argv) < 2:
        print("Usage: dealer_raw.py <endpoint>")
        sys.exit(1)

    endpoint = sys.argv[1]
    ctx = zmq.Context()
    dealer = ctx.socket(zmq.DEALER)
    
    # Connect to the rzmq ROUTER
    dealer.connect(endpoint)

    # Allow time for connection establishment
    time.sleep(0.2)

    print("READY")
    sys.stdout.flush()

    # Send raw frame. 
    # pyzmq DEALER sends exactly what we give it.
    print(f"Sending raw 'Hello' to {endpoint}")
    dealer.send(b"Hello")

    # Expect raw reply.
    try:
        msg = dealer.recv()
        print(msg)
        if msg == b"Reply":
            print("SUCCESS")
        else:
            print(f"FAILURE: Received {msg}")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()