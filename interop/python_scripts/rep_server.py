import zmq
import sys
import time

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)

    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    try:
        socket.bind(endpoint)
        print(f"REP Server bound to {endpoint}. READY.", flush=True)

        while True:
            message = socket.recv_string()
            print(f"Received request: {message}", flush=True)
            reply = f"{message}-reply"
            socket.send_string(reply)
            if message == "shutdown":
                break
                
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        print("REP Server shutting down.", flush=True)
        socket.close()
        context.term()
        time.sleep(0.1)

if __name__ == "__main__":
    main()