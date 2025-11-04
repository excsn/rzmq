# interop/python_scripts/router_server_for_req.py

import zmq
import sys
import time

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)

    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)

    # Set a linger period to ensure the final message is sent before closing.
    socket.setsockopt(zmq.LINGER, 1000) # 1 second linger

    try:
        socket.bind(endpoint)
        print(f"ROUTER Server bound to {endpoint}. READY.", flush=True)

        # A libzmq REQ socket sends a 3-part message to a ROUTER: [empty, empty, payload].
        # The ROUTER socket prepends its generated identity, consuming the first empty frame.
        # Therefore, the application sees [identity, empty, payload].
        print("Waiting for message from REQ client...", flush=True)
        frames = socket.recv_multipart()
        
        print(f"Received {len(frames)} frames: {frames}", flush=True)
        
        # We will be lenient here and just echo back whatever we receive.
        # This allows us to see what both rzmq and pyzmq REQ sockets actually send.
        
        # To reply to a REQ socket, a ROUTER must send [identity, empty, payload].
        # The ROUTER socket handles the identity part for routing.
        # What goes on the wire is [empty, payload].
        # We'll just echo back what we got, minus our generated identity.
        
        if len(frames) > 1:
            # Assumes first frame is identity
            identity = frames[0]
            payload_to_echo = frames[1:] # This will be [empty, payload] from a compliant REQ
            
            # Construct the full message for send_multipart: [identity, empty, payload_to_echo_part1, ...]
            frames_to_send = [identity] + payload_to_echo
            print(f"Echoing back frames: {frames_to_send}", flush=True)
            socket.send_multipart(frames_to_send)
        else:
            print("Received unexpected single-frame message, not replying.", file=sys.stderr, flush=True)


    except Exception as e:
        print(f"Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        print("ROUTER Server shutting down.", flush=True)
        socket.close()
        context.term()
        # Give a moment for TCP stack to close after linger
        time.sleep(0.1) 

if __name__ == "__main__":
    main()