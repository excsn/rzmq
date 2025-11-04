import zmq
import sys
import time
import threading
from zmq.utils.monitor import parse_monitor_message

# --- Monitor Thread ---
def monitor_thread_func(monitor_socket, stop_event, name="[MONITOR]"):
    print(f"{name} Monitor thread started.", flush=True)
    try:
        while not stop_event.is_set():
            # Poll with a timeout to allow checking the stop_event
            if not monitor_socket.poll(250):
                continue

            try:
                event_msg = monitor_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                continue
            except zmq.error.ZMQError as e:
                print(f"{name} ZMQError in monitor recv: {e}", flush=True)
                break

            try:
                event = parse_monitor_message(event_msg)
                event_name = event.get("event_name", "UNKNOWN")
                addr = event.get("addr", "").decode("utf-8", "replace")
                print(f"{name} >>>> EVENT: {event_name} | Address: {addr}", flush=True)
            except Exception as e:
                print(f"{name} Failed to parse monitor message: {e}", file=sys.stderr, flush=True)
                continue
    finally:
        try:
            monitor_socket.close(linger=0)
        except Exception:
            pass
        print(f"{name} Monitor thread finished.", flush=True)


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)

    endpoint = sys.argv[1]
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    
    # Set a reasonable linger period to ensure messages are sent on close
    socket.setsockopt(zmq.LINGER, 500)

    # --- SETUP MONITOR ---
    # We attach the monitor before connecting to see all events.
    monitor = socket.get_monitor_socket()
    monitor_stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_thread_func, args=(monitor, monitor_stop_event, "[PY-REQ-MON]")
    )
    monitor_thread.start()

    try:
        print(f"PY-REQ: Connecting to {endpoint}...", flush=True)
        socket.connect(endpoint)
        
        # Give a moment for the connection to establish and handshake events to fire
        time.sleep(0.5)
        
        request_payload = b"hello from pyzmq req"
        print(f"PY-REQ: Sending request: '{request_payload.decode()}'", flush=True)
        socket.send(request_payload)
        print("PY-REQ: Request sent.", flush=True)

        print("PY-REQ: Waiting for reply...", flush=True)
        # Set a receive timeout so the script doesn't hang forever if no reply comes
        socket.setsockopt(zmq.RCVTIMEO, 3000) # 3 seconds
        reply = socket.recv()

        expected_reply = b"reply from rzmq router"
        print(f"PY-REQ: Reply received: '{reply.decode()}'", flush=True)

        if reply == expected_reply:
            print("PY-REQ: SUCCESS - Received the expected reply.", flush=True)
            sys.exit(0) # Success
        else:
            print(f"PY-REQ: FAILURE - Reply '{reply.decode()}' did not match expected '{expected_reply.decode()}'.", file=sys.stderr, flush=True)
            sys.exit(1) # Failure

    except zmq.error.Again:
        print("PY-REQ: FAILURE - Timed out waiting for reply.", file=sys.stderr, flush=True)
        sys.exit(1) # Failure
    except Exception as e:
        print(f"PY-REQ: Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1) # Failure
    finally:
        print("PY-REQ: Shutting down.", flush=True)
        
        # Cleanly shut down the monitor thread
        monitor_stop_event.set()
        try:
            monitor.close(linger=0) # Closing the monitor unblocks its recv
        except Exception:
            pass
        monitor_thread.join(timeout=2)
        
        socket.close()
        context.term()

if __name__ == "__main__":
    main()