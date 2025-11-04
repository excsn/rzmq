#!/usr/bin/env python3
import zmq
import sys
import time
import threading

def router_thread_func(endpoint, context):
    """
    Bind a ROUTER socket, receive one message from a REQ peer, and send a reply.
    The ROUTER will accept either framing:
      [identity, b'', payload]
    or
      [identity, payload]
    and will mirror an appropriate reply envelope back so the REQ receives it.
    """
    socket = context.socket(zmq.ROUTER)
    socket.setsockopt(zmq.LINGER, 0)   # don't block on close
    # allow a generous recv timeout so test doesn't hang forever
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5s
    try:
        socket.bind(endpoint)
        print(f"[PY-ROUTER] Bound to {endpoint}. READY.", flush=True)

        frames = socket.recv_multipart()  # may raise zmq.error.Again on timeout
        print(f"[PY-ROUTER] Received frames: {frames!r}", flush=True)

        if len(frames) < 2:
            print("[PY-ROUTER] Unexpected frame format (too few frames).", file=sys.stderr, flush=True)
            return 1

        identity = frames[0]
        # detect whether an empty delimiter was present
        if len(frames) >= 3 and frames[1] == b'':
            incoming_payload = frames[2]
            reply_frames = [identity, b'', b"World"]
        else:
            incoming_payload = frames[1]
            reply_frames = [identity, b"World"]

        print(f"[PY-ROUTER] Payload from client: {incoming_payload!r}", flush=True)
        # send reply using same envelope style
        socket.send_multipart(reply_frames)
        print(f"[PY-ROUTER] Sent reply frames: {reply_frames!r}", flush=True)
        return 0

    except zmq.error.Again:
        print("[PY-ROUTER] Timed out waiting for a message.", file=sys.stderr, flush=True)
        return 2
    except Exception as e:
        print(f"[PY-ROUTER] Error: {e}", file=sys.stderr, flush=True)
        return 3
    finally:
        socket.close()
        print("[PY-ROUTER] Thread finished.", flush=True)


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <endpoint>", file=sys.stderr)
        sys.exit(1)

    endpoint = sys.argv[1]
    context = zmq.Context()

    # Start ROUTER thread
    router_thread = threading.Thread(target=router_thread_func, args=(endpoint, context), daemon=True)
    router_thread.start()

    # Give the ROUTER a moment to bind
    time.sleep(0.1)

    req_socket = context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.LINGER, 0)    # don't hang on close
    req_socket.setsockopt(zmq.SNDTIMEO, 2000)  # 2s send timeout
    req_socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5s recv timeout

    try:
        print(f"[PY-REQ] Connecting to {endpoint}...", flush=True)
        req_socket.connect(endpoint)

        # small pause to allow libzmq to complete the handshake in background threads
        time.sleep(0.05)

        print("[PY-REQ] Sending 'Hello'...", flush=True)
        # REQ will send a single-frame message
        req_socket.send(b"Hello")

        # Use recv_multipart to be robust to whether ROUTER sent an empty delimiter or not
        reply_frames = req_socket.recv_multipart()
        print(f"[PY-REQ] Received reply frames: {reply_frames!r}", flush=True)

        if not reply_frames:
            print(">>> TEST FAILED: No frames received.", file=sys.stderr, flush=True)
            sys.exit(1)

        # reply payload should be the last frame
        reply_payload = reply_frames[-1]
        print(f"[PY-REQ] Reply payload: {reply_payload!r}", flush=True)

        if reply_payload == b"World":
            print("\n---\n>>> TEST SUCCEEDED: REQ sent to ROUTER, router replied, and REQ received the reply.\n---", flush=True)
            sys.exit(0)
        else:
            print("\n---\n>>> TEST FAILED: Reply payload did not match expected b'World'.\n---", file=sys.stderr, flush=True)
            sys.exit(1)

    except zmq.error.Again:
        print("\n---\n>>> TEST FAILED: timed out while sending/receiving (zmq.error.Again).\n---", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"\n[PY-REQ] Unexpected error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        req_socket.close()
        # give router thread a bit of time to finish
        router_thread.join(timeout=2)
        context.term()
        print("[PY-REQ] Main finished.", flush=True)


if __name__ == "__main__":
    main()
