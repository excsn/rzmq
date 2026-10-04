"""PLAIN REP server. libzmq authenticates PLAIN through ZAP, so a ThreadAuthenticator
accepts exactly one username and password. Replies "<request>-reply" and exits on
"shutdown"."""
import sys
import time
import zmq
from zmq.auth.thread import ThreadAuthenticator


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <endpoint> <username> <password>", file=sys.stderr)
        sys.exit(1)
    endpoint, username, password = sys.argv[1:4]

    context = zmq.Context()
    auth = ThreadAuthenticator(context)
    auth.start()
    auth.configure_plain(domain="global", passwords={username: password})

    socket = context.socket(zmq.REP)
    socket.setsockopt(zmq.PLAIN_SERVER, 1)
    socket.setsockopt(zmq.ZAP_DOMAIN, b"global")
    socket.setsockopt(zmq.LINGER, 500)
    socket.bind(endpoint)
    print("PY-PLAIN-REP: READY", flush=True)

    try:
        while True:
            message = socket.recv_string()
            print(f"PY-PLAIN-REP: Received {message}", flush=True)
            socket.send_string(f"{message}-reply")
            if message == "shutdown":
                break
    finally:
        socket.close()
        auth.stop()
        context.term()
        time.sleep(0.1)


if __name__ == "__main__":
    main()
