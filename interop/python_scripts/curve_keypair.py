"""Prints a fresh CURVE keypair as Z85 for the Rust side to use."""
import zmq

public, secret = zmq.curve_keypair()
print(f"PUBLIC {public.decode()}", flush=True)
print(f"SECRET {secret.decode()}", flush=True)
print("READY", flush=True)
