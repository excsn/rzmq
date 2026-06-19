# Cancellation Safety in rzmq

## 1. Executive Summary

`rzmq` is designed to be **fully cancel-safe**. Dropping a `Future` returned by any `socket.send()` or `socket.recv()` method before it completes will not lead to data corruption, resource leaks, or an inconsistent application state.

This robustness is achieved through a core architectural principle: the **Actor Model**. User-facing API calls are decoupled from the actual I/O operations and state management, which are handled by dedicated, persistent background tasks (actors). When a user cancels an operation, they are only cancelling their *request* to an actor, not the actor itself.

## 2. Core Architectural Principles of Cancel Safety

The library's cancel safety relies on three fundamental patterns working in concert.

### 2.1. The Actor Model: Isolating State and I/O

Every `rzmq` socket is managed by a `SocketCore` actor. This actor, along with its child tasks (`SessionConnectionActorX` or the global `UringWorker`), are the only entities that own and modify critical state — network connections, protocol state machines, and message buffers.

*   **User Task vs. I/O Task**: When a user calls `socket.send().await`, their task is **not** the one writing bytes to the network. Instead, it routes the message through the socket's pattern logic and into a per-connection channel.
*   **State Protection**: Because the I/O actor owns the state, dropping the user's `Future` does not affect the actor's internal state. The actor continues running, ready to process the next command, ensuring the socket is never left in a corrupted state.

### 2.2. Asynchronous Command Channels: The Cancel-Safe Boundary

Communication between the user's task and the socket actors occurs over asynchronous, bounded message channels (`fibre::mpmc`).

*   **`send()` Safety**: When a user sends a message, they `await` pushing it into a per-peer channel. If this `Future` is dropped (e.g., because the channel is full and the task is cancelled), the `send` operation is safely aborted. The message remains owned by the user's task and is never delivered to the actor.
*   **`recv()` Safety**: When a user receives a message, they `await` the ingress engine's `pop()` which blocks on an activation-track channel. If cancelled, the item simply remains in its per-pipe channel, available for the next `recv()` call.

### 2.3. Ingress Engines: The Activation-Track Model

Incoming messages are handled entirely by session actors before the user's `recv()` call is involved. Two engines coordinate ingress delivery, both built on `ReadyPipeQueue<FrameBatch>`:

*   **`AnonymousIngressEngine`** — used by `PULL` and `SUB`. Provides `recv()` (single frame) and `recv_multipart()`. A small `local_cache` holds leftover frames when the user requests single-frame delivery from a multipart batch.
*   **`AddressedIngressEngine`** — used by `REQ`, `REP`, `DEALER`, and `ROUTER`. Provides `recv_logical_message()` which returns `(pipe_id, FrameBatch)`, preserving peer identity for routing.

**How `ReadyPipeQueue` works**: Each connection pipe gets a bounded `fibre::mpmc` channel. An `is_activated` atomic flag and a shared `activation_tx` channel form an O(1) activation track. When a session actor pushes a complete `FrameBatch` via its `PipeMessageSender`, it atomically transitions the pipe from quiet → active and enqueues the pipe's ID onto `activation_tx`. The consumer's `pop()` awaits `activation_rx.recv()` — a single blocking point. No polling loop, no `Notify` coalescing, no timer-flushed buffer.

**Cancel safety**: The user's `recv()` `await` is on `activation_rx.recv()`. Dropping the `Future` here is completely safe — the pipe's `FrameBatch` stays in its per-pipe channel and its activation signal stays in `activation_tx`. The next `recv()` call will find both exactly as they were left.

## 3. The Overall Process: Tracing a `send` and `recv` Call

### 3.1. `send` Lifecycle (e.g., `DEALER` socket)

1.  **User Call**: `socket.send(msg).await` is called.
2.  **State Check**: The `DealerSocket` implementation locks its state to begin a send transaction.
3.  **Queueing**: The message is placed into the `pending_outgoing_queue`. The user's task might `await` here if the queue is full.
    *   **User Cancel Point**: If the `Future` is dropped here, the message is never queued. The socket's state is perfectly valid.
4.  **Background Processing**: A dedicated `DealerSocketOutgoingProcessor` task dequeues the message.
5.  **Peer Selection**: The processor selects a peer via its `OutgoingMessageOrchestrator`.
6.  **Delegation**: The processor calls `send_multipart()` on the peer's `ISocketConnection`, pushing the message into the session actor's egress buffer.
7.  **Final I/O**: The `SessionConnectionActorX` drains its `EgressBuffer` and performs the actual write.

The user's cancellation point is completely decoupled from the final network I/O.

### 3.2. `recv` Lifecycle (e.g., `PULL` socket)

1.  **Background I/O**: A `SessionConnectionActorX` reads bytes from the network and assembles complete `FrameBatch`es via `ZmqMessageProcessor`.
2.  **Direct Push**: The actor calls `pipe_sender.send(batch).await` on its `PipeMessageSender` — directly pushing the complete batch into the per-pipe `fibre::mpmc` channel inside `ReadyPipeQueue`.
3.  **Activation Signal**: `PipeMessageSender::send` atomically signals the pipe active by sending its ID to `activation_tx`.
4.  **User Call**: The user's `socket.recv().await` is blocked inside `AnonymousIngressEngine::recv()`, waiting on `activation_rx.recv()` in `ReadyPipeQueue::pop()`.
5.  **Wakeup**: `pop()` receives the pipe ID, does a `try_recv()` on that pipe's channel, and returns the `FrameBatch`. For single-frame delivery, any remaining frames in the batch are stashed in `local_cache` for the next `recv()` call.
    *   **User Cancel Point**: If the `Future` is dropped at step 4, the `FrameBatch` remains in the per-pipe channel and the activation signal remains in `activation_tx`. The next `recv()` call picks up exactly where it left off. **No messages are lost.**

---

## 4. Socket-by-Socket Cancel Safety Analysis

| Socket | Method | Cancel Safety Analysis |
| :--- | :--- | :--- |
| **PULL** | `recv` / `recv_multipart` | **Safe**. The `await` point is `ReadyPipeQueue::pop()` inside `AnonymousIngressEngine`. Cancellation leaves the `FrameBatch` in its per-pipe channel and the activation signal in `activation_tx`. |
| | `send` / `send_multipart` | N/A. Returns an error immediately. |
| **PUSH** | `send` / `send_multipart` | **Safe**. The user `await`s peer availability or space in an outgoing channel. Cancellation aborts the send without affecting the background I/O actors. |
| | `recv` / `recv_multipart` | N/A. Returns an error immediately. |
| **PUB** | `send` / `send_multipart` | **Safe**. The method iterates through peers and `await`s sending to each per-peer channel. Cancellation stops the iteration; all connections remain valid. |
| | `recv` / `recv_multipart` | N/A. Returns an error immediately. |
| **SUB** | `recv` / `recv_multipart` | **Safe**. Identical to `PULL`. The `await` is on `ReadyPipeQueue::pop()` inside `AnonymousIngressEngine`. Non-matching messages are filtered inline by `FilteredAnonymous` before entering the queue. |
| | `send` / `send_multipart` | N/A. Returns an error immediately. |
| **REQ** | `send` | **Safe**. State is protected by a synchronous mutex. `await` points for peer selection and sending happen outside the lock. Cancellation before the send completes leaves state as `ReadyToSend`. |
| | `recv` / `recv_multipart` | **Safe**. The `await` is on `ReadyPipeQueue::pop()` inside `AddressedIngressEngine`. If cancelled, the reply remains in the per-pipe channel and the socket remains `ExpectingReply`. The user may call `recv` again. |
| **REP** | `recv` / `recv_multipart` | **Safe**. The `await` is on `AddressedIngressEngine`. If cancelled, the request remains in the per-pipe channel and the socket remains `ReadyToReceive`. |
| | `send` / `send_multipart` | **Safe**. State is atomically set to `ReadyToReceive` *before* the `await` on the outgoing send. If the send is cancelled, the socket is already in a valid state. |
| **DEALER** | `send` / `send_multipart` | **Safe**. The user `await`s pushing the message into a queue for a dedicated background processor. Cancellation simply prevents the message from being queued. |
| | `recv` / `recv_multipart` | **Safe**. The `await` is on `ReadyPipeQueue::pop()` inside `AddressedIngressEngine`. Identical safety guarantees to `PULL`. |
| **ROUTER** | `send` / `send_multipart` | **Safe**. `await` points are for peer identity lookup and per-connection send-permit acquisition (`Semaphore`). All are cancel-safe and do not modify state if dropped. |
| | `recv` / `recv_multipart` | **Safe**. The `await` is on `AddressedIngressEngine`, which returns `(pipe_id, FrameBatch)`. Cancellation leaves the batch and activation signal intact. |

## 5. Conclusion

The architectural separation between the user-facing API and the background I/O actors is the cornerstone of `rzmq`'s robustness. Session actors push complete `FrameBatch`es directly into per-pipe channels via `PipeMessageSender`, and the O(1) activation-track `ReadyPipeQueue` ensures that no message is ever lost or corrupted on cancellation. Dropping any `send` or `recv` `Future` is a safe, predictable operation that preserves the integrity of the socket and all in-flight data.
