# Cancellation Safety in rzmq

## 1. Executive Summary

`rzmq` is designed to be **fully cancel-safe**. Dropping a `Future` returned by any `socket.send()` or `socket.recv()` method before it completes will not lead to data corruption, resource leaks, or an inconsistent application state.

This robustness is achieved through a core architectural principle: the **Actor Model**. User-facing API calls are decoupled from the actual I/O operations and state management, which are handled by dedicated, persistent background tasks (actors). When a user cancels an operation, they are only cancelling their *request* to an actor, not the actor itself.

## 2. Core Architectural Principles of Cancel Safety

The library's cancel safety relies on three fundamental patterns working in concert.

### 2.1. The Actor Model: Isolating State and I/O

Every `rzmq` socket is managed by a `SocketCore` actor. This actor, along with its child tasks (like `SessionConnectionActorX` or the global `UringWorker`), are the only entities that own and modify critical state, such as network connections, protocol state machines, and message buffers.

*   **User Task vs. I/O Task**: When a user calls `socket.send().await`, their task is **not** the one writing bytes to the network. Instead, it sends a command to the `SocketCore`.
*   **State Protection**: Because the I/O actor owns the state, dropping the user's `Future` does not affect the actor's internal state. The actor continues running, ready to process the next command, ensuring the socket is never left in a corrupted state.

### 2.2. Asynchronous Command Channels: The Cancel-Safe Boundary

Communication between the user's task and the socket actors occurs over asynchronous, bounded message channels (`fibre::mpmc` or `fibre::mpsc`).

*   **`send()` Safety**: When a user sends a message, they `await` pushing it into a channel. If this `Future` is dropped (e.g., because the channel is full and the task is cancelled), the `send` operation on the channel is safely aborted. The message remains owned by the user's task and is never delivered to the actor.
*   **`recv()` Safety**: When a user receives a message, they `await` popping an item from a channel. If this is cancelled, the item simply remains in the channel, available for the next `recv()` call.

These channels act as a robust, cancel-safe boundary between the application's logic and the socket's internal machinery.

### 2.3. Internal Buffering: Preventing Message Loss on `recv`

All incoming messages are handled by I/O actors, fully parsed, and placed into an internal queue before the user's `recv()` call is even aware of them.

*   **`IncomingMessageOrchestrator`**: This component is central to receive operations. Background I/O actors read from the network, assemble complete logical messages (handling multi-part frames), and then push the complete message as a single item into the orchestrator's `FairQueue`.
*   **User Receives from a Buffer**: The user's `socket.recv().await` call is simply waiting to pop a fully-formed message from this internal `FairQueue`. Cancelling the `recv()` `Future` has no effect on the queue or the messages within it. **No messages are lost.**

## 3. The Overall Process: Tracing a `send` and `recv` Call

To make this concrete, let's trace the lifecycle of a `send` and `recv` operation.

### 3.1. `send` Lifecycle (e.g., `DEALER` socket)
1.  **User Call**: `socket.send(msg).await` is called.
2.  **State Check**: The `DealerSocket` implementation locks its state to begin a send transaction.
3.  **Queueing**: The message is placed into the `pending_outgoing_queue` (an `Arc<TokioMutex<...>>`). The user's task might `await` here if the queue is full or another send is in progress.
    *   **User Cancel Point**: If the `Future` is dropped here, the message is never queued. The socket's state is perfectly valid.
4.  **Background Processing**: A separate, long-lived task (`DealerSocketOutgoingProcessor`) is constantly waiting for items in the `pending_outgoing_queue`.
5.  **Peer Selection**: The processor task wakes up, takes the message, and uses its `LoadBalancer` to select a peer connection.
6.  **Delegation**: The processor calls `send_multipart()` on the peer's `ISocketConnection` interface. This sends the message over *another* channel to the `SessionConnectionActorX` responsible for that specific network connection.
7.  **Final I/O**: The `SessionConnectionActorX` receives the message and finally performs the actual `stream.write_all().await` call to send the bytes over the network.

The user's cancellation point is completely decoupled from the final network I/O.

### 3.2. `recv` Lifecycle (e.g., `PULL` socket)
1.  **Background I/O**: A `SessionConnectionActorX` `await`s data on its `TcpStream`.
2.  **Frame Parsing**: It reads bytes and parses a single ZMTP frame (`Msg`).
3.  **Command to Core**: It sends a `Command::PipeMessageReceived` containing this single frame to the `SocketCore` actor.
4.  **Message Accumulation**: The `SocketCore` calls `PullSocket::handle_pipe_event`, which in turn calls `IncomingMessageOrchestrator::accumulate_pipe_frame`. This method **synchronously** appends the frame to an internal buffer, checking if it's the last frame of a logical message.
5.  **Queueing**: Once a complete logical message is assembled, the `SocketCore` pushes the entire `Vec<Msg>` as a single item into the `IncomingMessageOrchestrator`'s `FairQueue`.
6.  **User Call**: The user's task, which called `socket.recv_multipart().await`, is `await`ing an item from this `FairQueue`.
    *   **User Cancel Point**: If the `Future` is dropped here, the `Vec<Msg>` item remains in the queue, unharmed and ready for the next `recv` call.

---

## 4. Socket-by-Socket Cancel Safety Analysis

| Socket | Method | Cancel Safety Analysis |
| :--- | :--- | :--- |
| **PULL** | `recv`/`recv_multipart` | **Safe**. The user's `await` is on an internal `FairQueue` managed by the `IncomingMessageOrchestrator`. Cancellation leaves the buffered message in the queue. |
| | `send`/`send_multipart` | N/A. Returns an error immediately. |
| **PUSH** | `send`/`send_multipart` | **Safe**. The user `await`s peer availability or space in an outgoing channel. Cancellation aborts the send request without affecting the background I/O actors. |
| | `recv`/`recv_multipart` | N/A. Returns an error immediately. |
| **PUB** | `send`/`send_multipart` | **Safe**. The method iterates through peers and `await`s sending to each one. Cancellation simply stops the iteration. The state of all underlying connections remains valid. |
| | `recv`/`recv_multipart` | N/A. Returns an error immediately. |
| **SUB** | `recv`/`recv_multipart` | **Safe**. Identical to `PULL`. The user `await`s on the `IncomingMessageOrchestrator`'s `FairQueue`. |
| | `send`/`send_multipart` | N/A. Returns an error immediately. |
| **REQ** | `send` | **Safe**. The state is protected by a synchronous mutex. The `await` points for peer selection and sending happen outside the lock. Cancellation before the send completes leaves the state as `ReadyToSend`. |
| | `recv`/`recv_multipart` | **Safe**. The `await` is on the `IncomingMessageOrchestrator`. If cancelled, the reply remains in the queue and the socket state remains `ExpectingReply`. The user can simply call `recv` again. |
| **REP** | `recv`/`recv_multipart` | **Safe**. The `await` is on the `IncomingMessageOrchestrator`. If cancelled, the request remains in the queue and the socket state remains `ReadyToReceive`. |
| | `send`/`send_multipart` | **Safe**. The state is atomically updated to `ReadyToReceive` *before* the `await` on the send operation. If the send is cancelled, the socket is already in a valid state to receive the next request. |
| **DEALER**| `send`/`send_multipart` | **Safe**. The user `await`s pushing the message into a queue for a dedicated background processor. Cancellation simply prevents the message from being queued. |
| | `recv`/`recv_multipart` | **Safe**. Identical to `PULL`. The user `await`s on the `IncomingMessageOrchestrator`'s `FairQueue`. |
| **ROUTER**| `send`/`send_multipart` | **Safe**. The `await` points are for looking up a peer's routing info and acquiring a per-connection send permit (`Semaphore`). All are cancel-safe operations that do not modify state if dropped. |
| | `recv`/`recv_multipart` | **Safe**. Identical to `PULL`, but the item in the `FairQueue` includes the peer's identity. Cancellation is handled safely. |

## 5. Conclusion

The architectural separation between the user-facing API and the background I/O actors is the cornerstone of `rzmq`'s robustness. By using cancel-safe channels as a communication boundary and performing all stateful operations within persistent, isolated tasks, the library ensures that dropping any `send` or `recv` `Future` is a safe, predictable operation that preserves the integrity of the socket.