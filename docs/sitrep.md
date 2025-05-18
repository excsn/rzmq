Okay, let's revise the SITREP to categorize features more clearly into **Implemented**, **Partially Implemented**, and **Unimplemented/Stubbed**.

**rzmq Implementation SITREP (Revised Categorization)**

**Overall Status:** Core architecture and basic functionality for standard socket patterns over TCP and IPC are functional. ZMTP handshakes (NULL sec), keepalives, message forwarding, basic HWM/RCVTIMEO, refined error reporting/cleanup, improved Linger, and basic SNDTIMEO handling are in place. Inproc transport is mostly functional. Major areas for completion include full security layers, blocking send waits, and `io_uring`.

**Component Status:**

**Implemented:**

1.  **Core Types (`error.rs`, `message/*.rs`)**: `ZmqError`, `Msg`, `MsgFlags`, `Metadata`, `Blob`.
2.  **Runtime Primitives (`runtime/*.rs`)**: `Command` enum structure (for implemented features), `MailboxSender`/`Receiver`, `mailbox()` factory.
3.  **Core Abstractions (`socket/mod.rs`, etc.)**: `ISocket`, `ISession`, `IEngine`, `Mechanism` trait definitions. `MechanismStatus` enum.
4.  **Context (`context.rs`)**: `ContextInner` state management, `Context` public handle and methods (`new`, `socket`, `shutdown`, `term`). Conditional `libsodium-rs` init.
5.  **SocketCore (`socket/core.rs`)**:
    *   `CoreState`, `EndpointInfo` structures.
    *   `SocketCore` actor structure, `create_and_spawn` factory.
    *   `run_pipe_reader_task`.
    *   Basic command handling loop structure.
    *   Pipe creation/management (`async_channel`).
    *   Endpoint state management via `endpoints` map.
    *   Pipe state management (`pipes_tx`, `pipe_reader_task_handles`).
    *   Cleanup helpers (`cleanup_session_state_by_uri`, `cleanup_session_state_by_pipe`).
    *   Standardized error/cleanup reporting handlers (`ReportError`, `CleanupComplete`).
    *   Basic option parsing helpers (`parse_i32_option`, etc.) and handling for core/TCP/keepalive/basic security options within `handle_set/get_option`.
    *   Delegation of user send/recv/options to `ISocket`.
    *   `send_msg_with_timeout` helper.
6.  **Transport Actors (`transport/*.rs`)**:
    *   `Endpoint` enum and `parse_endpoint` function.
    *   `TcpListener`/`TcpConnecter`: Bind/Accept/Connect, Session/Engine spawning, basic TCP option application (`socket2`), standardized reporting (`ReportError`, `CleanupComplete`).
    *   `IpcListener`/`IpcConnecter` (`feature=ipc`): Similar logic to TCP, socket file cleanup on Drop.
    *   `Inproc` Helpers (`feature=inproc`): `bind`/`connect`/`unbind`/`disconnect` logic using Context registry and direct pipe setup/teardown commands.
7.  **Session Actor (`session/base.rs`)**: Basic forwarding logic between Core pipe and Engine mailbox, `Attach`/`AttachPipe` handling, basic Engine status handling, standardized reporting.
8.  **Engine Actor (`engine/*.rs`)**:
    *   `ZmtpEngineCore`: Generic logic, ZMTP handshake (Greeting, NULL sec, READY exchange, peer identity extraction), message loop, basic PING/PONG command handling, ZMTP Keepalive timer logic.
    *   `ZmtpTcpEngine`/`ZmtpIpcEngine` Factories: Correct instantiation of `ZmtpEngineCore` with specific stream types and `ZmtpEngineConfig`.
    *   `ZmtpEngineConfig` definition.
9.  **Pattern Helpers (`socket/patterns/*.rs`)**: `LoadBalancer`, `FairQueue`, `Distributor`, `SubscriptionTrie`, `RouterMap` provide basic required functionality.
10. **Protocol/Codec (`protocol/zmtp/*.rs`)**: `ZmtpCodec` (framing), `ZmtpGreeting` (encode/decode), basic `ZmtpCommand` parsing/creation (PING/PONG/READY/ERROR).
11. **Security (`security/*.rs`)**: `NullMechanism`. `Mechanism` trait methods `set_error`/`error_reason`.

**Partially Implemented:**

1.  **SocketCore (`socket/core.rs`)**:
    *   `shutdown_logic`: Linger logic handles timed/zero wait; infinite wait needs refinement (doesn't block indefinitely on queue checks yet, just loops with sleeps).
    *   Option Handling: Covers many options, but dynamic HWM resizing for existing pipes is missing. Not all ZMQ options covered.
    *   Error Handling: Cleanup coordination improved, but edge cases (e.g., shutdown during error handling) might exist. Decision on whether child errors terminate `SocketCore` needed.
2.  **ISocket Implementations (`socket/*.rs`)**:
    *   Send Path (`PUSH`/`DEALER`/`REQ`/`ROUTER`): `SNDTIMEO` handling for HWM/timeout via `send_msg_with_timeout` is implemented; blocking waits (`SNDTIMEO = -1` or `>0` when no peer) are **stubbed (TODO)**.
    *   ROUTER: Identity prepending on receive works; delimiter logic between DEALER/ROUTER is basic (assumes empty delimiter needed); identity cleanup on send errors/disconnect needs refinement. `pipe_detached` ID mapping might need fixes for PUB/PUSH/ROUTER.
    *   Pattern-Specific Options: Basic handling exists (`SUBSCRIBE`/`UNSUBSCRIBE` in SUB), but most pattern-specific options are unsupported.
3.  **Security (`security/*.rs`)**: `PlainMechanism` (state machine implemented, ZAP integration **stubbed**).
4.  **Inproc Transport (`feature=inproc`)**: Connection logic implemented; robustness of state management in `SocketCore`'s `EndpointInfo` for Inproc (dummy task handle) could be improved.

**Unimplemented/Stubbed:**

1.  **Security (`security/*.rs`)**:
    *   `CurveMechanism` (`feature=curve`): Struct and trait impl exist but core crypto logic is unimplemented.
    *   ZAP Integration: `ZapClient` stubbed, calls within `PlainMechanism`/`CurveMechanism` stubbed, `SessionBase` ZAP handling stubbed, related `Command` variants unused.
2.  **`io_uring` Optimization**: Feature flag exists, config placeholders exist, but no actual `io_uring` code integrated into Engine.
3.  **Advanced Features/Options**: Reconnect logic (`RECONNECT_IVL*`), many ZMQ options, ROUTER behaviors (mandatory, handover), etc.
4.  **Comprehensive Testing**: Unit and integration tests are largely missing.
5.  **Blocking Send Waits**: The actual blocking/waiting logic for PUSH/DEALER/REQ when `SNDTIMEO != 0` and no peer/HWM prevents sending.

This breakdown provides a clearer view of what's solid, what's functional but needs work, and what's completely missing.