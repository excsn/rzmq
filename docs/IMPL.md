**rzmq Implementation Guide**

**Version:** 1.0

**Status:** Initial Draft

**Table of Contents:**

1.  [Introduction](#1-introduction)
    *   [1.1. Purpose](#11-purpose)
    *   [1.2. Scope](#12-scope)
    *   [1.3. Target Audience](#13-target-audience)
    *   [1.4. Prerequisites](#14-prerequisites)
    *   [1.5. Core Concepts Recap](#15-core-concepts-recap)
2.  [Project Setup & Structure](#2-project-setup--structure)
    *   [2.1. `Cargo.toml` Dependencies & Features](#21-cargotoml-dependencies--features)
    *   [2.2. Detailed Module Structure (`src/`)](#22-detailed-module-structure-src)
3.  [Core Data Types](#3-core-data-types)
    *   [3.1. `error.rs` (`ZmqError`)](#31-errorrs-zmqerror)
    *   [3.2. `message/msg.rs` (`Msg`)](#32-messagemsgrs-msg)
    *   [3.3. `message/flags.rs` (`MsgFlags`)](#33-messageflagsrs-msgflags)
    *   [3.4. `message/metadata.rs` (`Metadata`)](#34-messagemetadatars-metadata)
    *   [3.5. `message/blob.rs` (`Blob`)](#35-messageblobrs-blob)
4.  [Runtime Primitives](#4-runtime-primitives)
    *   [4.1. `runtime/command.rs` (`Command`)](#41-runtimecommandrs-command)
    *   [4.2. `runtime/mailbox.rs` (`MailboxSender`, `MailboxReceiver`)](#42-runtimemailboxrs-mailboxsender-mailboxreceiver)
    *   [4.3. `runtime/pipe.rs` (`PipeEnd`, `IPipeEvents`)](#43-runtimepipers-pipeend-ipipeevents)
5.  [Core Abstractions (Traits)](#5-core-abstractions-traits)
    *   [5.1. `socket/mod.rs` (`ISocket` Trait)](#51-socketmodrs-isocket-trait)
    *   [5.2. `session/mod.rs` (`ISession` Trait)](#52-sessionmodrs-isession-trait)
    *   [5.3. `engine/mod.rs` (`IEngine` Trait)](#53-enginemodrs-iengine-trait)
    *   [5.4. `security/mod.rs` (`Mechanism` Trait)](#54-securitymodrs-mechanism-trait)
6.  [Concrete Actor Implementations](#6-concrete-actor-implementations)
    *   [6.1. General Actor Pattern](#61-general-actor-pattern)
    *   [6.2. `context.rs` (`Context`, `ContextInner`)](#62-contextrs-context-contextinner)
    *   [6.3. `socket/core.rs` (`SocketCore`, `CoreState`)](#63-socketcorers-socketcore-corestate)
    *   [6.4. `session/base.rs` (`SessionBase`)](#64-sessionbasers-sessionbase)
    *   [6.5. `engine/zmtp_tcp.rs` (`ZmtpTcpEngine`)](#65-enginezmtptcprs-zmtptcpengine)
    *   [6.6. `engine/zmtp_ipc.rs` (`ZmtpIpcEngine`)](#66-enginezmtpipcrs-zmtpipcengine)
    *   [6.7. `transport/tcp.rs` (`TcpListener`, `TcpConnecter`)](#67-transporttcprs-tcplistener-tcpconnecter)
    *   [6.8. `transport/ipc.rs` (`IpcListener`, `IpcConnecter`)](#68-transportipcrs-ipclistener-ipcconnecter)
    *   [6.9. `transport/inproc.rs` (Inproc Logic)](#69-transportinprocrs-inproc-logic)
7.  [Socket Type Implementations (`socket/`)](#7-socket-type-implementations-socket)
    *   [7.1. Socket Type Structure](#71-socket-type-structure)
    *   [7.2. `socket/pub_socket.rs` (`PubSocket`)](#72-socketpub_socketrs-pubsocket)
    *   [7.3. `socket/sub_socket.rs` (`SubSocket`)](#73-socketsub_socketrs-subsocket)
    *   [7.4. `socket/req_socket.rs` (`ReqSocket`)](#74-socketreq_socketrs-reqsocket)
    *   [7.5. `socket/rep_socket.rs` (`RepSocket`)](#75-socketrep_socketrs-repsocket)
    *   [7.6. `socket/dealer_socket.rs` (`DealerSocket`)](#76-socketdealer_socketrs-dealersocket)
    *   [7.7. `socket/router_socket.rs` (`RouterSocket`)](#77-socketrouter_socketrs-routersocket)
    *   [7.8. `socket/push_socket.rs` (`PushSocket`)](#78-socketpush_socketrs-pushsocket)
    *   [7.9. `socket/pull_socket.rs` (`PullSocket`)](#79-socketpull_socketrs-pullsocket)
    *   [7.10. `socket/types.rs` (`SocketType`, Public `Socket` Enum)](#710-sockettypesrs-sockettype-public-socket-enum)
8.  [Pattern Helpers (`socket/patterns/`)](#8-pattern-helpers-socketpatterns)
    *   [8.1. `socket/patterns/distributor.rs`](#81-socketpatternsdistributorrs)
    *   [8.2. `socket/patterns/fair_queue.rs`](#82-socketpatternsfair_queuers)
    *   [8.3. `socket/patterns/load_balancer.rs`](#83-socketpatternsload_balancerrs)
    *   [8.4. `socket/patterns/router.rs`](#84-socketpatternsrouterrs)
    *   [8.5. `socket/patterns/trie.rs`](#85-socketpatternstriers)
9.  [Protocol Details (`protocol/zmtp/`)](#9-protocol-details-protocolzmtp)
    *   [9.1. `protocol/zmtp/codec.rs` (`ZmtpCodec`)](#91-protocolzmtpcodecrs-zmtpcodec)
    *   [9.2. `protocol/zmtp/command.rs`](#92-protocolzmtpcommandrs)
    *   [9.3. `protocol/zmtp/greeting.rs`](#93-protocolzmtpgreetingrs)
10. [Security Implementation (`security/`)](#10-security-implementation-security)
    *   [10.1. `security/mechanism.rs` (`Mechanism` Impls)](#101-securitymechanismrs-mechanism-impls)
    *   [10.2. `security/zap.rs` (`ZapClient`)](#102-securityzaprs-zapclient)
11. [Lifecycle & Error Handling Details](#11-lifecycle--error-handling-details)
    *   [11.1. Startup Sequence](#111-startup-sequence)
    *   [11.2. Shutdown Sequence (`Stop` Cascade)](#112-shutdown-sequence-stop-cascade)
    *   [11.3. Error Propagation](#113-error-propagation)
    *   [11.4. Linger Implementation](#114-linger-implementation)
    *   [11.5. Resource Cleanup](#115-resource-cleanup)
12. [Concurrency Model & Safety](#12-concurrency-model--safety)
    *   [12.1. Tokio Runtime](#121-tokio-runtime)
    *   [12.2. State Management](#122-state-management)
    *   [12.3. Blocking Operations](#123-blocking-operations)
13. [Testing Strategy](#13-testing-strategy)
    *   [13.1. Unit Tests](#131-unit-tests)
    *   [13.2. Actor / Integration Tests](#132-actor--integration-tests)
    *   [13.3. Stress & Performance Tests](#133-stress--performance-tests)

---

**1. Introduction**

*   **1.1. Purpose:** Provide detailed technical guidance for implementing the `rzmq` library according to Spec v1.1 (Revised).
*   **1.2. Scope:** Core `rzmq` components, reliable transports (TCP, IPC, inproc) with ZMTP/3.1, standard security, actor model, optional `io_uring` TCP optimizations.
*   **1.3. Target Audience:** Core library developers.
*   **1.4. Prerequisites:** Rust, Tokio, async/await, Actor Model, ZeroMQ concepts, Spec v1.1.
*   **1.5. Core Concepts Recap:** Actors (Tokio tasks), Mailboxes (bounded MPSC channels + `Command` enum), Layers (API, Core, Pattern, Session, Engine, Transport), Explicit Lifecycle (`Stop`/`CleanupComplete`/`ReportError`), ZMTP/3.1 over reliable transports.

**2. Project Setup & Structure**

*   **2.1. `Cargo.toml` Dependencies & Features:** (As specified in the previous version of this guide - focus on tokio, bytes, thiserror, async-trait, socket2, optionally nix, tokio-uring, security libs, bitflags). Define features `tcp`, `ipc`, `inproc`, `io-uring`, `plain`, `curve`, `security`.
*   **2.2. Detailed Module Structure (`src/`):**

    ```
    src/
    ├── lib.rs             # Public API exports (Context, Socket, Msg, Error, consts), top-level fns
    ├── error.rs           # ZmqError enum and impls
    ├── context.rs         # Context handle struct, ContextInner actor impl
    │
    ├── message/
    │   ├── mod.rs         # pub use Msg, MsgFlags, Metadata, Blob;
    │   ├── msg.rs         # Msg struct impl
    │   ├── flags.rs       # MsgFlags bitflags definition
    │   ├── metadata.rs    # Metadata struct impl, key type definitions
    │   └── blob.rs        # Blob struct impl (optional)
    │
    ├── socket/
    │   ├── mod.rs         # pub trait ISocket, pub use types::Socket; pub mod core; pub mod options; ... (declare all submodules)
    │   ├── core.rs        # SocketCore actor struct and impl, CoreState struct
    │   ├── options.rs     # ZMQ option constants (RZMQ_...), ZmtpTcpConfig struct, option parsing helpers
    │   ├── types.rs       # SocketType enum, public Socket enum definition and handle impls
    │   │                  # (Wraps Arc<dyn ISocket>, forwards calls to SocketCore mailbox)
    │   ├── pub_socket.rs  # pub struct PubSocket; impl ISocket for PubSocket;
    │   ├── sub_socket.rs  # pub struct SubSocket; impl ISocket for SubSocket;
    │   ├── req_socket.rs  # pub struct ReqSocket; impl ISocket for ReqSocket;
    │   ├── rep_socket.rs  # pub struct RepSocket; impl ISocket for RepSocket;
    │   ├── dealer_socket.rs # pub struct DealerSocket; impl ISocket for DealerSocket;
    │   ├── router_socket.rs # pub struct RouterSocket; impl ISocket for RouterSocket;
    │   ├── push_socket.rs # pub struct PushSocket; impl ISocket for PushSocket;
    │   └── pull_socket.rs # pub struct PullSocket; impl ISocket for PullSocket;
    │
    ├── socket/patterns/   # Pattern logic helper modules
    │   ├── mod.rs         # pub use distributor::Distributor; ...
    │   ├── distributor.rs # Distributor struct/trait for PUB logic
    │   ├── fair_queue.rs  # FairQueue struct/trait for REP/PULL/ROUTER logic
    │   ├── load_balancer.rs # LoadBalancer struct/trait for REQ/DEALER logic
    │   ├── router.rs      # RouterMap struct for ROUTER ID->Pipe mapping
    │   └── trie.rs        # SubscriptionTrie struct for SUB filtering logic
    │
    ├── session/
    │   ├── mod.rs         # pub trait ISession; pub use base::SessionBase;
    │   └── base.rs        # SessionBase actor struct and impl
    │
    ├── engine/
    │   ├── mod.rs         # pub trait IEngine; pub mod zmtp_tcp; #[cfg(feature="ipc")] pub mod zmtp_ipc;
    │   ├── zmtp_tcp.rs    # ZmtpTcpEngine actor impl (handles standard/io_uring TCP)
    │   └── zmtp_ipc.rs    # ZmtpIpcEngine actor impl [cfg(feature = "ipc")]
    │
    ├── transport/
    │   ├── mod.rs         # Common types/traits? pub mod endpoint; pub mod tcp; ...
    │   ├── endpoint.rs    # Endpoint struct definition, parsing function (using `url` feature?)
    │   ├── tcp.rs         # TcpListener, TcpConnecter actor impls
    │   ├── ipc.rs         # IpcListener, IpcConnecter actor impls [cfg(feature = "ipc")]
    │   └── inproc.rs      # Inproc binding/connection logic impl [cfg(feature = "inproc")]
    │
    ├── protocol/
    │   └── zmtp/
    │       ├── mod.rs     # pub use codec::ZmtpCodec; pub use command::ZmtpCommand; ...
    │       ├── codec.rs   # ZmtpCodec struct impl Encoder/Decoder
    │       ├── command.rs # ZmtpCommand enum/structs, parsing logic
    │       └── greeting.rs # ZMTP greeting constants and validation logic
    │
    ├── security/
    │   ├── mod.rs         # pub trait Mechanism; pub use mechanism::*; pub use zap::ZapClient; ...
    │   ├── mechanism.rs   # NullMechanism, PlainMechanism, CurveMechanism struct impls, MechanismStatus enum
    │   └── zap.rs         # ZapClient struct impl, ZapRequest/Reply struct definitions
    │
    └── runtime/
        ├── mod.rs         # pub use command::Command; pub use mailbox::*; pub use pipe::*;
        ├── command.rs     # Command enum definition
        ├── mailbox.rs     # MailboxSender/Receiver type aliases, const DEFAULT_MAILBOX_CAPACITY
        └── pipe.rs        # PipeEnd struct impl, IPipeEvents trait definition
    ```

**3. Core Data Types**

*   **3.1. `error.rs` (`ZmqError`):** Implement as described previously (thiserror, comprehensive variants including `Uring`, `From` impls, `Send + Sync + 'static`).
*   **3.2. `message/msg.rs` (`Msg`):** Implement `Msg` struct wrapping `Option<bytes::Bytes>`, `MsgFlags`, `Metadata`. Provide specified methods.
*   **3.3. `message/flags.rs` (`MsgFlags`):** Use `bitflags!` macro for `MORE`, `COMMAND`.
*   **3.4. `message/metadata.rs` (`Metadata`):** Implement type map using `HashMap<TypeId, Arc<dyn Any + Send + Sync>>`. Define key types (`PeerAddress`, etc.).
*   **3.5. `message/blob.rs` (`Blob`):** Implement optionally, wrapping `bytes::Bytes`.

**4. Runtime Primitives**

*   **4.1. `runtime/command.rs` (`Command`):** Define enum with all variants and `oneshot::Sender` reply channels where needed.
*   **4.2. `runtime/mailbox.rs` (`MailboxSender`, `MailboxReceiver`):** Define type aliases and capacity constant.
*   **4.3. `runtime/pipe.rs` (`PipeEnd`, `IPipeEvents`):** Implement `IPipeEvents` trait. Implement `PipeEnd` struct wrapping shared queue (`Arc<Mutex<VecDeque<Msg>>>`) and peer mailbox. Implement `pipepair` factory and `write_message`, `read_message`, `close` methods. Ensure `Drop` sends `PipeClosedCmd`.

**5. Core Abstractions (Traits)**

*   Use `#[async_trait]` for all traits.
*   **5.1. `socket/mod.rs` (`ISocket` Trait):** Define methods mirroring public `Socket` API plus internal `process_command`, `handle_pipe_event`.
*   **5.2. `session/mod.rs` (`ISession` Trait):** Define methods for engine interaction, ZAP handling, status callbacks.
*   **5.3. `engine/mod.rs` (`IEngine` Trait):** Define methods for sending, handshaking, ZAP interaction, status.
*   **5.4. `security/mod.rs` (`Mechanism` Trait):** Define methods for state machine processing (`process_token`, `produce_token`), status query.

**6. Concrete Actor Implementations**

*   **6.1. General Actor Pattern:** Follow `create_and_spawn` factory + `async fn run_loop` structure.
*   **6.2. `context.rs` (`Context`, `ContextInner`):** Implement as specified, managing shared state (`RwLock`ed maps, atomics, `Notify`).
*   **6.3. `socket/core.rs` (`SocketCore`, `CoreState`):** Implement the central actor logic. Use `Mutex` for `CoreState`. Manage endpoints, handle User commands, delegate to `ISocket`, handle lifecycle, manage Linger.
*   **6.4. `session/base.rs` (`SessionBase`):** Implement mediation logic, pipe handling, `ZapClient` usage, status propagation.
*   **6.5. `engine/zmtp_tcp.rs` (`ZmtpTcpEngine`):**
    *   Implement `IEngine`.
    *   Use `AsyncTcpStream` wrapper enum internally.
    *   Implement runtime feature detection for `io-uring` (`#[cfg(feature = "io-uring")]`).
    *   Implement handshake logic (greeting, security).
    *   Implement message read/write loop using the appropriate path (standard Tokio or `tokio-uring` methods for read/writev/sendmsg_zc/multishot recv based on config and detected features). Requires careful handling of zero-copy completions.
*   **6.6. `engine/zmtp_ipc.rs` (`ZmtpIpcEngine`):** Implement `IEngine` using `tokio::net::UnixStream`. No io_uring specifics. `#[cfg(feature = "ipc")]`.
*   **6.7. `transport/tcp.rs` (`TcpListener`, `TcpConnecter`):** Implement using `tokio::net::Tcp*`. Pass `ZmtpTcpConfig` down.
*   **6.8. `transport/ipc.rs` (`IpcListener`, `IpcConnecter`):** Implement using `tokio::net::Unix*`. Handle path cleanup. `#[cfg(feature = "ipc")]`.
*   **6.9. `transport/inproc.rs` (Inproc Logic):** Implement bind/connect interacting with `ContextInner::inproc_registry`. Create direct `PipeEnd` pairs between `SocketCore` actors. `#[cfg(feature = "inproc")]`.

**7. Socket Type Implementations (`socket/`)**

*   **7.1. Socket Type Structure:** Each file (`pub_socket.rs`, etc.) will define a struct (e.g., `PubSocket`) containing `core: Arc<SocketCore>` and pattern-specific state/helpers (e.g., `distributor: Distributor`). It will implement `impl ISocket for PubSocket { ... }`.
*   **7.2. `socket/pub_socket.rs` (`PubSocket`):** Implement `ISocket`. `process_command` handles `UserSend` by calling `distributor.send_to_all`. `handle_pipe_event` adds/removes pipes from `distributor`.
*   **7.3. `socket/sub_socket.rs` (`SubSocket`):** Implement `ISocket`. `process_command` handles `UserSetOpt` (SUBSCRIBE/UNSUBSCRIBE) updating `trie`. `handle_pipe_event` reads from pipe, checks `trie`, pushes to user if matched. Send subscriptions upstream on connect.
*   **7.4. `socket/req_socket.rs` (`ReqSocket`):** Implement `ISocket`. Maintain REQ state machine (ReadyToSend, ExpectingReply). `process_command` handles `UserSend` (uses `load_balancer`, transitions state), `UserRecv` (reads from pipe, transitions state).
*   **7.5. `socket/rep_socket.rs` (`RepSocket`):** Implement `ISocket`. Maintain REP state machine (ExpectingRequest, ReadyToSendReply). `process_command` handles `UserRecv` (reads from pipe, stores routing info if any, transitions state), `UserSend` (sends reply via stored pipe, transitions state). Use `fair_queue` for incoming.
*   **7.6. `socket/dealer_socket.rs` (`DealerSocket`):** Implement `ISocket`. `process_command` for `UserSend` uses `load_balancer`. `handle_pipe_event` reads from pipe, uses `fair_queue` logic for user `recv`. Prepend empty delimiter on send if needed?
*   **7.7. `socket/router_socket.rs` (`RouterSocket`):** Implement `ISocket`. `process_command` for `UserSend` uses first frame as ID, looks up pipe in `router_map`, sends remaining frames. `handle_pipe_event` reads from pipe, prepends routing ID frame, pushes to user `recv` via `fair_queue`. Manage `router_map`.
*   **7.8. `socket/push_socket.rs` (`PushSocket`):** Implement `ISocket`. `process_command` for `UserSend` uses `load_balancer` (round-robin or similar). `UserRecv` is an error.
*   **7.9. `socket/pull_socket.rs` (`PullSocket`):** Implement `ISocket`. `handle_pipe_event` reads from pipes using `fair_queue` logic for user `recv`. `UserSend` is an error.
*   **7.10. `socket/types.rs` (`SocketType`, Public `Socket` Enum):** Define `pub enum SocketType { ... }`. Define `pub struct Socket { inner: Arc<dyn ISocket> }`. Implement `Clone`, `Debug`. Implement all public async methods on `Socket` by getting the `SocketCore` mailbox (e.g., via a method on `ISocket`) and sending the appropriate `User*` command, awaiting the `oneshot` reply.

**8. Pattern Helpers (`socket/patterns/`)**

*   Implement reusable structs/traits in their respective files. They typically manage a `HashMap<usize, PipeEnd>` or `HashMap<usize, MailboxSender>` and implement logic for adding, removing, sending to one/all/round-robin, reading round-robin, or filtering.

**9. Protocol Details (`protocol/zmtp/`)**

*   **9.1. `protocol/zmtp/codec.rs` (`ZmtpCodec`):** Implement `tokio_util::codec::{Encoder<Msg>, Decoder}` for ZMTP/3.1 framing. Handle flags, length prefixing, COMMAND frames.
*   **9.2. `protocol/zmtp/command.rs`:** Define ZMTP command structures (READY, ERROR, PING, PONG, etc.) and parsing logic if needed by the codec or engine.
*   **9.3. `protocol/zmtp/greeting.rs`:** Implement logic for sending/receiving/validating the 64-byte ZMTP greeting.

**10. Security Implementation (`security/`)**

*   **10.1. `security/mechanism.rs` (`Mechanism` Impls):** Implement `NullMechanism`, `PlainMechanism`, `CurveMechanism` state machines, handling token processing and production. Feature-gate `CurveMechanism` (`#[cfg(feature = "curve")]`).
*   **10.2. `security/zap.rs` (`ZapClient`):** Implement logic within `SessionBase` (or a dedicated helper struct) to handle ZAP Request/Reply flow with an external authenticator via inproc.

**11. Lifecycle & Error Handling Details**

*   Implement startup, shutdown (`Stop` cascade), error propagation (`ReportError`), Linger delay (`SocketCore`), and resource cleanup (RAII, explicit close in `Stop`) precisely as specified.

**12. Concurrency Model & Safety**

*   Use Tokio tasks, bounded MPSC, `Mutex`/`RwLock` correctly. Use `spawn_blocking` for synchronous blocking code. Be mindful of `tokio-uring` context requirements if the feature is enabled.

**13. Testing Strategy**

*   Implement unit tests (codec, patterns, mechanisms), actor/integration tests (mailbox interaction, pipeline tests over loopback), and stress/performance tests (`criterion`). Include tests specific to the `io-uring` feature and its fallbacks.