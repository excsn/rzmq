// tests/reconnect_tests.rs

use rzmq::{
    socket::{options::{RCVTIMEO, RECONNECT_IVL, ROUTER_MANDATORY, ROUTING_ID, SNDTIMEO}, SocketEvent, MonitorReceiver},
    Blob, Context, Msg, MsgFlags, Socket, SocketType, ZmqError,
};
use serial_test::serial;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Assuming 'common' module exists and provides at least:
// - test_context()
// - recv_timeout()
// - wait_for_monitor_event()
mod common;

// --- Test Configuration Constants ---
const TEST_ENDPOINT_BASE_MULTI_RECONNECT: &str = "tcp://127.0.0.1";
const DEFAULT_PORT_MULTI_RECONNECT: u16 = 6000;

const CLIENT_SNDTIMEO_MS_MR: i32 = 500;
const CLIENT_RCVTIMEO_MS_MR: i32 = 500;
const SERVER_SNDTIMEO_MS_MR: i32 = 500;
const SERVER_RCVTIMEO_MS_MR: i32 = 500;

const RECONNECT_IVL_MS_MR: i32 = 75;

const MONITOR_REACTION_TIMEOUT_MR: Duration = Duration::from_millis(RECONNECT_IVL_MS_MR as u64 * 10); // Increased slightly
const MONITOR_POLL_INTERVAL_MR: Duration = Duration::from_millis(50); // Used by common::wait_for_monitor_event
const SERVER_DOWNTIME_MR: Duration = Duration::from_millis(RECONNECT_IVL_MS_MR as u64 * 4);

fn generate_multi_reconnect_endpoint(test_case_offset: u16) -> String {
    format!(
        "{}:{}",
        TEST_ENDPOINT_BASE_MULTI_RECONNECT,
        DEFAULT_PORT_MULTI_RECONNECT + test_case_offset
    )
}

// --- Local Helper Functions for this test file ---
async fn configure_client_socket_for_reconnect_with_monitor(
    socket: &Socket,
    endpoint_for_log: &str, // For logging context
    reconnect_ivl_ms: i32,
    snd_timo_ms: i32,
    rcv_timo_ms: i32,
) -> Result<MonitorReceiver, ZmqError> {
    println!("[CLIENT {}] Configuring socket options and monitor...", endpoint_for_log);
    socket
        .set_option_raw(SNDTIMEO, &snd_timo_ms.to_ne_bytes())
        .await?;
    socket
        .set_option_raw(RCVTIMEO, &rcv_timo_ms.to_ne_bytes())
        .await?;
    socket
        .set_option_raw(RECONNECT_IVL, &reconnect_ivl_ms.to_ne_bytes())
        .await?;
    
    let monitor_rx = socket.monitor_default().await?;
    println!("[CLIENT {}] Socket configured with monitor.", endpoint_for_log);
    Ok(monitor_rx)
}

async fn configure_server_socket_for_test( // Renamed from configure_router_for_reconnect_test for generality
    socket: &Socket,
    endpoint_for_log: &str, // For logging context
    snd_timo_ms: i32,
    rcv_timo_ms: i32,
) -> Result<(), ZmqError> {
    println!("[SERVER {}] Configuring socket options...", endpoint_for_log);
    socket
        .set_option_raw(SNDTIMEO, &snd_timo_ms.to_ne_bytes())
        .await?;
    socket
        .set_option_raw(RCVTIMEO, &rcv_timo_ms.to_ne_bytes())
        .await?;
    // Specific options like ROUTER_MANDATORY will be set in the test itself if needed
    println!("[SERVER {}] Socket configured.", endpoint_for_log);
    Ok(())
}


// --- Test Cases ---
// PUSH/PULL and REQ/REP tests also need to call the locally defined helpers.

#[rzmq_macros::test]
#[serial]
async fn test_push_pull_reconnect_advanced_style() -> Result<(), ZmqError> {
    let endpoint = generate_multi_reconnect_endpoint(0); // Changed function name
    println!("\n--- Starting test_push_pull_reconnect_advanced_style on {} ---", endpoint);
    let ctx = common::test_context();
    println!("[PUSH {}] Creating PUSH socket...", endpoint);
    let push_socket = ctx.socket(SocketType::Push)?;
    // CORRECTED: Call local helper
    let push_monitor_rx = configure_client_socket_for_reconnect_with_monitor(
        &push_socket, &endpoint, RECONNECT_IVL_MS_MR, CLIENT_SNDTIMEO_MS_MR, CLIENT_RCVTIMEO_MS_MR
    ).await?;
    push_socket.connect(&endpoint).await?;
    println!("[PUSH {}] Connect initiated.", endpoint);

    println!("[PUSH {}] Attempting initial send (PULL server not up)...", endpoint);
    let initial_send_result = push_socket.send(Msg::from_static(b"Initial PUSH Ping")).await;
    println!("[PUSH {}] Initial send result: {:?}", endpoint, initial_send_result);
    assert!(
        matches!(initial_send_result, Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout)),
        "[PUSH {}] Initial send should fail (ResourceLimitReached or Timeout)", endpoint
    );

    println!("[SYS {}] Waiting {}ms before starting PULL server...", endpoint, SERVER_DOWNTIME_MR.as_millis()); // Using SERVER_DOWNTIME for consistency
    sleep(SERVER_DOWNTIME_MR).await;

    println!("[PULL {}] Creating, configuring, and binding PULL socket...", endpoint);
    let pull_socket = ctx.socket(SocketType::Pull)?;
    // CORRECTED: Call local helper
    configure_server_socket_for_test(&pull_socket, &endpoint, SERVER_SNDTIMEO_MS_MR, SERVER_RCVTIMEO_MS_MR).await?;
    pull_socket.bind(&endpoint).await?;
    println!("[PULL {}] Server bound successfully.", endpoint);

    println!("[SYS {}] Waiting for PUSH to reconnect (HandshakeSucceeded)...", endpoint);
    // CORRECTED: Call common::wait_for_monitor_event (signature from your common.rs)
    let _connect_event = common::wait_for_monitor_event(
        &push_monitor_rx,
        MONITOR_REACTION_TIMEOUT_MR, // Total timeout for the event
        MONITOR_POLL_INTERVAL_MR,    // Interval to check recv on monitor_rx
        |e| matches!(e, SocketEvent::HandshakeSucceeded { endpoint: ep } if ep.contains(endpoint.strip_prefix("tcp://").unwrap_or(&endpoint)))
    ).await.map_err(|e_str| ZmqError::Internal(format!("[PUSH {}] Did not receive HandshakeSucceeded event: {}", endpoint, e_str)))?;
    println!("[PUSH {}] Monitor confirmed HandshakeSucceeded.", endpoint);

    let test_payload = b"PUSH message post-reconnect";
    println!("[PUSH {}] Sending payload: '{}'", endpoint, String::from_utf8_lossy(test_payload));
    push_socket.send(Msg::from_static(test_payload)).await?;
    println!("[PUSH {}] Payload sent successfully.", endpoint);

    println!("[PULL {}] Attempting to receive message...", endpoint);
    let received_msg = common::recv_timeout(&pull_socket, Duration::from_millis(CLIENT_RCVTIMEO_MS_MR as u64)).await?;
    println!("[PULL {}] Received: '{}'", endpoint, String::from_utf8_lossy(received_msg.data().unwrap()));
    assert_eq!(received_msg.data().unwrap(), test_payload, "[PULL {}] Received incorrect message", endpoint);

    println!("--- test_push_pull_reconnect_advanced_style on {} PASSED ---", endpoint);
    ctx.term().await?;
    Ok(())
}

#[rzmq_macros::test]
#[serial]
async fn test_req_rep_reconnect_advanced_style() -> Result<(), ZmqError> {
    let endpoint = generate_multi_reconnect_endpoint(1); // Changed function name
    println!("\n--- Starting test_req_rep_reconnect_advanced_style on {} ---", endpoint);
    let ctx = common::test_context();
    println!("[REQ {}] Creating REQ socket...", endpoint);
    let req_socket = ctx.socket(SocketType::Req)?;
    // CORRECTED: Call local helper
    let req_monitor_rx = configure_client_socket_for_reconnect_with_monitor(
        &req_socket, &endpoint, RECONNECT_IVL_MS_MR, CLIENT_SNDTIMEO_MS_MR, CLIENT_RCVTIMEO_MS_MR
    ).await?;
    req_socket.connect(&endpoint).await?;
    println!("[REQ {}] Connect initiated.", endpoint);

    println!("[REQ {}] Attempting initial send (REP server not up)...", endpoint);
    let initial_send_result = req_socket.send(Msg::from_static(b"Initial REQuest")).await;
    println!("[REQ {}] Initial send result: {:?}", endpoint, initial_send_result);
    assert!(
        matches!(initial_send_result, Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout)),
        "[REQ {}] Initial send should fail (ResourceLimitReached or Timeout)", endpoint
    );

    println!("[SYS {}] Waiting {}ms before starting REP server...", endpoint, SERVER_DOWNTIME_MR.as_millis());
    sleep(SERVER_DOWNTIME_MR).await;

    println!("[REP {}] Creating, configuring, and binding REP socket...", endpoint);
    let rep_socket = ctx.socket(SocketType::Rep)?;
    // CORRECTED: Call local helper
    configure_server_socket_for_test(&rep_socket, &endpoint, SERVER_SNDTIMEO_MS_MR, SERVER_RCVTIMEO_MS_MR).await?;
    rep_socket.bind(&endpoint).await?;
    println!("[REP {}] Server bound successfully.", endpoint);

    println!("[SYS {}] Waiting for REQ to reconnect (HandshakeSucceeded)...", endpoint);
    // CORRECTED: Call common::wait_for_monitor_event
    let _connect_event = common::wait_for_monitor_event(
        &req_monitor_rx,
        MONITOR_REACTION_TIMEOUT_MR,
        MONITOR_POLL_INTERVAL_MR,
        |e| matches!(e, SocketEvent::HandshakeSucceeded { endpoint: ep } if ep.contains(endpoint.strip_prefix("tcp://").unwrap_or(&endpoint)))
    ).await.map_err(|e_str| ZmqError::Internal(format!("[REQ {}] Did not receive HandshakeSucceeded event: {}", endpoint, e_str)))?;
    println!("[REQ {}] Monitor confirmed HandshakeSucceeded.", endpoint);

    let req_payload = b"REQ message post-reconnect";
    println!("[REQ {}] Sending payload: '{}'", endpoint, String::from_utf8_lossy(req_payload));
    req_socket.send(Msg::from_static(req_payload)).await?;
    println!("[REQ {}] Payload sent successfully.", endpoint);

    println!("[REP {}] Attempting to receive request...", endpoint);
    let received_on_rep = common::recv_timeout(&rep_socket, Duration::from_millis(CLIENT_RCVTIMEO_MS_MR as u64)).await?;
    println!("[REP {}] Received: '{}'", endpoint, String::from_utf8_lossy(received_on_rep.data().unwrap()));
    assert_eq!(received_on_rep.data().unwrap(), req_payload, "[REP {}] Received incorrect request", endpoint);

    let rep_payload = b"REPly post-reconnect";
    println!("[REP {}] Sending reply: '{}'", endpoint, String::from_utf8_lossy(rep_payload));
    rep_socket.send(Msg::from_static(rep_payload)).await?;
    println!("[REP {}] Reply sent successfully.", endpoint);

    println!("[REQ {}] Attempting to receive reply...", endpoint);
    let received_on_req = common::recv_timeout(&req_socket, Duration::from_millis(CLIENT_RCVTIMEO_MS_MR as u64)).await?;
    println!("[REQ {}] Received: '{}'", endpoint, String::from_utf8_lossy(received_on_req.data().unwrap()));
    assert_eq!(received_on_req.data().unwrap(), rep_payload, "[REQ {}] Received incorrect reply", endpoint);

    println!("--- test_req_rep_reconnect_advanced_style on {} PASSED ---", endpoint);
    ctx.term().await?;
    Ok(())
}


#[rzmq_macros::test]
#[serial]
async fn test_dealer_router_multiple_disconnect_reconnect_cycle() -> Result<(), ZmqError> {
    let endpoint = generate_multi_reconnect_endpoint(0); // Re-used offset 0, ensure it's unique if running all tests
    let cycles = 10;
    println!(
        "\n--- Starting test_dealer_router_multiple_disconnect_reconnect_cycle on {} for {} cycles ---",
        endpoint, cycles
    );

    let ctx = common::test_context();

    // 1. Setup DEALER (client)
    let dealer_id_val = "dealer_multi_reconnect_id_000"; // Made ID more specific for this test
    println!("[DEALER {}] Creating DEALER socket (ID: {})...", endpoint, dealer_id_val);
    let dealer_socket = ctx.socket(SocketType::Dealer)?;
    // CORRECTED: Call local helper
    let dealer_monitor_rx = configure_client_socket_for_reconnect_with_monitor(
        &dealer_socket, &endpoint, RECONNECT_IVL_MS_MR, CLIENT_SNDTIMEO_MS_MR, CLIENT_RCVTIMEO_MS_MR
    ).await?;
    dealer_socket.set_option_raw(ROUTING_ID, dealer_id_val.as_bytes()).await?;
    dealer_socket.connect(&endpoint).await?;
    println!("[DEALER {}] Connect initiated.", endpoint);

    for cycle in 1..=cycles {
        println!("{} [CYCLE {}/{}] Starting...", endpoint, cycle, cycles);

        // 2. Start ROUTER (server)
        println!("[ROUTER {}] Cycle {}: Creating and binding ROUTER socket...", endpoint, cycle);
        let router_socket = ctx.socket(SocketType::Router)?;
        // CORRECTED: Call local helper
        configure_server_socket_for_test(
            &router_socket, &endpoint, SERVER_SNDTIMEO_MS_MR, SERVER_RCVTIMEO_MS_MR
        ).await?;
        router_socket.set_option_raw(ROUTER_MANDATORY, &1i32.to_ne_bytes()).await?;
        router_socket.bind(&endpoint).await?;
        println!("[ROUTER {}] Cycle {}: Server bound.", endpoint, cycle);

        // 3. Wait for DEALER to connect (verified by monitor)
        println!("[DEALER {}] Cycle {}: Waiting for HandshakeSucceeded monitor event...", endpoint, cycle);
        // CORRECTED: Call common::wait_for_monitor_event
        let connect_event = common::wait_for_monitor_event(
            &dealer_monitor_rx,
            MONITOR_REACTION_TIMEOUT_MR,
            MONITOR_POLL_INTERVAL_MR,
            |e| matches!(e, SocketEvent::HandshakeSucceeded { endpoint: ep } if ep.contains(endpoint.strip_prefix("tcp://").unwrap_or(&endpoint)))
        ).await.map_err(|e_str| ZmqError::Internal(format!("[DEALER {}] Cycle {}: Did not receive HandshakeSucceeded: {}", endpoint, cycle, e_str)))?;
        println!("[DEALER {}] Cycle {}: Monitor confirmed HandshakeSucceeded: {:?}", endpoint, cycle, connect_event);

        // 4. Perform a message exchange
        let msg_to_send = format!("Message from DEALER, Cycle {}", cycle);
        println!("[DEALER {}] Cycle {}: Sending: '{}'", endpoint, cycle, msg_to_send);
        dealer_socket.send(Msg::from_vec(msg_to_send.as_bytes().to_vec())).await?;

        println!("[ROUTER {}] Cycle {}: Receiving message from DEALER...", endpoint, cycle);
        // ROUTER application expects: [identity_frame, payload_frame_1, ..., payload_frame_N]
        // When a DEALER sends a single payload message, the ROUTER application gets 2 frames:
        let router_recv_id = common::recv_timeout(&router_socket, Duration::from_millis(SERVER_RCVTIMEO_MS_MR as u64)).await?;
        let router_recv_payload = common::recv_timeout(&router_socket, Duration::from_millis(SERVER_RCVTIMEO_MS_MR as u64)).await?;
        // NO DELIMITER is received by the application from the RouterSocket's queue.

        println!("[ROUTER {}] Cycle {}: Received ID: {:?}", endpoint, cycle, String::from_utf8_lossy(router_recv_id.data().unwrap()));
        println!("[ROUTER {}] Cycle {}: Received Payload: '{}'", endpoint, cycle, String::from_utf8_lossy(router_recv_payload.data().unwrap()));

        assert_eq!(router_recv_id.data().unwrap(), dealer_id_val.as_bytes(), "[ROUTER {}] Cycle {}: Incorrect dealer ID", endpoint, cycle);
        assert!(router_recv_id.is_more(), "[ROUTER {}] Cycle {}: Identity frame should have MORE flag as payload follows", endpoint, cycle);
        
        assert_eq!(router_recv_payload.data().unwrap(), msg_to_send.as_bytes(), "[ROUTER {}] Cycle {}: Incorrect payload", endpoint, cycle);
        assert!(!router_recv_payload.is_more(), "[ROUTER {}] Cycle {}: Payload frame should be the last part (no MORE flag)", endpoint, cycle);
        
        println!("[ROUTER {}] Cycle {}: Received dealer message correctly.", endpoint, cycle);

        let reply_to_send = format!("Reply from ROUTER, Cycle {}", cycle);
        let mut id_frame_reply = Msg::from_bytes(router_recv_id.data_bytes().unwrap());
        id_frame_reply.set_flags(MsgFlags::MORE);
        router_socket.send(id_frame_reply).await?;
        let mut delim_frame_reply = Msg::new();
        delim_frame_reply.set_flags(MsgFlags::MORE);
        router_socket.send(delim_frame_reply).await?;
        router_socket.send(Msg::from_vec(reply_to_send.as_bytes().to_vec())).await?;
        println!("[ROUTER {}] Cycle {}: Sent reply.", endpoint, cycle);

        let dealer_reply = common::recv_timeout(&dealer_socket, Duration::from_millis(CLIENT_RCVTIMEO_MS_MR as u64)).await?;
        assert_eq!(dealer_reply.data().unwrap(), reply_to_send.as_bytes());
        println!("[DEALER {}] Cycle {}: Received router reply correctly.", endpoint, cycle);
        println!("[SYS {}] Cycle {}: Message exchange successful.", endpoint, cycle);

        // 5. Stop the ROUTER server
        println!("[ROUTER {}] Cycle {}: Closing ROUTER socket...", endpoint, cycle);
        router_socket.close().await?;
        drop(router_socket);
        sleep(Duration::from_millis(RECONNECT_IVL_MS_MR as u64 / 2)).await; 
        println!("[ROUTER {}] Cycle {}: Server stopped.", endpoint, cycle);

        // 6. Verify DEALER sees a Disconnected event
        println!("[DEALER {}] Cycle {}: Waiting for Disconnected monitor event...", endpoint, cycle);
        // CORRECTED: Call common::wait_for_monitor_event
        let disconnect_event = common::wait_for_monitor_event(
            &dealer_monitor_rx,
            MONITOR_REACTION_TIMEOUT_MR,
            MONITOR_POLL_INTERVAL_MR,
            |e| matches!(e, SocketEvent::Disconnected { endpoint: ep } if ep.contains(endpoint.strip_prefix("tcp://").unwrap_or(&endpoint)))
        ).await.map_err(|e_str| ZmqError::Internal(format!("[DEALER {}] Cycle {}: Did not receive Disconnected event: {}", endpoint, cycle, e_str)))?;
        println!("[DEALER {}] Cycle {}: Monitor confirmed Disconnected: {:?}", endpoint, cycle, disconnect_event);
        
        if cycle < cycles {
            println!("[SYS {}] Cycle {}: Server downtime for {}ms...", endpoint, cycle, SERVER_DOWNTIME_MR.as_millis());
            sleep(SERVER_DOWNTIME_MR).await;
        }
    } 

    println!("--- test_dealer_router_multiple_disconnect_reconnect_cycle on {} PASSED ---", endpoint);
    ctx.term().await?;
    Ok(())
}