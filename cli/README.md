# RZMQ CLI (`rzmq`)

Command-line utility for RZMQ, a pure-Rust asynchronous ZeroMQ implementation.

**License:** Mozilla Public License v2.0 (MPL-2.0)

## Overview

The `rzmq` CLI provides helpful utilities for working with the RZMQ library. Currently, its primary function is to generate cryptographic keypairs for use with RZMQ's security mechanisms, such as Noise_XX.

## Installation

### Prerequisites

*   Rust and Cargo (latest stable version recommended). You can install them from [rustup.rs](https://rustup.rs/).

### From Crates.io (Once Published)

```bash
cargo install rzmq_cli
```

### From Source (This Repository)

1.  Clone the RZMQ repository (if the CLI is part of the main repo) or the standalone `rzmq_cli` repository.
    ```bash
    # Example if it's in the main rzmq repo in a subdirectory:
    git clone https://github.com/your_username/rzmq.git
    cd rzmq/rzmq_cli 
    # Or if it's a standalone repo:
    # git clone https://github.com/your_username/rzmq_cli.git
    # cd rzmq_cli
    ```
2.  Build and install the CLI:
    ```bash
    cargo install --path .
    ```
    This will install the `rzmq` binary into your Cargo bin directory (usually `~/.cargo/bin/`). Ensure this directory is in your system's `PATH`.

## Usage

The CLI is invoked using the `rzmq` command, followed by subcommands.

### Key Generation

The `keygen` subcommand is used to generate cryptographic keys.

#### Noise_XX (X25519) Keypair Generation

To generate a static X25519 keypair (secret key and public key) for the Noise_XX protocol pattern:

```bash
rzmq keygen noise-xx --name <KEY_NAME> [OPTIONS]
```

**Arguments & Options:**

*   `--name <KEY_NAME>` (Required, alias: `-n`)
    *   A base name for the generated key files. For example, if `<KEY_NAME>` is "server", files `server.noise.sk` (secret key) and `server.noise.pk` (public key) will be created.
*   `--output-dir <DIR>` (Optional, alias: `-o`, default: current directory `.`)
    *   The directory where the generated key files will be saved. The directory will be created if it doesn't exist.
*   `--file-format <FORMAT>` (Optional, default: `hex`)
    *   Specifies the format of the content stored *inside* the generated key files.
    *   Possible values:
        *   `hex`: Keys are stored as hexadecimal strings. (Default)
        *   `base64`: Keys are stored as URL-safe Base64 strings (no padding).
        *   `binary`: Keys are stored as raw 32-byte binary data.
*   `--display-format <FORMAT>` (Optional, default: `hex`)
    *   Specifies the format for displaying the generated keys on *stdout*.
    *   Possible values:
        *   `hex`: Display keys as hexadecimal strings. (Default)
        *   `base64`: Display keys as URL-safe Base64 strings.
        *   `rust`: Display keys as Rust `[u8; 32]` array literals, suitable for embedding in code.
*   `--force` (Optional, alias: `-f`)
    *   A flag that, if present, will cause the command to overwrite existing key files with the same name without prompting.

**File Naming Convention:**

*   Secret Key: `<KEY_NAME>.noise.sk`
*   Public Key: `<KEY_NAME>.noise.pk`

**File Permissions (Unix-like systems):**

*   Secret key files (`.noise.sk`) are created with permissions `0600` (read/write for owner only).
*   Public key files (`.noise.pk`) are created with permissions `0644` (read/write for owner, read-only for group and others).

**Examples:**

1.  Generate a keypair named "my_server" in the current directory, with hex format in files and hex display on stdout (default formats):
    ```bash
    rzmq keygen noise-xx --name my_server
    ```
    Output will show keys in hex and create `./my_server.noise.sk` and `./my_server.noise.pk` containing hex strings.

2.  Generate a keypair named "client01", save raw binary keys into a `keys/` subdirectory, and display keys as Rust arrays:
    ```bash
    rzmq keygen noise-xx --name client01 --output-dir ./keys --file-format binary --display-format rust
    ```
    This creates `./keys/client01.noise.sk` and `./keys/client01.noise.pk` with raw binary content.

**Using Generated Keys with RZMQ Library:**

The RZMQ library's socket options for Noise_XX (`NOISE_XX_STATIC_SECRET_KEY`, `NOISE_XX_REMOTE_STATIC_PUBLIC_KEY`) expect **raw 32-byte arrays**.

*   If you generate keys with `--file-format binary`, you can read these files directly into a `[u8; 32]` array in your Rust application.
*   If you use `--file-format hex` or `--file-format base64`, your application code will need to read the string from the file and then decode it into raw bytes before setting the socket option.

**Example: Loading a hex-encoded key in your RZMQ application:**
```rust
// Assuming you have `hex = "0.4"` in your application's Cargo.toml
use rzmq::{Context, SocketType, ZmqError};
use rzmq::socket::options::NOISE_XX_STATIC_SECRET_KEY; // Import the option constant
use std::fs;

async fn setup_socket_with_hex_key(ctx: &Context, sk_file_path: &str) -> Result<rzmq::Socket, anyhow::Error> {
    let socket = ctx.socket(SocketType::Dealer)?; // Or any other type

    let sk_hex_str = fs::read_to_string(sk_file_path)?;
    let sk_bytes_vec = hex::decode(sk_hex_str.trim())?;
    let sk_array: [u8; 32] = sk_bytes_vec.try_into()
        .map_err(|_| anyhow::anyhow!("Secret key file content has incorrect length after hex decoding"))?;
    
    socket.set_option_raw(NOISE_XX_STATIC_SECRET_KEY, &sk_array).await?;
    // ... set other options like remote public key ...
    Ok(socket)
}
```

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the Mozilla Public License v2.0 (MPL-2.0). See the [LICENSE](../LICENSE) file for details.