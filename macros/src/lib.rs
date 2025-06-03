// rzmq-macros/src/lib.rs
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// An attribute macro to automatically select the appropriate Tokio runtime
/// for the main function of an application using `rzmq`.
///
/// Behavior:
/// - If the crate compiling with this attribute has enabled the `io-uring`
///   feature for its `rzmq` dependency, AND the compilation target is Linux,
///   this macro expands to `#[tokio_uring::main]`.
/// - Otherwise (not Linux, or the `rzmq/io-uring` feature is not enabled by the
///   consuming crate), this macro expands to `#[tokio::main]`.
///
/// This allows users to write `#[rzmq::main]` consistently and let the
/// build configuration and target OS determine the underlying runtime setup,
/// facilitating optional `io_uring` usage.
///
/// The function this attribute is applied to MUST be an `async fn`.
///
/// # Example
/// ```ignore
/// // In user's main.rs
/// use rzmq::main as rzmq_main; // Or just use rzmq::main if no conflicts
///
/// #[rzmq_main] // Or #[rzmq::main]
/// async fn main() {
///     // ... application logic ...
/// }
/// ```
/// In the user's `Cargo.toml`:
/// ```toml
/// [dependencies]
/// rzmq = { version = "...", features = ["io-uring"] } # To enable io_uring path
/// # or
/// # rzmq = "..." # To use standard Tokio path
/// tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
/// # tokio-uring might be an indirect dependency via rzmq's io-uring feature,
/// # or the user might need to add it if rzmq doesn't re-export tokio_uring::main directly.
/// # However, the macro itself just generates the attribute name.
/// ```
#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
  // Parse the input tokens into a syntax tree representing the function.
  let input_fn = parse_macro_input!(item as ItemFn);

  // Preserve the original function's visibility (e.g., `pub`), signature,
  // attributes (like `#[test]`), and block of code.
  let fn_vis = &input_fn.vis;
  let fn_sig = &input_fn.sig; // This includes the function name, asyncness, generics, inputs, output
  let fn_block = &input_fn.block;
  let fn_attrs = &input_fn.attrs; // Retain other attributes on the function

  // Ensure the function signature is `async`.
  if fn_sig.asyncness.is_none() {
    // If not async, produce a compile error.
    let error_msg = "`#[rzmq::main]` attribute can only be used on `async` functions";
    return syn::Error::new_spanned(&fn_sig.fn_token, error_msg)
      .to_compile_error()
      .into();
  }

  // Conditionally determine which Tokio main macro attribute to apply.
  // The `cfg!(feature = "io-uring")` here checks if the "io-uring" feature
  // is active in the crate where this macro attribute (`#[rzmq::main]`) is being used.
  // This is typically the end-user's application crate, which would enable
  // the "io-uring" feature for its `rzmq` dependency.
  let underlying_main_macro_path = if cfg!(all(target_os = "linux", feature = "io-uring")) {
    eprintln!("RZMQ_MACRO_DIAGNOSTIC (Expansion Context): Trying tokio_uring::main. (is_linux: {}, io-uring_feature_in_this_compilation_unit: {})", cfg!(target_os = "linux"), cfg!(feature = "io-uring"));
    quote! { tokio_uring::main }
  } else {
    eprintln!("RZMQ_MACRO_DIAGNOSTIC (Expansion Context): Using tokio::main. (is_linux: {}, io-uring_feature_in_this_compilation_unit: {})", cfg!(target_os = "linux"), cfg!(feature = "io-uring"));
    quote! { tokio::main }
  };

  // Reconstruct the function with the chosen Tokio main macro attribute,
  // preserving all other original parts of the function.
  let expanded_code = quote! {
      #(#fn_attrs)* // Apply any other attributes the user had on their main function
      #[#underlying_main_macro_path]
      #fn_vis #fn_sig // Includes async fn original_name(...) -> ReturnType
      #fn_block
  };

  // Convert the generated token stream back into a TokenStream.
  TokenStream::from(expanded_code)
}

/// Attribute macro for `async` test functions in rzmq.
/// Automatically selects `#[tokio_uring::test]` on Linux when the `io-uring` feature
/// is enabled for the consuming crate, otherwise defaults to `#[tokio::test]`.
///
/// This allows test functions to run on the appropriate runtime for testing
/// rzmq's behavior with or without the io_uring backend.
#[proc_macro_attribute]
pub fn test(_attr: TokenStream, item: TokenStream) -> TokenStream {
  let input_fn = parse_macro_input!(item as ItemFn);
  let fn_vis = &input_fn.vis; // Usually `pub(crate)` or none for tests
  let fn_sig = &input_fn.sig; // Includes name, async, inputs, output
  let fn_block = &input_fn.block;
  let fn_attrs = &input_fn.attrs; // Keep other attributes like `#[should_panic]`

  if fn_sig.asyncness.is_none() {
    let error_msg = "`#[rzmq::test]` attribute can only be used on `async` functions";
    return syn::Error::new_spanned(&fn_sig.fn_token, error_msg)
      .to_compile_error()
      .into();
  }

  // Conditionally choose the underlying Tokio test macro attribute
  let underlying_test_macro_path = if cfg!(all(target_os = "linux", feature = "io-uring")) {
    eprintln!(
      "RZMQ_MACRO_DIAGNOSTIC (test): Using tokio_uring::test. (is_linux: {}, io-uring_feature: {})",
      cfg!(target_os = "linux"),
      cfg!(feature = "io-uring")
    );
    // Ensure tokio-uring provides a #[test] attribute.
    // As of tokio-uring 0.5, it *does* provide `tokio_uring::test`.
    quote! { tokio_uring::test }
  } else {
    eprintln!(
      "RZMQ_MACRO_DIAGNOSTIC (test): Using tokio::test. (is_linux: {}, io-uring_feature: {})",
      cfg!(target_os = "linux"),
      cfg!(feature = "io-uring")
    );
    quote! { tokio::test }
  };

  // Reconstruct the function with the chosen Tokio test macro attribute,
  // and importantly, ensure the standard #[test] attribute is also present
  // if the chosen macro doesn't imply it (tokio::test and tokio_uring::test do).
  let expanded_code = quote! {
      #(#fn_attrs)* // Apply existing attributes first (e.g. #[should_panic])
      // The Tokio test macros typically handle the #[test] part.
      #[#underlying_test_macro_path]
      #fn_vis #fn_sig // Includes async fn original_name(...) -> ReturnType
      #fn_block
  };

  TokenStream::from(expanded_code)
}
