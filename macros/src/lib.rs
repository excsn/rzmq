// rzmq-macros/src/lib.rs
extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
  let input_fn = parse_macro_input!(item as ItemFn);

  // Preserve original function signature and block
  let fn_vis = &input_fn.vis;
  let fn_sig = &input_fn.sig;
  let fn_block = &input_fn.block;
  let fn_attrs = &input_fn.attrs; // Keep other attributes like #[test]

  // Ensure the function is async
  if fn_sig.asyncness.is_none() {
    return syn::Error::new_spanned(&fn_sig, "`#[rzmq::main]` attribute can only be used on async functions")
      .to_compile_error()
      .into();
  }

  // Conditionally choose the underlying Tokio main macro
  // The `feature = "io-uring"` here will be active if the rzmq crate (which depends on this)
  // is compiled with its "io-uring" feature.
  let main_macro = if cfg!(all(target_os = "linux", feature = "io-uring")) {
    // This assumes that if the `io-uring` feature is enabled for `rzmq`,
    // `tokio-uring` will be available in the user's project (as a dependency of rzmq).
    quote! { tokio_uring::main }
  } else {
    quote! { tokio::main }
  };

  let expanded = quote! {
    #(#fn_attrs)* // Apply original attributes first
    #[#main_macro]
    #fn_vis #fn_sig // Use the original signature (which includes async fn name(...) -> T)
    #fn_block
  };

  TokenStream::from(expanded)
}
