// Generates the language bindings from the #[uniffi::export] annotations
// in lib.rs (using proc-macro scaffolding, no separate .udl file needed).
//
// Usage:
//   cargo run --bin uniffi-bindgen generate --library target/debug/libsync_core.so \
//     --language kotlin --out-dir ../clients/android/generated
//   cargo run --bin uniffi-bindgen generate --library target/debug/libsync_core.dylib \
//     --language swift --out-dir ../clients/ios/generated

fn main() {
    uniffi::uniffi_bindgen_main()
}
