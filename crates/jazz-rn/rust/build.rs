fn main() {
    println!("cargo:rerun-if-changed=src/os_log_shim.c");

    // The unified-logging shim only exists on Apple platforms; Android and wasm
    // builds must not try to compile it (no <os/log.h>, no C toolchain needed).
    if std::env::var("CARGO_CFG_TARGET_VENDOR").as_deref() == Ok("apple") {
        cc::Build::new()
            .file("src/os_log_shim.c")
            .compile("jazz_rn_os_log_shim");
    }
}
