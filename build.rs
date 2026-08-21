fn main() {
    // Only compile the fabric shim when the efa feature is enabled.
    if std::env::var("CARGO_FEATURE_EFA").is_ok() {
        // Find EFA include/lib paths
        let efa_prefix = std::env::var("EFA_PREFIX")
            .unwrap_or_else(|_| "/opt/amazon/efa".to_string());
        let include_dir = format!("{}/include", efa_prefix);
        let lib_dir = format!("{}/lib64", efa_prefix);

        // Compile the C shim
        cc::Build::new()
            .file("src/fabric_shim.c")
            .include(&include_dir)
            .opt_level(2)
            .compile("fabric_shim");

        // Link libfabric (dynamic)
        println!("cargo:rustc-link-search=native={}", lib_dir);
        println!("cargo:rustc-link-lib=dylib=fabric");
    }
}
