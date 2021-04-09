fn main() {
    println!("cargo:rerun-if-changed=proto");
    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(&["."]);

    tonic_build::configure()
        .compile_with_config(
            prost_build,
            &["proto/event.proto", "proto/vector.proto"],
            &["proto/"],
        )
        .unwrap();

    built::write_built_file().expect("Failed to acquire build-time information");
}

// fn main() {
//     println!("cargo:rerun-if-changed=proto/prometheus-remote.proto");
//     println!("cargo:rerun-if-changed=proto/prometheus-types.proto");
//     let mut prost_build = prost_build::Config::new();
//     prost_build.btree_map(&["."]);
//     // It would be nice to just add these derives to all the types, but
//     // prost automatically adds them already to enums, which causes the
//     // extra derives to conflict with itself.
//     prost_build.type_attribute("Label", "#[derive(Eq, Hash, Ord, PartialOrd)]");
//     prost_build.type_attribute("MetricType", "#[derive(num_enum::TryFromPrimitive)]");
//     prost_build
//         .compile_protos(&["proto/prometheus-remote.proto"], &["proto/"])
//         .unwrap();
// }
