use std::fs;
use std::path::PathBuf;

fn main() {
    let proto_root = PathBuf::from("../otlp-proto");
    let search_dir = proto_root.join("");

    let proto_files: Vec<PathBuf> = find_protos(&search_dir);

    if proto_files.is_empty() {
        panic!("No .proto files found in {}", search_dir.display());
    }

    println!("cargo:warning=Compiling {} proto files", proto_files.len());

    for proto in &proto_files {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    prost_build::compile_protos(&proto_files, &[&proto_root])
        .expect("Failed to compile protobuf files");
}

fn find_protos(dir: &PathBuf) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("Failed to read directory {}: {}", dir.display(), e))
        .map(|entry| entry.expect("Failed to read directory entry").path())
        .flat_map(|path| {
            if path.is_dir() {
                find_protos(&path)
            } else if path.extension().is_some_and(|ext| ext == "proto") {
                vec![path]
            } else {
                vec![]
            }
        })
        .collect()
}
