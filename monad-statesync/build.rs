use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=../monad-cxx/monad-execution");
    println!("cargo:rerun-if-env-changed=TRIEDB_TARGET");
    let build_execution_lib =
        env::var("TRIEDB_TARGET").is_ok_and(|target| target == "triedb_driver");
    if build_execution_lib {
        let target = "monad_statesync";
        let dst = cmake::Config::new("../monad-cxx/monad-execution")
            .define("CMAKE_BUILD_TARGET", target)
            .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
            .define("BUILD_SHARED_LIBS", "ON")
            .build_target(target)
            .build();

        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dst.display());
        println!(
            "cargo:rustc-link-search=native={}/build/category/statesync",
            dst.display()
        );
        println!("cargo:rustc-link-lib=dylib={}", &target);
    }

    let bindings = bindgen::Builder::default()
        .header("../monad-cxx/monad-execution/category/statesync/statesync_messages.h")
        .header("../monad-cxx/monad-execution/category/statesync/statesync_client.h")
        .header("../monad-cxx/monad-execution/category/statesync/statesync_version.h")
        .clang_arg("-I../monad-cxx/monad-execution")
        .clang_arg("-std=c23")
        // invalidate on header change
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("state_sync.rs"))
        .expect("Couldn't write bindings!");
}
