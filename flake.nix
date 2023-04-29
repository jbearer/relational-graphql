{
  description = "Relational ORM for GraphQL queries";

  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.flake-compat.url = "github:edolstra/flake-compat";
  inputs.flake-compat.flake = false;
  inputs.rust-overlay.url = "github:oxalica/rust-overlay";

  outputs = { self, nixpkgs, flake-utils, flake-compat, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.rust-bin.stable.latest.minimal.override {
          extensions = [ "rustfmt" "clippy" "llvm-tools-preview" "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };
        nightlyToolchain = pkgs.rust-bin.nightly."2022-11-01".minimal.override {
          extensions = ["rustfmt" "clippy" "llvm-tools-preview" "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        };
        nixWithFlakes = pkgs.writeShellScriptBin "nix" ''
          exec ${pkgs.nixVersions.stable}/bin/nix --experimental-features "nix-command flakes" "$@"
        '';
        devPkgs = with pkgs;
            [
              postgresql
            ];
        rustDeps = with pkgs;
            [
              openssl
              bash
              curl
              cargo-llvm-cov
              cargo-sort
              wasm-pack
              openssl
              binaryen
              clang
            ] ++ lib.optionals stdenv.isDarwin [
              # required to build some crates on MacOS
              darwin.apple_sdk.frameworks.Security
              darwin.apple_sdk.frameworks.CoreFoundation
            ];
      in {
        devShell = pkgs.mkShell {
          buildInputs = with pkgs;
          [
            nixWithFlakes
            git
            rustToolchain
          ] ++ devPkgs ++ rustDeps;
          shellHook = "export CC=${pkgs.clang}/bin/clang";
          OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          RUST_LOG = "info";
          RUST_BACKTRACE = "1";
        };
        devShells = {
          nightlyShell = pkgs.mkShell {
            buildInputs = with pkgs;
            [
              nixWithFlakes
              nightlyToolchain
              cargo-expand
            ] ++ devPkgs ++ rustDeps;
            RUST_LOG = "info";
            RUST_BACKTRACE = "1";
          };
        };
      }
    );
}
