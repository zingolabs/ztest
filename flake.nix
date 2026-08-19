{
  description = "ztest — Zcash test harness on Kubernetes";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # `nix develop` — toolchain + cluster tooling. Cluster bring-up: docs/ops-local-cluster.md
  # Gates (fmt, clippy, proto drift, tests) live in .github/workflows/ci.yml, not here

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };

        # rust-toolchain.toml pin, not nixpkgs' rustc (edition 2024 + 1.95)
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };

        ztest = pkgs.callPackage ./nix/ztest.nix { inherit rustPlatform; };

        vmTests = import ./nix/vm-test.nix { inherit pkgs ztest; };
      in
      {
        packages = {
          inherit ztest;
          default = ztest;
        };

        # add nix run .#{test,vm}-{docker,podman}
        inherit (vmTests) apps;

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            rustToolchain
            pkg-config

            protobuf
            cargo-nextest
            cargo-release
            rust-analyzer

            # `ztest cluster setup --install-storage` shells out to both
            kind
            kubectl
            git
          ];

          # protoc = maintainer-only (`cargo xtask regen-proto`); bindings checked in, so
          # pin it rather than let codegen probe PATH
          PROTOC = "${pkgs.protobuf}/bin/protoc";
        };

        formatter = pkgs.nixfmt-rfc-style;
      });
}
