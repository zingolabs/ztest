{
  description = "infrastructure — Zcash test harness on Kubernetes";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # `nix develop`           — drop into the dev shell (rustc + native build deps + kind/kubectl)
  # `nix flake check`       — `cargo fmt` + `cargo clippy --all-targets -- -D warnings`
  # `nix fmt`               — format Nix files
  #
  # The shell sets PROTOC / ROCKSDB_{LIB,INCLUDE}_DIR / LIBCLANG_PATH so
  # `cargo build -p zcash_kube_net` works out of the box on NixOS.
  #
  # Local cluster bring-up (requires a Docker / Podman daemon — enable
  # `virtualisation.docker.enable = true` on NixOS first):
  #
  #     kind create cluster --name zkn          # ~30s on first boot
  #     kubectl cluster-info --context kind-zkn
  #     # …run tests with --ignored…
  #     kind delete cluster --name zkn
  #
  # The default kind cluster ships CoreDNS — pod-to-pod DNS by service
  # name works out of the box, which is what zcash_kube_net relies on.
  #
  # OpenShift Local (CRC) bring-up — for tests that need the OpenShift API
  # surface (Routes, SCCs, ClusterOperators, OperatorHub) rather than plain
  # Kubernetes. Prerequisites the flake CAN'T provide:
  #   * NixOS host with `virtualisation.libvirtd.enable = true;` and the
  #     user in the `libvirtd` group (CRC drives a QEMU/KVM VM).
  #   * A Red Hat pull secret from console.redhat.com/openshift/create/local
  #     saved as ~/pull-secret.json. CRC ships OpenShift, not OKD — the
  #     secret is mandatory even for local use.
  #
  #     crc config set memory 16384             # 9 GiB default is too tight
  #     crc config set cpus 6
  #     crc setup                               # one-time host pre-flight
  #     crc start -p ~/pull-secret.json         # ~5–15 min first boot
  #     eval "$(crc oc-env)"                    # puts `oc` + KUBECONFIG on PATH
  #     crc console --credentials               # prints kubeadmin password
  #     crc stop                                # `crc delete` to wipe

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-gh/xTkxKHL4eiRXzWv8KP7vfjSk61Iq48x47BEDFgfk=";
        };

        nativeBuildInputs = with pkgs; [
          rustToolchain
          protobuf
          pkg-config
          cmake
          # Sets LIBCLANG_PATH so librocksdb-sys's bindgen finds libclang
          # without dragging an unpinned LLVM into PATH.
          rustPlatform.bindgenHook
          cargo-nextest
          cargo-deny
          rust-analyzer

          # Cluster tooling — `kind` brings up a local k8s cluster on
          # top of Docker/Podman; `kubectl` talks to it; `kubernetes-helm`
          # is here for the eventual observability stack.
          kind
          kubectl
          kubernetes-helm

          # OpenShift tooling — `crc` (OpenShift Local) boots a single-node
          # OpenShift VM via libvirt/KVM for tests that exercise the
          # OpenShift API surface. `openshift` provides the `oc` client
          # standalone so you can target a cluster without sourcing
          # `crc oc-env` first. See the header comment for pull-secret /
          # libvirtd prerequisites.
          crc
          openshift
        ];

        # stdenv.cc.cc.lib provides libstdc++.so.6 / libgcc_s.so.1 that
        # rocksdb's C++ code transitively needs at link / run time.
        buildInputs = with pkgs; [ rocksdb_8_11 stdenv.cc.cc.lib ];

        env = {
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          PROTOC_INCLUDE = "${pkgs.protobuf}/include";

          # Use nixpkgs' librocksdb instead of librocksdb-sys's bundled
          # C++ compile (which doesn't work cleanly under the Nix clang
          # wrapper).
          ROCKSDB_LIB_DIR = "${pkgs.rocksdb_8_11}/lib";
          ROCKSDB_INCLUDE_DIR = "${pkgs.rocksdb_8_11}/include";
        };
      in
      {
        devShells.default = pkgs.mkShell ({
          inherit nativeBuildInputs buildInputs;
          # Test binaries dynamically link libstdc++ / libgcc_s from
          # rocksdb's C++ code at runtime; mkShell doesn't propagate
          # buildInputs into LD_LIBRARY_PATH, so do it explicitly.
          LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath buildInputs;
        } // env);

        # `nix flake check` runs fmt + clippy on the workspace. Heavier
        # nextest/doc builds stay out of the gate — local laptop CI.
        checks = {
          fmt = pkgs.runCommand "cargo-fmt" {
            inherit nativeBuildInputs;
            src = ./.;
          } ''
            cp -r $src/. .
            chmod -R +w .
            cargo fmt --all -- --check
            touch $out
          '';
        };

        formatter = pkgs.nixfmt-rfc-style;
      });
}
