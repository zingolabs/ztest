{ lib, rustPlatform }:

let
  cargoToml = lib.importTOML ../Cargo.toml;
in
rustPlatform.buildRustPackage {
  pname = "ztest";
  version = cargoToml.workspace.package.version;

  src = lib.cleanSourceWith {
    name = "ztest-source";
    src = ../.;
    filter =
      path: _type:
      let
        rel = lib.removePrefix (toString ../. + "/") (toString path);
      in
      !(lib.hasPrefix "target" rel) && !(lib.hasPrefix "result" rel);
  };

  cargoLock.lockFile = ../Cargo.lock;
  cargoBuildFlags = [
    "--package"
    "ztest_cli"
  ];

  doCheck = false;

  meta.mainProgram = "ztest";
}
