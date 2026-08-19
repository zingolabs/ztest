{ pkgs, ztest }:

let
  inherit (pkgs) lib;

  # Distinct offsets → both engines' VMs can hold at once (vhost-vsock cid is global)
  vsockOffset = runtime: if runtime == "docker" then 2 else 3;
  cid = runtime: vsockOffset runtime + 1;

  day = 24 * 60 * 60;

  diskSizeMb = 32768;

  zaino = {
    url = "https://github.com/zingolabs/zaino.git";
    branch = "dev";
    dest = "/root/zaino";
  };

  # Probed by the wrapper: shell opens only once boot + clone have settled
  readyMarker = "/root/.vm-ready";

  node = runtime: {
    virtualisation = {
      # kind node + control plane + pulled images; defaults (1G/1core/1G disk) fail
      memorySize = 8192;
      cores = 4;
      diskSize = diskSizeMb;

      qemu.drives = lib.mkForce [
        {
          file = ''"$NIX_DISK_IMAGE"'';
          driveExtraOpts = {
            werror = "stop";
            aio = "io_uring"; # default = threads
          };
          deviceExtraOpts.iothread = "root-io"; # virtio-blk off the qemu main loop
        }
      ];
      qemu.options = [ "-object iothread,id=root-io" ];

      # 1 engine/VM → runtime::sole_usable() unambiguous (dockerCompat off: a `docker`
      # alias = both engines answer)
      docker.enable = runtime == "docker";
      docker.package = pkgs.docker_29; # nixpkgs default docker_28 = insecure-flagged
      podman.enable = runtime == "podman";
    };

    environment.systemPackages = [
      ztest
      pkgs.kind
      pkgs.kubectl
      pkgs.git
    ];

    environment.variables = {
      KUBECONFIG = "/root/.kube/config";
    }

    # Auto-inject the KIND_EXPERIMENTAL_PROVIDER env for podman test vms
    // lib.optionalAttrs (runtime == "podman") {
      KIND_EXPERIMENTAL_PROVIDER = "podman";
    };
  };

  # Shell VM only — the test VM stays a plain non-interactive machine.
  richTerminal = {
    environment.enableAllTerminfo = true; # kitty/wezterm/foot ship their own TERM
    i18n.defaultLocale = "en_US.UTF-8"; # supports-unicode reads LANG/LC_*

    # TERM rides the pty; the rest only arrive if forwarded (supports-color reads COLORTERM)
    services.openssh.settings.AcceptEnv = "COLORTERM TERM_PROGRAM TERM_PROGRAM_VERSION";
  };

  clusterTest =
    runtime:
    pkgs.testers.runNixOSTest {
      name = "ztest-cluster-${runtime}";
      nodes.machine = node runtime;
      interactive.sshBackdoor.enable = true;

      testScript = ''
        start_all()
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("sockets.target")
        machine.succeed("${runtime} version")

        # No AWS_* and no bucket.toml anywhere in these VMs: reads are public, so a green
        # bucket line proves the consumer path (credentials = `snapshot push` only, host-side)

        # --keep-vm-state reruns hit a surviving cluster
        machine.execute("kind delete cluster --name ztest")
        machine.succeed("kind create cluster --name ztest --wait 120s")
        machine.succeed("kubectl cluster-info")

        with subtest("profile adopts the engine owning the node container"):
            out = machine.succeed("ztest cluster add ztest --kind --set-default")
            print(out)
            assert "runtime: ${runtime} (probed)" in out, out

        # 2>&1 throughout: driver execute() captures stdout only, and ztest's status +
        # gate lines are eprintln! (console-only otherwise)
        with subtest("check names the storage gap, and the bucket needs no credentials"):
            status, out = machine.execute("ztest cluster check 2>&1")
            print(out)
            assert status != 0, "stock kind is missing required storage; check must fail"
            assert "no snapshot-capable StorageClass" in out, out
            assert "public ·" in out, out

        with subtest("stock kind cannot snapshot; unattended setup refuses to fix it"):
            status, out = machine.execute(
                "ztest cluster setup --non-interactive --no-observability --no-metrics-api 2>&1"
            )
            print(out)
            assert status != 0, "expected the snapshot gate to block setup"
            assert "--install-storage" in out, out

        with subtest("setup provisions once storage is installed"):
            print(machine.succeed(
                "ztest cluster setup --non-interactive --install-storage"
                " --no-observability --no-metrics-api 2>&1"
            ))

        with subtest("check goes green once setup has run"):
            status, out = machine.execute("ztest cluster check 2>&1")
            print(out)
            assert status == 0, out
            assert "cannot work here" not in out, out
      '';
    };

  # Only `.driver` is ever taken from this, never `.test` — so run.nix's
  # `assert !config.sshBackdoor.enable` stays unevaluated and the backdoor can sit at the
  # top level, where it reaches the plain (REPL-free) driver
  shellVm =
    runtime:
    pkgs.testers.runNixOSTest {
      name = "ztest-shell-${runtime}";
      nodes.machine = lib.mkMerge [
        (node runtime)
        richTerminal
      ];
      sshBackdoor = {
        enable = true;
        vsockOffset = vsockOffset runtime;
      };
      globalTimeout = 2 * day; # driver kills every resource past this

      testScript = ''
        start_all()
        machine.wait_for_unit("multi-user.target")
        machine.wait_for_unit("sockets.target")

        # clone zaino for ztest run validation
        status, out = machine.execute("git clone --branch ${zaino.branch} ${zaino.url} ${zaino.dest}")

        if status != 0:
            print(f"zaino clone failed (shell still opens):\n{out}")

        machine.succeed("touch ${readyMarker}")

        import time
        time.sleep(${toString day})
      '';
    };

  # State dir = driver cwd: root qcow2 + sockets both land here
  # - driver.py hardcodes XDG_RUNTIME_DIR (no option, no qemu flag) → env = the only lever
  # - default lands the root image on tmpfs @ 10% RAM → wedges the login session
  # - image itself: created by qemu-vm.nix, dropped by the driver unless --keep-machine-state
  vmDisk = flavor: runtime: ''
    export XDG_RUNTIME_DIR="''${XDG_CACHE_HOME:-$HOME/.cache}/ztest/vm/${flavor}-${runtime}"
    mkdir -p "$XDG_RUNTIME_DIR"
    chmod 700 "$XDG_RUNTIME_DIR" # qemu monitor sock + unauthenticated ssh backdoor live here
  '';

  # driver puts qemu in its own pgroup (no start_new_session upstream) + traps no SIGTERM
  # → signalling the driver alone leaves qemu orphaned; kill the group
  reapDriver = ''
    trap 'kill -TERM -- "-$pgid" 2>/dev/null || true
          wait "$pgid" 2>/dev/null || true' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM HUP
  '';

  # DISPLAY/WAYLAND_DISPLAY unset → driver adds -nographic
  testApp =
    runtime:
    pkgs.writeShellApplication {
      name = "ztest-vm-test-${runtime}";
      runtimeInputs = [ pkgs.coreutils ]; # mkdir must not come from the caller's PATH
      text = ''
        unset DISPLAY WAYLAND_DISPLAY

        ${vmDisk "test" runtime}

        # REPL needs the tty, and bash defers traps under a fg job → hand the process over
        case " $* " in
          *" --interactive "*) exec ${lib.getExe (clusterTest runtime).driver} "$@" ;;
        esac

        set -m # background job leads its own pgroup
        ${lib.getExe (clusterTest runtime).driver} "$@" </dev/null &
        pgid=$!
        set +m

        ${reapDriver}

        status=0
        wait "$pgid" || status=$?
        exit "$status"
      '';
    };

  shellApp =
    runtime:
    pkgs.writeShellApplication {
      name = "ztest-vm-${runtime}";
      runtimeInputs = [
        pkgs.openssh
        pkgs.coreutils # mkdir/sleep must not come from the caller's PATH
      ];
      text = ''
        unset DISPLAY WAYLAND_DISPLAY

        ${vmDisk "vm" runtime}

        log="$XDG_RUNTIME_DIR/console.log" # outlives the state dir the driver wipes
        echo "VM console log: $log"

        set -m # background job leads its own pgroup
        ${lib.getExe (shellVm runtime).driver} "$@" </dev/null >"$log" 2>&1 &
        pgid=$!
        set +m

        ${reapDriver}

        # Host key is regenerated per boot; the backdoor is unauthenticated by design
        ssh_opts=(
          -o User=root
          -o StrictHostKeyChecking=no
          -o UserKnownHostsFile=/dev/null
          -o LogLevel=ERROR
          -o SendEnv=COLORTERM
          -o SendEnv=TERM_PROGRAM
          -o SendEnv=TERM_PROGRAM_VERSION
        )

        # Bounded, not liveness-polled: a just-exited background child is not reliably
        # distinguishable from a live one, and a slow boot must not hang forever
        echo -n "waiting for vsock/${toString (cid runtime)}"
        ready=""
        for _ in $(seq 1 150); do
          if ssh "''${ssh_opts[@]}" -o ConnectTimeout=2 "vsock/${toString (cid runtime)}" test -e ${readyMarker} 2>/dev/null; then
            ready=1
            break
          fi
          echo -n "."
          sleep 2
        done
        echo
        if [ -z "$ready" ]; then
          echo "vsock/${toString (cid runtime)} never became ready; see $log" >&2
          exit 1
        fi

        ssh "''${ssh_opts[@]}" -t "vsock/${toString (cid runtime)}"
      '';
    };
in
{
  apps = lib.listToAttrs (
    lib.concatMap
      (runtime: [
        (lib.nameValuePair "test-${runtime}" {
          type = "app";
          program = lib.getExe (testApp runtime);
          meta.description = "Run the ${runtime} cluster test, network-live, exit with its status";
        })
        (lib.nameValuePair "vm-${runtime}" {
          type = "app";
          program = lib.getExe (shellApp runtime);
          meta.description = "Boot the ${runtime} VM (zaino @ ${zaino.branch} cloned) and open a root shell";
        })
      ])
      [
        "docker"
        "podman"
      ]
  );
}
