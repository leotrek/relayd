#!/usr/bin/env python3
"""Configure and run repeatable contact-window experiments on Raspberry Pis.

No Python packages, tokens, firewall changes or external scheduler required.
"""
import argparse
import concurrent.futures
import contextlib
import datetime
import hashlib
import fcntl
import http.server
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

RELAY_PORT = 18080
LAB_PORT = 18082
ORBITAL_PORT = 18081
IMAGE = "relayd/lab:dev"
ROOT = "/opt/relayd-lab"
MODES = ("ground", "source", "random", "relayd")
ARCHES = ("arm64", "arm", "amd64")
HOST_ARCHES = {"aarch64": "arm64", "arm64": "arm64", "armv7l": "arm", "x86_64": "amd64"}
SAFE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
STOP = threading.Event()


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def validate(cfg):
    nodes = cfg["nodes"]
    if "ssh_password" in cfg or any("ssh_password" in node for node in nodes):
        raise ValueError("set RELAYD_SSH_PASSWORD in the environment instead of the config")
    names = [n["name"] for n in nodes]
    if len(names) < 2 or len(set(names)) != len(names):
        raise ValueError("need one ground and at least one satellite, with unique names")
    if sum(n["role"] == "ground" for n in nodes) != 1:
        raise ValueError("exactly one node must have role ground")
    for n in nodes:
        if not SAFE.fullmatch(n["name"]) or n["role"] not in ("ground", "satellite"):
            raise ValueError("invalid node name or role")
        if "arch" in n and n["arch"] not in ARCHES:
            raise ValueError("unsupported node architecture")
        socket.inet_aton(n["ip"])
        if not re.fullmatch(r"[A-Za-z0-9_.@-]+", n["ssh"]) or n["ssh"].startswith("-"):
            raise ValueError("invalid SSH target")
    if len({n["ip"] for n in nodes}) != len(nodes):
        raise ValueError("each node needs a distinct IPv4 address")
    for key in ("duration_s", "bandwidth_mbps"):
        if not 0 < cfg[key] < 86400:
            raise ValueError(f"invalid {key}")
    acquisitions = {a["id"]: a for a in cfg["acquisitions"]}
    if len(acquisitions) != len(cfg["acquisitions"]) or not acquisitions:
        raise ValueError("acquisition IDs must be unique")
    satellites = [n["name"] for n in nodes if n["role"] == "satellite"]
    for a in acquisitions.values():
        if (not SAFE.fullmatch(a["id"]) or a["source"] not in satellites
                or not isinstance(a["size_mib"], int) or not 1 <= a["size_mib"] <= 256):
            raise ValueError("invalid acquisition")
        if not isinstance(a["seed"], int):
            raise ValueError("acquisition seed must be an integer")
    ids = set()
    for job in cfg["jobs"]:
        if not SAFE.fullmatch(job["id"]) or job["id"] in ids:
            raise ValueError("invalid or duplicate job ID")
        ids.add(job["id"])
        if (job["requester"] not in satellites or not job["inputs"]
                or len(set(job["inputs"])) != len(job["inputs"])
                or any(i not in acquisitions for i in job["inputs"])):
            raise ValueError("invalid requester or inputs")
        if not 0 <= job["release_s"] < job["deadline_s"] <= cfg["duration_s"]:
            raise ValueError("job release/deadline outside experiment")
        if not isinstance(job["rounds"], int) or not 1 <= job["rounds"] <= 10000:
            raise ValueError("rounds must be a positive bounded integer")
    selectors = names + ["*satellite", "*ground"]
    for c in cfg["contacts"]:
        if c["a"] not in selectors or c["b"] not in selectors:
            raise ValueError("unknown contact endpoint")
        if not 0 <= c["start_s"] or not 0 < c["duration_s"] <= c["period_s"]:
            raise ValueError("invalid contact window")
    return cfg


def selected(selector, node):
    return selector == node["name"] or selector == "*" + node["role"]


def contact_open(cfg, a, b, elapsed):
    if a["name"] == b["name"]:
        return True
    if elapsed < 0 or elapsed >= cfg["duration_s"]:
        return False
    for c in cfg["contacts"]:
        matches = ((selected(c["a"], a) and selected(c["b"], b)) or
                   (selected(c["b"], a) and selected(c["a"], b)))
        age = elapsed - c["start_s"]
        if matches and age >= 0 and age % c["period_s"] < c["duration_s"]:
            return True
    return False


def origin(cfg, job, mode):
    if mode == "ground":
        return next(n["name"] for n in cfg["nodes"] if n["role"] == "ground")
    if mode == "source":
        return next(a["source"] for a in cfg["acquisitions"] if a["id"] == job["inputs"][0])
    if mode == "random":
        # Stable per-job random placement, independent of Python's process hash.
        rng = random.Random(str(cfg.get("seed", 1)) + ":" + job["id"])
        return rng.choice([n["name"] for n in cfg["nodes"] if n["role"] == "satellite"])
    return job["requester"]


def peer_url(cfg, name, service="relay"):
    index = next(i for i, n in enumerate(cfg["nodes"]) if n["name"] == name)
    return f"http://127.0.0.1:{19000 + index * 2 + (service == 'lab')}"


def http_json(url, body=None, timeout=3):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    # Never inherit a machine's HTTP proxy settings for experiment traffic.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as response:
        return json.load(response)


class Events:
    def __init__(self, path):
        self.path, self.lock = path, threading.Lock()

    def emit(self, event, **fields):
        with self.lock, self.path.open("a") as out:
            out.write(json.dumps({"time": time.time(), "event": event, **fields}) + "\n")


class TCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = False  # Join handlers so final byte counters are flushed.


def start_proxy(cfg, local, remote, service, epoch, events):
    """Gate a real TCP stream; reset both ends at contact close.

    Bandwidth is limited per connection/direction, not per physical interface.
    """
    class Proxy(socketserver.BaseRequestHandler):
        def handle(self):
            def available():
                return not STOP.is_set() and contact_open(cfg, local, remote, time.time() - epoch)
            if not available():
                return
            destination = RELAY_PORT if service == "relay" else LAB_PORT
            try:
                upstream = socket.create_connection((remote["ip"], destination), timeout=2)
            except OSError:
                return
            pair = (self.request, upstream)
            done = threading.Event()
            counts = [0, 0]

            def pump(src, dst, direction):
                rate = cfg["bandwidth_mbps"] * 1_000_000 / 8
                try:
                    while available() and not done.is_set():
                        src.settimeout(.1)
                        try:
                            data = src.recv(8192)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        until = time.monotonic() + len(data) / rate
                        while time.monotonic() < until and available() and not done.is_set():
                            done.wait(min(.05, max(0, until - time.monotonic())))
                        if not available() or done.is_set():
                            break
                        dst.sendall(data)
                        counts[direction] += len(data)
                except OSError:
                    pass
                finally:
                    done.set()

            workers = [threading.Thread(target=pump, args=(pair[i], pair[1-i], i), daemon=True) for i in (0, 1)]
            for worker in workers:
                worker.start()
            while available() and not done.wait(.05):
                pass
            done.set()
            for conn in pair:
                with contextlib.suppress(OSError):
                    conn.shutdown(socket.SHUT_RDWR)
                conn.close()
            for worker in workers:
                worker.join(timeout=1)
            events.emit("connection", peer=remote["name"], service=service,
                        sent_bytes=counts[0], received_bytes=counts[1])

    port = int(peer_url(cfg, remote["name"], service).rsplit(":", 1)[1])
    server = TCPServer(("127.0.0.1", port), Proxy)
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .05}, daemon=True).start()
    return server


def sample_bytes(seed, size):
    """Stable incompressible-ish synthetic data, bounded to a 1 MiB generator buffer."""
    rng = random.Random(seed)
    while size:
        n = min(size, 1 << 20)
        yield rng.randbytes(n)
        size -= n


def prepare_inputs(cfg, node, root, binaries):
    inputs = root / "input"
    inputs.mkdir()
    for acquisition in cfg["acquisitions"]:
        if acquisition["source"] != node["name"]:
            continue
        size = acquisition["size_mib"] << 20
        path = inputs / (acquisition["id"] + ".raw")
        with path.open("wb") as stream:
            for chunk in sample_bytes(acquisition["seed"], size):
                stream.write(chunk)
        descriptor = {
            "schema_version": 1, "acquisition_id": acquisition["id"],
            "sensor_type": "optical", "timestamp": "2026-01-01T00:00:00Z",
            "width": 1024, "height": size // 1024, "bands": 1,
            "sample_bit_depth": 8, "storage_bit_depth": 8,
            "sample_type": "uint", "byte_order": "little", "layout": "BIP",
        }
        write_json(path.with_suffix(".json"), descriptor)
        subprocess.run([str(binaries / "fluximport"), "-kind", "raw", "-backend", "identity",
                        "-input", str(path), "-state-dir", str(root / "flux")], check=True)


def launch(command, log, children):
    with log.open("ab") as output:
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
    children.append(child)
    return child


def wait_ready(test, children, timeout=25):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        for child in children:
            if child.poll() is not None:
                raise RuntimeError("daemon exited; inspect node logs")
        try:
            if test():
                return
        except (OSError, ValueError):
            pass
        time.sleep(.2)
    raise RuntimeError("daemon did not become ready")


def submit_job(cfg, job, mode, epoch, events):
    release, deadline = epoch + job["release_s"], epoch + job["deadline_s"]
    if STOP.wait(max(0, release - time.time())):
        return
    task = {"id": job["id"], "name": "lab-reduce", "image": IMAGE,
            "inputs": job["inputs"], "deadline": datetime.datetime.fromtimestamp(
                deadline, datetime.timezone.utc).isoformat(),
            "run_timeout": f"{int(cfg['duration_s'])}s", "memory": "128Mi",
            "assigned": mode != "relayd",
            "payload": {"job_id": job["id"], "rounds": job["rounds"]}}
    while not STOP.is_set() and time.time() < deadline:
        try:
            # Client timeout exceeds relayd's peer operation timeout. If it still
            # loses an ack, retrying this same origin/ID reads its journal.
            result = http_json(f"http://127.0.0.1:{RELAY_PORT}/v1/tasks", task, timeout=35)
            events.emit("assignment", job=job["id"], assignment=result)
            return
        except (OSError, ValueError) as exc:
            events.emit("submit_retry", job=job["id"], error=str(exc))
            STOP.wait(1)
    events.emit("submission_deadline", job=job["id"])


def find_report(job_id):
    # Read actual function outputs; do not infer delivery from management logs.
    tasks = http_json(f"http://127.0.0.1:{ORBITAL_PORT}/v1/tasks")["tasks"]
    for task in tasks:
        if task.get("result_status") != "success" or not task.get("output_dir"):
            continue
        path = Path(task["output_dir"]) / "report.json"
        try:
            report = read_json(path)
        except (OSError, ValueError):
            continue
        if report.get("job_id") == job_id:
            return report
    return None


def verify_report(job, report, expected):
    if report.get("job_id") != job["id"] or report.get("rounds") != job["rounds"]:
        return False
    records = report.get("inputs", [])
    if len(records) != len(job["inputs"]) or len({r.get("id") for r in records}) != len(records):
        return False
    return {r["id"]: (r.get("sha256"), r.get("bytes")) for r in records} == {
        key: (expected[key]["sha256"], expected[key]["bytes"]) for key in job["inputs"]}


def collect_results(cfg, ground, epoch, events, output):
    pending = {j["id"]: j for j in cfg["jobs"]}
    received = {}
    # Independent readers prevent one slow/unreachable node from hiding contacts
    # available to another node. No management channel is used for result delivery.
    def observe(node):
        while not STOP.is_set() and time.time() < epoch + cfg["duration_s"]:
            with result_lock:
                jobs = list(pending.values())
            for job in jobs:
                if time.time() < epoch + job["release_s"]:
                    continue
                endpoint = (f"http://127.0.0.1:{LAB_PORT}" if node == ground else
                            peer_url(cfg, node["name"], "lab"))
                try:
                    report = http_json(endpoint + "/reports/" + job["id"], timeout=2)
                    arrival = time.time()
                    if arrival >= epoch + cfg["duration_s"]:
                        continue
                    valid = verify_report(job, report, cfg["expected"])
                    with result_lock:
                        if job["id"] not in pending:
                            continue
                        if valid:
                            received[job["id"]] = {"report": report, "received_at": arrival,
                                                   "executor": node["name"]}
                            pending.pop(job["id"])
                        events.emit("ground_receipt", job=job["id"], valid=valid,
                                    executor=node["name"], report=report)
                except (OSError, ValueError):
                    pass
            STOP.wait(.5)

    result_lock = threading.Lock()
    workers = [threading.Thread(target=observe, args=(n,), daemon=True) for n in cfg["nodes"]]
    for worker in workers:
        worker.start()
    STOP.wait(max(0, epoch + cfg["duration_s"] - time.time()))
    for worker in workers:
        worker.join(timeout=3)
    rows = []
    for job in cfg["jobs"]:
        got = received.get(job["id"])
        rows.append({"job": job["id"], "delivered": got is not None,
                     "on_time": bool(got and got["received_at"] <= epoch + job["deadline_s"]),
                     "delivery_latency_s": got["received_at"] - epoch - job["release_s"] if got else None,
                     "execution_finished_s": got["report"]["finished_at"] - epoch if got else None,
                     "executor": got["executor"] if got else None})
    write_json(output, rows)


def node_main(args):
    if not SAFE.fullmatch(args.run_id) or args.mode not in MODES:
        raise ValueError("invalid run ID or mode")
    cfg = validate(read_json(args.config))
    node = next(n for n in cfg["nodes"] if n["name"] == args.node)
    base = Path("/var/lib/relayd-lab")
    base.mkdir(parents=True, exist_ok=True)
    lock = (base / ".lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    # Refuse to interfere with a pre-existing service using experiment ports.
    for port in (RELAY_PORT, ORBITAL_PORT, LAB_PORT):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("0.0.0.0", port))
    root = Path("/var/lib/relayd-lab") / args.run_id / args.mode
    root.mkdir(parents=True, exist_ok=False)  # Never overwrite an earlier run.
    (root / "node.pid").write_text(str(os.getpid()))
    # Keep Unix socket paths short even with a long run ID.
    import tempfile
    sockets = Path(tempfile.mkdtemp(prefix="rl-", dir="/tmp"))
    binaries = Path(args.install) / "bin"
    events = Events(root / "events.jsonl")
    children, servers = [], []
    orbital_owned = False
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    try:
        prepare_inputs(cfg, node, root, binaries)
        for remote in cfg["nodes"]:
            if remote != node:
                for service in ("relay", "lab"):
                    servers.append(start_proxy(cfg, node, remote, service, args.epoch, events))
        write_json(root / "flux.json", {"listen_socket": str(sockets / "flux.sock"),
                    "state_dir": str(root / "flux"), "backend": "identity"})
        launch([str(binaries / "fluxd"), "-config", str(root / "flux.json")], root / "flux.log", children)
        wait_ready(lambda: (sockets / "flux.sock").exists(), children)
        peers = [peer_url(cfg, n["name"]) for n in cfg["nodes"] if n["role"] == "satellite" and n != node]
        launch([str(binaries / "relayd"), "-node", node["name"], "-state-dir", str(root / "relay"),
                "-listen", f"0.0.0.0:{RELAY_PORT}", "-socket", str(sockets / "relay.sock"),
                "-fluxd-socket", str(sockets / "flux.sock"), "-orbitald", f"http://127.0.0.1:{ORBITAL_PORT}",
                "-peers", ",".join(peers)], root / "relay.log", children)
        launch([str(binaries / "orbitald"), "-listen", f"127.0.0.1:{ORBITAL_PORT}",
                "-state-dir", str(root / "orbital"), "-fluxd-socket", str(sockets / "relay.sock")],
               root / "orbital.log", children)
        wait_ready(lambda: http_json(f"http://127.0.0.1:{ORBITAL_PORT}/v1/status")["runtime"].get("error", "") == "", children)
        wait_ready(lambda: (sockets / "relay.sock").exists(), children)
        orbital_owned = True

        class Reports(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                job_id = self.path.removeprefix("/reports/")
                if not self.path.startswith("/reports/") or not SAFE.fullmatch(job_id):
                    self.send_error(404)
                    return
                try:
                    report = find_report(job_id)
                except (OSError, ValueError):
                    report = None
                if report is None:
                    self.send_error(404)
                    return
                body = json.dumps(report).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                with contextlib.suppress(OSError):
                    self.wfile.write(body)

            def log_message(self, *_):
                pass

        reports = http.server.ThreadingHTTPServer(("0.0.0.0", LAB_PORT), Reports)
        servers.append(reports)
        threading.Thread(target=reports.serve_forever, kwargs={"poll_interval": .05}, daemon=True).start()
        if time.time() >= args.epoch:
            raise RuntimeError("setup missed start epoch; increase --lead-seconds")
        events.emit("ready", epoch=args.epoch, mode=args.mode, node=node["name"])
        for job in cfg["jobs"]:
            if origin(cfg, job, args.mode) == node["name"]:
                threading.Thread(target=submit_job, args=(cfg, job, args.mode, args.epoch, events), daemon=True).start()
        if node["role"] == "ground":
            collect_results(cfg, node, args.epoch, events, root / "results.json")
        else:
            STOP.wait(max(0, args.epoch + cfg["duration_s"] - time.time()))
    finally:
        STOP.set()
        if orbital_owned:
            with contextlib.suppress(OSError, ValueError):
                windows = http_json(f"http://127.0.0.1:{ORBITAL_PORT}/v1/state")["windows"]
                for window in windows:
                    if window["status"] == "running":
                        with contextlib.suppress(OSError, ValueError):
                            http_json(f"http://127.0.0.1:{ORBITAL_PORT}/v1/tasks/{window['id']}/stop", {}, timeout=8)
        for child in reversed(children):
            child.terminate()
        for child in children:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for server in servers:
            server.shutdown()
            server.server_close()
        shutil.rmtree(sockets)
        (root / "node.pid").unlink(missing_ok=True)
        lock.close()


def ssh_target(node):
    username = os.environ.get("RELAYD_SSH_USER")
    if not username or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", username):
        raise RuntimeError("set RELAYD_SSH_USER to a valid SSH username")
    return username + "@" + node["ssh"].rsplit("@", 1)[-1]


def connection(node, program="ssh"):
    password = os.environ.get("RELAYD_SSH_PASSWORD")
    env = dict(os.environ)
    prefix = []
    options = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=8"]
    if password:
        if not shutil.which("sshpass"):
            raise RuntimeError("Password login requires sshpass on the workstation")
        prefix = ["sshpass", "-e"]
        env["SSHPASS"] = password
        options = ["-o", "BatchMode=no", "-o", "ConnectTimeout=8",
                   "-o", "PreferredAuthentications=password,keyboard-interactive",
                   "-o", "NumberOfPasswordPrompts=1"]
    return prefix + [program] + options, env


def public_config(cfg):
    # Experiment manifests go to the Pis and archives; never include login secrets.
    result = {k: v for k, v in cfg.items() if k != "ssh_password"}
    result["nodes"] = [{k: v for k, v in node.items() if k != "ssh_password"}
                       for node in cfg["nodes"]]
    return result


def ssh(node, command, **kwargs):
    prefix, env = connection(node)
    return subprocess.run(prefix + [ssh_target(node), shlex.join([str(x) for x in command])],
                          env=env, check=True, **kwargs)


def upload(node, files, destination):
    prefix, env = connection(node, "scp")
    subprocess.run(prefix + ["-q", *map(str, files), ssh_target(node) + ":" + destination],
                   env=env, check=True)


def build(args):
    workspace = Path(args.workspace).resolve()
    output = Path(args.output).resolve()
    if args.config:
        cfg = validate(read_json(args.config))
        if any("arch" not in node for node in cfg["nodes"]):
            raise ValueError("build --config requires arch on each node")
        architectures = sorted({node["arch"] for node in cfg["nodes"]})
    else:
        architectures = [args.arch]
    targets = {"relayd": ("relayd", "./cmd/relayd"),
               "fluxd": ("fluxd", "./cmd/fluxd"),
               "fluximport": ("fluxd", "./cmd/fluximport"),
               "orbitald": ("orbitald", ".")}
    for arch in architectures:
        destination = output / arch if args.config else output
        destination.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, GOOS="linux", GOARCH=arch, CGO_ENABLED="0", GOARM="7")
        for name, (repo, package) in targets.items():
            subprocess.run(["go", "build", "-o", str(destination / name), package],
                           cwd=workspace / repo, env=env, check=True)
        (destination / ".target-arch").write_text(arch)
        print(f"Built Linux {arch} binaries in {destination}")


def binary_files(root, arch):
    directory = root / arch if (root / arch).is_dir() else root
    marker = directory / ".target-arch"
    if marker.exists() and marker.read_text().strip() != arch:
        raise ValueError(f"binaries in {directory} do not match {arch}")
    files = [directory / name for name in ("relayd", "fluxd", "fluximport", "orbitald")]
    if not all(path.is_file() for path in files):
        raise ValueError(f"missing {arch} binaries: run build --config first")
    return files


def setup(args):
    cfg = validate(read_json(args.config))
    directory = Path(__file__).resolve().parent
    binaries = Path(args.binaries).resolve()

    def install(node):
        ssh(node, ["sudo", "-n", "true"])
        host_arch = ssh(node, ["uname", "-m"], capture_output=True, text=True).stdout.strip()
        arch = HOST_ARCHES.get(host_arch)
        requested = node.get("arch", arch) if args.arch == "auto" else args.arch
        if arch is None or arch != requested:
            raise ValueError(f"{node['name']}: architecture {host_arch} incompatible with {requested}")
        files = binary_files(binaries, arch)
        if args.bootstrap:
            ssh(node, ["sudo", "-n", "apt-get", "update"])
            ssh(node, ["sudo", "-n", "apt-get", "install", "-y", "python3", "containerd", "docker.io"])
            ssh(node, ["sudo", "-n", "systemctl", "enable", "--now", "containerd", "docker"])
        stage = ssh(node, ["mktemp", "-d", "/tmp/relayd-install-XXXXXX"],
                    capture_output=True, text=True).stdout.strip()
        try:
            upload(node, files + [directory / name for name in ("lab.py", "workload.py", "Dockerfile")], stage + "/")
            ssh(node, ["sudo", "-n", "mkdir", "-p", ROOT + "/bin", ROOT + "/image"])
            ssh(node, ["sudo", "-n", "cp", *[stage + "/" + p.name for p in files], ROOT + "/bin/"])
            ssh(node, ["sudo", "-n", "chmod", "+x", *[ROOT + "/bin/" + p.name for p in files]])
            ssh(node, ["sudo", "-n", "cp", stage + "/lab.py", ROOT + "/lab.py"])
            ssh(node, ["sudo", "-n", "cp", stage + "/workload.py", stage + "/Dockerfile", ROOT + "/image/"])
            ssh(node, ["sudo", "-n", "docker", "build", "-t", IMAGE, ROOT + "/image"])
            # Run the pipeline inside a fixed Python program, avoiding shell interpolation.
            program = ("import subprocess; p=subprocess.Popen(['docker','save','" + IMAGE + "'],stdout=subprocess.PIPE); "
                       "q=subprocess.run(['ctr','--namespace','orbitald','images','import','-'],stdin=p.stdout); "
                       "p.stdout.close(); a=p.wait(); raise SystemExit(a or q.returncode)")
            ssh(node, ["sudo", "-n", "python3", "-c", program])
            print(f"{node['name']}: installed", flush=True)
        finally:
            ssh(node, ["rm", "-rf", "--", stage])

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(install, cfg["nodes"]))


def expected_hashes(cfg):
    result = {}
    for a in cfg["acquisitions"]:
        h = hashlib.sha256()
        size = a["size_mib"] << 20
        for chunk in sample_bytes(a["seed"], size):
            h.update(chunk)
        result[a["id"]] = {"sha256": h.hexdigest(), "bytes": size}
    return result


def plan(cfg):
    for mode in MODES:
        print(mode + ":")
        for job in cfg["jobs"]:
            print(f"  {job['id']}: submit on {origin(cfg, job, mode)}, inputs={job['inputs']}, "
                  f"release={job['release_s']}s, deadline={job['deadline_s']}s")
    print(f"Duration: {cfg['duration_s']}s per baseline. One ground + {len(cfg['nodes'])-1} satellites.")
    print("Contacts:")
    for c in cfg["contacts"]:
        print(f"  {c['a']} <-> {c['b']}: start={c['start_s']}s, open={c['duration_s']}s, period={c['period_s']}s")


def run(args):
    cfg = validate(read_json(args.config))
    if not SAFE.fullmatch(args.run_id):
        raise ValueError("run ID must contain only letters, digits, underscore or hyphen")
    if args.lead_seconds < 10:
        raise ValueError("allow at least 10 seconds setup lead time")
    cfg["expected"] = expected_hashes(cfg)
    output = Path(args.output).resolve() / args.run_id
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "manifest.json", public_config(cfg))
    ground = next(n for n in cfg["nodes"] if n["role"] == "ground")
    # Wall clocks drive contacts and container timestamps. Require NTP-synced nodes.
    for node in cfg["nodes"]:
        before = time.time()
        clock = float(ssh(node, ["python3", "-c", "import time; print(time.time())"],
                          capture_output=True, text=True).stdout)
        after = time.time()
        if abs(clock - (before + after) / 2) > 1:
            raise RuntimeError(f"{node['name']}: clock differs by >1 second; synchronize clocks")
    manifest = output / "manifest.json"
    modes = args.modes.split(",")
    if not modes or any(mode not in MODES for mode in modes) or len(set(modes)) != len(modes):
        raise ValueError("choose distinct modes from " + ",".join(MODES))
    for mode in modes:
        remote_cfg = f"/tmp/relayd-{args.run_id}-{mode}.json"
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda node: upload(node, [manifest], remote_cfg), cfg["nodes"]))
        epoch = time.time() + args.lead_seconds
        processes, logs = [], []
        print(f"{mode}: starts in {args.lead_seconds}s; duration {cfg['duration_s']}s", flush=True)
        try:
            for node in cfg["nodes"]:
                log = (output / f"{mode}-{node['name']}.log").open("wb")
                logs.append(log)
                command = ["sudo", "-n", "python3", ROOT + "/lab.py", "node", "--config", remote_cfg,
                           "--node", node["name"], "--mode", mode, "--epoch", str(epoch),
                           "--run-id", args.run_id]
                prefix, env = connection(node)
                processes.append(subprocess.Popen(prefix + [ssh_target(node), shlex.join(command)],
                                                   env=env, stdout=log, stderr=subprocess.STDOUT))
            end = epoch + cfg["duration_s"] + 30
            while any(p.poll() is None for p in processes):
                if any(p.poll() not in (None, 0) for p in processes):
                    raise RuntimeError(f"{mode}: a node failed; inspect local node logs")
                if time.time() > end:
                    raise RuntimeError("nodes exceeded experiment timeout")
                time.sleep(.5)
        finally:
            # Stop only processes created for this run. Never kill unrelated daemons.
            for node in cfg["nodes"]:
                cleanup = ("import os,signal; from pathlib import Path; "
                           f"p=Path('/var/lib/relayd-lab/{args.run_id}/{mode}/node.pid'); "
                           "pid=int(p.read_text()) if p.exists() else None; "
                           "cmd=Path('/proc/'+str(pid)+'/cmdline') if pid else None; "
                           f"ok=cmd and cmd.exists() and b'{args.run_id}' in cmd.read_bytes() and b'lab.py' in cmd.read_bytes(); "
                           "os.kill(pid,signal.SIGTERM) if ok else None")
                with contextlib.suppress(subprocess.CalledProcessError):
                    ssh(node, ["sudo", "-n", "python3", "-c", cleanup], stdout=subprocess.DEVNULL)
            for p in processes:
                try:
                    p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    p.terminate()
                    p.wait(timeout=5)
            for log in logs:
                log.close()
            for node in cfg["nodes"]:
                with contextlib.suppress(subprocess.CalledProcessError):
                    with (output / f"{mode}-{node['name']}.tar.gz").open("wb") as archive:
                        ssh(node, ["sudo", "-n", "tar", "-C", f"/var/lib/relayd-lab/{args.run_id}",
                                   "--exclude=" + mode + "/input", "--exclude=" + mode + "/flux",
                                   "--exclude=" + mode + "/relay/cache", "-czf", "-", mode], stdout=archive)
                with contextlib.suppress(subprocess.CalledProcessError):
                    ssh(node, ["rm", "-f", "--", remote_cfg])
        result = ssh(ground, ["sudo", "-n", "cat", f"/var/lib/relayd-lab/{args.run_id}/{mode}/results.json"],
                     capture_output=True, text=True).stdout
        (output / f"{mode}-results.json").write_text(result)
    summarize(output)


def summarize(output):
    import csv
    import tarfile
    rows = []
    for path in sorted(output.glob("*-results.json")):
        mode = path.name.removesuffix("-results.json")
        results = read_json(path)
        network_bytes = 0
        for archive in output.glob(mode + "-*.tar.gz"):
            with tarfile.open(archive) as tar:
                member = next((m for m in tar.getmembers() if m.name.endswith("/events.jsonl")), None)
                if member:
                    for line in tar.extractfile(member):
                        event = json.loads(line)
                        if event["event"] == "connection":
                            network_bytes += event["sent_bytes"] + event["received_bytes"]
        latencies = [r["delivery_latency_s"] for r in results if r["delivered"]]
        rows.append({"mode": mode, "jobs": len(results), "delivered": sum(r["delivered"] for r in results),
                     "on_time": sum(r["on_time"] for r in results), "tcp_payload_bytes": network_bytes,
                     "mean_delivered_latency_s": sum(latencies) / len(latencies) if latencies else ""})
    if rows:
        with (output / "summary.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        for row in rows:
            print(json.dumps(row))
    print(f"Results: {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build_parser = commands.add_parser("build", help="cross-build the three services and importer")
    build_parser.add_argument("--workspace", default=str(Path(__file__).resolve().parents[3]))
    build_parser.add_argument("--output", default="./pi-bin")
    build_parser.add_argument("--arch", choices=ARCHES, default="arm64")
    build_parser.add_argument("--config", help="build each architecture in a mixed cluster")
    for name in ("validate", "plan", "setup", "run"):
        p = commands.add_parser(name)
        p.add_argument("--config", required=True)
        if name == "setup":
            p.add_argument("--binaries", default="./pi-bin")
            p.add_argument("--arch", choices=("auto",) + ARCHES, default="auto")
            p.add_argument("--bootstrap", action="store_true", help="install Debian packages on dedicated test nodes")
        if name == "run":
            p.add_argument("--run-id", default=datetime.datetime.now().strftime("run-%Y%m%d-%H%M%S"))
            p.add_argument("--output", default="./results")
            p.add_argument("--modes", default=",".join(MODES))
            p.add_argument("--lead-seconds", type=int, default=60)
    p = commands.add_parser("summary")
    p.add_argument("directory", type=Path)
    p = commands.add_parser("node", help=argparse.SUPPRESS)
    for name in ("config", "node", "run-id", "mode"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--epoch", type=float, required=True)
    p.add_argument("--install", default=ROOT)
    args = parser.parse_args()
    if args.command == "build":
        build(args)
    elif args.command == "setup":
        setup(args)
    elif args.command == "run":
        run(args)
    elif args.command == "node":
        node_main(args)
    elif args.command == "summary":
        summarize(args.directory)
    else:
        cfg = validate(read_json(args.config))
        if args.command == "plan":
            plan(cfg)
        else:
            print("Configuration valid")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
