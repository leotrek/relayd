"""Unit tests require no SSH, root, listening sockets or connected Pis."""
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import lab
import workload


class LabTests(unittest.TestCase):
    def setUp(self):
        self.cfg = lab.read_json(Path(__file__).with_name("smoke.example.json"))
        self.ground, self.a, self.b, self.requester = self.cfg["nodes"]

    def test_examples_validate(self):
        for name in ("cluster.example.json", "smoke.example.json", "cluster.hardware.example.json"):
            lab.validate(lab.read_json(Path(__file__).with_name(name)))

    def test_hardware_inventory_has_exactly_fifteen_supported_nodes(self):
        cfg = lab.validate(lab.read_json(Path(__file__).with_name("cluster.hardware.example.json")))
        self.assertEqual(len(cfg["nodes"]), 15)
        nodes = {node["name"]: node for node in cfg["nodes"]}
        self.assertEqual(nodes["nuc-1"]["role"], "ground")
        self.assertEqual(nodes["rpi-9"]["arch"], "arm")
        self.assertEqual({n["arch"] for n in nodes.values()}, set(lab.ARCHES))
        self.assertNotIn("nano-1", nodes)
        self.assertNotIn("opi-1", nodes)
        self.assertNotIn("opi-2", nodes)
        self.assertNotIn("orin-3", nodes)

    def test_mixed_build_and_binary_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(config=str(Path(__file__).with_name("cluster.hardware.example.json")),
                                   workspace=directory, output=str(root / "bin"), arch="arm64")
            with mock.patch.object(lab.subprocess, "run") as run, \
                 mock.patch("sys.stdout", new=io.StringIO()):
                lab.build(args)
            self.assertEqual(run.call_count, 12)
            self.assertEqual({call.kwargs["env"]["GOARCH"] for call in run.call_args_list}, set(lab.ARCHES))
            for arch in lab.ARCHES:
                for name in ("relayd", "fluxd", "fluximport", "orbitald"):
                    (root / "bin" / arch / name).touch()
                files = lab.binary_files(root / "bin", arch)
                self.assertTrue(all(file.parent.name == arch for file in files))
            (root / "bin" / "arm" / ".target-arch").write_text("arm64")
            with self.assertRaises(ValueError):
                lab.binary_files(root / "bin", "arm")

    def test_password_and_username_come_from_environment(self):
        with mock.patch.dict(lab.os.environ, {"RELAYD_SSH_USER": "tester",
                                              "RELAYD_SSH_PASSWORD": "test-password!#"}, clear=True), \
             mock.patch.object(lab.shutil, "which", return_value="/usr/bin/sshpass"):
            command, env = lab.connection(self.cfg["nodes"][0])
            self.assertEqual(command[:3], ["sshpass", "-e", "ssh"])
            self.assertNotIn("test-password!#", command)
            self.assertEqual(env["SSHPASS"], "test-password!#")
            self.assertEqual(lab.ssh_target(self.cfg["nodes"][0]), "tester@192.168.50.10")
            scp, _ = lab.connection(self.cfg["nodes"][0], "scp")
            self.assertEqual(scp[:3], ["sshpass", "-e", "scp"])
            self.assertNotIn("test-password!#", json.dumps(lab.public_config(self.cfg)))

    def test_key_login_still_works_without_password(self):
        with mock.patch.dict(lab.os.environ, {"RELAYD_SSH_USER": "tester"}, clear=True):
            command, _ = lab.connection(self.cfg["nodes"][0])
        self.assertEqual(command[0], "ssh")
        self.assertIn("BatchMode=yes", command)

    def test_config_passwords_are_rejected(self):
        self.cfg["ssh_password"] = "unused-password"
        with self.assertRaises(ValueError):
            lab.validate(self.cfg)

    def test_username_is_required_for_connections(self):
        with mock.patch.dict(lab.os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                lab.ssh_target(self.cfg["nodes"][0])

    def test_contact_boundaries_and_recurrence(self):
        for elapsed, expected in [(-1, False), (0, True), (9.99, True),
                                  (10, False), (39.99, False), (40, True),
                                  (50, False), (80, True), (90, False)]:
            with self.subTest(elapsed=elapsed):
                self.assertEqual(lab.contact_open(self.cfg, self.ground, self.a, elapsed), expected)
                self.assertEqual(lab.contact_open(self.cfg, self.a, self.ground, elapsed), expected)

    def test_peer_contacts_independent_of_ground(self):
        self.assertTrue(lab.contact_open(self.cfg, self.a, self.b, 15))
        self.assertFalse(lab.contact_open(self.cfg, self.a, self.ground, 15))
        self.assertFalse(lab.contact_open(self.cfg, self.a, self.b, 36))
        self.assertTrue(lab.contact_open(self.cfg, self.a, self.a, 36))

    def test_explicit_contact_does_not_open_other_links(self):
        self.cfg["contacts"] = [{"a": "ground", "b": "sat01", "start_s": 0,
                                 "duration_s": 5, "period_s": 20}]
        self.assertTrue(lab.contact_open(self.cfg, self.ground, self.a, 2))
        self.assertFalse(lab.contact_open(self.cfg, self.ground, self.b, 2))
        self.assertFalse(lab.contact_open(self.cfg, self.a, self.b, 2))

    def test_baselines_only_change_placement(self):
        original = copy.deepcopy(self.cfg)
        job = self.cfg["jobs"][0]
        self.assertEqual(lab.origin(self.cfg, job, "ground"), "ground")
        self.assertEqual(lab.origin(self.cfg, job, "source"), "sat01")
        self.assertEqual(lab.origin(self.cfg, job, "relayd"), "sat03")
        first = lab.origin(self.cfg, job, "random")
        self.assertEqual(first, lab.origin(self.cfg, job, "random"))
        self.assertNotEqual(first, "ground")
        self.assertEqual(self.cfg, original)

    def test_invalid_config_rejected(self):
        changes = [lambda c: c["nodes"][1].update(name="../bad"),
                   lambda c: c["nodes"][1].update(ssh="-oProxyCommand=bad"),
                   lambda c: c["nodes"][1].update(role="ground"),
                   lambda c: c["nodes"][1].update(ip=c["nodes"][0]["ip"]),
                   lambda c: c["jobs"][0].update(inputs=["missing"]),
                   lambda c: c["jobs"][0].update(deadline_s=999),
                   lambda c: c["jobs"][0].update(rounds=0),
                   lambda c: c["contacts"][0].update(duration_s=1000),
                   lambda c: c.update(bandwidth_mbps=0)]
        for change in changes:
            cfg = copy.deepcopy(self.cfg)
            change(cfg)
            with self.assertRaises(ValueError):
                lab.validate(cfg)

    def test_deterministic_data_and_actual_workload(self):
        data = b"".join(lab.sample_bytes(42, (1 << 20) + 7))
        self.assertEqual(len(data), (1 << 20) + 7)
        self.assertEqual(data, b"".join(lab.sample_bytes(42, len(data))))
        self.assertNotEqual(data, b"".join(lab.sample_bytes(43, len(data))))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x" / "samples.raw"
            path.parent.mkdir()
            path.write_bytes(data)
            records = workload.reduce_files([path], 2)
        self.assertEqual(records, [{"id": "x", "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}])

    def test_report_validation_checks_identity_content_and_work(self):
        job = self.cfg["jobs"][0]
        expected = {"image-01": {"sha256": "abc", "bytes": 123}}
        report = {"job_id": job["id"], "rounds": job["rounds"],
                  "inputs": [{"id": "image-01", "sha256": "abc", "bytes": 123}]}
        self.assertTrue(lab.verify_report(job, report, expected))
        for field, value in [("job_id", "wrong"), ("rounds", 999), ("inputs", [])]:
            changed = copy.deepcopy(report)
            changed[field] = value
            self.assertFalse(lab.verify_report(job, changed, expected))
        changed = copy.deepcopy(report)
        changed["inputs"][0]["sha256"] = "corrupt"
        self.assertFalse(lab.verify_report(job, changed, expected))

    def test_retries_keep_identical_task_and_fixed_destination(self):
        job = copy.deepcopy(self.cfg["jobs"][0])
        job.update(release_s=0, deadline_s=10)
        seen = []

        def submit(url, body, **_):
            seen.append((url, copy.deepcopy(body)))
            if len(seen) == 1:
                raise OSError("lost acknowledgement")
            return {"state": "accepted"}

        events = mock.Mock()
        with mock.patch.object(lab, "http_json", side_effect=submit), \
             mock.patch.object(lab.time, "time", return_value=100), \
             mock.patch.object(lab.STOP, "wait", return_value=False):
            lab.submit_job(self.cfg, job, "random", 100, events)
        self.assertEqual(len(seen), 2)
        self.assertEqual(seen[0], seen[1])
        self.assertTrue(seen[0][1]["assigned"])
        self.assertEqual(events.emit.call_args.args[0], "assignment")

    def test_proxy_routes_and_namespace_do_not_collide(self):
        urls = {lab.peer_url(self.cfg, node["name"], service)
                for node in self.cfg["nodes"] for service in ("relay", "lab")}
        self.assertEqual(len(urls), len(self.cfg["nodes"]) * 2)
        self.assertTrue(all(url.startswith("http://127.0.0.1:") for url in urls))

    def test_contact_close_interrupts_an_existing_tcp_stream(self):
        class FakeServer:
            def __init__(self, address, handler):
                self.handler = handler
            def serve_forever(self, **_):
                pass

        caller, incoming = socket.socketpair()
        upstream, destination = socket.socketpair()
        available = threading.Event()
        available.set()
        events = mock.Mock()
        try:
            for conn in (caller, incoming, upstream, destination):
                conn.settimeout(1)
            with mock.patch.object(lab, "TCPServer", FakeServer), \
                 mock.patch.object(lab, "contact_open", side_effect=lambda *_: available.is_set()), \
                 mock.patch.object(lab.socket, "create_connection", return_value=upstream):
                server = lab.start_proxy(self.cfg, self.a, self.b, "relay", 0, events)
                worker = threading.Thread(target=server.handler, args=(incoming, ("local", 0), server))
                worker.start()
                caller.sendall(b"x")
                self.assertEqual(destination.recv(1), b"x")
                available.clear()
                self.assertEqual(destination.recv(1), b"")
                self.assertEqual(caller.recv(1), b"")
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive())
                self.assertEqual(events.emit.call_args.kwargs["sent_bytes"], 1)
        finally:
            for conn in (caller, incoming, upstream, destination):
                conn.close()

    def test_summary_keeps_missing_results_in_denominator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lab.write_json(root / "source-results.json", [
                {"delivered": True, "on_time": True, "delivery_latency_s": 10},
                {"delivered": False, "on_time": False, "delivery_latency_s": None}])
            with mock.patch("sys.stdout", new=io.StringIO()):
                lab.summarize(root)
            text = (root / "summary.csv").read_text()
            self.assertIn("source,2,1,1,0,10.0", text)


if __name__ == "__main__":
    unittest.main()
