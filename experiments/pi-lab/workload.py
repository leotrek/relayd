"""Repeatable data reduction workload; used unchanged in every baseline."""
import hashlib
import json
import os
import time
from pathlib import Path


def reduce_files(paths, rounds):
    records = []
    for path in paths:
        digest = ""
        for _ in range(rounds):
            h = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1 << 20), b""):
                    h.update(chunk)
            digest = h.hexdigest()
        records.append({"id": path.parent.name, "sha256": digest,
                        "bytes": path.stat().st_size})
    return records


def main():
    payload = json.loads(Path(os.environ["ORBITALD_PAYLOAD_PATH"]).read_text())
    started = time.time()
    root = Path(os.environ["ORBITALD_INPUT_DIR"])
    records = reduce_files(sorted(root.glob("*/samples.raw")), payload["rounds"])
    if not records:
        raise ValueError("no inputs")
    report = {"job_id": payload["job_id"], "inputs": records,
              "rounds": payload["rounds"], "started_at": started,
              "finished_at": time.time()}
    output = Path(os.environ["ORBITALD_OUTPUT_DIR"])
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report))
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
