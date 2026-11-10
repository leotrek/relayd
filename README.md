# relayd

A small peer agent beside **orbitald + fluxd**, written in Go using only the
standard library. No dedicated controller or separate catalogue service.

Each agent prefers local execution, delegates once to a reachable peer holding
all inputs, or fetches missing inputs and executes locally. Peers are configured
explicitly; their local inventories are queried on demand. This is the first
working slice of a distributed data engine, not a contact-aware optimizer.

```text
Node A                              Node B
relayd <--------- HTTP -----------> relayd
  ├── local fluxd socket              ├── local fluxd socket
  ├── verified encoded cache          ├── verified encoded cache
  └── orbitald HTTP API               └── orbitald HTTP API
        └── relayd input socket            └── relayd input socket
```

## Build and test

Requires Go 1.24+. Running function containers requires Linux and containerd.

```sh
go build -o bin/relayd ./cmd/relayd
go test -race ./...
go vet ./...
```

Tests use in-memory HTTP transports and framed connections, with isolated disk
state. They cover delegation, split-input transfers, encoded checksum and size
checks, incomplete transfers, durable deduplication, concurrent retries,
ambiguous acknowledgements, deadlines, request validation, and the orbitald input
socket protocol. They do not launch real Fluxd, orbitald or containerd.

## Run on each node

Start the existing Fluxd with a sample or imported acquisitions. Use a persistent,
private state directory and a private Unix socket directory. On node A:

```sh
./bin/relayd \
  -node A \
  -listen 0.0.0.0:8090 \
  -state-dir /var/lib/relayd \
  -socket /run/relayd/inputs.sock \
  -fluxd-socket /run/fluxd/fluxd.sock \
  -orbitald http://127.0.0.1:8080 \
  -peers http://NODE_B:8090
```

Create `/run/relayd` first. Run one relayd process per state directory. On node B,
use `-node B` and `-peers http://NODE_A:8090`. Redirects are disabled.

Point **orbitald at relayd's input socket**, not directly at Fluxd:

```sh
orbitald \
  -state-dir /var/lib/orbitald \
  -fluxd-socket /run/relayd/inputs.sock
```

The service identities must have access to the appropriate sockets. Relayd creates
its socket with mode 0600. It refuses to overwrite an existing socket; after an
unclean exit, remove the stale socket only after checking the old process is gone.
Preload compatible OCI function images into the `orbitald` containerd namespace
on every candidate execution node. No changes to Fluxd or orbitald are required.

## Submit a task

Create `task.json`, using a future UTC deadline and actual acquisition IDs:

```json
{
  "id": "mission-unique-task-001",
  "name": "flux-inspect",
  "image": "orbitald/example-flux-inspect:dev",
  "inputs": ["optical-0001"],
  "deadline": "2026-12-01T12:00:00Z",
  "run_timeout": "2m",
  "memory": "64Mi",
  "payload": {}
}
```

```sh
curl --fail-with-body http://NODE_A:8090/v1/tasks \
  -H 'Content-Type: application/json' \
  --data-binary @task.json

curl --fail-with-body http://NODE_A:8090/v1/tasks/mission-unique-task-001

curl --fail-with-body http://NODE_A:8090/v1/tasks/mission-unique-task-001/execution
```

The submission response records `node`, `state` and the orbitald acknowledgement
in `response`. `accepted` means queued, not completed. The `/execution` endpoint
queries the destination and returns orbitald's current task details, including
result status and output directory when available. Output files remain on that
node; automatic product transfer is outside this version.

`deadline` is the latest dispatch/start-window deadline, not a completion or
result-delivery guarantee. Orbitald enforces its own start window and run timeout.

## Placement and ownership

1. Check local inputs and orbitald runtime health.
2. If not ready, ask peers in configured order whether all inputs are local and
   their orbitald runtime is healthy. Delegate to the first ready peer.
3. Otherwise fetch missing acquisitions from peers into the local cache. Verify
   encoded byte counts and SHA-256 before atomic publication, then submit locally.
4. Persist the selected destination **before** the submission side effect.

The receiving agent does not forward the task again (`assigned: true`), preventing
loops. It rechecks availability rather than trusting the sender's readiness view.
Orbitald fetches through the local adapter, decompresses, and verifies raw samples
using its existing decoder. Relayd never writes inside Fluxd's private store.
The adapter implements GetStatus, GetAcquisition, OpenAcquisition and Release;
it does not implement ListAcquisitions. Peer discovery uses exact input IDs.

Task IDs and acquisition IDs must be globally unique and immutable. A task is
submitted through **one designated origin agent**; retry that same origin with
the same ID and body. Do not independently submit it to multiple origin agents.
Each origin persists its assignment and rejects conflicting reuse of an ID.
This is a simple ownership protocol, not distributed consensus or global
exactly-once execution. Never delete journals to retry an uncertain task blindly.

If a submission acknowledgement is lost, or the agent crashes during submission,
the assignment becomes `uncertain`. It is not retried or reassigned automatically:
orbitald's task-start API does not accept an idempotency key. Inspect the selected
node's orbitald tasks before deciding how to recover. This deliberately preserves
ambiguity rather than creating duplicate execution after a partition.

## Deliberate limits

- Fixed peers and on-demand replica lookup; no gossip, replicated catalogue,
  contact prediction, leader election or automatic ownership failover.
- Readiness checks runtime health and input presence. They do not reserve CPU,
  memory, images or storage. Orbitald performs actual execution admission.
- Serialized placement/submission per agent; other tasks wait while a transfer is
  in progress. HTTP peer operations time out after 30 seconds. Data transfer is
  whole-acquisition; interrupted copies are discarded and may be retried by
  resubmitting a task that has not yet received an assignment.
- Missing data or unavailable peers produce an error before assignment; there is
  no background durable retry queue. Once assigned, retries return that assignment.
- The encoded byte limit defaults to 1 GiB per acquisition and per local task.
  Orbitald independently limits encoded and decoded inputs. The persistent cache
  has no eviction or total disk quota; operators must provision and manage it.
- A hard crash can leave unpublished `.incoming-*` cache directories. Remove
  those only while the daemon is stopped. Published cache entries are immutable.
- No automatic acquisition-ready triggers, model deployment, output replication,
  result delivery or side-effect fencing. These can follow after the two-node
  input-transfer and delegation tests work on actual hardware.

## Raspberry Pi experiments

The [Pi contact-window runner](experiments/pi-lab/README.md) provisions one ground
Pi and satellite Pis, replays intermittent contacts, and compares ground,
source-local, random and relayd placement with verified ground delivery metrics.
