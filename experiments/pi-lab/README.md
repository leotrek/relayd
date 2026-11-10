# Raspberry Pi contact-window study

Use **one ground Pi and up to fourteen satellite Pis**. Run the commands below
from your workstation; the workstation provisions the experiment and retrieves
logs after it finishes. Ground result delivery happens on the ground Pi, through
the simulated contacts, not through SSH.

This runner compares four **placement policies** using the same orbitald runtime,
relayd data-transfer code, identity-coded Fluxd acquisitions and function image:

| Mode | Placement |
|---|---|
| `ground` | Always execute on the ground Pi; download inputs during ground contacts. |
| `source` | Always execute at the first input's source. With multiple inputs, fetch the others there. |
| `random` | Choose a satellite using a fixed per-job seed; fetch its missing inputs. |
| `relayd` | Submit at the configured requester; allow relayd to delegate or fetch inputs. |

The first three set `assigned: true` to disable relayd's placement decisions.
Thus a ground-versus-onboard result is not confounded by different compression,
input verification, transfer implementations or container runtimes. The ground baseline executes on whichever device has role `ground`; its compute
capacity must be reported alongside the satellite hardware. There is no centralized data-aware planning baseline yet.

## Requirements

- Dedicated Linux test Pis on Ethernet, preferably with the same hardware, 64-bit
  OS, cooling and storage. Python 3.9+, containerd, runc, Docker, and an available
  overlayfs snapshotter are needed on every Pi.
- SSH keys or password login and noninteractive `sudo -n` from the workstation
  to every Pi. Password login requires `sshpass` installed on the workstation.
  Configure normal SSH host verification beforehand. No application tokens are used.
- Synchronized clocks: enable NTP on all nodes. The runner rejects clock offsets
  exceeding one second. Small timing differences should still be reported.
- Go 1.24+ on the workstation, the sibling `relayd`, `orbitald` and `fluxd` checkouts,
  and internet access during setup to fetch Go dependencies and the Python image.
- Free TCP ports **18080**, **18081**, **18082** and **19000–19029** on each node.
  The image is installed in containerd's `orbitald` namespace. Avoid other
  orbitald experiments sharing that namespace while this runs.

## Selected 15-node hardware cluster

The local `cluster.json` is configured with the supplied addresses. The public
`cluster.hardware.example.json` has the same inventory. SSH credentials come
from the workstation environment:

- Ground: `nuc-1` (192.168.10.156, amd64).
- Satellites: `nuc-2`, `orin-1`, `orin-2`, and `rpi-1` through `rpi-11`.
- `rpi-9` uses ARMv7 binaries; both NUCs use amd64; the other nodes use arm64.
- `nano-1`, both Orange Pis and `orin-3` are excluded.

For this mixed cluster, build and provision with:

```sh
python3 lab.py build --config cluster.json --output ./pi-bin
python3 lab.py setup --config cluster.json --binaries ./pi-bin
python3 lab.py run --config cluster.json --run-id mixed-01
```

Setup detects each machine's architecture, checks it against the manifest, and
selects binaries from `pi-bin/arm`, `pi-bin/arm64` or `pi-bin/amd64`. The function
image is built natively on each node. Add `--bootstrap` only when dependencies need
installing; existing services on the shared Orins should be checked beforehand.
Sources are rpi-1 through rpi-6; nuc-2 is the dedicated requester.

This is a heterogeneous study: the ground NUC and satellite devices have different
compute capacities. Preserve those roles across policies, measure per-node function
runtime, and repeat a Pi-only subset to separate hardware effects from placement.
The current agent checks runtime health and input presence, not CPU speed or GPU
capability; this workload does not use the Orin GPUs.

## 1. Configure the Pis

```sh
cd /path/to/workspace/relayd/experiments/pi-lab
cp smoke.example.json cluster.json
```

Set the SSH credentials in the workstation shell before `setup` or `run`:

```sh
export RELAYD_SSH_USER='your-ssh-username'
read -r -s -p 'SSH password: ' RELAYD_SSH_PASSWORD; printf '\n'
export RELAYD_SSH_PASSWORD
```

The prompt command above works in Bash. In Zsh use:

```zsh
read -r -s 'RELAYD_SSH_PASSWORD?SSH password: '; printf '\n'
export RELAYD_SSH_PASSWORD
```

`RELAYD_SSH_USER` is required for SSH connections and applies to all nodes.
`RELAYD_SSH_PASSWORD` enables password login through `sshpass`; unset it to use
SSH keys instead. `sshpass` receives the password through its environment, not
command-line arguments. Password fields in JSON are rejected. Credentials are
not written to uploaded manifests, results, or example configurations.
Noninteractive sudo (`sudo -n`) is still required on the nodes.

```sh
unset RELAYD_SSH_PASSWORD  # Use SSH keys instead of a password.
```

Edit `cluster.json`: replace the sample IPv4 addresses and SSH hosts with your
node addresses. The `ssh` field contains only the host/IP; the username is taken
from `RELAYD_SSH_USER`. Start with the four-node example for a smoke test, or use
the selected 15-node inventory above.

```sh
python3 lab.py validate --config cluster.json
python3 lab.py plan --config cluster.json
```

`plan` prints each baseline's execution origin, task releases and contacts without
changing any Pi. `random` placement varies with the manifest's `seed`, while the
acquisition `seed` independently controls input contents.

## 2. Build and provision

```sh
python3 lab.py build --arch arm64 --output ./pi-bin
python3 lab.py setup --config cluster.json --binaries ./pi-bin --arch arm64
```

If the dedicated Pis need their dependencies installed, use:

```sh
python3 lab.py setup --config cluster.json --binaries ./pi-bin --arch arm64 --bootstrap
```

`--bootstrap` installs Debian packages `python3`, `containerd`, and `docker.io` and
enables Docker and containerd. Otherwise setup installs only the experiment files
and builds/imports the function image, using existing dependencies. Files go under
`/opt/relayd-lab`; experiment state goes under `/var/lib/relayd-lab`.

For a 32-bit ARMv7 OS, use `--arch arm` for **both** build and setup. ARMv6 is not
supported. For mixed architectures, use the per-node `arch` fields and `build --config` workflow above.

Setup builds the function on each Pi, then imports it into containerd. It does not
replace or enable the standalone relayd, Fluxd or orbitald systemd services.

## 3. Run the experiment

Start with source and relayd modes:

```sh
python3 lab.py run --config cluster.json --modes source,relayd --run-id smoke-01
```

Then run all four baselines:

```sh
python3 lab.py run --config cluster.json --run-id study-01
```

Every mode starts fresh service state and caches, using the same generated inputs
and relative contact schedule. A mode lasts `duration_s`, plus a 60-second setup
lead. Increase `--lead-seconds 120` if slower Pis cannot prepare before the common
start epoch. Existing run directories are never overwritten; choose a new run ID.

The smoke example lasts 90 seconds per baseline. Ground is in contact at seconds
0–10, 40–50 and 80–90; satellite contacts are 12–35 and 52–75. Tasks appear at
12 and 14 seconds. Each has an 8 MiB input and an 80-second delivery deadline.
A result arriving during the final ground contact can be delivered but late.

The 15-Pi example lasts 300 seconds per baseline. Six source satellites each hold
one input; sat14 is the dedicated requester. Other satellites are available for
random placement and peer discovery. This keeps initial relayd submissions on
one requester; it does not claim to measure fourteen concurrent schedulers.

Stop the workstation runner with Ctrl+C to request cleanup of this run's remote
processes. Completed runs also stop their experiment daemons and proxies. They
retain state and logs. If the workstation loses SSH connectivity entirely, nodes
normally stop at the configured end time; inspect the node logs after recovery.

## What a contact means here

Every remote relayd URL points to a **local TCP proxy**, which forwards to the
remote Pi only while that pair's contact is open. The same applies to ground
report retrieval. Closing a contact closes existing streams in both directions;
new connections fail until the next contact. Computation continues offline.

SSH, installation traffic and post-run log collection bypass the proxies. No
firewall rules or interface settings are changed. The experiment is an
application-traffic contact emulation, **not IP-level isolation**: manually calling
a remote service's real address would bypass it. The runner routes its experiment
traffic, including relayd peer calls, through the proxies.

`bandwidth_mbps` caps each TCP connection in each direction. It is **not an
aggregate satellite-radio capacity model**: simultaneous connections can exceed
that rate in total. Counts include forwarded TCP payload, HTTP framing and
coordination, but exclude IP/TCP headers, TCP retransmissions and SSH traffic.
There is no orbital propagation model, shared radio contention or multi-hop
store-and-forward router in this version.

Contact windows are symmetric. The example opens all ground-to-satellite links
simultaneously and then all satellite-to-satellite links. That is a simple
repeatable synthetic scenario, not a computed orbit. To use pair-specific windows:

```json
{"a":"sat01","b":"sat02","start_s":25,"duration_s":20,"period_s":120}
```

Selectors `*ground` and `*satellite` match node roles. Multiple matching windows
are combined. `start_s` and `period_s` define recurrence; windows are half-open,
so a window starting at 25 for 20 seconds is open at 25 and closed at 45.

## Workload and result delivery

Inputs are deterministic synthetic raw acquisitions, imported through Fluxd before
services start. Identity compression makes the configured byte size predictable.
Acquisitions are preloaded; `release_s` emulates the arrival of a processing task,
not a sensor ingest pipeline. All modes receive the same advance task definitions.

The function repeatedly scans each decoded input and computes SHA-256, producing
a small JSON report. `rounds` controls real work; there is no artificial sleep.
This is a reproducible data-reduction microbenchmark, **not wildfire inference**.
Replace the workload with a real model later while keeping it identical across
baselines. Use measured execution times to choose rounds and deadlines.

A shared, per-node harness retries **unassigned** submissions at the same origin
with the same ID until the task deadline. It does not reassign an uncertain
submission. Ground polls actual successful function reports over contact-gated
connections and checks input IDs, sizes, hashes, and rounds against the manifest.
Reports are obtained from the executor, so they need not travel back through the
requester. Local completion alone does not count as delivered.

This retry loop and result collection belong to the **experiment harness**; they
are not background production features of relayd. Whole-acquisition transfers
still have relayd's 30-second per-request timeout and no partial-download resume.
A payload too large to fit any contact or timeout may never transfer. That failure
is recorded, not hidden by a separate resumable baseline transport. Keep transfer
sizes below both limits for initial correctness tests, then deliberately cross
them to characterize the limitation.

## Outputs

```text
results/study-01/
  manifest.json
  summary.csv
  ground-results.json
  source-results.json
  random-results.json
  relayd-results.json
  <mode>-<node>.log
  <mode>-<node>.tar.gz
```

The summary contains jobs attempted, verified reports delivered, reports delivered
before their deadline, mean latency **among delivered reports**, and TCP payload
bytes forwarded by all proxies. Missing reports remain in the job denominator.
Do not interpret the delivered-only mean without the deadline success rate.

Per-task files include delivery latency and the executor. Archives retain service
logs, assignment journals and execution state for diagnosis, excluding the raw
source acquisitions and relay cache. Post-run archive traffic does not count as
simulated delivery or network usage. Rebuild a summary with:

```sh
python3 lab.py summary results/study-01
```

To assess scaling, repeat with 3, 5, 10 and 14 satellite nodes. Vary ground contact
duration, inter-contact gaps, bandwidth, input size and workload rounds one at a
time. Repeat seeds, rotate baseline order with `--modes`, and rotate hardware roles.
For independent single-input jobs, source-local and relayd may tie; report that.
Use split-input jobs to investigate the coordination/placement contribution, and
add a capable centralized planner before claiming decentralization is superior.

## Local checks

```sh
python3 -m unittest discover -s . -v
```

Tests cover contact boundaries, pair selection, stream interruption during a
contact, deterministic workloads, placement policies, identical retry requests,
report verification and summaries. They require no SSH or Pis. Deployment and
real container execution must still be checked on the Linux hardware.
