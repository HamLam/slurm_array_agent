# Single-agent Slurm campaign controller

A working conservative Python controller with SQLite bookkeeping, immutable indexed
input discovery, bounded array waves, atomic output publication, audit records,
preauthorized retries, and an offline scheduler. No live cluster jobs have been submitted.
See [verified progress and deployment gaps](docs/PROGRESS.md) before production use.

Python 3.11+ is required (this host's default python3 is 3.6). Runtime uses only the
standard library. From this repository:

```bash
/usr/bin/python3.11 -m venv /tmp/slurm-controller-venv
/tmp/slurm-controller-venv/bin/python -m pip install --no-deps .
/tmp/slurm-controller-venv/bin/python -m unittest discover -s tests -v
/tmp/slurm-controller-venv/bin/python -m examples.offline_demo
/tmp/slurm-controller-venv/bin/python -m examples.benchmark
```

The package build pins setuptools 75.8.0; installation may need access to a package
mirror or preloaded wheel. Offline source execution works with
`/usr/bin/python3.11 -m slurm_controller` and requires no installation or downloads.
Demo and benchmark emit JSON under artifacts/. Demo outputs are temporary and removed
when finished. The benchmark inserts one million synthetic records in bounded batches;
it measures database behavior, not real filesystem or application throughput.

To keep an offline campaign and its outputs:

```bash
mkdir -p /tmp/campaign-inputs
printf 'example\n' > /tmp/campaign-inputs/sample
/usr/bin/python3.11 -m slurm_controller prepare /tmp/my-campaign --profile examples/fake-profile.json
/usr/bin/python3.11 -m slurm_controller validate /tmp/my-campaign
/usr/bin/python3.11 -m slurm_controller start /tmp/my-campaign
/usr/bin/python3.11 -m slurm_controller run /tmp/my-campaign
/usr/bin/python3.11 -m slurm_controller status /tmp/my-campaign
```

Profiles contain an explicit argv (no shell interpolation). `{input}` and `{output}`
are replaced inside each argument. The bundled application contract preserves bytes,
verified by size and SHA-256 against the immutable input. Output names are stable
relative-path SHA-256 identities plus `.out`; the units table maps them back to inputs.
No application-specific scientific validator, container environment manager or checkpoint
adapter is bundled. Supply and test those integrations before using another application.
Use absolute paths to executables; do not embed credentials in commands.

The database is created once; discovery streams files in filesystem traversal order,
assigning indexed groups without sorting the entire tree. Symbolic links are skipped.
Inventory path, size and nanosecond mtime are immutable. Inputs must remain unchanged.
Preparation currently uses one transaction; an interrupted preparation must be removed
and recreated after confirming it has no submitted waves. Do not reuse output directories
between campaigns. Directory-level exclusive ownership is an operating requirement.

`start` authorizes state transition; `run` is the persistent execution loop. `pause`
prevents new waves while already submitted arrays continue, including pending elements.
`resume` permits new waves again. `stop` is durable and irreversible; the running
controller cancels outstanding work on its next poll, then reconciles accounting.
Stop does not synchronously run scancel. Keep the service alive until reconciled.
SIGTERM gracefully exits monitoring and leaves jobs running for the next service start.
`run --once` performs one rate-limited iteration. A daemon lock prevents duplicate loops;
a short operation lock serializes mutations and manual controls.

Only one wave is outstanding, so all retries share a global throttle and submission
budget. Throttle never becomes zero. Adjustments use validated throughput, failures,
free space, cooldowns and conservative +/-1 steps. They never kill running jobs.
Resource consumption reporting is conservative reserved CPU-seconds (including rounded Slurm
walltime); it is not measured billing.
No automatic memory/time increases or regrouping are permitted. All policy fields and
defaults are in `slurm_controller/core.py:DEFAULTS`.

A submission intent and budget reservation are committed before sbatch. A lost response
is reconciled against token-bearing queue and accounting identities; zero or multiple
matches hold the intent indefinitely. There is no automatic blind resubmission.
Queue disappearance is never terminal evidence. Accounting terminal state plus a grace
period enables reconciliation. Finished units retain their outputs; retries run only
unfinished units. NODE_FAIL, BOOT_FAIL, PREEMPTED and invalid outputs under COMPLETED
are eligible for bounded retry. Other failures are held; cancellation is respected.
Widespread failure pauses future waves. Every attempt consumes an allocation reservation,
including ambiguous submissions. Scheduler errors persist exponentially backed-off deadlines.

Audit records, including progress and decisions, are timestamped Unix UTC in the audit
table. Status reports counts by final/current outcome and output path. Throughput appears
in audit `throughput` entries. Persistent unresolved/ready/cancelled counts remain visible.
External notifications are not implemented; an approved service can read status/audit.

See [deployment, authentication and recovery](docs/DEPLOYMENT.md),
[references](docs/REFERENCES.md), and the repository-local
[Codex workflow skill](skills/slurm-campaign/SKILL.md).
