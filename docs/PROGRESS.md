# Verified progress and continuation brief

2026-11-11. The repository originally contained only prompt.txt. It is preserved;
IMPLEMENTATION_BRIEF.md is an exact copy. A later user-created hcl_note was preserved.
No real scheduler submission, cluster allocation, or paid model call was made.

Implemented and exercised:
- Python package and prepare/validate/start/status/pause/resume/stop/run CLI.
- SQLite manifest, immutable input metadata, indexed task lookups, streamed discovery,
  bounded batches, stable path identities, per-unit expected output and retry mapping.
- Exclusive controller/daemon locks, timestamped audit, durable submission intents,
  conservative resource reservations and persistent attempt/submission/model limits.
- Slurm adapter for association/partition/array preflight, scripts, token reconciliation,
  batched accounting, deliberate cancellation and positive-only throttle updates.
- Fake scheduler with persistent identities, local compute execution and injected failures.
- Per-task atomic completion receipts (no compute DB writes), partial-output isolation,
  atomic publication, byte-preserving size/SHA-256 validation, retry only unfinished units.
- Accounting grace, ambiguous-evidence hold, restart reconciliation, scheduler backoff,
  failure circuit breaker and +/-1 throttle adaptation under cooldown and bounds.
- Replaceable fake/Codex diagnostic backend, strict response checks, 16 KB evidence,
  one serialized call, timeouts/process-group termination, durable call/time reservation,
  incident deduplication, advisory-only outcomes. Routine work runs without a model.
- Example profiles, service template, focused Codex skill, usage/deployment/recovery docs.

Evidence:
- artifacts/tests.txt: 21 distinct offline tests, including adapter parsing, shared limits,
  transient recovery, incomplete results, lost responses/restart, ambiguity, outage backoff,
  model unavailability/invalid advice, cancellation, attempt/budget exhaustion and throttle.
- artifacts/offline-demo.json: 24/24 validated, three simulated submissions, maximum two
  attempts; restart after lost response, transient failure and unavailable model exercised.
- artifacts/million-manifest.json: measured million-entry synthetic SQLite insertion,
  1,000 indexed task queries, database size, elapsed time and Linux process peak memory.
  Linux VmHWM is used because getrusage on this host inherits the launcher's peak.
- Installed CLI 0.162.1 and supported exec flags/features inspected; runtime call not made.
- Skill frontmatter checked locally. Bundled official quick_validate.py could not run
  under Python 3.11 because PyYAML is not installed; this does not affect runtime code.

Remaining work before broad production deployment:
1. Supply target cluster, actual application, shared paths and explicit execution
   authorization. Verify Slurm command behavior/permissions and site policies there.
2. Extend application profiles with scientific validators, application environment/version
   capture, containers, supported checkpoint resume and workload-aware balancing/pilot.
   Current profile supports byte-preserving independent units grouped by file count.
3. Full automatic derivation of account/QoS/submission/resource ceilings and quota is
   not implemented. Preflight checks a subset and requires a manually verified profile.
4. Add staged read-only compute manifests for sites where direct SQLite shared reads
   are unsuitable. Partial discovery is not resumable; no incremental directory rescan.
5. Add approved held-task release, memory/time escalation, regrouping and storage-error
   recovery adapters. Current behavior holds unsafe/unknown failures; it never changes
   scientific parameters. Stop is irreversible; pause does not hold existing pending jobs.
6. Integrate automatic bounded unfamiliar-failure evidence/planning calls. Diagnostics are
   currently explicit CLI advisory calls; deploy strong OS/service containment and test
   unattended authentication/model availability. Token/dollar budgets require service caps.
7. Add actual measured Slurm resource accounting, task duration statistics, configurable
   notification delivery and filesystem/application scale measurements. Present resource
   report is a conservative CPU-time reservation, not billing or observed CPU usage.
8. Fault-injection covers controller logic and adapters with fake responses; real Slurm
   requeue/admin transitions, cross-host storage and multi-process crash timing remain
   unverified. One outstanding wave is intentionally conservative and limits throughput.

Continue from IMPLEMENTATION_PLAN.md. Reproduce offline tests/demo/benchmark with
Python 3.11+ using README commands. Do not treat simulated results as live verification.

Final verification: clean Python 3.11 virtual-environment build/install succeeded,
including the installed `slurm-campaign --help` entry point. Final synthetic benchmark:
1,000,000 entries inserted in 49.41 s; 1,000 indexed queries in 0.0604 s;
Linux VmHWM 26,680 KiB (26.1 MiB), database 227,856,384 bytes. These are local
synthetic database results only. Final offline suite: 21 tests passed in 2.483 s.
