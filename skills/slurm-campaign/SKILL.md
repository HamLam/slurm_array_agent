---
name: slurm-campaign
description: Prepare and operate this repository's persistent single-agent Slurm campaign controller, including offline rehearsal, recovery evidence, status, and deployment readiness.
---

Use Python 3.11+ and the `slurm_controller` CLI. Read IMPLEMENTATION_BRIEF.md,
README.md and docs/PROGRESS.md in the controller repository before extending it.
The controller owns durable execution; never run an independent scheduler loop or delegate agents.

Carry out the seven responsibilities:
1. Understand input, destination, command and expected outputs. The current validator
   supports byte-preserving applications only; never invent scientific parameters.
2. Inspect approved cluster identity, account, partition, QoS, limits, shared paths,
   quota, application and Python availability. Missing evidence is a deployment hold.
3. Plan grouped work and bounded waves within aggregate CPU/time and concurrency limits.
4. Prepare the immutable indexed manifest, then validate the campaign and generated scripts.
5. Start only within supplied authorization; use the persistent service for monitoring.
   This development session has no live-workload authorization. Offline rehearsal is permitted.
6. Let predefined controller policies recover infrastructure and incomplete-output failures.
   Inspect held tasks and audit evidence before proposing resource/application changes.
   A missing job response is ambiguous: never submit again to resolve uncertainty.
7. Report validated outcomes, retry counts, conservative reserved resource budget,
   unresolved work and output destination. Distinguish simulation from live evidence.

Commands: `python -m slurm_controller prepare CAMPAIGN --profile PROFILE.json`,
`validate`, `start`, `run`, `status`, `pause`, `resume`, `stop` with CAMPAIGN.
Start/resume for Slurm require `--authorize-live` and an authorized cluster profile.
Pause prevents future waves; submitted array elements continue. Stop requests cancellation
on the next controller poll. Keep the controller running to reconcile cancellation.
Use `diagnose CAMPAIGN` only after unattended Codex authentication and containment
have been configured. Recommendations are advisory and cannot authorize mutations.
Do not copy model credentials to jobs or reset budgets to bypass an exhausted campaign.
