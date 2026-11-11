# Deployment and recovery

Use an approved management host or approved controller allocation with automatic
restart. Do not assume a laptop stays connected or a login node permits a daemon.
Install Python 3.11+ and a site-approved Codex CLI version (0.162.1 inspected here).
Install this package in a virtual environment. Adapt controller.service and have the
site administrator install it in the approved service manager. For a controller
allocation, use the site's documented restart mechanism and walltime policy; no
controller allocation is submitted by this repository.

Store SQLite, receipts, logs, scripts and outputs on persistent storage. SQLite
locking and fsync must work correctly; use controller-local durable storage where
shared-filesystem locking is not supported. Compute nodes need read access to the
manifest and runner, and write access to their own receipt/log and output paths.
The current direct runner design therefore requires a site-approved shared storage
arrangement with reliable SQLite read access; a separate staging replica is future work.
Back up a quiescent database or use SQLite's backup API. Never copy a live DB incompletely.
A service restart preserves submission identities, budget reservations, retry counts,
model incidents and query deadlines. SIGTERM stops monitoring without cancelling jobs.

The controller host needs Slurm client binaries and approved identity/account access;
compute nodes need the recorded absolute Python and application paths. When Codex runs
elsewhere, install and authenticate the runtime CLI on the controller host as well, or
provide an independently secured backend implementation. An interactive Codex session
on a workstation does not supply remote-host authentication. Shared paths and exact
executable locations must match the target machines. Runtime diagnostics need outbound
model-service connectivity; compute tasks do not need it.

## Unattended Codex

Consult the installed `codex login --help` and official authentication guidance for
your approved authentication method. Authenticate as the actual service identity using
its private CODEX_HOME, or use the supported API-key login flow with credentials provided
by an approved secret manager. Do not place keys in profiles, batch scripts or outputs.
Provision restrictive permissions; test noninteractive authentication under the service
account and configure service-side spend/rate limits. Subscription access is not an
unlimited allowance. Model availability for gpt-6-sol must be verified for that account.
The controller bounds call counts, reserved call duration and timeout, not billed token
cost. Account/project spend ceilings must enforce monetary limits externally.

`python -m slurm_controller diagnose CAMPAIGN` uses an advisory Codex backend. It
invokes noninteractive exec with a JSON schema, a 16 KB evidence ceiling, GPT-6 Sol,
medium reasoning, an isolated temporary cwd, ignored user config/rules, disabled shell,
apps, hooks, code-mode and agent delegation, read-only sandbox and no approvals.
The invocation inherits only the service's authentication environment; this must be a
clean service identity without ambient external tools or untrusted plugins. For strict
production isolation place diagnostics under an unprivileged identity/container with
no Slurm credentials, no scheduler sockets, no campaign writes and model-service-only
egress. Validate that containment at the site before enabling diagnostics. CLI feature
controls are version sensitive; changing the CLI requires revalidation. This development
run did not invoke a paid model or validate operating-system containment on the cluster.

A successful response can recommend only hold or inspect. It cannot change campaign,
application, scientific parameters or resource policy. Responses are checked again in
Python before audit storage. Call reservation and incident identity are committed first;
a timeout kills the process group. Identical incidents are deduplicated, including failed
calls, rather than retried in a tight loop. Routine monitoring is independent of diagnosis.
Diagnostics serialize with campaign mutations for at most the configured timeout.
Automatic unfamiliar-failure diagnosis and planning integration remain future work;
held work is safe without them. Evidence may contain private paths: review what is sent.

## Small live example (instructions only)

No execution is authorized in this development session. After receiving the actual
cluster, application, paths and execution authorization:

1. Copy examples/cluster-profile.json to an external campaign profile. Replace all
   placeholders; use a few byte-preserving test files for this first supported application.
2. Verify identity, account, partition, QoS, MaxArraySize, MaxSubmitJobs, CPU/memory/walltime,
   account/QoS constraints and storage quota with the facility. Record evidence separately.
   Set verified_limits=true only after this review, execution_authorized=true only under
   the supplied execution authorization. Automatic preflight checks observable association,
   partition and array size; it cannot derive every site/account/QoS limit.
3. Confirm input/output access, persistent shared storage, free space and application
   availability on the compute nodes. Quota/capacity observations can be incomplete.
4. Prepare and validate using the copied profile; inspect the manifest and configured caps.
5. Once authorized, run `python -m slurm_controller start CAMPAIGN --authorize-live`
   and start the approved persistent service. `status CAMPAIGN` is read-only.

Do not run step 5 until actual execution authorization is supplied. There is no live
verification evidence in artifacts/.

## Recovery investigation

Read status and the audit table before changing anything. For an intent without job ID,
search queue/accounting using its exact `sc-...` token and creation time. Never delete
the intent or reset counters to cause resubmission. If accounting cannot prove absence,
keep it held and consult the scheduler administrator. Incomplete receipts are ignored
until atomically published and authoritative terminal evidence is old enough.

For application, input, memory, runtime or storage errors, held units remain indexed
and independently completed units are preserved. Resume enables ready work after a
pause; it does not release held units. Resource increases, held-unit release, checkpoint
resume and task regrouping have no implemented authorized recovery adapter yet. Resolve
these through reviewed code/profile changes and a separately planned campaign rather
than editing scientific arguments silently. Do not bypass limits by modifying the DB.
Cancellation or deliberate stop is never automatically undone.
