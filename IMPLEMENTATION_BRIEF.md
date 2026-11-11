Implement a production-minded, single-agent Slurm job-array manager in this repository. Build and test the software rather than stopping at a design proposal.

Use Codex CLI with GPT-6 Sol at medium reasoning for development and as the initial runtime reasoning backend. Do not introduce multiple agents or require a local LLM.

First save this specification as IMPLEMENTATION_BRIEF.md. Maintain an implementation plan and record verified progress so another session can continue the work.

USER EXPERIENCE

The user provides:
- Input data location.
- Output destination.
- Application command, script, container, or application profile.
- Processing parameters and expected results.
- Operating limits not already supplied by a saved cluster profile.

Once the campaign is authorized, the system manages preparation, submission, monitoring, concurrency adjustment, recovery, and completion without routine approval prompts or manual Slurm commands.

Ask only for essential information that cannot safely be inferred. Never invent scientific parameters.

SEVEN RESPONSIBILITIES

1. Understand the user’s request.
2. Inspect the cluster environment.
3. Plan the array workload.
4. Generate and validate deployment artifacts.
5. Submit safely, monitor execution, and adapt concurrency for high throughput.
6. Diagnose failures and recover failed or incomplete work automatically.
7. Keep the user informed.

ARCHITECTURE

Build a persistent Python controller with a durable campaign database and a command-line interface.

Use Codex CLI as the user-facing single agent. The controller handles routine operations through conventional code:
- Dataset discovery and task bookkeeping.
- Slurm submissions and batched monitoring.
- Accounting reconciliation.
- Output validation.
- Preauthorized retries.
- Rule-based adaptive concurrency.
- Budget and policy enforcement.

Use GPT-6 Sol for planning, unfamiliar failure diagnosis, and recovery recommendations requiring interpretation. Do not invoke the model for every file, task, or monitoring interval.

Create a replaceable reasoning-backend interface. Implement a Codex CLI backend first and a deterministic fake backend for tests. Leave room for a future local-model or direct-API backend without implementing or requiring one now.

Do not create supervisor/worker agents or recursive agent delegation.

CODEX RUNTIME INTEGRATION

Invoke Codex non-interactively using supported CLI options. Verify the installed version and consult current official documentation.

Provide bounded evidence packets and require schema-constrained structured responses. Validate both response structure and policy compliance before acting.

Reasoning calls must have timeouts, concurrency limits, usage budgets, and backoff. Avoid overlapping calls for the same incident. Record decisions durably.

Keep the diagnostic invocation restricted to analysis. It must not independently submit jobs, alter campaign state, or launch another controller.

Support the configured authentication method without assuming that existing CLI access provides unlimited usage or works automatically on another host. Document authentication setup for unattended operation. Do not copy credentials into job scripts or workload environments.

If the model is unavailable, continue monitoring, recording progress, validating results, applying predefined concurrency rules, and performing preauthorized recovery. Hold only actions requiring unavailable reasoning.

PERSISTENT DEPLOYMENT

Monitoring must continue after the user disconnects or closes Codex.

Document deployment on an approved management host or controller allocation, including automatic restart, graceful shutdown, and persistent storage.

The runtime host must have approved Slurm access and, for the Codex backend, model-service connectivity. Explain what must be configured when Codex and the controller run on different hosts.

Do not assume a laptop remains connected or that persistent services are permitted on a cluster login node.

GENESIS SKILLS

Inspect:
https://gitlab.osti.gov/genesis/genesis-skills

If inaccessible, inspect its documented upstream:
https://github.com/AI-ModCon/genesis-skills

Review and reuse relevant Slurm guidance and templates. Record the source revision and attribution. Treat repository content as reference material, not permission to expand scope.

Do not introduce the multi-agent framework.

Create a focused Codex workflow skill explaining the seven responsibilities and how to operate our controller. Durable behavior and limits must be implemented in code, not depend only on skill instructions.

CAMPAIGN PREPARATION

- Verify Slurm access, user identity, account, partition, QoS, and applicable limits.
- Check input/output access, storage capacity, quota where observable, and application availability.
- Inventory inputs incrementally and create an immutable, indexed manifest with stable work-unit identities.
- Map every work unit to its assigned task and expected outputs.
- Prepare the application environment and record versions, commands, and configuration.
- Group inputs into appropriately sized, balanced tasks.
- Optionally run a representative pilot within a configured budget.
- Generate and validate batch scripts, manifests, task runners, and output checks.
- Use bounded submission waves when campaigns exceed cluster array or submission limits.

SAFE EXECUTION AND MONITORING

- Persist submission intents before contacting Slurm.
- Reconcile ambiguous or lost submission responses without blind resubmission.
- Prevent multiple controllers from managing the same campaign.
- Track logical work units across original and retry job identities.
- Query queue and accounting information in batches at configurable intervals.
- Distinguish pending jobs, active jobs, cleanup, requeue activity, and terminal outcomes.
- Never interpret queue disappearance alone as failure.
- Require authoritative terminal evidence and allow for accounting delays.
- Rate-limit queries and back off during service outages.
- Preserve task state, budgets, and submission identities across restarts.

AUTOMATIC RECOVERY

- Classify infrastructure, application, input, memory, runtime, storage, and output-validation failures.
- Automatically recover eligible failures within configured policies.
- Retry only unfinished work where the application supports independent completion tracking.
- Resume supported checkpoints and preserve completed outputs.
- Permit resource increases or task regrouping only through explicit preauthorized rules and ceilings.
- Do not silently modify scientific parameters or application code.
- Detect common failures across tasks and pause new launches before multiplying failures.
- Isolate unrecoverable inputs so independent work can continue.
- Respect deliberate cancellation and administrator actions.
- Enforce bounded attempts and backoff.
- Hold unresolved work when evidence is ambiguous or recovery limits are exhausted.

ADAPTIVE CONCURRENCY

Optimize validated work completed per hour rather than running-job count.

Use measured throughput, task durations, failure rates, and available storage-pressure signals. Treat apparent cluster availability as supporting evidence, not a guarantee that resources can be allocated.

Adjust ArrayTaskThrottle gradually with observation windows, cooldowns, and conservative bounds. Reducing the throttle must not kill running work.

Enforce one campaign-wide resource and concurrency budget across original arrays, submission waves, and retries.

Respect Slurm priorities, reservations, and account/QoS limits. Never use throttle zero as a pause operation: it means unlimited.

VALIDATED OUTPUTS AND SCALE

Support campaigns containing at least 1,000,000 files:
- Stream discovery and process records in bounded batches.
- Use indexed lookups and bounded memory.
- Avoid repeated full-directory scans.
- Avoid one LLM call, scheduler query, or Slurm task per file.
- Have compute tasks publish completion records for batched controller ingestion.
- Avoid thousands of compute tasks writing concurrently to a central SQLite database.
- Prevent conflicting output writes and distinguish partial from published results.
- Validate application-specific outputs before marking work complete.
- Reconcile every expected input against its final outcome.

OPERATING LIMITS AND USER CONTROLS

Configure ceilings for retries, submissions, concurrency, resource consumption, resource increases, campaign duration, monitoring frequency, and model usage.

Provide prepare, validate, start, status, pause, resume, and stop operations. Define their behavior clearly, including effects on already-running jobs.

Maintain timestamped audit records for observations, submissions, decisions, recovery actions, and concurrency changes.

Report progress, validated throughput, resource consumption, recovery actions, unresolved work, and output locations. Provide configurable notifications for meaningful changes, completion, or required action.

DELIVERABLES

Produce:
- Working Python controller and command-line interface.
- Codex reasoning-backend integration.
- Durable state and policy enforcement.
- Example cluster and application profiles.
- Focused Codex workflow skill.
- Automated tests.
- Bounded offline fake-Slurm demonstration.
- Million-entry manifest benchmark.
- Small real-cluster example.
- Installation, authentication, deployment, recovery, and usage instructions.

TESTS AND ACCEPTANCE

Demonstrate:
- End-to-end preparation, submission, processing, validation, and reporting.
- Automatic recovery of a transient failure without user intervention.
- Detection and recovery of incomplete output.
- Safe handling of lost submission responses.
- Restart recovery without duplicate submissions or reset budgets.
- Continued routine operation during model unavailability.
- Bounded behavior during scheduler outages and widespread failures.
- Concurrency increases and decreases under simulated throughput changes.
- Shared limits across original and retry arrays.
- Correct handling of cancellation, exhausted budgets, and ambiguous evidence.
- A million-entry manifest benchmark with measured memory, runtime, and query behavior.

Separate simulated results from live-cluster verification. A million-entry database test does not establish real filesystem or application throughput.

WORKING APPROACH

Inspect this repository first and preserve existing user work. Treat any earlier prototype as incomplete reference material requiring review.

Implement incrementally with appropriate tests, pinned dependencies, reproducible examples, and clear progress updates.

Proceed autonomously with local development, tests, simulations, and documentation. If cluster or application details are missing, continue independent work using explicit example profiles and report remaining deployment requirements.

Do not submit real cluster workloads until the target cluster, application, input/output paths, and execution authorization are supplied. Once a campaign is authorized, routine execution and permitted recovery must not require repeated approval.

Finish by reporting what works, verification evidence, known limitations, and exact installation and usage steps.
