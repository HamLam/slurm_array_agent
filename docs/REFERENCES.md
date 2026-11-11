# References and attribution

Reviewed 2026-11-11. Installed CLI: codex-cli 0.162.1; `codex exec --help`
and `codex features list` checked locally. Runtime targets `gpt-6-sol` with
`model_reasoning_effort="medium"`; no runtime model call was made during development.
The active development session's model is selected by the host; the code does not
claim to have changed that host selection.

Official noninteractive CLI documentation:
https://learn.chatgpt.com/docs/non-interactive-mode
(also reachable through https://developers.openai.com/codex/noninteractive/).
Reviewed supported exec, read-only sandbox and schema/output options.
Model access and authentication remain deployment checks.

Genesis primary https://gitlab.osti.gov/genesis/genesis-skills returned HTTP 502.
Reviewed upstream https://github.com/AI-ModCon/genesis-skills at
`70dce5aca946e2804f9b8579da01910cd2861858`:
- skills/hpc-skills/slurm/SKILL.md
- skills/hpc-skills/slurm/references/job-control.md
- skills/hpc-skills/slurm/references/accounting.md
- skills/hpc-skills/slurm/assets/templates/ (array template)
- baseeval Perlmutter check_status.sh and run.sh.tmpl (reference only).

Reused guidance on array indexing, `%A_%a` logs, throttle, associations and separate
queue/accounting evidence. Implementation is independently written; no multi-agent
framework was imported. HPC subtree is Apache-2.0; its license and attribution are
retained in third-party/. Baseeval is also separately licensed Apache-2.0; no code
was copied from it. Root Genesis BSD terms do not supersede subtree licenses.
