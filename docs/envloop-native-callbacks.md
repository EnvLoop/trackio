# Managed native callbacks

Managed `sync()` and `freeze()` flush the active cloud spool and return `None`.
They never create or publish an HF Space, Bucket, or SQLite snapshot. The managed
context keeps this boundary after `finish()`. Returning no Space identity also
prevents Transformers from adding a misleading public Space badge to the model.
Flush preserves pending data until the service acknowledges its transaction.

`tests/cloud/native_callback_probe.py` exercises the unmodified, installed native
Transformers callback with synthetic arguments and a four-parameter model facade.
It executes initialization, config updates, scalar logging, `on_push_begin`, and
finish against the scoped in-process HTTP service, then reads the actual event
back. It does not download a model, use hardware, upload to HF, or replace any
recipe packages. The framework target and fallback dependency paths are explicit
environment variables; existing interpreter dependencies take precedence over
the fallback. The explicit framework target is for checking a particular recipe
version in an isolated process, never an instruction to replace installed TF.

Verified CPU combinations on November 6, 2026:

| Transformers | huggingface-hub | Result |
| --- | --- | --- |
| 5.17.0 | 1.33.0 | Native callback, committed scalar readback, no implicit HF publication |
| 5.18.0 | 2.4.0 | Existing platform interpreter; same native callback flow passes |
| 5.17.0 | 2.4.0 | Incompatible: Transformers rejects HF >=2 during import |

The fork accepts `huggingface-hub>=1.31.0,<3`, matching the current upstream
Trackio declaration. The lock retains the existing 1.33.0 resolution. This
Trackio compatibility does not override Transformers 5.17's own `<2` constraint.
A recipe with that incompatible combination must fail the isolated installation
probe and remain inactive. No shared environment was upgraded for these checks.
Final built-wheel installation and the platform installer are separate acceptance
steps; source callback checks do not establish that installation has passed.
