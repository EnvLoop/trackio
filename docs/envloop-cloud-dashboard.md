# Scoped native metrics Dashboard

A platform can embed Trackio’s Metrics and System metrics pages for an authorized Job or Trial. The service advertises `dashboard_embed_v1` on `GET /ready` when the wheel contains the native embed entry and its JS and CSS assets.

## Serving the iframe

1. Authenticate the browser and resolve the requested Job or Trial to its permitted cloud scopes on the platform.
2. Fetch `GET /v1/dashboard/assets/index.html` using the platform service bearer. Prefix the HTML’s `/assets/` references with the resource’s content path. Set `window.__trackio_base` to that path and `window.__trackio_embed = true` before the module loads. Forward packaged JS and CSS using the same service bearer.
3. For each browser `POST <content path>/api/<action>`, reauthorize the resource. Forward its params to `POST /v1/dashboard/query` with the authorized scopes, action and service bearer. The browser receives the native response and uses its platform session.
4. Serve HTML and assets under the platform’s resource authorization and frame policy. The native plots use Vega’s expression interpreter when embedded. A nonce can authorize the platform’s inline configuration script. The same-origin parent can send `{type: "envloop.dashboard.theme", theme: "dark"}` or its `"light"` counterpart to the iframe on load and whenever the host theme changes. The Dashboard applies the host theme to its controls and plots.

## Query contract

The service requires one to 64 scopes from a single workspace and project. Each scope identifies one run bound to one Job. A non-null Trial further limits that run. `start` and `end` are inclusive Unix timestamps in seconds. Overlapping scopes return each observation once.

```json
{
  "scopes": [
    {
      "workspace_id": "example-workspace",
      "project_id": "example-project",
      "job_id": "example-job",
      "run_id": "example-run",
      "trial_id": "example-trial",
      "start": 1000,
      "end": 2000
    }
  ],
  "action": "get_embed_context",
  "params": {}
}
```

`get_embed_context` returns `{data: {project, readonly: true, runs, truncated}}`. Each series includes an opaque `id`, display `name`, cloud `run_id`, `trial_id` and `writer_id`. Only series with recorded observations in the requested scope appear. These identities keep writers and Trials separate in plot legends, smoothing and zoom.

`get_logs_batch` reads experiment events. `get_system_logs_batch` reads compute events. Pass the returned project and up to 64 selectors in `params.runs`, each shaped as `{run_id: id, run: name}`. Experiment queries also accept `scalar_only: true`. The result is `{data: [{run_id, run, logs, truncated}]}`. Logs contain scalar fields, the source step (including null) and the UTC observation timestamp. A metric name that collides with a native field or reserved group has a leading tilde on the affected name or path segment. A literal leading tilde is doubled.

Each query returns up to 256 series in the context or the most recent 10,000 events in a batch, with a 512 KiB response limit. Each batch is ordered chronologically within each series. The Dashboard reports a truncated list or history when either limit applies. Use the elapsed time axis for experiment observations with an unknown step. The visible Dashboard refreshes charts every ten seconds and discovers series every minute, or every ten seconds while the scope is empty. For terminal resources the platform can set `window.__trackio_live = false` in the bootstrap to disable periodic reads. Refresh reloads the scope immediately.
