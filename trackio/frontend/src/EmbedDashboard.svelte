<script>
  import { onMount, tick } from "svelte";
  import { callApi } from "./lib/api.js";
  import { applyTheme, detectSystemTheme, isDark } from "./lib/theme.js";
  import Metrics from "./pages/Metrics.svelte";
  import SystemMetrics from "./pages/SystemMetrics.svelte";

  let context = $state(null);
  let error = $state("");
  let loading = $state(true);
  let page = $state("metrics");
  let xAxis = $state("step");
  let smoothing = $state(0);
  let selectedIds = $state([]);
  let selectedRuns = $derived(context?.runs.filter(run => selectedIds.includes(run.id)) ?? []);
  let controller;
  let chart = $state(null);
  let lastContextLoad = 0;

  function applyHostTheme(event) {
    if (event.source !== window.parent || event.origin !== window.location.origin) return;
    const message = event.data;
    if (message?.type !== "envloop.dashboard.theme") return;
    if (message.theme === "dark" && !isDark()) applyTheme("dark");
    if (message.theme === "light" && isDark()) applyTheme("default");
  }

  async function refresh() {
    controller?.abort();
    controller = new AbortController();
    loading = true;
    error = "";
    try {
      const result = await callApi("get_embed_context", {}, {signal: controller.signal});
      if (result?.readonly !== true || !Array.isArray(result.runs)) {
        throw new Error("Invalid read-only Dashboard context");
      }
      const previousIds = new Set(context?.runs.map(run => run.id));
      const selected = new Set(selectedIds);
      selectedIds = result.runs
        .filter(run => !previousIds.has(run.id) || selected.has(run.id))
        .map(run => run.id);
      context = result;
      lastContextLoad = Date.now();
      await tick();
      void chart?.refresh();
    } catch (e) {
      if (e?.name === "AbortError") return;
      error = e.message || "Dashboard unavailable";
      context = null;
    } finally {
      loading = false;
    }
  }

  onMount(() => {
    applyTheme(detectSystemTheme());
    void refresh();
    if (window.__trackio_live === false) return () => controller?.abort();
    const timer = setInterval(() => {
      if (document.hidden || loading) return;
      if (!context?.runs.length || Date.now() - lastContextLoad >= 60000) {
        void refresh();
      } else {
        void chart?.refresh();
      }
    }, 10000);
    return () => {
      clearInterval(timer);
      controller?.abort();
    };
  });
</script>

<svelte:window onmessage={applyHostTheme} />

<main>
  <header>
    <strong>Trackio</strong>
    <nav aria-label="Metric categories">
      <button aria-pressed={page === "metrics"} onclick={() => page = "metrics"}>Metrics</button>
      <button aria-pressed={page === "system"} onclick={() => page = "system"}>System metrics</button>
    </nav>
    <button onclick={refresh} disabled={loading}>Refresh</button>
  </header>
  {#if !context && loading}
    <p role="status">Loading metrics…</p>
  {:else if error}
    <p role="alert">{error}</p>
  {:else if context}
    {#if context.truncated}
      <p role="status">The list of series was truncated.</p>
    {/if}
    {#if context.runs.length}
      <section aria-label="Chart options">
        <details>
          <summary>Series ({selectedIds.length}/{context.runs.length})</summary>
          {#each context.runs as run (run.id)}
            <label><input type="checkbox" value={run.id} bind:group={selectedIds} /> {run.name}</label>
          {/each}
        </details>
        {#if page === "metrics"}
          <label>Horizontal axis
            <select bind:value={xAxis}><option value="step">Step</option><option value="time">Elapsed time</option></select>
          </label>
          <label>Smoothing <input type="range" min="0" max="50" bind:value={smoothing} /> {smoothing}</label>
        {/if}
      </section>
      {#if !selectedRuns.length}
        <p>Select a series to view its metrics.</p>
      {:else if page === "metrics"}
        <Metrics bind:this={chart} project={context.project} {selectedRuns} allRuns={context.runs} {smoothing} {xAxis}
          appBootstrapReady={true} embedded={true} realtimeEnabled={false} />
      {:else}
        <SystemMetrics bind:this={chart} project={context.project} {selectedRuns} allRuns={context.runs}
          appBootstrapReady={true} embedded={true} realtimeEnabled={false} />
      {/if}
    {:else}
      <p>No metrics were recorded in this scope.</p>
    {/if}
  {/if}
</main>

<style>
  :global(body) { margin: 0; background: var(--background-fill-primary); color: var(--body-text-color); font: 14px/1.5 system-ui, sans-serif; }
  header, section, nav, section > label { display: flex; flex-wrap: wrap; align-items: center; gap: .65rem; }
  header, section { padding: .75rem 1rem; border-bottom: 1px solid var(--border-color-primary); }
  header > button { margin-left: auto; }
  button, select { font: inherit; color: inherit; background: var(--background-fill-secondary); border: 1px solid var(--border-color-primary); border-radius: 4px; padding: .25rem .6rem; }
  button, summary, select { cursor: pointer; }
  button[aria-pressed="true"] { border-color: var(--color-accent); }
  details label { display: block; overflow-wrap: anywhere; }
  p { margin: 1rem; }
</style>
