import { afterEach, expect, it, vi } from "vitest";
import { mount, unmount } from "svelte";

const base = "/api/v1/jobs/job/monitor/dashboard/content";
let instance;
let target;
afterEach(async () => {
  if (instance) await unmount(instance);
  instance = undefined;
  target?.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function setup(handler) {
  vi.stubGlobal("__trackio_base", base);
  vi.stubGlobal("__trackio_embed", true);
  return vi.stubGlobal("fetch", vi.fn(handler));
}

it("uses only the scoped read API and excludes Trackio OAuth/write headers", async () => {
  setup(() => Response.json({data: []}));
  sessionStorage.setItem("trackio_oauth_session", "unused");
  try {
    const {callApi, getLogsBatch} = await import("./lib/api.js");
    await getLogsBatch("project", [{id:"series",name:"writer"}], {scalar_only:true});
    expect(fetch.mock.calls.map(call => call[0])).toEqual([base + "/api/get_logs_batch"]);
    expect(fetch.mock.calls[0][1].headers).toEqual({"Content-Type":"application/json"});
    await expect(callApi("delete_run", {})).rejects.toThrow();
    expect(fetch.mock.calls).toHaveLength(1);
  } finally {
    sessionStorage.removeItem("trackio_oauth_session");
  }
});

async function render() {
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(null);
  const {default: Dashboard} = await import("./EmbedDashboard.svelte");
  target = document.createElement("div");
  document.body.append(target);
  instance = mount(Dashboard, {target});
}

it("renders native experiment and compute pages with scoped query feedback", async () => {
  setup((url) => {
    if (url.endsWith("/get_embed_context")) return Response.json({data:{
      project:"project",readonly:true,runs:[{id:"series",name:"Trial / writer"}],truncated:false
    }});
    if (url.endsWith("/get_logs_batch")) return Response.json({data:[
      {run_id:"series",run:"Trial / writer",logs:[],truncated:true}
    ]});
    return new Response("",{status:503});
  });
  await render();
  await vi.waitFor(() => expect(target.querySelector(".metrics-page")).toBeTruthy());
  await vi.waitFor(() => expect(target.textContent).toContain("truncated"));
  expect(target.querySelector('input[type="range"]').value).toBe("15");
  expect(target.textContent).toContain("Trial / writer");
  expect(target.textContent).not.toContain("trackio.init");
  [...target.querySelectorAll("button")].find(button => button.textContent.includes("System metrics")).click();
  await vi.waitFor(() => expect(target.textContent).toContain("503"));
  expect(fetch.mock.calls.every(([url]) => url.startsWith(base + "/api/"))).toBe(true);
});

it("reports context failures without offering standalone project navigation", async () => {
  setup(() => new Response("",{status:403}));
  await render();
  await vi.waitFor(() => expect(target.querySelector('[role="alert"]')?.textContent).toContain("403"));
  expect(target.querySelector("a")).toBeNull();
});

it("shows a step-less series on the time axis in the native chart", async () => {
  vi.stubGlobal("IntersectionObserver", class {
    observe() {}
    disconnect() {}
  });
  vi.stubGlobal("ResizeObserver", class {
    observe() {}
    disconnect() {}
  });
  setup(url => Response.json({
    data: url.endsWith("/get_embed_context")
      ? { project: "project", readonly: true, runs: [{ id: "a", name: "writer" }] }
      : [{
        run_id: "a",
        run: "writer",
        logs: [
          { step: null, timestamp: "2026-01-01T12:00:00Z", "train/loss": 2 },
          { step: null, timestamp: "2026-01-01T12:00:02Z", "train/loss": 1 },
        ],
      }]
  }));
  await render();
  await vi.waitFor(() => expect(target.textContent).toContain("without a step"));
  expect(target.querySelector("[data-metric]")).toBeNull();
  const axis = target.querySelector("select");
  axis.value = "time";
  axis.dispatchEvent(new Event("change", { bubbles: true }));
  await vi.waitFor(() => expect(target.querySelector('[data-metric="train/loss"]')).toBeTruthy());
  expect(target.textContent).not.toContain("without a step");
});

it("refreshes visible charts and discovers a writer after an empty initial scope", async () => {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  let contexts = 0;
  try {
    setup(url => {
      if (url.endsWith("/get_embed_context")) {
        contexts++;
        return Response.json({ data: {
          project: "project", readonly: true,
          runs: contexts === 1 ? [] : [{ id: "a", name: "writer" }]
        } });
      }
      return Response.json({ data: [{ run_id: "a", logs: [] }] });
    });
    await render();
    await vi.waitFor(() => expect(target.textContent).toContain("No metrics were recorded"));
    const hidden = vi.spyOn(document, "hidden", "get").mockReturnValue(true);
    await vi.advanceTimersByTimeAsync(10000);
    expect(contexts).toBe(1);
    hidden.mockReturnValue(false);
    await vi.advanceTimersByTimeAsync(10000);
    await vi.waitFor(() => expect(target.querySelector(".metrics-page")).toBeTruthy());
    const getLogs = () => fetch.mock.calls.filter(([url]) => url.endsWith("/get_logs_batch"));
    await vi.waitFor(() => expect(getLogs()).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(10000);
    await vi.waitFor(() => expect(getLogs()).toHaveLength(2));
    expect(contexts).toBe(2);
  } finally {
    vi.useRealTimers();
  }
});

it("leaves terminal resources on manual refresh", async () => {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  vi.stubGlobal("__trackio_live", false);
  try {
    setup(() => Response.json({ data: { project: "project", readonly: true, runs: [] } }));
    await render();
    await vi.waitFor(() => expect(target.textContent).toContain("No metrics were recorded"));
    await vi.advanceTimersByTimeAsync(60000);
    expect(fetch).toHaveBeenCalledTimes(1);
    [...target.querySelectorAll("button")].find(button => button.textContent === "Refresh").click();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
  } finally {
    vi.useRealTimers();
  }
});

it.each(["Metrics", "System metrics"])("refreshes backfilled bounded history in %s", async category => {
  vi.stubGlobal("__trackio_live", false);
  vi.stubGlobal("IntersectionObserver", class { observe() {} disconnect() {} });
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
  let backfilled = false;
  let batches = 0;
  setup(url => {
    if (url.endsWith("/get_embed_context")) {
      return Response.json({ data: {
        project: "project", readonly: true, runs: [{ id: "a", name: "writer" }]
      } });
    }
    batches++;
    const early = backfilled
      ? { step: 2, timestamp: "2026-01-01T12:00:02Z", delayed: 2 }
      : { step: 1, timestamp: "2026-01-01T12:00:01Z", evicted: 1 };
    return Response.json({ data: [{ run_id: "a", truncated: true, logs: [
      early, { step: 3, timestamp: "2026-01-01T12:00:03Z", latest: 3 },
    ] }] });
  });
  await render();
  if (category === "System metrics") {
    [...target.querySelectorAll("button")].find(button => button.textContent === category).click();
  }
  await vi.waitFor(() => expect(target.querySelector('[data-metric="evicted"]')).toBeTruthy());
  const before = batches;
  backfilled = true;
  [...target.querySelectorAll("button")].find(button => button.textContent === "Refresh").click();
  await vi.waitFor(() => expect(batches).toBeGreaterThan(before));
  await vi.waitFor(() => expect(target.querySelector('[data-metric="delayed"]')).toBeTruthy());
  expect(target.querySelector('[data-metric="evicted"]')).toBeNull();
});

it("accepts theme changes only from the same-origin parent", async () => {
  setup(() => Response.json({ data: { project: "project", readonly: true, runs: [] } }));
  await render();
  const send = (theme, origin = location.origin, source = window.parent) => {
    window.dispatchEvent(new MessageEvent("message", {
      data: { type: "envloop.dashboard.theme", theme, scrollbarStyle: "/* " + theme + " */" }, origin, source
    }));
  };
  send("dark", "https://unrelated.invalid");
  send("dark", location.origin, null);
  expect(document.documentElement.dataset.theme).not.toBe("dark");
  expect(document.querySelector("style[data-console-scrollbars]")).toBeNull();
  send("dark");
  expect(document.documentElement.dataset.theme).toBe("dark");
  send("invalid");
  expect(document.documentElement.dataset.theme).toBe("dark");
  send("light");
  await vi.waitFor(() => expect(document.querySelector("style[data-console-scrollbars]")?.textContent).toBe("/* light */"));
  expect(document.querySelectorAll("style[data-console-scrollbars]")).toHaveLength(1);
  expect(document.querySelector("style[data-console-scrollbars]").textContent).toBe("/* light */");
  expect(document.documentElement.dataset.theme).not.toBe("dark");
});

it("uses blue for the first embedded series and distinct colors for later series", async () => {
  const {buildColorMap} = await import("./lib/stores.js");
  const colors = buildColorMap(["first", "next"], {embedded: true});
  expect(colors.first).toBe("#3B82F6");
  expect(new Set(Object.values(colors)).size).toBe(2);
  expect(buildColorMap(["only"]).only).toBe("#A8769B");
});

it("measures the lower edge of the second visible chart row across groups", async () => {
  const {twoRowHeight} = await import("./lib/embedLayout.js");
  const root = document.createElement("main");
  root.innerHTML = '<div class="plot-grid"><i></i><i></i></div><div class="plot-grid"><i></i><i></i><i></i></div>';
  const rects = [[100, 300], [100, 315], [0, 0], [355, 581.5], [601, 780]];
  [...root.querySelectorAll("i")].forEach((el, index) => {
    const [top, bottom] = rects[index];
    el.getBoundingClientRect = () => ({top, bottom});
  });
  expect(twoRowHeight(root)).toBe(582);
});

it("sends changed layout heights to the parent and disconnects on unmount", async () => {
  const {reportLayout} = await import("./lib/embedLayout.js");
  const parent = {postMessage: vi.fn()};
  const disconnect = vi.fn();
  let resize;
  vi.stubGlobal("parent", parent);
  vi.stubGlobal("ResizeObserver", class {
    constructor(callback) { resize = callback; }
    observe() {}
    disconnect = disconnect;
  });
  const root = document.createElement("main");
  root.getBoundingClientRect = () => ({top: 0, height: 320});
  const action = reportLayout(root);
  resize();
  expect(parent.postMessage).toHaveBeenCalledTimes(1);
  root.getBoundingClientRect = () => ({top: 0, height: 540});
  resize();
  expect(parent.postMessage).toHaveBeenLastCalledWith({type: "envloop.dashboard.layout", height: 540}, location.origin);
  action.destroy();
  expect(disconnect).toHaveBeenCalledTimes(2);
});
