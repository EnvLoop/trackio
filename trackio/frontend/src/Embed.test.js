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
