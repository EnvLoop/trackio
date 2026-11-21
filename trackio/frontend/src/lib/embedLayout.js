const cards = ".plot-grid > *";

export function twoRowHeight(root) {
  const bounds = root.getBoundingClientRect();
  let previousTop;
  let rows = 0;
  let edge = 0;
  for (const card of root.querySelectorAll(cards)) {
    const {top, bottom} = card.getBoundingClientRect();
    if (bottom <= top) continue;
    if (top !== previousTop) {
      previousTop = top;
      if (++rows > 2) break;
    }
    edge = Math.max(edge, bottom - bounds.top);
  }
  return Math.ceil(rows ? edge : bounds.height);
}

export function reportLayout(root) {
  if (window.parent === window || !window.ResizeObserver) return;
  let previousHeight;
  function report() {
    const measured = twoRowHeight(root);
    if (!Number.isFinite(measured) || measured <= 0) return;
    const height = Math.max(200, Math.min(4000, measured));
    if (height === previousHeight) return;
    previousHeight = height;
    window.parent.postMessage({type: "envloop.dashboard.layout", height}, window.location.origin);
  }

  const sizes = new ResizeObserver(report);
  function observeCards() {
    sizes.disconnect();
    sizes.observe(root);
    for (const card of root.querySelectorAll(cards)) sizes.observe(card);
    report();
  }

  const changes = new MutationObserver(records => {
    if (records.some(record => record.type === "attributes" || !record.target.closest?.(cards))) observeCards();
  });
  changes.observe(root, {childList: true, subtree: true, attributes: true, attributeFilter: ["open"]});
  observeCards();
  return {destroy() { changes.disconnect(); sizes.disconnect(); }};
}
