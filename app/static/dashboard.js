/* Workflow Orchestration Engine — dashboard.
 *
 * No build step, no framework: fetch the JSON API, render it, and for a run
 * in progress keep a WebSocket open to /ws/runs/{id} for live updates. Falls
 * back to polling GET /runs/{id} if the socket cannot be opened.
 */

const RUN_STATUS_LABEL = {
  pending: "pending",
  running: "running",
  completed: "completed",
  failed: "failed",
  cancelled: "cancelled",
};

const TASK_STATUS_LABEL = {
  pending: "pending",
  queued: "queued",
  running: "running",
  success: "success",
  failed: "failed",
  cancelled: "cancelled",
};

function esc(value) {
  const div = document.createElement("div");
  div.textContent = value == null ? "" : String(value);
  return div.innerHTML;
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function shortId(id) {
  return id ? id.slice(0, 8) : "";
}

function badge(kind, status) {
  const label = kind === "run" ? (RUN_STATUS_LABEL[status] || status) : (TASK_STATUS_LABEL[status] || status);
  return `<span class="badge badge-${status}"><span class="dot"></span>${esc(label)}</span>`;
}

async function getJSON(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    let detail = res.statusText;
    try { detail = (await res.json()).detail || detail; } catch (_e) { /* not json */ }
    throw new Error(detail);
  }
  return res.json();
}

// --- list view --------------------------------------------------------------

async function renderListView() {
  const [workflows, runs] = await Promise.all([
    getJSON("/workflows"),
    getJSON("/runs"),
  ]);

  const workflowById = new Map(workflows.map((w) => [w.id, w]));

  const wBody = document.getElementById("workflows-body");
  wBody.innerHTML = workflows.map((w) => `
    <tr>
      <td>${esc(w.name)}</td>
      <td>${w.schedule ? `<span class="mono">${esc(w.schedule)}</span>` : '<span class="muted">manual only</span>'}</td>
      <td>${fmtTime(w.created_at)}</td>
      <td class="mono">${shortId(w.id)}</td>
      <td><button data-trigger="${w.id}">Trigger</button></td>
    </tr>
  `).join("");
  document.getElementById("workflows-empty").hidden = workflows.length > 0;

  wBody.querySelectorAll("[data-trigger]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      try {
        const run = await getJSON(`/workflows/${btn.dataset.trigger}/trigger`, { method: "POST" });
        window.location.href = `/dashboard?run=${run.id}`;
      } catch (err) {
        alert(`could not trigger workflow: ${err.message}`);
        btn.disabled = false;
      }
    });
  });

  const rBody = document.getElementById("runs-body");
  rBody.innerHTML = runs.map((r) => {
    const wf = workflowById.get(r.workflow_id);
    return `
      <tr class="clickable" data-run="${r.id}">
        <td>${badge("run", r.status)}</td>
        <td>${wf ? esc(wf.name) : `<span class="mono">${shortId(r.workflow_id)}</span>`}</td>
        <td>${fmtTime(r.triggered_at)}</td>
        <td>${fmtTime(r.completed_at)}</td>
        <td class="mono">${shortId(r.id)}</td>
      </tr>
    `;
  }).join("");
  document.getElementById("runs-empty").hidden = runs.length > 0;

  rBody.querySelectorAll("[data-run]").forEach((row) => {
    row.addEventListener("click", () => {
      window.location.href = `/dashboard?run=${row.dataset.run}`;
    });
  });
}

// --- DAG rendering ------------------------------------------------------------

function renderGraph(svg, levels, edges, statusByName) {
  const colWidth = 190;
  const rowHeight = 56;
  const nodeW = 132;
  const nodeH = 34;
  const marginX = 20;
  const marginY = 20;

  const pos = {};
  levels.forEach((level, colIndex) => {
    level.forEach((name, rowIndex) => {
      pos[name] = {
        x: marginX + colIndex * colWidth,
        y: marginY + rowIndex * rowHeight,
      };
    });
  });

  const maxRows = Math.max(1, ...levels.map((l) => l.length));
  const width = marginX * 2 + Math.max(1, levels.length) * colWidth - (colWidth - nodeW);
  const height = marginY * 2 + maxRows * rowHeight - (rowHeight - nodeH);

  svg.setAttribute("width", Math.max(width, 200));
  svg.setAttribute("height", Math.max(height, 80));
  svg.setAttribute("viewBox", `0 0 ${Math.max(width, 200)} ${Math.max(height, 80)}`);

  const parts = [];

  edges.forEach(([from, to]) => {
    const a = pos[from];
    const b = pos[to];
    if (!a || !b) return;
    const x1 = a.x + nodeW, y1 = a.y + nodeH / 2;
    const x2 = b.x, y2 = b.y + nodeH / 2;
    const midX = (x1 + x2) / 2;
    parts.push(
      `<path class="edge-line" d="M${x1},${y1} C${midX},${y1} ${midX},${y2} ${x2},${y2}" />`
    );
  });

  Object.entries(pos).forEach(([name, p]) => {
    const status = statusByName[name] || "pending";
    parts.push(`
      <g>
        <rect class="node-box status-${status}" x="${p.x}" y="${p.y}" width="${nodeW}" height="${nodeH}" rx="7"></rect>
        <text class="node-label" x="${p.x + nodeW / 2}" y="${p.y + nodeH / 2 + 4}" text-anchor="middle">${esc(name)}</text>
      </g>
    `);
  });

  svg.innerHTML = parts.join("");
}

// --- detail view --------------------------------------------------------------

let currentSocket = null;
let pollTimer = null;

function stopLiveUpdates() {
  if (currentSocket) { currentSocket.close(); currentSocket = null; }
  if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
}

function setLive(connected, label) {
  document.getElementById("live-dot").classList.toggle("off", !connected);
  document.getElementById("live-label").textContent = label;
}

let graphCache = null; // { levels, edges } for the run's workflow

function applyRunDetail(detail) {
  document.getElementById("run-id-short").textContent = shortId(detail.id);
  document.getElementById("run-workflow-name").textContent = shortId(detail.workflow_id);
  document.getElementById("run-triggered").textContent = fmtTime(detail.triggered_at);
  document.getElementById("run-completed").textContent = fmtTime(detail.completed_at);
  document.getElementById("run-status-badge").innerHTML = badge("run", detail.status);

  const terminal = ["completed", "failed", "cancelled"].includes(detail.status);
  document.getElementById("cancel-btn").disabled = terminal;

  const statusByName = {};
  detail.tasks.forEach((t) => { statusByName[t.task_name] = t.status; });

  if (graphCache) {
    renderGraph(document.getElementById("graph-svg"), graphCache.levels, graphCache.edges, statusByName);
  }

  const body = document.getElementById("tasks-body");
  body.innerHTML = detail.tasks.map((t) => `
    <tr>
      <td>${esc(t.task_name)}</td>
      <td>${badge("task", t.status)}</td>
      <td>${t.retry_count}/${t.max_retries}</td>
      <td class="mono">${t.worker_id ? esc(t.worker_id) : "—"}</td>
      <td>${fmtTime(t.started_at)}</td>
      <td>${fmtTime(t.completed_at)}</td>
      <td class="error-cell">${t.error_message ? esc(t.error_message) : ""}</td>
      <td><button data-logs="${esc(t.task_name)}">logs</button></td>
    </tr>
  `).join("");

  body.querySelectorAll("[data-logs]").forEach((btn) => {
    btn.addEventListener("click", () => openLogs(detail.id, btn.dataset.logs));
  });

  return detail;
}

async function openLogs(runId, taskName) {
  const modal = document.getElementById("logs-modal");
  document.getElementById("logs-title").textContent = `logs — ${taskName}`;
  document.getElementById("logs-body").textContent = "loading…";
  modal.hidden = false;
  try {
    const data = await getJSON(`/runs/${runId}/tasks/${encodeURIComponent(taskName)}/logs`);
    document.getElementById("logs-body").textContent = data.logs || "(no output captured)";
  } catch (err) {
    document.getElementById("logs-body").textContent = `error loading logs: ${err.message}`;
  }
}

function pollOnce(runId) {
  getJSON(`/runs/${runId}`)
    .then((detail) => {
      const d = applyRunDetail(detail);
      if (!["completed", "failed", "cancelled"].includes(d.status)) {
        pollTimer = setTimeout(() => pollOnce(runId), 1500);
      } else {
        setLive(false, "finished");
      }
    })
    .catch((err) => setLive(false, `error: ${err.message}`));
}

function connectSocket(runId) {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${proto}://${window.location.host}/ws/runs/${runId}`);
  currentSocket = socket;

  socket.addEventListener("open", () => setLive(true, "live"));
  socket.addEventListener("message", (event) => {
    let detail;
    try { detail = JSON.parse(event.data); } catch (_e) { return; }
    if (detail.error) { setLive(false, detail.error); return; }
    applyRunDetail(detail);
    if (["completed", "failed", "cancelled"].includes(detail.status)) {
      setLive(false, "finished");
    }
  });
  socket.addEventListener("close", () => {
    if (currentSocket === socket) {
      currentSocket = null;
      // The socket closes itself once the run is terminal — that is not a
      // failure to fall back from.
    }
  });
  socket.addEventListener("error", () => {
    socket.close();
    setLive(false, "reconnecting via polling…");
    pollOnce(runId);
  });
}

async function renderDetailView(runId) {
  document.getElementById("list-view").hidden = true;
  document.getElementById("detail-view").hidden = false;
  setLive(false, "connecting…");

  document.getElementById("cancel-btn").onclick = async () => {
    if (!confirm("Cancel this run? Tasks already in flight will still finish.")) return;
    try {
      await getJSON(`/runs/${runId}/cancel`, { method: "POST" });
    } catch (err) {
      alert(`could not cancel: ${err.message}`);
    }
  };

  try {
    const initial = await getJSON(`/runs/${runId}`);
    const graph = await getJSON(`/workflows/${initial.workflow_id}/graph`);
    graphCache = graph;
    const wf = await getJSON(`/workflows/${initial.workflow_id}`).catch(() => null);
    if (wf) document.getElementById("run-workflow-name").textContent = wf.name;
    applyRunDetail(initial);
  } catch (err) {
    setLive(false, `error: ${err.message}`);
    return;
  }

  if ("WebSocket" in window) {
    connectSocket(runId);
  } else {
    pollOnce(runId);
  }
}

// --- boot ----------------------------------------------------------------------

function boot() {
  document.getElementById("logs-close").addEventListener("click", () => {
    document.getElementById("logs-modal").hidden = true;
  });
  document.getElementById("logs-modal").addEventListener("click", (e) => {
    if (e.target.id === "logs-modal") e.target.hidden = true;
  });

  const params = new URLSearchParams(window.location.search);
  const runId = params.get("run");

  if (runId) {
    renderDetailView(runId);
  } else {
    document.getElementById("refresh-btn").addEventListener("click", () => renderListView());
    renderListView();
  }

  window.addEventListener("beforeunload", stopLiveUpdates);
}

boot();
