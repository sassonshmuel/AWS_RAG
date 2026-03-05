// static/app.js (UPDATED: KB docs refresh only after ingestion completes)

async function apiGet(url) {
  const res = await fetch(url, { credentials: "same-origin" });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

async function apiPostJson(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    credentials: "same-origin",
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

function bytesToHuman(n) {
  if (n === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setStatusUI(st) {
  const badge = document.getElementById("ingestionBadge");
  const dot = document.getElementById("statusDot");
  const text = document.getElementById("statusText");

  badge.className = "badge rounded-pill ";
  dot.className = "status-dot ";
  let label = st.status || "IDLE";

  if (label === "IN_PROGRESS") {
    badge.classList.add("text-bg-primary");
    dot.classList.add("inprogress");
    text.textContent = "In progress";
  } else if (label === "FAILED") {
    badge.classList.add("text-bg-danger");
    dot.classList.add("failed");
    text.textContent = "Failed";
  } else {
    badge.classList.add("text-bg-secondary");
    dot.classList.add("idle");
    text.textContent = "Idle";
  }
  badge.textContent = label;
}

async function loadFiles(force = false) {
  const tbody = document.getElementById("filesTbody");
  const meta = document.getElementById("filesMeta");

  const data = await apiGet(`/files${force ? "?force=1" : ""}`);
  const files = data.files || [];

  if (!files.length) {
    tbody.innerHTML = `<tr><td colspan="3" class="text-muted">No files found.</td></tr>`;
  } else {
    tbody.innerHTML = files
      .map(
        (f) => `
        <tr>
          <td class="text-truncate" title="${escapeHtml(f.key)}">${escapeHtml(f.key)}</td>
          <td class="text-end text-muted">${bytesToHuman(f.size || 0)}</td>
          <td class="text-end">
            <button class="btn btn-outline-danger btn-sm" data-delete-key="${escapeHtml(f.key)}">Delete</button>
          </td>
        </tr>`
      )
      .join("");
  }

  const ts = data.last_refresh ? new Date(data.last_refresh * 1000).toLocaleString() : "—";
  meta.textContent = `Files: ${files.length} • Last refresh: ${ts}`;

  document.querySelectorAll("[data-delete-key]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const key = btn.getAttribute("data-delete-key");
      await apiPostJson("/delete", { key });
      await loadFiles(true);
    });
  });
}

function renderHistory(history) {
  const el = document.getElementById("chatHistory");
  el.innerHTML = "";

  for (const msg of history) {
    const div = document.createElement("div");
    div.className = "chat-bubble " + (msg.role === "user" ? "chat-user" : "chat-assistant");
    div.innerHTML = `<div class="small text-muted mb-1">${escapeHtml(msg.role)}</div>
                     <div>${escapeHtml(msg.text)}</div>`;
    el.appendChild(div);
  }

  el.scrollTop = el.scrollHeight;
}

async function loadHistory() {
  const data = await apiGet("/history");
  renderHistory(data.history || []);
}

/* ---------------------------
   KB Documents view
--------------------------- */

function kbStatusBadge(status) {
  const s = String(status || "—").toUpperCase();
  let cls = "text-bg-secondary";
  if (s === "INDEXED") cls = "text-bg-success";
  else if (s.includes("FAIL")) cls = "text-bg-danger";
  else if (s.includes("PROGRESS") || s === "PENDING" || s === "STARTING") cls = "text-bg-primary";
  else if (s.includes("DELETE")) cls = "text-bg-warning";
  return `<span class="badge ${cls}">${escapeHtml(s)}</span>`;
}

function formatIso(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString();
}

async function loadKbDocs() {
  const tbody = document.getElementById("kbDocsTbody");
  const meta = document.getElementById("kbDocsMeta");

  try {
    const data = await apiGet("/kb/documents?limit=200");
    const docs = data.documents || [];

    if (!docs.length) {
      tbody.innerHTML = `<tr><td colspan="3" class="text-muted">No documents returned.</td></tr>`;
    } else {
      tbody.innerHTML = docs
        .map((d) => {
          const idOrUri = d.uri || d.customId || "—";
          const reason = d.statusReason ? String(d.statusReason) : "";
          const titleAttr = reason ? `title="${escapeHtml(reason)}"` : "";
          return `
            <tr>
              <td class="text-truncate" title="${escapeHtml(idOrUri)}">${escapeHtml(idOrUri)}</td>
              <td ${titleAttr}>${kbStatusBadge(d.status)}</td>
              <td class="text-end text-muted">${escapeHtml(formatIso(d.updatedAt))}</td>
            </tr>`;
        })
        .join("");
    }

    meta.textContent = `Documents: ${docs.length}`;
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="3" class="text-danger">Failed to load KB documents: ${escapeHtml(String(e))}</td></tr>`;
    meta.textContent = "";
  }
}

/* ---------------------------
   Status tick with "refresh KB docs only after ingestion completes"
--------------------------- */

// Remember the last time we refreshed KB docs due to an ingestion completion.
let lastKbDocsRefreshMarker = 0;

function isSuccessfulCompletionStatus(s) {
  const v = String(s || "").toUpperCase();
  return v === "COMPLETE" || v === "SUCCEEDED" || v === "SUCCESS";
}

async function tickStatus() {
  try {
    const st = await apiGet("/health");
    setStatusUI(st);

    // Refresh KB documents ONLY when:
    // - status is IDLE
    // - last_job_status indicates success
    // - last_event_time advanced since last refresh
    const marker = Number(st.last_event_time || 0);
    if (
      st.status === "IDLE" &&
      isSuccessfulCompletionStatus(st.last_job_status) &&
      marker > 0 &&
      marker > lastKbDocsRefreshMarker
    ) {
      lastKbDocsRefreshMarker = marker;
      await loadKbDocs();
    }
  } catch (e) {
    // ignore
  }
}

/* ---------------------------
   Wire up UI
--------------------------- */

document.addEventListener("DOMContentLoaded", async () => {
  await Promise.allSettled([loadFiles(false), loadHistory(), tickStatus(), loadKbDocs()]);

  document.getElementById("refreshFilesBtn").addEventListener("click", async () => {
    await loadFiles(true);
  });

  document.getElementById("refreshHistoryBtn").addEventListener("click", async () => {
    await loadHistory();
  });

  const kbBtn = document.getElementById("refreshKbDocsBtn");
  if (kbBtn) {
    kbBtn.addEventListener("click", async () => {
      await loadKbDocs();
    });
  }

  document.getElementById("uploadForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const err = document.getElementById("uploadError");
    err.classList.add("d-none");

    const input = document.getElementById("uploadFile");
    if (!input.files || !input.files[0]) return;

    const fd = new FormData();
    fd.append("file", input.files[0]);

    const res = await fetch("/upload", {
      method: "POST",
      body: fd,
      credentials: "same-origin",
    });

    if (!res.ok) {
      err.textContent = await res.text();
      err.classList.remove("d-none");
      return;
    }

    input.value = "";
    await loadFiles(true);

  });

  document.getElementById("chatForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("chatInput");
    const prompt = (input.value || "").trim();
    if (!prompt) return;

    const chatErr = document.getElementById("chatError");
    chatErr.classList.add("d-none");

    input.value = "";
    try {
      const data = await apiPostJson("/ask", { prompt });
      renderHistory(data.history || []);
    } catch (err) {
      chatErr.textContent = String(err);
      chatErr.classList.remove("d-none");
    }
  });

  setInterval(tickStatus, 3000);

  setInterval(() => loadFiles(false), 15000);

});
