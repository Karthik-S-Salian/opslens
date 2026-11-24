const $ = (id) => document.getElementById(id);
const state = { token: localStorage.getItem("opslens.token") || "dev-token", incident: null, run: null, reader: null };

$("token").value = state.token;
$("token").addEventListener("change", () => {
  state.token = $("token").value.trim();
  localStorage.setItem("opslens.token", state.token);
  loadIncidents();
});

async function api(path, options = {}) {
  const res = await fetch(path, {
    ...options,
    headers: { "content-type": "application/json", Authorization: `Bearer ${state.token}`, ...(options.headers || {}) },
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.status === 204 ? null : res.json();
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function setConn(ok) {
  $("conn").className = "dot " + (ok ? "on" : "off");
}

// ---------------------------------------------------------------- incidents

async function loadIncidents() {
  try {
    const incidents = await api("/incidents");
    setConn(true);
    const list = $("incidents");
    list.innerHTML = "";
    for (const incident of incidents) {
      const item = el("li", state.incident?.id === incident.id ? "active" : "");
      item.append(el("span", "", incident.title), el("span", "sev", incident.severity));
      item.onclick = () => selectIncident(incident);
      list.append(item);
    }
  } catch (error) {
    setConn(false);
    console.error(error);
  }
}

$("new-incident").onclick = () => $("incident-form").classList.toggle("hidden");

$("incident-form").onsubmit = async (event) => {
  event.preventDefault();
  const incident = await api("/incidents", {
    method: "POST",
    body: JSON.stringify({
      title: $("inc-title").value,
      severity: $("inc-severity").value,
      services: $("inc-services").value.split(",").map((s) => s.trim()).filter(Boolean),
    }),
  });
  $("incident-form").classList.add("hidden");
  $("inc-title").value = "";
  $("inc-services").value = "";
  await loadIncidents();
  selectIncident(incident);
};

async function selectIncident(incident) {
  state.incident = incident;
  $("empty").classList.add("hidden");
  $("incident-panel").classList.remove("hidden");
  $("incident-title").textContent = incident.title;
  $("incident-meta").textContent =
    `${incident.severity} · ${(incident.services || []).join(", ") || "no services"}`;
  $("timeline").innerHTML = "";
  await loadIncidents();
  try {
    const timeline = await api(`/incidents/${incident.id}/timeline`);
    for (const message of timeline.messages) addTurn(message.role, message.content);
    const last = timeline.runs[timeline.runs.length - 1];
    if (last && !["answered", "cancelled", "failed", "budget_exceeded"].includes(last.status)) {
      attach(last.id);
    }
  } catch (error) {
    console.error(error);
  }
}

// ---------------------------------------------------------------- rendering

function addTurn(role, text) {
  const turn = el("div", "turn");
  turn.append(el("div", "who", role === "assistant" ? "agent" : role));
  const bubble = el("div", `bubble ${role === "assistant" ? "answer" : role}`);
  bubble.textContent = text;
  turn.append(bubble);
  $("timeline").append(turn);
  return bubble;
}

function streamBubble() {
  let bubble = $("timeline").querySelector(".bubble.streaming");
  if (!bubble) {
    const turn = el("div", "turn");
    turn.append(el("div", "who", "agent"));
    bubble = el("div", "bubble answer streaming");
    turn.append(bubble);
    $("timeline").append(turn);
  }
  return bubble;
}

function addToolCard(name, args) {
  const card = el("div", "tool");
  card.append(el("strong", "", name));
  const compact = Object.entries(args || {}).map(([k, v]) => `${k}=${v}`).join(" ");
  if (compact) card.append(el("span", "args", " " + compact));
  card.append(el("div", "", "running…"));
  $("timeline").append(card);
  return card;
}

function renderCitations(bubble, citations) {
  if (!citations || !citations.length) return;
  const row = el("div", "row");
  for (const id of citations) {
    const chip = el("span", id === "unknown" ? "chip unknown" : "chip", id.slice(0, 8));
    chip.title = id;
    chip.onclick = () => showEvidence(id);
    row.append(chip);
  }
  bubble.append(row);
}

async function showEvidence(id) {
  if (id === "unknown") return;
  try {
    const evidence = await api(`/evidence/${id}`);
    $("evidence").classList.remove("hidden");
    document.querySelector("main").classList.add("with-evidence");
    $("evidence-body").textContent =
      `kind: ${evidence.kind}\nref: ${JSON.stringify(evidence.ref, null, 2)}\n\n${evidence.excerpt || ""}`;
  } catch (error) {
    console.error(error);
  }
}

$("close-evidence").onclick = () => {
  $("evidence").classList.add("hidden");
  document.querySelector("main").classList.remove("with-evidence");
};

// ---------------------------------------------------------------- run + SSE

$("run-form").onsubmit = async (event) => {
  event.preventDefault();
  if (!state.incident) return;
  const question = $("question").value.trim() || "Diagnose this incident.";
  addTurn("user", question);
  $("question").value = "";
  const { run_id: runId } = await api(`/incidents/${state.incident.id}/runs`, {
    method: "POST",
    body: JSON.stringify({ question }),
  });
  attach(runId);
};

$("stop-btn").onclick = async () => {
  if (state.run) await api(`/runs/${state.run}/cancel`, { method: "POST" });
};

$("steer-btn").onclick = async () => {
  const text = $("steer").value.trim();
  if (!state.run || !text) return;
  await api(`/runs/${state.run}/steer`, { method: "POST", body: JSON.stringify({ text, interrupt: true }) });
  $("steer").value = "";
};

function setRunning(running) {
  $("run-btn").classList.toggle("hidden", running);
  $("stop-btn").classList.toggle("hidden", !running);
  $("steer").classList.toggle("hidden", !running);
  $("steer-btn").classList.toggle("hidden", !running);
}

async function attach(runId) {
  state.run = runId;
  setRunning(true);
  const res = await fetch(`/runs/${runId}/events`, {
    headers: { Authorization: `Bearer ${state.token}` },
  });
  if (!res.ok || !res.body) {
    setRunning(false);
    addTurn("assistant", `stream failed: ${res.status}`);
    return;
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let index;
    while ((index = buffer.indexOf("\n\n")) >= 0) {
      const frame = buffer.slice(0, index);
      buffer = buffer.slice(index + 2);
      handleFrame(frame);
    }
  }
  setRunning(false);
  state.run = null;
  document.querySelectorAll(".bubble.streaming").forEach((node) => node.classList.remove("streaming"));
}

function handleFrame(frame) {
  if (!frame || frame.startsWith(":")) return;
  let type = "message";
  let data = "";
  for (const line of frame.split("\n")) {
    if (line.startsWith("event:")) type = line.slice(6).trim();
    if (line.startsWith("data:")) data += line.slice(5).trim();
  }
  let payload = {};
  try { payload = JSON.parse(data || "{}"); } catch { /* ignore */ }

  if (type === "token") {
    const bubble = streamBubble();
    bubble.textContent += payload.text || "";
    bubble.scrollIntoView({ block: "end" });
  } else if (type === "message") {
    const bubble = $("timeline").querySelector(".bubble.streaming");
    if (bubble) {
      bubble.classList.remove("streaming");
      bubble.textContent = payload.text || bubble.textContent;
      renderCitations(bubble, payload.citations);
    } else {
      const fresh = addTurn("assistant", payload.text || "");
      renderCitations(fresh, payload.citations);
    }
  } else if (type === "tool_start") {
    addToolCard(payload.tool, payload.args);
  } else if (type === "tool_end") {
    const cards = document.querySelectorAll(".tool");
    const card = cards[cards.length - 1];
    if (card) {
      card.lastElementChild.textContent = payload.summary || "done";
      if (payload.evidence_id) {
        const chip = el("span", "chip", payload.evidence_id.slice(0, 8));
        chip.title = payload.evidence_id;
        chip.onclick = () => showEvidence(payload.evidence_id);
        card.append(chip);
      }
    }
  } else if (type === "steer_ack") {
    const line = el("div", "status-line", `↳ steer accepted: ${payload.text}`);
    $("timeline").append(line);
  } else if (type === "done") {
    const line = el("div", "status-line", `run ${payload.status}`);
    $("timeline").append(line);
  }
}

loadIncidents();
