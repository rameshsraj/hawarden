/* hawarden console SPA — no build step. Live data rides one SSE channel fed
   by the shared `hawarden monitor` subprocess on the gateway. */
"use strict";

const S = {
  csrf: "", operator: "", snap: null, screen: "overview",
  connected: false, lastEvent: null, opLog: [], monState: null,
};

const $ = (s) => document.querySelector(s);
const el = (tag, cls, text) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
};
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const dash = (v) => (v === undefined || v === null || v === "" || v < 0 ? "—" : v);
const bytes = (n) => {
  if (n === undefined || n === null || n < 0) return "—";
  const u = ["B","KiB","MiB","GiB","TiB"]; let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(n >= 100 || i === 0 ? 0 : 1) + " " + u[i];
};
const fmtDur = (ns) => {
  if (typeof ns !== "number") return String(ns);
  const s = ns / 1e9;
  if (s >= 60) return (s / 60) + "m";
  return (Number.isInteger(s) ? s : s.toFixed(2)) + "s";
};
const ago = (ts) => {
  const d = (Date.now() - new Date(ts).getTime()) / 1000;
  if (d < 2) return "just now";
  if (d < 60) return Math.floor(d) + "s ago";
  if (d < 3600) return Math.floor(d / 60) + "m ago";
  return Math.floor(d / 3600) + "h ago";
};

async function api(path, opts = {}) {
  const r = await fetch(path, {
    credentials: "same-origin",
    headers: { "X-CSRF": S.csrf, "Content-Type": "application/json" },
    ...opts,
  });
  if (r.status === 401 && path !== "/api/login") { showLogin(); throw new Error("unauthenticated"); }
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || ("HTTP " + r.status));
  return body;
}

/* ---------- auth ---------- */
function showLogin() {
  $("#login").classList.remove("hidden");
  $("#shell").classList.add("hidden");
}
function showShell() {
  $("#login").classList.add("hidden");
  $("#shell").classList.remove("hidden");
  connectStream();
  render();
}

$("#login-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  $("#login-err").textContent = "";
  try {
    const r = await api("/api/login", {
      method: "POST",
      body: JSON.stringify({ token: $("#login-token").value, operator: $("#login-operator").value }),
    });
    S.csrf = r.csrf; S.operator = r.operator;
    showShell();
  } catch (e) { $("#login-err").textContent = e.message; }
});
$("#logout-btn").addEventListener("click", async () => {
  await api("/api/logout", { method: "POST", body: "{}" }).catch(() => {});
  location.reload();
});
$("#theme-btn").addEventListener("click", () => {
  const cur = document.documentElement.getAttribute("data-theme");
  const next = cur === "light" ? "" : "light";
  if (next) document.documentElement.setAttribute("data-theme", next);
  else document.documentElement.removeAttribute("data-theme");
  try { localStorage.setItem("hw-theme", next); } catch (_) {}
});
try {
  if (localStorage.getItem("hw-theme") === "light")
    document.documentElement.setAttribute("data-theme", "light");
} catch (_) {}

/* ---------- SSE ---------- */
let es = null;
function connectStream() {
  if (es) es.close();
  es = new EventSource("/api/stream");
  es.addEventListener("snapshot", (e) => {
    const ev = JSON.parse(e.data);
    S.snap = ev.data; S.connected = true; S.lastEvent = Date.now();
    // the configuration screen does not use live data; rebuilding it every
    // snapshot would wipe wizard input
    if (S.screen === "configuration") renderHeader(); else render();
  });
  es.addEventListener("collect_error", (e) => {
    const ev = JSON.parse(e.data);
    S.lastEvent = Date.now();
    if (S.snap) S.snap._collectErr = ev.err;
    renderHeader();
  });
  es.addEventListener("monitor", (e) => {
    S.monState = JSON.parse(e.data); S.connected = false;
    renderHeader();
    if (S.screen === "operations") render();
  });
  es.addEventListener("op", (e) => {
    const ev = JSON.parse(e.data);
    S.opLog.unshift(ev.data);
    if (S.opLog.length > 100) S.opLog.pop();
    if (S.screen === "operations" || S.screen === "configuration") render();
  });
  es.onerror = () => { S.connected = false; renderHeader(); };
}

/* ---------- header ---------- */
function renderHeader() {
  const s = S.snap;
  $("#hdr-cluster").textContent = s ? s.cluster : "hawarden";
  const live = $("#hdr-live");
  if (S.connected && s) {
    const stale = (Date.now() - new Date(s.taken).getTime()) > 10000;
    live.textContent = stale ? "STALE" : "LIVE";
    live.className = "pill " + (stale ? "stale" : "live");
  } else {
    live.textContent = S.monState ? "MONITOR DOWN" : "connecting…";
    live.className = "pill stale";
  }
  $("#hdr-age").textContent = s ? "sample " + ago(s.taken) : "";
  $("#hdr-cycle").textContent = s ? "cycle " + s.duration_ms + "ms" : "";
  const d = $("#hdr-degraded");
  if (s && s.degraded) { d.classList.remove("hidden"); d.title = s.degraded_reason || ""; }
  else d.classList.add("hidden");
}

/* ---------- navigation ---------- */
document.querySelectorAll(".nav-item").forEach(a => {
  a.addEventListener("click", () => {
    S.screen = a.dataset.screen;
    document.querySelectorAll(".nav-item").forEach(x => x.classList.toggle("active", x === a));
    render();
  });
});

/* ---------- render root ---------- */
function render() {
  S.renderSeq = (S.renderSeq || 0) + 1;
  renderHeader();
  const fn = {
    overview: renderOverview, topology: renderTopology, patroni: renderPatroni,
    etcd: renderEtcd, vip: renderVip, events: renderEvents,
    operations: renderOperations, configuration: renderConfiguration,
  }[S.screen] || renderOverview;
  fn($("#main"));
}

function card(title, span, inner) {
  const c = el("div", "card " + span);
  c.append(el("h3", "", title));
  if (typeof inner === "string") c.innerHTML += inner; else if (inner) c.append(inner);
  return c;
}
function tbl(cols, rows) {
  const t = el("table");
  t.innerHTML = "<thead><tr>" + cols.map(c => `<th>${esc(c)}</th>`).join("") + "</tr></thead>" +
    "<tbody>" + rows.map(r => "<tr>" + r.map(c => `<td>${c}</td>`).join("") + "</tr>").join("") + "</tbody>";
  return t;
}
const chip = (txt, cls) => `<span class="chip ${cls}">${esc(txt)}</span>`;
const sevChip = (sev) => chip(sev, sev === "CRIT" ? "crit" : sev === "WARN" ? "warn" : "info");
const okChip = (ok) => chip(ok ? "ok" : "down", ok ? "ok" : "crit");

/* ---------- overview ---------- */
function renderOverview(m) {
  m.innerHTML = "";
  const s = S.snap;
  if (!s) { m.append(card("live command centre", "c12", "<p class='muted'>waiting for first snapshot…</p>")); return; }
  const grid = el("div", "grid");

  const tiles = el("div", "tiles");
  const patroniOK = s.patroni && s.patroni.available;
  const nCrit = (s.findings || []).filter(f => f.severity === "CRIT").length;
  const nWarn = (s.findings || []).filter(f => f.severity === "WARN").length;
  const vipTxt = (s.vip || []).map(v => v.observed ? v.address + "→" + v.holders.join(",") :
    v.absent ? v.address + " ABSENT" : v.address + " ?").join("  ") || "—";
  const t = (lbl, val, cls) => `<div class="tile ${cls}"><div class="lbl">${esc(lbl)}</div><div class="val">${esc(val)}</div></div>`;
  tiles.innerHTML =
    t("HA health", s.degraded ? "degraded" : "ok", s.degraded ? "warn" : "ok") +
    t("Patroni leader", patroniOK ? (s.patroni.leader || "none") : "unavailable", patroniOK ? (s.patroni.leader ? "ok" : "crit") : "crit") +
    t("DCS", s.etcd.quorum_known ? (s.etcd.quorum_lost ? "quorum lost" : "quorum ok") : "unknown",
      s.etcd.quorum_known ? (s.etcd.quorum_lost ? "crit" : "ok") : "unknown") +
    t("VIP", vipTxt, (s.vip || []).some(v => v.absent) ? "crit" : "ok") +
    t("Findings", nCrit + " crit / " + nWarn + " warn", nCrit ? "crit" : nWarn ? "warn" : "ok") +
    t("Paused", s.patroni && s.patroni.paused ? "YES — failover off" : "no",
      s.patroni && s.patroni.paused ? "warn" : "ok");
  grid.append(card("summary", "c12", tiles));

  grid.append(card("patroni members", "c6", patroniTable(s)));
  grid.append(card("etcd coordination", "c6", etcdTable(s)));
  grid.append(card("findings", "c6", findingsList(s, 6)));
  grid.append(card("virtual addresses", "c6", vipTable(s)));
  if (s.proxy) grid.append(card("haproxy routing", "c12", proxyTable(s)));
  grid.append(card("source coverage", "c12", sourceTable(s)));
  m.append(grid);
}

function patroniTable(s) {
  if (!s.patroni || !s.patroni.available)
    return `<p class="muted">unavailable — ${esc(s.patroni ? s.patroni.err : "")}</p>`;
  return tbl(["name","host","role","state","tl","lag","restart","api"],
    (s.patroni.members || []).map(x => [
      esc(x.name), esc(x.host),
      chip(x.role || "?", x.role === "leader" ? "ok" : ""),
      esc(x.state || "?"), dash(x.timeline),
      x.lag_bytes < 0 ? "—" : bytes(x.lag_bytes),
      x.pending_restart ? chip("pending", "warn") : "—",
      okChip(x.api_reachable),
    ]));
}

function etcdTable(s) {
  if (!s.etcd || !(s.etcd.members || []).length)
    return `<p class="muted">unavailable — ${esc(s.etcd ? s.etcd.err : "")}</p>`;
  const q = s.etcd.quorum_known ? (s.etcd.quorum_lost ? chip("quorum lost", "crit") : chip("quorum ok", "ok")) : chip("quorum unknown", "warn");
  const div = el("div");
  div.innerHTML = `<p>version ${esc(s.etcd.version || "?")} · leader ${esc(s.etcd.leader_endpoint || "?")} · ${q}</p>`;
  div.append(tbl(["name","endpoint","healthy","role","db size","leader changes"],
    s.etcd.members.map(x => [
      esc(x.name || "—"), esc(x.endpoint), okChip(x.reachable && x.healthy),
      x.is_leader ? chip("leader","ok") : (x.has_leader ? "follower" : "—"),
      bytes(x.db_size_bytes), dash(x.leader_changes),
    ])));
  return div;
}

function vipTable(s) {
  const rows = [];
  (s.keepalived || []).forEach(n => rows.push([
    esc(n.host), n.reachable ? okChip(true) : chip("no ssh", ""), n.reachable ? okChip(n.process_up) : "—",
    chip(n.vrrp_state || "unknown", n.vrrp_state === "MASTER" ? "ok" : n.vrrp_state === "BACKUP" ? "info" : "warn"),
    n.vip_present || (s.vip || []).some(v => (v.holders || []).includes(n.host)) ? chip("holds VIP","ok") : "—", esc(n.vip_interface || "—"),
  ]));
  (s.vip || []).forEach(v => rows.push([
    "VIP " + esc(v.address) + " (" + esc(v.policy || "?") + ")", "", "", "",
    v.observed ? chip("held: " + v.holders.join(","), "ok") :
      v.absent ? chip("ABSENT", "crit") : chip("unknown", "warn"), esc(v.evidence || ""),
  ]));
  if (!rows.length) return "<p class='muted'>no keepalived hosts or VIPs configured</p>";
  return tbl(["host","reachable","process","vrrp","vip","iface / evidence"], rows);
}

function proxyTable(s) {
  const nodes = s.proxy.nodes || [];
  return tbl(["listener", "port", "patroni check", "routes to", "VIP"].concat(nodes.map(n => n.host)),
    (s.proxy.listeners || []).map(l => {
      const vo = Object.values(l.vip_open || {});
      return [
        esc(l.name) + " " + chip(l.write ? "write" : "read", l.write ? "ok" : "info"), l.port, `<code>GET ${esc(l.check)}</code>`,
        (l.eligible || []).length ? l.eligible.map(m => chip(m, "ok")).join(" ") : chip("none", l.write ? "crit" : "warn"),
        vo.length ? okChip(vo.every(Boolean)) : "—",
      ].concat(nodes.map(n => n.open[l.name] ? chip("open", "ok") : chip("closed", "crit")));
    }));
}

function findingsList(s, limit) {
  const f = (s.findings || []);
  if (!f.length) return "<p class='muted'>no findings</p>";
  const div = el("div");
  f.slice(0, limit || f.length).forEach(x => {
    const d = el("div", "finding " + x.severity);
    d.innerHTML = `<span class="code">${esc(x.code)}</span> ${sevChip(x.severity)} — ${esc(x.subject)}` +
      (x.evidence || []).map(e => `<div class="ev">evidence: ${esc(e)}</div>`).join("") +
      (x.next || []).map(n => `<div class="ev">next: <code>${esc(n)}</code></div>`).join("");
    div.append(d);
  });
  return div;
}

function sourceTable(s) {
  return tbl(["source","target","ok","ms","last ok","error"],
    (s.sources || []).map(x => [
      esc(x.source), esc(x.target), x.skipped ? chip("skipped", "") : okChip(x.ok), x.duration_ms,
      x.last_ok ? ago(x.last_ok) : "—", esc(x.err || "—"),
    ]));
}

/* ---------- topology graph ---------- */
/* Interactive cluster graph built from the live snapshot, poolwarden-style:
   clients → VIP → keepalived[MASTER] → pg[leader]; WAL arcs beneath the PG
   row; etcd raft ensemble in a coordination group; DCS-watch edges dotted.
   Relations are ownership/supervision — client-traffic edges are labelled,
   never implied as per-edge bandwidth. */

const topoState = { tx: 0, ty: 0, k: 1, sel: null, drag: null };

const EDGE_C = {
  client: "#58a6ff", read: "#39c5cf", wal: "#3fb950", walWarn: "#d29922", walCrit: "#f85149",
  vrrp: "#d29922", dcs: "#8b98a9", raft: "#a371f7", super: "#8b98a9",
};

function buildTopoGraph(s) {
  const nodes = [], edges = [], groups = [];
  const NW = 196, KROW_Y = 110, PGROW_Y = 420, COL = 300, X0 = 340;

  const members = (s.patroni && s.patroni.members) || [];
  const kalive = s.keepalived || [];
  const vips = s.vip || [];
  const etcM = (s.etcd && s.etcd.members) || [];
  const leader = (s.patroni && s.patroni.leader) || "";

  // edge tier row — keepalived hosts ∪ haproxy nodes, merged per host
  const proxy = s.proxy || null;
  const pNodes = (proxy && proxy.nodes) || [];
  const listeners = (proxy && proxy.listeners) || [];
  const tierHosts = [...new Set([...kalive.map(n => n.host), ...pNodes.map(n => n.host)])].sort();
  const holders = new Set(vips.flatMap(v => v.holders || []));
  const kx = new Map(tierHosts.map((h, i) => [h, X0 + i * COL]));
  tierHosts.forEach(h => {
    const n = kalive.find(k => k.host === h);
    const pn = pNodes.find(p => p.host === h);
    const isHolder = holders.has(h);
    const anyOpen = pn && listeners.some(l => pn.open[l.name]);
    const rows = [];
    if (n) {
      rows.push(["VRRP", n.reachable
        ? chip(n.vrrp_state, n.vrrp_state === "MASTER" ? "ok" : n.vrrp_state === "BACKUP" ? "info" : "warn")
        : isHolder ? chip("MASTER (inferred)", "ok") : chip("n/a · no ssh", "")]);
      if (n.reachable) rows.push(["PROC", chip(n.process_up ? "up" : "down", n.process_up ? "ok" : "crit")]);
    }
    rows.push(["VIP", isHolder ? chip("holds VIP", "ok") : n && n.reachable ? "<span class='muted'>—</span>" : "<span class='muted'>not holder</span>"]);
    if (pn) rows.push(["LISTEN", listeners.map(l => chip(":" + l.port + (pn.open[l.name] ? "" : " closed"), pn.open[l.name] ? "ok" : "crit")).join(" ")]);
    nodes.push({
      id: "ka-" + h, x: kx.get(h), y: KROW_Y, w: NW, h: 104,
      cls: isHolder ? "primary" : (n && n.reachable) || anyOpen ? "standby" : pn && !anyOpen ? "down" : "unknown",
      name: h + " · " + [pn ? "haproxy" : "", n ? "keepalived" : ""].filter(Boolean).join(" + "),
      rows,
      detail: n && n.reachable ? (n.transition_evidence || "") : pn ? "routing via Patroni health checks" : (n && n.err) || "",
      data: { kind: "keepalived", ref: n || { host: h }, proxy: pn },
    });
  });

  // VRRP advert arcs between keepalived hosts (above the row)
  for (let i = 0; i < kalive.length; i++)
    for (let j = i + 1; j < kalive.length; j++)
      edges.push({
        id: `e-vrrp-${i}-${j}`, from: ["ka-" + kalive[i].host, "t", i + j], to: ["ka-" + kalive[j].host, "t", i + j],
        color: EDGE_C.vrrp, dash: "6 5", label: "VRRP adverts (unicast)",
      });

  // clients + VIP nodes
  nodes.push({ id: "clients", x: 30, y: KROW_Y - 40, w: 160, h: 62, cls: "unknown", name: "clients", rows: [], detail: "application connections", data: { kind: "clients" } });
  vips.forEach((v, i) => {
    const vid = "vip-" + i;
    const ports = listeners.map(l => {
      const open = l.vip_open && l.vip_open[v.address];
      return open === undefined ? "" : chip(":" + l.port + (open ? "" : " closed"), open ? "ok" : "crit");
    }).join(" ");
    const rows = [["policy", `<span>${esc(v.policy || "leader_only")}</span>`]];
    if (ports) rows.push(["PORTS", ports]);
    nodes.push({
      id: vid, x: 30, y: KROW_Y + 150 + i * 130, w: 160, h: 80, cls: v.observed ? "vip" : "down",
      name: "VIP " + v.address, rows,
      detail: v.observed ? "held by " + v.holders.join(",") + (v.evidence ? " · " + v.evidence.split(":")[0] : "") : v.absent ? "ABSENT" : "holder unverified",
      data: { kind: "vip", ref: v },
    });
    edges.push({ id: "e-cli-" + i, from: ["clients", "b", i], to: [vid, "t", i], color: EDGE_C.client, width: 2, label: "app connects to VIP" });
    // VIP → edge tier: solid to the observed holder, dashed failover path to the rest
    const tierKa = tierHosts;
    tierKa.forEach((h, j) => {
      const held = (v.holders || []).includes(h);
      // first column: enter from the left; further columns: enter from below so
      // the curve runs in the gap between rows instead of behind other nodes
      const into = j === 0 ? ["ka-" + h, "l", 0] : ["ka-" + h, "b", 6];
      edges.push(held
        ? { id: `e-vip-${i}-${h}`, from: [vid, "r", 0], to: into, color: EDGE_C.client, width: 2.6, anim: true,
            label: "active" + (v.evidence ? " (" + v.evidence.split(":")[0] + ")" : "") }
        : { id: `e-vip-${i}-${h}`, from: [vid, "r", j + 1], to: ["ka-" + h, "b", 8], color: EDGE_C.super, width: 1.2, dash: "4 6",
            labelT: 0.8, label: v.observed ? "failover (VRRP backup)" : "holder unverified" });
    });
  });
  if (!vips.length && tierHosts.length) edges.push({ id: "e-cli-0", from: ["clients", "r", 0], to: ["ka-" + tierHosts[0], "l", 0], color: EDGE_C.client, dash: "5 5" });

  // pg row — order under same-named keepalived host first
  const names = members.map(x => x.name);
  const order = [
    ...tierHosts.filter(h => names.includes(h)),
    ...names.filter(n => !tierHosts.includes(n)),
  ];
  const pgX = new Map(order.map((n, i) => [n, X0 + i * COL]));
  members.forEach(x => {
    const be = (s.backends || []).find(b => b.host === x.name || b.host === x.host);
    const rows = [
      ["PATRONI", chip(x.role || "?", x.role === "leader" ? "ok" : "") + " " + esc(x.state || "?")],
      ["API", okChip(x.api_reachable)],
      ["TL/LAG", `<span class="muted">tl ${dash(x.timeline)} · ${x.lag_bytes < 0 ? "lag —" : "lag " + bytes(x.lag_bytes)}</span>`],
    ];
    if (listeners.length) rows.push(["ROUTES", listeners.filter(l => (l.eligible || []).includes(x.name))
      .map(l => chip((l.write ? "W" : "R") + " :" + l.port, l.write ? "ok" : "info")).join(" ") || "<span class='muted'>none</span>"]);
    if (be) rows.push(["PG",
      chip(be.in_recovery ? "replica" : "primary", be.in_recovery ? "info" : "ok") +
      (be.replay_lsn >= 0 ? ` <span class="muted">lsn ${be.replay_lsn.toString(16).toUpperCase()}</span>` : "")]);
    const xpos = pgX.get(x.name) !== undefined ? pgX.get(x.name) : X0;
    nodes.push({
      id: "pg-" + x.name, x: xpos, y: PGROW_Y, w: NW, h: 118,
      cls: x.role === "leader" ? "primary" : !x.api_reachable ? "down" : "standby",
      name: x.name + " · pg", rows, detail: x.api_url || x.host || "",
      data: { kind: "pg", ref: x, backend: be },
    });
  });

  if (listeners.length) {
    // haproxy → pg: each listener routes to the members its Patroni check admits.
    // Drawn from the VIP holder (the live path); from every proxy if unverified.
    const src = pNodes.map(p => p.host).filter(h => holders.has(h));
    const from = src.length ? src : pNodes.map(p => p.host);
    from.forEach((h, hi) => listeners.forEach((l, li) => (l.eligible || []).forEach(m => {
      if (!names.includes(m)) return;
      edges.push({
        id: `e-route-${h}-${l.name}-${m}`, from: ["ka-" + h, "b", li * 2 + 1], to: ["pg-" + m, "t", li * 2 + 1],
        color: l.write ? EDGE_C.client : EDGE_C.read, width: src.length ? 2.4 : 1.4, anim: !!src.length,
        dash: src.length ? undefined : "5 5", labelT: 0.75,
        label: hi === 0 ? `:${l.port} ${l.write ? "write" : "read"} · ${l.check}=200` : "",
      });
    })));
  } else {
    // keepalived on the PG hosts: holder → pg leader is the client landing point
    vips.forEach((v, i) => {
      (v.holders || []).forEach(hn => {
        if (leader && names.includes(leader))
          edges.push({
            id: `e-path-${i}`, from: ["ka-" + hn, "b", 0], to: ["pg-" + leader, "t", 0],
            color: hn === leader ? EDGE_C.client : EDGE_C.walCrit,
            width: 2, dash: hn === leader ? undefined : "6 4",
            label: hn === leader ? "VIP→leader" : "VIP→standby (policy violation)",
          });
      });
    });
  }

  // WAL arcs beneath the pg row: leader → replicas (animated when streaming)
  const leadNode = members.find(x => x.role === "leader");
  let walK = 0;
  members.forEach(x => {
    if (x.role !== "replica" || !leadNode) return;
    walK++;
    const sev = x.lag_bytes > 1 << 30 ? EDGE_C.walCrit : x.lag_bytes > 1 << 26 ? EDGE_C.walWarn : EDGE_C.wal;
    edges.push({
      id: `e-wal-${x.name}`, from: ["pg-" + leadNode.name, "b", walK], to: ["pg-" + x.name, "b", walK],
      color: sev, width: 2.4, anim: x.state === "streaming" || x.state === "running",
      label: `WAL ${x.state}${x.lag_bytes >= 0 ? " · " + bytes(x.lag_bytes) + " lag" : ""}`,
    });
  });

  // etcd ensemble. Members come from the cluster's own roster (MemberList),
  // so a member added to etcd appears here without any config change;
  // probed endpoints are matched to roster entries by member ID.
  const roster = (s.etcd && s.etcd.roster) || [];
  const links = (s.etcd && s.etcd.links) || [];
  const leaderId = (s.etcd && s.etcd.leader_id) || "";
  const ens = roster.map(r => ({ key: r.id, id: r.id, name: r.name, r, m: etcM.find(e => e.id === r.id) || null }));
  etcM.filter(e => !e.id || !roster.some(r => r.id === e.id))
    .forEach(e => ens.push({ key: e.id || e.endpoint, id: e.id || "", name: e.name || e.endpoint, r: null, m: e }));
  ens.sort((a, b) => a.name.localeCompare(b.name));
  const eName = (id) => (ens.find(x => x.id === id) || {}).name || id;
  if (ens.length) {
    const SP = 250, EW = 220, EH = 96;
    const gx = X0 - 30, gw = Math.max(ens.length * SP - (SP - EW), 300), gy = 720;
    const depth = 60 + (ens.length - 1) * SP * 0.15;
    const q = s.etcd.quorum_known ? (s.etcd.quorum_lost ? "LOST" : "ok") : "?";
    groups.push({ x: gx - 10, y: gy - 34, w: gw + 20, h: 34 + EH + depth * 0.75 + 40,
      label: `etcd raft ensemble · ${ens.length} members (from member list) · quorum ${q}` });
    ens.forEach((x, i) => {
      const m = x.m, isLeader = (leaderId && x.id === leaderId) || (m && m.is_leader);
      const rows = [];
      if (m && m.reachable) {
        rows.push(["RAFT", isLeader ? chip("leader", "ok") : chip(m.has_leader ? "follower" : "no leader", m.has_leader ? "info" : "warn")]);
        rows.push(["DB", `<span class="muted">${bytes(m.db_size_bytes)} · term ${dash(m.raft_term)} · idx ${dash(m.raft_index)}</span>`]);
      } else if (m) {
        rows.push(["RAFT", chip("unreachable", "crit")]);
      } else {
        rows.push(["RAFT", isLeader ? chip("leader", "ok") : chip("member", "info")]);
        rows.push(["SRC", chip("discovered · not in config", "warn")]);
      }
      rows.push(["ID", `<span class="muted">${esc(x.id || "?")}</span>`]);
      nodes.push({
        id: "etcd-" + x.key, x: gx + i * SP, y: gy, w: EW, h: EH,
        cls: m && !m.reachable ? "down" : isLeader ? "primary" : m ? "unknown" : "standby",
        name: x.name + (m ? " · " + m.endpoint : ""), rows,
        detail: (m && m.err) || (x.r ? "peer " + (x.r.peer_urls || []).join(",") : ""),
        data: { kind: "etcd", ref: m || { endpoint: (x.r.client_urls || []).join(",") }, roster: x.r, id: x.id },
      });
    });
    // raft streams: every member keeps a peer stream (:2380) to every other;
    // log replication flows leader → followers. Evidence is each sender's
    // etcd_network_active_peers / peer_sent_bytes_total.
    const link = (a, b) => links.find(l => l.from === a && l.to === b);
    const rate = (l) => l && l.bps >= 0 ? (l.bps >= 1024 ? (l.bps / 1024).toFixed(1) + " KiB/s" : l.bps.toFixed(0) + " B/s") : "";
    let k = 0;
    for (let i = 0; i < ens.length; i++)
      for (let j = i + 1; j < ens.length; j++) {
        const A = ens[i], B = ens[j];
        const lead = leaderId && (A.id === leaderId || B.id === leaderId);
        const [src, dst] = lead && B.id === leaderId ? [B, A] : [A, B];
        const fwd = link(src.id, dst.id), rev = link(dst.id, src.id);
        const seen = [fwd, rev].filter(Boolean);
        const down = seen.some(l => !l.active);
        const up = seen.length && !down;
        k++;
        if (lead) {
          edges.push({
            id: `e-raft-${src.key}-${dst.key}`, from: ["etcd-" + src.key, "b", k % 3 + 2], to: ["etcd-" + dst.key, "b", k % 3 + 2],
            color: down ? EDGE_C.walCrit : up ? EDGE_C.raft : EDGE_C.super, width: up ? 2.2 : 1.4,
            anim: !!up, dash: up ? undefined : "5 5",
            label: down ? "raft stream DOWN" : up ? `raft append${rate(fwd) ? " · " + rate(fwd) : ""}` : "raft (unverified)",
          });
        } else {
          edges.push({
            id: `e-peer-${A.key}-${B.key}`, from: ["etcd-" + A.key, "b", k % 3 + 2], to: ["etcd-" + B.key, "b", k % 3 + 2],
            color: down ? EDGE_C.walCrit : EDGE_C.super, width: 1, dash: "2 4",
            label: down ? "peer stream DOWN" : !leaderId ? "peer (no raft leader)" : up ? "peer stream" : "",
          });
        }
      }
    // Patroni → etcd: every member keeps its member key and watches the
    // leader key; the leader also renews the leader key + lease every
    // loop_wait. Patroni's client picks one endpoint, so the edge targets
    // the ensemble rather than a guessed member.
    order.forEach((n, oi) => {
      const node = "pg-" + n;
      if (!nodes.some(nd => nd.id === node)) return;
      const isL = n === leader;
      edges.push({
        id: "e-dcs-" + n, from: [node, "b", 5], to: [{ x: gx + gw * (oi + 1) / (order.length + 1), y: gy - 34 }, "t", 0],
        color: isL ? EDGE_C.client : EDGE_C.dcs, dash: "2 5", width: isL ? 1.6 : 1, labelT: 0.22,
        label: isL ? "leader key + lease renew" : "member key + watch",
      });
    });
  }

  return { nodes, edges, groups };
}

const nodeById = (g, id) => g.nodes.find(n => n.id === id);

function anchor(g, ref) {
  // ref = [nodeOrPoint, side, slot] — slot fans parallel edges apart
  const n = typeof ref[0] === "string" ? nodeById(g, ref[0]) : null;
  const p = n ? { x: n.x, y: n.y, w: n.w, h: n.h } : { x: ref[0].x, y: ref[0].y, w: 0, h: 0 };
  const slot = (ref[2] || 0) * 14 - 30;
  switch (ref[1]) {
    case "t": return { x: p.x + p.w / 2 + slot, y: p.y };
    case "b": return { x: p.x + p.w / 2 + slot, y: p.y + p.h };
    case "l": return { x: p.x, y: p.y + p.h / 2 + slot * 0.4 };
    case "r": return { x: p.x + p.w, y: p.y + p.h / 2 + slot * 0.4 };
  }
}

function edgeCtrl(a, b, fs, ts) {
  // each control point leaves/enters along its own anchor side; same-side
  // arcs (b→b, t→t) deepen with span so parallel arcs nest instead of overlap
  const d = fs === ts && (fs === "b" || fs === "t") ? 60 + Math.abs(b.x - a.x) * 0.15 : 60;
  const off = (p, s) => s === "b" ? [p.x, p.y + d] : s === "t" ? [p.x, p.y - d] : s === "l" ? [p.x - d, p.y] : [p.x + d, p.y];
  return [off(a, fs), off(b, ts)];
}

function edgePath(a, b, fs, ts) {
  const [c1, c2] = edgeCtrl(a, b, fs, ts);
  return `M${a.x} ${a.y} C${c1[0]} ${c1[1]},${c2[0]} ${c2[1]},${b.x} ${b.y}`;
}

// point on the drawn cubic at parameter t (labels sit on the curve itself)
function edgePoint(a, b, fs, ts, t) {
  const [c1, c2] = edgeCtrl(a, b, fs, ts);
  const u = 1 - t, w0 = u * u * u, w1 = 3 * u * u * t, w2 = 3 * u * t * t, w3 = t * t * t;
  return { x: w0 * a.x + w1 * c1[0] + w2 * c2[0] + w3 * b.x, y: w0 * a.y + w1 * c1[1] + w2 * c2[1] + w3 * b.y };
}

function renderTopoEdges(g, svgNS) {
  const parts = [];
  parts.push(`<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="context-stroke"/></marker></defs>`);
  g.edges.forEach(e => {
    const a = anchor(g, e.from), b = anchor(g, e.to);
    const d = edgePath(a, b, e.from[1], e.to[1]);
    const w = e.width || 1.4;
    parts.push(`<path d="${d}" fill="none" stroke="${e.color}" stroke-width="${w}"${e.dash ? ` stroke-dasharray="${e.dash}"` : ""}${e.anim ? ' class="edge-anim" stroke-dasharray="10 8"' : ""} marker-end="url(#arr)" pointer-events="stroke" style="pointer-events:visibleStroke"/>`);
    if (e.label) {
      const p = edgePoint(a, b, e.from[1], e.to[1], e.labelT || 0.5);
      const mx = p.x, my = p.y - 6;
      parts.push(`<text x="${mx}" y="${my}" text-anchor="middle" fill="${e.color}" font-size="10" font-family="ui-monospace,monospace" style="paint-order:stroke;stroke:var(--bg);stroke-width:4">${esc(e.label)}</text>`);
    }
  });
  return parts.join("");
}

function topoNodeHTML(n) {
  const rows = n.rows.map(r => `<div class="trow"><span class="tlbl">${r[0]}</span>${r.slice(1).join(" ")}</div>`).join("");
  return `<div class="tname"><span>${esc(n.name)}</span></div>${rows}${n.detail ? `<div class="tdetail" title="${esc(n.detail)}">${esc(n.detail)}</div>` : ""}`;
}

let topoEls = null;
function renderTopology(m) {
  m.innerHTML = "";
  const s = S.snap;
  const grid = el("div", "grid");
  const wrapCard = card("cluster topology & data flow", "c12");
  const wrap = el("div", "topo-wrap");
  wrap.innerHTML = `<div class="topo-zoom">
    <button data-z="in" title="zoom in">+</button><button data-z="out" title="zoom out">−</button>
    <button data-z="fit" title="fit">⤢</button></div>
    <div class="topo-view"><svg class="topo-edges" width="4000" height="1600"></svg><div class="topo-nodes"></div></div>`;
  const legend = el("div", "topo-legend");
  legend.innerHTML = `<span><b style="color:${EDGE_C.client}">━▶</b> client / write path (clients → VIP → holder → primary)</span>
    <span><b style="color:${EDGE_C.read}">━▶</b> read path (Patroni /replica check)</span>
    <span><b style="color:${EDGE_C.super}">┄▶</b> VIP failover target / unverified</span>
    <span><b style="color:${EDGE_C.wal}">━▶</b> WAL replication (animated = streaming)</span>
    <span><b style="color:${EDGE_C.raft}">━▶</b> etcd raft append leader → follower (:2380)</span>
    <span><b style="color:${EDGE_C.super}">┄┄</b> etcd follower peer stream</span>
    <span><b style="color:${EDGE_C.vrrp}">┄┄</b> VRRP adverts</span>
    <span><b style="color:${EDGE_C.dcs}">┄┄</b> DCS watch/lease</span>
    <span>click a node for evidence · drag to pan · wheel to zoom</span>`;
  wrapCard.append(wrap, legend);
  grid.append(wrapCard);
  m.append(grid);

  if (!s) { wrap.append(el("p", "muted", "waiting for snapshot…")); return; }
  const g = buildTopoGraph(s);
  const view = wrap.querySelector(".topo-view");
  const nodesDiv = wrap.querySelector(".topo-nodes");
  const svg = wrap.querySelector("svg");
  nodesDiv.innerHTML = "";
  g.groups.forEach(gr => {
    const d = el("div", "topo-group", gr.label);
    Object.assign(d.style, { left: gr.x + "px", top: gr.y + "px", width: gr.w + "px", height: gr.h + "px" });
    nodesDiv.append(d);
  });
  g.nodes.forEach(n => {
    const d = el("div", "topo-node " + n.cls + (topoState.sel === n.id ? " sel" : ""));
    d.dataset.nodeid = n.id;
    Object.assign(d.style, { left: n.x + "px", top: n.y + "px", width: n.w + "px" });
    d.innerHTML = topoNodeHTML(n);
    d.addEventListener("click", (ev) => { ev.stopPropagation(); topoState.sel = n.id; renderTopoDrawer(wrap, n); });
    nodesDiv.append(d);
    n.el = d;
  });
  // anchor edges on rendered node boxes (content height varies with rows)
  g.nodes.forEach(n => { if (n.el.offsetHeight) n.h = n.el.offsetHeight; });
  svg.innerHTML = renderTopoEdges(g, "http://www.w3.org/2000/svg");
  // keep an open drawer in sync with the refreshed graph
  if (topoState.sel) {
    const sn = nodeById(g, topoState.sel);
    if (sn) renderTopoDrawer(wrap, sn);
  }
  topoEls = { wrap, view, g };
  applyTopoTransform(view);
  wireTopoPan(wrap, view, g);
}

function applyTopoTransform(view) {
  view.style.transform = `translate(${topoState.tx}px,${topoState.ty}px) scale(${topoState.k})`;
}

function topoFit(wrap, g) {
  let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9;
  g.nodes.concat(g.groups).forEach(n => {
    x0 = Math.min(x0, n.x); y0 = Math.min(y0, n.y);
    x1 = Math.max(x1, n.x + (n.w || 0)); y1 = Math.max(y1, n.y + (n.h || 80));
  });
  const pad = 60, bw = wrap.clientWidth, bh = wrap.clientHeight;
  topoState.k = Math.min(1.6, Math.max(0.3, Math.min((bw - pad) / (x1 - x0 || 1), (bh - pad) / (y1 - y0 || 1))));
  topoState.tx = (bw - (x1 - x0) * topoState.k) / 2 - x0 * topoState.k;
  topoState.ty = (bh - (y1 - y0) * topoState.k) / 2 - y0 * topoState.k;
}

function wireTopoPan(wrap, view, g) {
  wrap.querySelectorAll(".topo-zoom button").forEach(b => b.addEventListener("click", (ev) => {
    ev.stopPropagation();
    if (b.dataset.z === "fit") topoFit(wrap, g);
    else topoState.k = Math.min(2.2, Math.max(0.25, topoState.k * (b.dataset.z === "in" ? 1.25 : 0.8)));
    applyTopoTransform(view);
  }));
  view.addEventListener("mousedown", (e) => {
    if (e.target.closest(".topo-node")) return;
    topoState.drag = { x: e.clientX - topoState.tx, y: e.clientY - topoState.ty };
  });
  wrap.addEventListener("mousemove", (e) => {
    if (!topoState.drag) return;
    topoState.tx = e.clientX - topoState.drag.x;
    topoState.ty = e.clientY - topoState.drag.y;
    applyTopoTransform(view);
  });
  window.addEventListener("mouseup", () => { topoState.drag = null; }, { once: true });
  wrap.addEventListener("wheel", (e) => {
    e.preventDefault();
    const r = wrap.getBoundingClientRect();
    const mx = e.clientX - r.left, my = e.clientY - r.top;
    const k2 = Math.min(2.2, Math.max(0.25, topoState.k * (e.deltaY < 0 ? 1.12 : 0.9)));
    topoState.tx = mx - (mx - topoState.tx) * (k2 / topoState.k);
    topoState.ty = my - (my - topoState.ty) * (k2 / topoState.k);
    topoState.k = k2;
    applyTopoTransform(view);
  }, { passive: false });
  if (topoState.k === 1 && topoState.tx === 0) { topoFit(wrap, g); applyTopoTransform(view); }
  wrap.addEventListener("click", () => {
    const dr = wrap.querySelector(".topo-drawer");
    if (dr) dr.remove();
    if (topoState.sel) { topoState.sel = null; wrap.querySelectorAll(".topo-node.sel").forEach(x => x.classList.remove("sel")); }
  });
}

function renderTopoDrawer(wrap, n) {
  wrap.querySelectorAll(".topo-node.sel").forEach(x => x.classList.remove("sel"));
  wrap.querySelectorAll(`.topo-node[data-nodeid="${n.id}"]`).forEach(x => x.classList.add("sel"));
  let dr = wrap.querySelector(".topo-drawer");
  if (dr) dr.remove();
  dr = el("div", "topo-drawer");
  const s = S.snap;
  const r = n.data.ref || {};
  const kv = [];
  kv.push(["node", n.name]);
  if (n.data.kind === "pg") {
    kv.push(["role", r.role || "?"], ["state", r.state || "?"], ["api", r.api_url || "?"],
      ["api reachable", String(r.api_reachable)], ["timeline", dash(r.timeline)],
      ["lag", r.lag_bytes < 0 ? "—" : bytes(r.lag_bytes)],
      ["pending restart", String(!!r.pending_restart)]);
    if (n.data.backend) {
      const b = n.data.backend;
      kv.push(["pg in_recovery", String(b.in_recovery)], ["receive lsn", dash(b.receive_lsn)],
        ["replay lsn", dash(b.replay_lsn)], ["wal dir", bytes(b.wal_dir_bytes)]);
    }
  } else if (n.data.kind === "keepalived") {
    if (r.vrrp_state !== undefined)
      kv.push(["reachable", String(r.reachable)], ["process", r.reachable ? (r.process_up ? "up" : "down") : "unknown (no ssh)"],
        ["vrrp state", r.vrrp_state], ["vip present", String(!!r.vip_present)],
        ["iface", r.vip_interface || "—"], ["last transition", r.last_transition || "—"],
        ["evidence", r.transition_evidence || "—"]);
    (s.vip || []).forEach(v => kv.push(["VIP " + v.address,
      (v.holders || []).includes(r.host) ? "held here · " + (v.evidence || "") : v.observed ? "held by " + v.holders.join(",") : "unverified"]));
    if (n.data.proxy) ((s.proxy && s.proxy.listeners) || []).forEach(l => kv.push([
      `listen ${l.name} :${l.port}`,
      `${n.data.proxy.open[l.name] ? "open" : "CLOSED"} · GET ${l.check} → [${(l.eligible || []).join(",") || "none"}]`]));
  } else if (n.data.kind === "etcd") {
    const e = s.etcd || {};
    const nm = (id) => ((e.roster || []).find(x => x.id === id) || {}).name || id;
    kv.push(["member id", n.data.id || "?"], ["endpoint", r.endpoint || "—"],
      ["configured", r.reachable === undefined ? "no — discovered from member list" : "yes"]);
    if (n.data.roster) kv.push(["peer urls", (n.data.roster.peer_urls || []).join(", ")],
      ["client urls", (n.data.roster.client_urls || []).join(", ")]);
    if (r.reachable !== undefined)
      kv.push(["reachable", String(r.reachable)], ["healthy", String(r.healthy)],
        ["raft role", r.is_leader ? "leader" : r.has_leader ? "follower" : "no leader"],
        ["raft term / index", `${dash(r.raft_term)} / ${dash(r.raft_index)}`],
        ["db size", bytes(r.db_size_bytes)], ["leader changes", dash(r.leader_changes)], ["err", r.err || "—"]);
    (e.links || []).filter(l => l.from === n.data.id || l.to === n.data.id).forEach(l => kv.push([
      `stream ${nm(l.from)} → ${nm(l.to)}`,
      (l.active ? "active" : "DOWN") + (l.bps >= 0 ? ` · ${l.bps.toFixed(0)} B/s` : "") + (l.from === e.leader_id ? " · leader append" : "")]));
  } else if (n.data.kind === "vip") {
    kv.push(["address", r.address], ["policy", r.policy || "?"], ["iface", r.interface || "any"],
      ["holders", (r.holders || []).join(", ") || "none"],
      ["state", r.observed ? "observed" : r.absent ? "absent" : "unverified"],
      ["evidence", r.evidence || "—"]);
    ((s.proxy && s.proxy.listeners) || []).forEach(l => {
      const o = l.vip_open && l.vip_open[r.address];
      if (o !== undefined) kv.push([`${r.address}:${l.port}`, (o ? "accepting" : "CLOSED") + " · " + l.name]);
    });
  }
  // matching source-health rows are the freshness evidence
  (s.sources || []).filter(x => n.name.startsWith(x.target) || (r.endpoint && x.target === r.endpoint))
    .forEach(x => kv.push([`src ${x.source}`, `${x.ok ? "ok" : "FAIL"} · ${x.duration_ms}ms${x.err ? " · " + x.err : ""}`]));
  dr.innerHTML = `<h4>${esc(n.name)}</h4><div class="kv">` +
    kv.map(([k, v]) => `<div class="k">${esc(k)}</div><div>${esc(String(v))}</div>`).join("") + `</div>`;
  const close = el("button", "btn sec", "close");
  close.style.marginTop = "0.8rem";
  close.addEventListener("click", (ev) => { ev.stopPropagation(); dr.remove(); topoState.sel = null; renderTopology(document.querySelector("#main")); });
  dr.append(close);
  dr.addEventListener("click", e => e.stopPropagation());
  wrap.append(dr);
}

/* ---------- detail screens ---------- */
function renderPatroni(m) {
  m.innerHTML = "";
  const s = S.snap; const grid = el("div", "grid");
  grid.append(card("members", "c8", s ? patroniTable(s) : ""));
  const hist = s && s.patroni && (s.patroni.history || []).length ?
    tbl(["tl","lsn","reason","time","new leader"],
      s.patroni.history.map(h => [h.timeline, h.lsn, esc(h.reason), esc(h.timestamp), esc(h.new_leader)])) :
    "<p class='muted'>no history observed</p>";
  grid.append(card("role history (/history)", "c4", hist));
  m.append(grid);
}
function renderEtcd(m) {
  m.innerHTML = "";
  const s = S.snap; const grid = el("div", "grid");
  grid.append(card("members & consensus", "c12", s ? etcdTable(s) : ""));
  m.append(grid);
}
function renderVip(m) {
  m.innerHTML = "";
  const s = S.snap; const grid = el("div", "grid");
  grid.append(card("keepalived & VIP matrix", "c8", s ? vipTable(s) : ""));
  const pol = (s && s.vip || []).map(v =>
    `<div class="kv"><div class="k">${esc(v.address)}</div><div>policy: ${esc(v.policy || "leader_only")} · iface ${esc(v.interface || "any")}</div></div>`).join("");
  grid.append(card("placement policy", "c4", pol || "<p class='muted'>no VIPs configured</p>"));
  m.append(grid);
}
function renderEvents(m) {
  m.innerHTML = "";
  const s = S.snap; const grid = el("div", "grid");
  grid.append(card("patroni history / transitions", "c12",
    s ? (s.patroni && (s.patroni.history || []).length ?
      tbl(["tl","lsn","reason","time","new leader"],
        s.patroni.history.map(h => [h.timeline, h.lsn, esc(h.reason), esc(h.timestamp), esc(h.new_leader)]))
      : "<p class='muted'>no events observed yet</p>") : ""));
  m.append(grid);
}

/* ---------- operations console ---------- */
async function renderOperations(m) {
  // build off-DOM and swap once all fetches finish: clearing first left the
  // page blank during the awaits (visible flicker on every snapshot)
  const seq = S.renderSeq;
  const grid = el("div", "grid");

  let mon = { running: false, seq: 0 };
  try { mon = await api("/api/monitor"); } catch (_) {}
  const state = `<div class="kv">
    <div class="k">monitor process</div><div>${mon.running ? chip("running","ok") : chip("down/restarting","warn")} · seq ${mon.seq}</div>
    <div class="k">stream</div><div>${S.connected ? chip("connected","ok") : chip("disconnected","warn")}</div>
    <div class="k">operator</div><div>${esc(S.operator)}</div>
    <div class="k">degraded</div><div>${S.snap && S.snap.degraded ? esc(S.snap.degraded_reason || "yes") : "no"}</div></div>`;
  grid.append(card("pipeline status", "c4", state));

  const log = el("div", "logwin");
  const lines = (S.snap && S.snap.sources || []).map(x =>
    `${x.ok ? "OK  " : "FAIL"} ${x.source}@${x.target} ${x.duration_ms}ms ${x.err || ""}`);
  log.textContent = (S.monState ? "monitor: " + JSON.stringify(S.monState) + "\n" : "") +
    lines.join("\n") || "no source events yet";
  log.scrollTop = log.scrollHeight;
  grid.append(card("source event log (latest snapshot)", "c8", log));

  // ---- service control: status/start/stop/restart via `hawarden service` ----
  let cfg = null;
  try { cfg = await api("/api/config"); } catch (_) {}
  const svcCard = el("div");
  svcCard.append(card("service control", "c12", svcMatrix(cfg)));
  grid.append(svcCard.children[0]);
  wireServiceCtl(svcCard);

  let ops = { ops: [] };
  try { ops = await api("/api/ops"); } catch (_) {}
  const all = [...S.opLog, ...(ops.ops || [])];
  const seen = new Set(); const uniq = all.filter(o => { if (seen.has(o.id)) return false; seen.add(o.id); return true; });
  const rows = uniq.slice(0, 50).map(o => [
    new Date(o.at).toLocaleTimeString(), esc(o.operator), esc(o.action),
    esc(o.input), o.ok ? chip("ok","ok") : chip("failed","crit"), esc((o.result || "").slice(0, 60)),
  ]);
  grid.append(card("operations journal", "c12",
    rows.length ? tbl(["time","operator","action","target","result","detail"], rows)
                : "<p class='muted'>no operations recorded</p>"));
  if (seq !== S.renderSeq) return; // a newer render superseded this one
  m.replaceChildren(grid);
  log.scrollTop = log.scrollHeight;
}

// unit → role + observed state from the live snapshot
const SVC_UNITS = ["patroni", "postgresql", "keepalived", "etcd"];
const unitRole = { patroni: "patroni", postgresql: "pg", keepalived: "keepalived", etcd: "etcd" };

function svcState(host, unit, s) {
  if (!s) return ["unknown", "mut"];
  const byName = (arr, f) => (arr || []).find(f);
  switch (unit) {
    case "patroni": {
      const m = byName(s.patroni && s.patroni.members, x => x.name === host.name || x.host === host.name || x.host === host.address);
      if (!m) return ["n/a", "mut"];
      if (!m.api_reachable) return ["api down", "crit"];
      return [m.state || "?", m.state === "running" || m.state === "streaming" ? "ok" : "warn"];
    }
    case "postgresql": {
      const b = byName(s.backends, x => x.host === host.name || x.host === host.address);
      if (!b) return ["n/a", "mut"];
      if (!b.reachable) return ["down", "crit"];
      return [b.in_recovery ? "replica" : "primary", "ok"];
    }
    case "keepalived": {
      if (host.ssh_disabled) {
        // No SSH by config: VRRP state is unobservable; only ARP holder evidence exists.
        const holds = (s.vip || []).some(v => (v.holders || []).includes(host.name));
        return holds ? ["VIP holder (arp)", "ok"] : ["not observed · no ssh", "mut"];
      }
      const k = byName(s.keepalived, x => x.host === host.name || x.host === host.address);
      if (!k) return ["n/a", "mut"];
      if (!k.reachable) return ["unreachable", "crit"];
      if (!k.process_up) return ["proc down", "crit"];
      return [k.vrrp_state || "?", k.vrrp_state === "MASTER" || k.vrrp_state === "BACKUP" ? "ok" : "warn"];
    }
    case "etcd": {
      const e = byName(s.etcd && s.etcd.members, x => {
        const h = (x.endpoint || "").split(":")[0];
        return h === host.name || h === host.address || (x.name && x.name === host.name);
      });
      if (!e) return ["n/a", "mut"];
      if (!e.reachable) return ["down", "crit"];
      return [e.healthy ? (e.is_leader ? "healthy·leader" : "healthy") : "unhealthy", e.healthy ? "ok" : "warn"];
    }
  }
  return ["?", "mut"];
}

function svcMatrix(cfg) {
  const s = S.snap;
  const hosts = (cfg && cfg.hosts || []).filter(h => (h.roles || []).length);
  if (!hosts.length) return "<p class='muted'>no machines configured — add one in the Configuration console</p>";
  const rows = hosts.map(h => {
    const cells = SVC_UNITS.map(u => {
      if (!(h.roles || []).includes(unitRole[u])) return `<td class="muted">—</td>`;
      const [st, sev] = svcState(h, u, s);
      const dis = h.ssh_disabled
        ? `disabled title="service control runs over SSH; ${esc(h.name)} has ssh_disabled: true"` : "";
      return `<td><span class="svc-cell">${chip(st, sev)}</span>
        <span class="svc-btns">
          <button class="btn mini" data-svc="status" data-unit="${u}" data-host="${esc(h.name)}" ${dis}>st</button>
          <button class="btn mini" data-svc="start" data-unit="${u}" data-host="${esc(h.name)}" ${dis}>▶</button>
          <button class="btn mini" data-svc="stop" data-unit="${u}" data-host="${esc(h.name)}" ${dis}>■</button>
          <button class="btn mini" data-svc="restart" data-unit="${u}" data-host="${esc(h.name)}" ${dis}>⟳</button>
        </span></td>`;
    }).join("");
    return `<tr><td><b class="mono">${esc(h.name)}</b><div class="muted" style="font-size:10px">${esc((h.roles || []).join(" "))}</div></td>${cells}</tr>`;
  }).join("");
  const noSSH = hosts.filter(h => h.ssh_disabled).map(h => h.name);
  const sshNote = noSSH.length ? `<p class="muted" style="margin-top:.4rem">${chip("controls disabled", "warn")}
    no SSH access configured for ${esc(noSSH.join(", "))} (ssh_disabled: true) — start/stop/restart/status
    execute over SSH, so they are unavailable for these hosts. Enable sshd + key on the host and clear
    ssh_disabled to turn them on.</p>` : "";
  return `<table class="tbl"><thead><tr><th>HOST</th>${SVC_UNITS.map(u => "<th>" + u.toUpperCase() + "</th>").join("")}</tr></thead><tbody>${rows}</tbody></table>${sshNote}
    <p class="muted" style="margin-top:.4rem">state = observed from the live snapshot · ▶ start · ■ stop · ⟳ restart · st status — mutations need typed host confirmation and are journaled</p>`;
}

function wireServiceCtl(root) {
  root.querySelectorAll("[data-svc]").forEach(b => b.addEventListener("click", async () => {
    const verb = b.dataset.svc, unit = b.dataset.unit, host = b.dataset.host;
    let confirm = "";
    if (verb !== "status") {
      confirm = window.prompt(`${verb} ${unit} on ${host}\ntype the host name to confirm`);
      if (confirm === null) return;
    }
    b.disabled = true;
    try {
      const res = await api("/api/service", {
        method: "POST",
        body: JSON.stringify({ verb, unit, host, confirm }),
      });
      const det = (res.evidence || []).join(" · ");
      alert(`${res.ok ? "ok" : "FAILED"}: ${verb} ${unit} on ${host}\n` +
        `manager: ${res.manager} · running: ${res.running}` +
        (res.pids ? ` · pid ${res.pids}` : "") + (det ? `\n${det}` : "") + (res.err ? `\nerr: ${res.err}` : ""));
    } catch (e) { alert(e.message); }
    render();
  }));
}

/* ---------- configuration console ---------- */
const wizard = { step: 0, host: { name:"", address:"", ssh_user:"root", roles:[] },
  patroni_port: 8008, pg_port: 5432, pg_adapter: "", probe: null, msg: "" };

async function renderConfiguration(m) {
  const seq = S.renderSeq;
  const grid = el("div", "grid");

  // current config
  let cfg = null, cfgErr = "";
  try { cfg = await api("/api/config"); } catch (e) { cfgErr = e.message; }
  const hosts = cfg ? (cfg.hosts || []) : [];
  const hostRows = hosts.map(h => [
    esc(h.name), esc(h.address), esc((h.roles || []).join(", ")), esc(h.ssh_user || "root"),
    `<button class="btn danger" data-delhost="${esc(h.name)}">remove</button>`,
  ]);
  const ht = el("div");
  ht.append(card("monitored machines", "c8",
    hostRows.length ? tbl(["name","address","roles","ssh user",""], hostRows)
                    : "<p class='muted'>no machines configured — add one below</p>"));
  const cfgInfo = cfg ? `<div class="kv">
    <div class="k">cluster</div><div>${esc(cfg.cluster)}</div>
    <div class="k">interval</div><div>${esc(fmtDur(cfg.interval))}</div>
    <div class="k">vips</div><div>${esc((cfg.vips || []).map(v => v.address).join(", ") || "—")}</div>
    <div class="k">etcd endpoints</div><div>${esc((cfg.etcd && cfg.etcd.endpoints || []).join(", ") || "—")}</div>
    <div class="k">replication ctx</div><div>${cfg.replication_context ? "enabled" : "disabled"}</div></div>`
    : `<p class="error">${esc(cfgErr)}</p>`;
  const c2 = card("cluster settings", "c4", cfgInfo);
  grid.append(ht.children[0]); grid.append(c2);

  // wizard
  grid.append(card("add machine to monitoring", "c12", wizardView()));
  if (seq !== S.renderSeq) return;
  m.replaceChildren(grid);

  m.querySelectorAll("[data-delhost]").forEach(b => b.addEventListener("click", async () => {
    if (!confirm("Remove " + b.dataset.delhost + " from monitoring?")) return;
    try {
      await api("/api/config/remove-host", { method: "POST", body: JSON.stringify({ name: b.dataset.delhost }) });
      render();
    } catch (e) { alert(e.message); }
  }));
  wireWizard(m);
}

function wizardView() {
  const w = wizard;
  const steps = ["identity","roles & endpoints","test connection","review & save"];
  const div = el("div");
  div.innerHTML = `<div class="steps">` + steps.map((s, i) =>
    `<span class="step ${i === w.step ? "active" : i < w.step ? "done" : ""}">${i + 1}. ${s}</span>`).join("") + `</div>`;
  const body = el("div"); div.append(body);

  if (w.step === 0) {
    body.innerHTML = `<div class="form-grid">
      <div class="field"><label>machine name</label><input id="wz-name" value="${esc(w.host.name)}" placeholder="db4"></div>
      <div class="field"><label>address (DNS / IPv4 / IPv6)</label><input id="wz-addr" value="${esc(w.host.address)}" placeholder="10.0.0.14"></div>
      <div class="field"><label>ssh user</label><input id="wz-user" value="${esc(w.host.ssh_user)}" placeholder="root"></div>
    </div>`;
  } else if (w.step === 1) {
    const r = w.host.roles;
    body.innerHTML = `<div class="form-grid">
      <div class="field"><label>roles on this machine</label><div class="checks">
        ${["patroni","pg","keepalived","etcd"].map(x =>
          `<label><input type="checkbox" class="wz-role" value="${x}" ${r.includes(x) ? "checked" : ""}> ${x}</label>`).join("")}
      </div></div>
      <div class="field"><label>patroni api port</label><input id="wz-pport" type="number" value="${w.patroni_port}"></div>
      <div class="field"><label>postgres port</label><input id="wz-pgport" type="number" value="${w.pg_port}"></div>
      <div class="field"><label>pg replication context adapter</label>
        <select id="wz-pgad"><option value="">disabled</option>
        <option value="ssh-psql" ${w.pg_adapter === "ssh-psql" ? "selected" : ""}>ssh-psql (least privilege)</option></select></div>
      <div class="field"><label>ssh telemetry</label><div class="checks">
        <label><input type="checkbox" id="wz-ssh" ${w.ssh === false ? "" : "checked"}> SSH telemetry on this machine
          (keepalived, /proc/net/dev, ssh-psql)</label></div></div>
    </div>`;
  } else if (w.step === 2) {
    body.innerHTML = `<p class="muted">Run the connectivity probe before saving. Every check is reported separately.</p>
      <button class="btn sec" id="wz-probe">run probe</button><div id="wz-probe-out"></div>`;
    if (w.probe) {
      const p = w.probe;
      body.querySelector("#wz-probe-out").innerHTML = `<div class="probe">` +
        p.checks.map(c => `<div class="row"><span>${esc(c.name)}</span><span>${c.ok ? chip("ok "+c.ms+"ms","ok") : chip("fail","crit")} ${esc(c.detail || "")}</span></div>`).join("") +
        `</div>`;
    }
  } else {
    const h = wizardHostSpec();
    body.innerHTML = `<pre class="cfg">${esc(JSON.stringify(h, null, 2))}</pre>
      ${w.probe ? `<p>${w.probe.ok ? chip("all probes passed","ok") : chip("some probes failed — saving anyway is allowed (partial setup)","warn")}</p>` : ""}
      <p class="muted">Saving adds this machine to hawarden.yaml and restarts the live monitor. It does not change Patroni, etcd or keepalived settings on the machine.</p>`;
  }
  if (w.msg) body.append(el("p", "error", w.msg));

  const nav = el("div"); nav.style.marginTop = "1rem";
  if (w.step > 0) nav.append(Object.assign(el("button", "btn sec", "back"), { onclick: () => { w.step--; wizard.msg = ""; render(); } }));
  nav.append(el("span", "", " "));
  if (w.step < 3) nav.append(Object.assign(el("button", "btn", "next"), { onclick: () => wizardNext(body) }));
  else nav.append(Object.assign(el("button", "btn", "save & monitor"), { onclick: wizardSave }));
  div.append(nav);
  return div;
}

function wizardHostSpec() {
  const w = wizard;
  const h = { name: w.host.name, address: w.host.address, ssh_user: w.host.ssh_user || "root", roles: w.host.roles };
  if (w.ssh === false) h.ssh_disabled = true;
  if (w.host.roles.includes("patroni")) h.patroni = {};
  if (w.host.roles.includes("pg")) {
    h.pg = { port: w.pg_port };
    if (w.pg_adapter) h.pg.adapter = w.pg_adapter;
  }
  if (w.host.roles.includes("keepalived")) h.keepalived = { state_file: "/run/keepalived.hawarden.state" };
  if (w.host.roles.includes("etcd")) h.etcd = {};
  return h;
}

function wizardNext(body) {
  const w = wizard;
  if (w.step === 0) {
    w.host.name = body.querySelector("#wz-name").value.trim();
    w.host.address = body.querySelector("#wz-addr").value.trim();
    w.host.ssh_user = body.querySelector("#wz-user").value.trim() || "root";
    if (!w.host.name || !w.host.address) { w.msg = "name and address are required"; render(); return; }
  } else if (w.step === 1) {
    w.host.roles = [...body.querySelectorAll(".wz-role:checked")].map(x => x.value);
    w.patroni_port = parseInt(body.querySelector("#wz-pport").value) || 8008;
    w.pg_port = parseInt(body.querySelector("#wz-pgport").value) || 5432;
    w.pg_adapter = body.querySelector("#wz-pgad").value;
    w.ssh = body.querySelector("#wz-ssh").checked;
    if (!w.host.roles.length) { w.msg = "pick at least one role"; render(); return; }
  }
  w.msg = ""; w.step++; render();
}

async function wizardSave() {
  wizard.msg = "";
  try {
    await api("/api/config/add-host", { method: "POST", body: JSON.stringify(wizardHostSpec()) });
    wizard.step = 0; wizard.host = { name:"", address:"", ssh_user:"root", roles:[] };
    wizard.probe = null;
    render();
  } catch (e) { wizard.msg = e.message; render(); }
}

function wireWizard(m) {
  const pb = m.querySelector("#wz-probe");
  if (pb) pb.addEventListener("click", async () => {
    pb.disabled = true; pb.textContent = "probing…";
    try {
      wizard.probe = await api("/api/config/test", { method: "POST", body: JSON.stringify(wizardHostSpec()) });
    } catch (e) { wizard.probe = { ok: false, checks: [{ name: "probe", ok: false, detail: e.message }] }; }
    pb.disabled = false; pb.textContent = "run probe"; render();
  });
}

/* ---------- boot ---------- */
(async () => {
  try {
    const me = await api("/api/me");
    S.csrf = me.csrf; S.operator = me.operator;
    showShell();
  } catch (_) { showLogin(); }
})();
