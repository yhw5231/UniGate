/* UniGate WebUI */
"use strict";

// ---- 基础 ----
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

let TOKEN = localStorage.getItem("unigate_token") || "";
let STATE = null;          // /admin/api/state 缓存
let editChannel = null;    // 正在编辑的渠道对象（深拷贝）
let logsTimer = null;

function toast(msg, isError) {
  let el = $(".toast");
  if (el) el.remove();
  el = document.createElement("div");
  el.className = "toast" + (isError ? " error" : "");
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), isError ? 5000 : 2500);
}

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (TOKEN) opts.headers["Authorization"] = "Bearer " + TOKEN;
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opts);
  const text = await resp.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
  if (resp.status === 401 && path !== "/login") { logout(); throw new Error("登录已过期"); }
  if (!resp.ok) {
    const msg = (data && (data.error && (data.error.message || data.error))) || ("HTTP " + resp.status);
    throw new Error(typeof msg === "string" ? msg : JSON.stringify(msg));
  }
  return data;
}

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

// ---- 版本号显示（无需鉴权） ----
(async () => {
  try {
    const r = await (await fetch("/api/version")).json();
    const v = r && r.version ? r.version : "dev";
    $("#loginVersion").textContent = v;
    $("#topVersion").textContent = v;
  } catch { /* 后端未提供版本时保持默认 dev */ }
})();

// ---- 登录 ----
function showLogin() {
  $("#loginView").classList.remove("hidden");
  $("#appView").classList.add("hidden");
}
function showApp() {
  $("#loginView").classList.add("hidden");
  $("#appView").classList.remove("hidden");
}
function logout() {
  localStorage.removeItem("unigate_token");
  TOKEN = "";
  if (logsTimer) clearInterval(logsTimer);
  if (errorsTimer) clearInterval(errorsTimer);
  if (routeTimer) clearInterval(routeTimer);
  showLogin();
}

$("#loginForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  $("#loginErr").classList.add("hidden");
  try {
    const resp = await fetch("/login", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: $("#loginUser").value, password: $("#loginPass").value }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data && data.error ? (data.error.message || data.error) : "登录失败");
    TOKEN = data.token;
    localStorage.setItem("unigate_token", TOKEN);
    $("#whoami").textContent = data.user || "";
    showApp();
    await loadState();
  } catch (err) {
    $("#loginErr").textContent = err.message;
    $("#loginErr").classList.remove("hidden");
  }
});
$("#logoutBtn").addEventListener("click", logout);

// ---- Tab 切换 ----
$$(".tab").forEach((btn) => btn.addEventListener("click", () => {
  $$(".tab").forEach((b) => b.classList.remove("active"));
  btn.classList.add("active");
  $$(".tabpane").forEach((p) => p.classList.add("hidden"));
  $("#tab-" + btn.dataset.tab).classList.remove("hidden");
  if (btn.dataset.tab === "logs") refreshLogs();
  if (btn.dataset.tab === "errors") refreshErrors();
  if (btn.dataset.tab === "pin") renderPinPage();
  if (btn.dataset.tab === "route") refreshRoute();
  if (btn.dataset.tab === "test") refreshTestTab();
  if (btn.dataset.tab === "usage") refreshUsage();
  if (btn.dataset.tab === "leases") { renderPools(); refreshLeases(); }
  if (btn.dataset.tab === "settings") fillSettingsForm();
}));

// ---- 状态加载 ----
async function loadState() {
  STATE = await api("GET", "/admin/api/state");
  renderChannels();
  renderGWKeys();
  renderPools();
  fillSettingsForm();
  if (!$("#tab-leases").classList.contains("hidden")) refreshLeases();
}

// ---- 路由策略设置 ----
// state.settings = 前端显式设置（留空字段 = 未设置）；state.policy = 当前生效值
function fillSettingsForm() {
  const s = (STATE && STATE.settings) || {};
  const p = (STATE && STATE.policy) || {};
  $("#setRateLimitCooldown").value = s.rate_limit_cooldown_sec ?? "";
  $("#setRotateAfter5xx").value = s.rotate_after_5xx ?? "";
  $("#setMaxRouteTries").value = s.max_route_tries ?? "";
  $("#setKeepaliveSec").value = s.keepalive_sec ?? "";
  $("#setProbeIdleSec").value = s.probe_idle_sec ?? "";
  $("#setDefaultSchedule").value = s.default_schedule || "";
  // RoutePolicy 无 json tag：生效值按 Go 字段名下发，Duration 序列化为纳秒
  const ns = (v) => Math.round((v || 0) / 1e9);
  const tries = (p.MaxRouteTries || 0) === 0 ? "全部" : p.MaxRouteTries;
  const ka = ns(p.KeepaliveInterval);
  const pi = ns(p.ProbeIdleInterval);
  const sched = p.DefaultSchedule === "round_robin" ? "顺序轮询" : "故障转移";
  $("#policyNow").textContent =
    `429 冷却 ${ns(p.RateLimitCooldown)}s · 连续 5xx 超过 ${p.RotateAfter5xx ?? 3} 次换出口（0=关闭） · 单请求最多尝试 ${tries} 个 key · 流式心跳 ${ka > 0 ? ka + "s" : "关闭"} · 空闲探测 ${pi > 0 ? pi + "s" : "关闭"} · 默认账号调度 ${sched}`;
}

$("#settingsSaveBtn").addEventListener("click", async () => {
  const body = {};
  const num = (sel, name) => {
    const v = $(sel).value.trim();
    if (v === "") return; // 留空 = 恢复环境变量默认
    const n = parseInt(v, 10);
    if (!Number.isFinite(n) || n < 0) { throw new Error(`${name} 必须是不小于 0 的整数`); }
    body[name] = n;
  };
  try {
    num("#setRateLimitCooldown", "rate_limit_cooldown_sec");
    num("#setRotateAfter5xx", "rotate_after_5xx");
    num("#setMaxRouteTries", "max_route_tries");
    num("#setKeepaliveSec", "keepalive_sec");
    num("#setProbeIdleSec", "probe_idle_sec");
    const sched = $("#setDefaultSchedule").value;
    if (sched !== "") body.default_schedule = sched; // 留空 = 恢复环境变量默认
    await api("PUT", "/admin/api/settings", body);
    toast("设置已保存并生效");
    await loadState();
  } catch (e) { toast(e.message, true); }
});

// ---- 路由页：按模型展示候选 key 与实时状态（可用/冷却中/停用），支持
// 逐 (key, 模型) 精确解除冷却、一键清空全部冷却、切换全局默认账号调度 ----
let routeTimer = null;
let routeState = null;

async function refreshRoute() {
  try {
    routeState = await api("GET", "/admin/api/route");
    renderRoute();
  } catch (e) { toast(e.message, true); }
}

function renderRoute() {
  if (!routeState) return;
  const p = routeState;
  // 调度下拉始终反映当前生效值（渠道级覆盖在本页各 key 行以「轮询」徽标提示）
  $("#routeDefaultSchedule").value = p.default_schedule === "round_robin" ? "round_robin" : "failover";
  $("#routeSummary").innerHTML =
    `模型 <b>${p.models.length}</b> 个 · 候选 key ${p.total} · 可用 <b>${p.available}</b> · 冷却中 ${p.cooling}` +
    ` · 默认调度 ${p.default_schedule === "round_robin" ? "顺序轮询" : "故障转移"}`;

  const q = ($("#routeModelFilter").value || "").trim().toLowerCase();
  const showOff = $("#routeShowOff").checked;
  const groups = (p.models || []).filter((g) => !q || (g.model || "").toLowerCase().includes(q));
  const el = $("#routeModels");
  if (!groups.length) {
    el.innerHTML = `<p class="muted">${(p.models || []).length ? "没有匹配过滤条件的模型。" : "暂无候选。先在渠道中配置或从上游拉取模型列表。"}</p>`;
    return;
  }
  el.innerHTML = groups.map((g) => routeModelHTML(g, showOff)).join("");
  el.querySelectorAll('[data-act="clearcoolmodel"]').forEach((b) => b.addEventListener("click", async () => {
    try {
      const r = await api("POST", "/admin/api/cooling/clear-model", { key_id: b.dataset.key, model: b.dataset.model });
      toast(r.cleared > 0 ? "已解除该模型冷却" : "该冷却已过期");
      await refreshRoute();
      await loadState(); // 同步渠道页的冷却明细
    } catch (e) { toast(e.message, true); }
  }));
}

// routeModelHTML 单个模型分组的候选行。候选顺序 = 网关实际转发顺序
//（渠道配置序 + key 配置序），可直接当作「下一个请求会用谁」的预览。
function routeModelHTML(g, showOff) {
  const keys = (g.keys || []).filter((k) => showOff || k.status !== "disabled");
  const rows = keys.map((k) => {
    let badge, extra = "";
    if (k.status === "ok") {
      badge = '<span class="badge on">可用</span>';
    } else if (k.status === "cooling") {
      const left = k.left_ms > 0 ? Math.round(k.left_ms / 1000) : 0;
      badge = `<span class="badge warn" title="冷却到期后自动恢复；点「解除」立即恢复">冷却中 · 剩 ${fmtLeft(left)}</span>`;
      extra = `<button class="btn small" data-act="clearcoolmodel" data-key="${esc(k.key_id)}" data-model="${esc(g.model)}" title="只解除该 (key, 模型) 的冷却">解除</button>`;
    } else {
      badge = `<span class="badge off">${k.channel_enabled ? "key 停用" : "渠道停用"}</span>`;
    }
    return `<div class="route-key ${k.status}">
      ${badge}
      <span class="rk-name">${esc(k.key || "(未命名)")}</span>
      <span class="muted">@ ${esc(k.channel)}</span>
      ${k.schedule === "round_robin" ? '<span class="badge info" title="该渠道为顺序轮询调度">轮询</span>' : ""}
      ${extra}
    </div>`;
  }).join("");
  const zero = g.available === 0 ? '<span class="badge off" title="该模型当前没有可路由的 key，请求会失败（全部冷却时网关会穿透最早到期的候选试探）">无可用 key</span>' : "";
  return `<div class="route-model">
    <div class="route-model-head">
      <span class="rk-model" title="${esc(g.model)}">${esc(g.model || "（未声明模型列表 · 对全部模型放行）")}</span>
      ${zero}
      <span class="badge on">可用 ${g.available}</span>
      ${g.cooling ? `<span class="badge warn">冷却 ${g.cooling}</span>` : ""}
      <span class="muted">候选 ${g.total}</span>
    </div>
    ${rows ? `<div class="route-keys">${rows}</div>` : '<div class="key-line muted">该模型没有候选渠道/key（渠道未启用或模型未声明）</div>'}
  </div>`;
}

$("#routeRefreshBtn").addEventListener("click", refreshRoute);
$("#routeModelFilter").addEventListener("input", renderRoute);
$("#routeShowOff").addEventListener("change", renderRoute);
$("#routeAuto").addEventListener("change", (e) => {
  if (routeTimer) clearInterval(routeTimer);
  if (e.target.checked) routeTimer = setInterval(refreshRoute, 5000);
});

// 一键清空全部冷却（所有 key、所有模型粒度）
$("#routeClearAllBtn").addEventListener("click", async () => {
  if (!confirm("清空全部冷却？所有 key、所有模型立即恢复路由。")) return;
  try {
    const r = await api("POST", "/admin/api/cooling/clear-all", {});
    toast(`已清除 ${r.cleared} 条冷却`);
    await refreshRoute();
    await loadState();
  } catch (e) { toast(e.message, true); }
});

// 全局默认账号调度：保存完整设置对象（PUT 为全量替换，需带上其余已设项）
$("#routeDefaultSchedule").addEventListener("change", async (e) => {
  const cur = (STATE && STATE.settings) || {};
  try {
    await api("PUT", "/admin/api/settings", {
      rate_limit_cooldown_sec: cur.rate_limit_cooldown_sec ?? null,
      rotate_after_5xx: cur.rotate_after_5xx ?? null,
      max_route_tries: cur.max_route_tries ?? null,
      keepalive_sec: cur.keepalive_sec ?? null,
      default_schedule: e.target.value,
    });
    toast("默认账号调度已保存并生效");
    await refreshRoute();
    await loadState(); // 同步设置页显示
  } catch (e2) { toast(e2.message, true); }
});

// ---- 渠道列表 ----
// 渠道过滤：按关键字（名称/分组/BaseURL/模型）与分组下拉筛选
function filterChannels(chans) {
  const q = ($("#channelSearch").value || "").trim().toLowerCase();
  const g = $("#channelGroupFilter").value;
  return chans.filter((ch) => {
    if (g && (ch.group || "") !== g) return false;
    if (!q) return true;
    const hay = [ch.name, ch.group, ch.base_url, (ch.models || []).join(" ")].join(" ").toLowerCase();
    return hay.includes(q);
  });
}

// 分组下拉选项 + 编辑器 datalist 候选
function refreshGroupOptions() {
  const chans = (STATE && STATE.channels) || [];
  const groups = [...new Set(chans.map((c) => (c.group || "").trim()).filter(Boolean))].sort();
  const sel = $("#channelGroupFilter");
  const cur = sel.value;
  sel.innerHTML = '<option value="">全部分组</option>' +
    groups.map((g) => `<option value="${esc(g)}">${esc(g)}</option>`).join("");
  if (groups.includes(cur)) sel.value = cur;
  $("#groupSuggestions").innerHTML = groups.map((g) => `<option value="${esc(g)}">`).join("");
}

function renderChannels() {
  refreshGroupOptions();
  const wrap = $("#channelList");
  const chans = filterChannels((STATE && STATE.channels) || []);
  const cooling = coolingByKeyID();
  if (!(STATE && STATE.channels || []).length) {
    wrap.innerHTML = `<p class="muted">还没有渠道。点击右上角「新建渠道」添加第一个 OpenAI 兼容上游。</p>`;
    return;
  }
  if (!chans.length) {
    wrap.innerHTML = `<p class="muted">没有匹配筛选条件的渠道。</p>`;
    return;
  }
  wrap.innerHTML = chans.map((ch) => {
    const keys = ch.keys || [];
    const coolingN = keys.filter((k) => cooling[k.id]).length;
    const keyLines = keys.map((k) => {
      const p = keyProxyDesc(k, ch);
      const clearBtn = cooling[k.id]
        ? `<button class="btn small" data-act="clearcool" data-key="${esc(k.id)}" title="手动解除该 key 的全部冷却（key 级与按模型冷却）">解除冷却</button>`
        : "";
      return `<div class="key-line">
        <span class="badge ${k.enabled ? "on" : "off"}">${k.enabled ? "启用" : "停用"}</span>
        <span>${esc(k.name || "(未命名)")}</span>
        ${keyCoolHTML(ch, k.id, cooling)}
        <span class="pname">代理: ${esc(p)}</span>
        ${clearBtn}
      </div>`;
    }).join("");
    const modelLine = (ch.models || []).length
      ? `<div class="key-line"><span class="pname">模型 ${ch.models.length} 个：${esc(ch.models.slice(0, 4).join("、"))}${ch.models.length > 4 ? " …" : ""}</span></div>`
      : "";
    const pinnedModels = Object.keys(ch.model_pins || {}).filter((m) => {
      const p = ch.model_pins[m];
      return p && ((p.upstreams || []).length || (p.exclude || []).length || p.sort);
    }).length;
    return `<div class="channel-card" data-id="${esc(ch.id)}">
      <div class="head">
        <span class="badge ${ch.enabled ? "on" : "off"}">${ch.enabled ? "启用" : "停用"}</span>
        <span class="name">${esc(ch.name)}</span>
        ${ch.group ? `<span class="badge group">${esc(ch.group)}</span>` : ""}
        <span class="muted">${esc(ch.base_url)}</span>
        ${ch.endpoint_type === "responses" ? '<span class="badge info">responses</span>' : ""}
        ${ch.rewrite_reasoning ? '<span class="badge info">reasoning改写</span>' : ""}
        ${ch.cooldown_scope === "key_model" ? '<span class="badge info">按(Key,模型)冷却</span>' : ""}
        ${ch.schedule === "round_robin" ? '<span class="badge info" title="每次请求从下一个 key 开始轮流分配">顺序轮询</span>' : ""}
        ${ch.auto_probe ? '<span class="badge info" title="key 冷却恢复/连续 8 小时无调用时自动发加法题验证账号状态">自动探测</span>' : ""}
        ${ch.proxy && ch.proxy.kind ? '<span class="badge info">渠道代理</span>' : ""}
        ${pinnedModels ? `<span class="badge info" title="该渠道有模型的内部渠道被固定（请求注入 provider.only/order，不再随机路由）">已固定 ${pinnedModels} 个模型</span>` : ""}
        ${coolingN ? `<span class="badge warn">${coolingN} 个 key 冷却中</span>` : ""}
        <span class="spacer"></span>
        ${ch.endpoint_type !== "responses" ? `<button class="btn small" data-act="pin">内部渠道固定</button>` : ""}
        <button class="btn small" data-act="edit">编辑</button>
        <button class="btn small danger" data-act="del">删除</button>
      </div>
      ${modelLine}
      ${keyLines || '<div class="key-line muted">无 key</div>'}
    </div>`;
  }).join("");

  wrap.querySelectorAll('[data-act="edit"]').forEach((b) => b.addEventListener("click", () => {
    const id = b.closest(".channel-card").dataset.id;
    openChannelEditor(JSON.parse(JSON.stringify(chans.find((c) => c.id === id))));
  }));
  wrap.querySelectorAll('[data-act="pin"]').forEach((b) => b.addEventListener("click", () => {
    const id = b.closest(".channel-card").dataset.id;
    // 固定设置在独立「渠道固定」页：切换过去并定位该渠道
    document.querySelector('[data-tab="pin"]').click();
    setTimeout(() => {
      const el = document.querySelector(`#pinChannels .channel-card[data-ch="${CSS.escape(id)}"]`);
      if (el) el.scrollIntoView({ block: "start" });
    }, 150);
  }));
  wrap.querySelectorAll('[data-act="del"]').forEach((b) => b.addEventListener("click", async () => {
    const id = b.closest(".channel-card").dataset.id;
    const ch = chans.find((c) => c.id === id);
    if (!confirm(`删除渠道「${ch.name}」？其绑定的代理池租约将被释放。`)) return;
    try { await api("DELETE", "/admin/api/channels/" + id); toast("已删除"); await loadState(); }
    catch (e) { toast(e.message, true); }
  }));
  wrap.querySelectorAll('[data-act="clearcool"]').forEach((b) => b.addEventListener("click", async () => {
    const chID = b.closest(".channel-card").dataset.id;
    try {
      const r = await api("POST", "/admin/api/cooling/clear", { key_id: b.dataset.key, channel_id: chID });
      toast(r.cleared > 0 ? `已解除 ${r.cleared} 条冷却` : "该 key 当前没有冷却");
      await loadState();
    } catch (e) { toast(e.message, true); }
  }));
  wrap.querySelectorAll('[data-act="clearcoolmodel"]').forEach((b) => b.addEventListener("click", async () => {
    try {
      const r = await api("POST", "/admin/api/cooling/clear-model", { key_id: b.dataset.key, model: b.dataset.model });
      toast(r.cleared > 0 ? `已解除 ${b.dataset.model} 的冷却` : "该冷却已过期");
      await loadState();
    } catch (e) { toast(e.message, true); }
  }));
}
$("#channelSearch").addEventListener("input", renderChannels);
$("#channelGroupFilter").addEventListener("change", renderChannels);

// ---- 渠道固定（独立设置页，upstreampin）----
// ---- 渠道固定（独立设置页）----
// 每个渠道一块、渠道的每个模型一行：探测（发现该上游内部渠道清单与管线类型）/
// 验证（逐渠道测试）/ 固定顺序·排除·模式·排序。所有改动经专用端点立即落盘生效，
// 不与「编辑渠道」弹窗耦合（编辑渠道保存时原样保留 model_pins，不会清空配置）。
// pinExtras：手工添加、尚不在渠道模型列表/model_pins 中的模型行（渠道ID → [模型]）。
let pinExtras = {};

// renderPinPage 渲染渠道固定页（渠道 tab 切入与刷新时调用）。
function renderPinPage() {
  const wrap = $("#pinChannels");
  if (!wrap) return;
  const chans = (STATE && STATE.channels) || [];
  if (!chans.length) {
    wrap.innerHTML = '<p class="muted">还没有渠道。先在「渠道」页添加 OpenAI 兼容上游。</p>';
    return;
  }
  wrap.innerHTML = chans.map((ch) => pinChannelSectionHTML(ch)).join("");
  wirePinPage();
}

// pinChannelSectionHTML 单个渠道块：模型行 + 手工添加模型输入。
function pinChannelSectionHTML(ch) {
  const models = [...new Set([...(ch.models || []), ...Object.keys(ch.model_pins || {}), ...(pinExtras[ch.id] || [])])];
  const noKey = !(ch.keys || []).some((k) => k.enabled);
  const rows = models.length
    ? models.map((m) => pinPageRowHTML(ch, m)).join("")
    : '<p class="muted" style="font-size:12px;margin:4px 0">该渠道未声明模型：在下方输入模型 ID 后再设置固定。</p>';
  return `<div class="channel-card" data-ch="${esc(ch.id)}">
    <div class="head">
      <span class="badge ${ch.enabled ? "on" : "off"}">${ch.enabled ? "启用" : "停用"}</span>
      <span class="name">${esc(ch.name)}</span>
      <span class="muted">${esc(ch.base_url)}</span>
      ${noKey ? '<span class="badge warn" title="渠道没有启用的 key：探测/验证需要真实上游请求，请先在渠道页启用 key">无启用 key</span>' : ""}
    </div>
    ${rows}
    <div class="pin-row">
      <input placeholder="添加模型 ID…" style="width:240px" data-act="pinp-model-input" data-ch="${esc(ch.id)}">
      <button class="btn small" data-act="pinp-add-model" data-ch="${esc(ch.id)}">添加模型</button>
      <span class="muted" style="font-size:11px">渠道未声明模型列表时可在此添加要固定的模型</span>
    </div>
  </div>`;
}

// pinPageRowHTML 单个模型的固定配置行（改动即落盘）。
function pinPageRowHTML(ch, model) {
  const p = (ch.model_pins || {})[model] || {};
  const upstreams = p.upstreams || [], exclude = p.exclude || [], known = p.known || [];
  const configured = (upstreams.length || exclude.length || p.sort) ? '<span class="badge info" title="该模型已固定内部渠道，改动立即落盘生效">已配置</span>' : "";
  const probeInfo = p.pipeline
    ? `管线 <b>${esc(p.pipeline)}</b>${p.last_provider ? ` · 最近 <b>${esc(p.last_provider)}</b>` : ""}${p.probed_at ? ` · ${new Date(p.probed_at * 1000).toLocaleString()}` : ""}`
    : "未探测";
  const chipBtn = (list, i, slug) => {
    const up = list === "upstreams" && i > 0 ? `<button class="chip-btn" data-act="pinp-move" data-ch="${esc(ch.id)}" data-model="${esc(model)}" data-idx="${i}" data-dir="-1" title="上移">↑</button>` : "";
    const down = list === "upstreams" && i < upstreams.length - 1 ? `<button class="chip-btn" data-act="pinp-move" data-ch="${esc(ch.id)}" data-model="${esc(model)}" data-idx="${i}" data-dir="1" title="下移">↓</button>` : "";
    return `<span class="chip">${esc(slug)}${up}${down}<button class="chip-btn" data-act="pinp-remove" data-ch="${esc(ch.id)}" data-model="${esc(model)}" data-list="${list}" data-idx="${i}" title="移除">×</button></span>`;
  };
  const allKnown = [...new Set([...known, ...upstreams, ...exclude])];
  const knownChips = allKnown.map((k) => {
    const cls = upstreams.includes(k) ? "info" : (exclude.includes(k) ? "warn" : "");
    const tag = upstreams.includes(k) ? "固定" : (exclude.includes(k) ? "排除" : "未固定");
    return `<span class="chip clickable" data-act="pinp-toggle" data-ch="${esc(ch.id)}" data-model="${esc(model)}" data-slug="${esc(k)}" title="点击在 固定→排除→移除 间切换（点击即落盘生效）"><span class="badge ${cls}">${esc(k)}</span><span class="muted" style="font-size:11px">${tag}</span></span>`;
  }).join("");
  return `<div class="pin-model" data-model="${esc(model)}">
    <div class="pin-row">
      <span class="rk-name">${esc(model)}</span> ${configured}
      <select data-act="pinp-mode" data-ch="${esc(ch.id)}" data-model="${esc(model)}" style="width:auto" title="严格固定：按固定列表逐个独占尝试（only=[渠道]）；固定优先：一条请求给出完整 order，由上游按序自选">
        <option value="strict" ${p.mode !== "preferred" ? "selected" : ""}>严格固定</option>
        <option value="preferred" ${p.mode === "preferred" ? "selected" : ""}>固定优先（order）</option>
      </select>
      <select data-act="pinp-sort" data-ch="${esc(ch.id)}" data-model="${esc(model)}" style="width:auto" title="排序指标（direct 管线自动映射为 price/latency/throughput）">
        <option value="">不排序</option>
        <option value="cost" ${p.sort === "cost" ? "selected" : ""}>价格最低</option>
        <option value="ttft" ${p.sort === "ttft" ? "selected" : ""}>首字最快</option>
        <option value="tps" ${p.sort === "tps" ? "selected" : ""}>吞吐最高</option>
      </select>
      <button class="btn small" data-act="pinp-probe" data-ch="${esc(ch.id)}" data-model="${esc(model)}" title="发两条小请求：识别管线类型与实际服务的渠道，并把 only 钉到不存在渠道让上游报出全部可用渠道（立即落盘）">探测</button>
      <button class="btn small" data-act="pinp-validate" data-ch="${esc(ch.id)}" data-model="${esc(model)}" title="对每个已知内部渠道发固定小请求，验证可用性">验证</button>
      <button class="btn small danger" data-act="pinp-clear" data-ch="${esc(ch.id)}" data-model="${esc(model)}" title="清除该模型的固定配置（探测产物保留）">清除</button>
      <span class="muted" style="font-size:11px" data-act="pinp-info" data-ch="${esc(ch.id)}" data-model="${esc(model)}">${probeInfo}</span>
    </div>
    <div class="pin-chips"><span class="muted" style="font-size:11px">固定顺序：</span>${upstreams.map((k, i) => chipBtn("upstreams", i, k)).join("") || '<span class="muted" style="font-size:11px">（无——上游随机路由）</span>'}
      <span class="muted" style="font-size:11px">排除：</span>${exclude.map((k, i) => chipBtn("exclude", i, k)).join("") || '<span class="muted" style="font-size:11px">（无）</span>'}</div>
    ${knownChips ? `<div class="pin-chips"><span class="muted" style="font-size:11px">已知渠道（点击切换 固定→排除→移除）：</span>${knownChips}</div>` : ""}
    <div class="pin-row"><input placeholder="手工添加渠道 slug…" style="width:200px" data-act="pinp-slug" data-ch="${esc(ch.id)}" data-model="${esc(model)}">
      <button class="btn small" data-act="pinp-add-up" data-ch="${esc(ch.id)}" data-model="${esc(model)}">加为固定</button>
      <button class="btn small" data-act="pinp-add-ex" data-ch="${esc(ch.id)}" data-model="${esc(model)}">加为排除</button>
      <span class="muted" style="font-size:11px" data-act="pinp-validate-out" data-ch="${esc(ch.id)}" data-model="${esc(model)}"></span></div>
  </div>`;
}

// pinSavedPin 有内容的判定（与后端 normalize 一致）：任一字段非空即保留条目。
function pinSavedPin(p) {
  if (!p) return false;
  return (p.upstreams || []).length > 0 || (p.exclude || []).length > 0 || !!p.sort ||
    !!p.pipeline || (p.known || []).length > 0 || !!p.canonical_slug || !!p.last_provider || !!p.probed_at;
}

// applyPinState 把 model-pin 保存/探测结果写回本地 STATE 并重渲染（不整页刷新）。
function applyPinState(chID, model, pin) {
  const ch = ((STATE && STATE.channels) || []).find((c) => c.id === chID);
  if (!ch) { renderPinPage(); return; }
  ch.model_pins = ch.model_pins || {};
  if (pinSavedPin(pin)) {
    ch.model_pins[model] = pin;
    pinExtras[chID] = (pinExtras[chID] || []).filter((m) => m !== model);
  } else {
    delete ch.model_pins[model];
  }
  renderPinPage();
}

// wirePinPage 绑定渠道固定页事件（innerHTML 重建后调用）。
function wirePinPage() {
  const wrap = $("#pinChannels");
  const args = (el) => ({ ch: el.dataset.ch, model: el.dataset.model });
  const slugInput = (chID, model) =>
    wrap.querySelector(`.pin-model[data-model="${CSS.escape(model)}"] [data-act="pinp-slug"]`);

  // 已知渠道 chip：固定→排除→移除 三态切换，每步立即落盘
  wrap.querySelectorAll('[data-act="pinp-toggle"]').forEach((chip) => chip.addEventListener("click", async () => {
    const { ch, model } = args(chip);
    const slug = chip.dataset.slug;
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    const upstreams = [...(p.upstreams || [])], exclude = [...(p.exclude || [])];
    let patch;
    if (upstreams.includes(slug)) {
      patch = { mode: p.mode || "strict", sort: p.sort || "", upstreams: upstreams.filter((x) => x !== slug), exclude: [...exclude, slug] };
    } else if (exclude.includes(slug)) {
      patch = { mode: p.mode || "strict", sort: p.sort || "", upstreams, exclude: exclude.filter((x) => x !== slug) };
    } else {
      patch = { mode: p.mode || "strict", sort: p.sort || "", upstreams: [...upstreams, slug], exclude };
    }
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, ...patch });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  // 固定/排除 chips 的移除与排序
  wrap.querySelectorAll('[data-act="pinp-remove"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    const patch = { mode: p.mode || "strict", sort: p.sort || "", upstreams: [...(p.upstreams || [])], exclude: [...(p.exclude || [])] };
    patch[b.dataset.list].splice(Number(b.dataset.idx), 1);
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, ...patch });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  wrap.querySelectorAll('[data-act="pinp-move"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    const upstreams = [...(p.upstreams || [])];
    const i = Number(b.dataset.idx), j = i + Number(b.dataset.dir);
    [upstreams[i], upstreams[j]] = [upstreams[j], upstreams[i]];
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, mode: p.mode || "strict", sort: p.sort || "", upstreams, exclude: [...(p.exclude || [])] });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  // 模式 / 排序
  wrap.querySelectorAll('[data-act="pinp-mode"]').forEach((sel) => sel.addEventListener("change", async () => {
    const { ch, model } = args(sel);
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, mode: sel.value, sort: p.sort || "", upstreams: [...(p.upstreams || [])], exclude: [...(p.exclude || [])] });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  wrap.querySelectorAll('[data-act="pinp-sort"]').forEach((sel) => sel.addEventListener("change", async () => {
    const { ch, model } = args(sel);
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, mode: p.mode || "strict", sort: sel.value, upstreams: [...(p.upstreams || [])], exclude: [...(p.exclude || [])] });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  // 手工 slug：加为固定 / 加为排除
  wrap.querySelectorAll('[data-act="pinp-add-up"], [data-act="pinp-add-ex"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    const slug = ((slugInput(ch, model) || {}).value || "").trim();
    if (!slug) { toast("先输入渠道 slug", true); return; }
    const p = ((STATE.channels.find((c) => c.id === ch) || {}).model_pins || {})[model] || {};
    let upstreams = [...(p.upstreams || [])], exclude = [...(p.exclude || [])];
    if (b.dataset.act === "pinp-add-up") { exclude = exclude.filter((x) => x !== slug); if (!upstreams.includes(slug)) upstreams.push(slug); }
    else { upstreams = upstreams.filter((x) => x !== slug); if (!exclude.includes(slug)) exclude.push(slug); }
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model, mode: p.mode || "strict", sort: p.sort || "", upstreams, exclude });
      applyPinState(ch, model, r.pin);
    } catch (e) { toast(e.message, true); }
  }));
  // 清除该模型固定配置（探测产物保留）
  wrap.querySelectorAll('[data-act="pinp-clear"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    try {
      const r = await api("PUT", `/admin/api/channels/${ch}/model-pin`, { model });
      applyPinState(ch, model, r.pin);
      toast("已清除该模型的固定配置");
    } catch (e) { toast(e.message, true); }
  }));
  // 探测：识别管线 + 收割内部渠道清单（结果立即落盘）
  wrap.querySelectorAll('[data-act="pinp-probe"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    b.disabled = true;
    try {
      const r = await api("POST", `/admin/api/channels/${ch}/probe-upstreams`, { model });
      const n = (r.known || []).length;
      if (n) toast(`探测完成：管线 ${r.pipeline || "未知"}，发现 ${n} 个内部渠道（已落盘）`);
      else toast("探测完成，但未发现内部渠道——上游未点名可用渠道，可手工输入渠道 slug", true);
      await loadState();
      renderPinPage();
    } catch (e) { toast(e.message, true); }
    finally { b.disabled = false; }
  }));
  // 验证：逐渠道测试，结果展示在该行
  wrap.querySelectorAll('[data-act="pinp-validate"]').forEach((b) => b.addEventListener("click", async () => {
    const { ch, model } = args(b);
    b.disabled = true;
    const out = wrap.querySelector(`.pin-model[data-model="${CSS.escape(model)}"] [data-act="pinp-validate-out"]`);
    const label = { ok: "可用", limited: "限流", bad: "不可用", auth: "鉴权失败", unknown: "未知" };
    try {
      const r = await api("POST", `/admin/api/channels/${ch}/validate-upstreams`, { model });
      if (out) out.innerHTML = r.results.map((x) =>
        `<span class="chip"><span class="badge ${x.status === "ok" ? "on" : (x.status === "limited" ? "warn" : "off")}">${label[x.status] || x.status}</span>${esc(x.upstream)} ${x.ms}ms</span>`).join("");
    } catch (e) { toast(e.message, true); }
    finally { b.disabled = false; }
  }));
  // 手工添加模型行（渠道未声明模型列表时）
  wrap.querySelectorAll('[data-act="pinp-add-model"]').forEach((b) => b.addEventListener("click", () => {
    const chID = b.dataset.ch;
    const input = wrap.querySelector(`[data-act="pinp-model-input"][data-ch="${CSS.escape(chID)}"]`);
    const model = (input.value || "").trim();
    if (!model) { toast("先输入模型 ID", true); return; }
    pinExtras[chID] = [...new Set([...(pinExtras[chID] || []), model])];
    renderPinPage();
  }));
}

$("#pinPageRefreshBtn").addEventListener("click", async () => {
  try { await loadState(); renderPinPage(); toast("已刷新"); }
  catch (e) { toast(e.message, true); }
});

// 代理池查找：按 ID（新格式引用）或 URL（旧内联格式）→ 池实体
function poolById(id) { return ((STATE && STATE.proxy_pools) || []).find((p) => p.id === id); }

// ---- 冷却状态 ----
// state.cooling: [{key_id, model, until_unix, left_ms}]，按 keyID 索引
function coolingByKeyID() {
  const m = {};
  for (const c of (STATE && STATE.cooling) || []) {
    if (!m[c.key_id] || c.left_ms > m[c.key_id].left_ms) m[c.key_id] = c;
  }
  return m;
}
function coolingBadge(cooling, keyID) {
  const c = cooling[keyID];
  if (!c) return "";
  const left = c.left_ms > 0 ? Math.round(c.left_ms / 1000) : 0;
  const scope = c.model ? `（${esc(c.model)}）` : "";
  return `<span class="badge warn" title="该 key 因上游故障处于冷却中，网关转发会跳过它；渠道测试成功或点「解除冷却」可立即恢复">冷却中${scope} · 剩 ${fmtLeft(left)}</span>`;
}

// keyCoolDetail 某 key 的冷却明细（渠道卡片与编辑弹窗共用）：null = 无生效冷却。
//   whole=true → 整 key 冷却（key 粒度渠道，或 key_model 渠道残留的整体条目）；
//   否则 cooled=[{model,left}] 为逐模型冷却，available 为渠道启用模型中未冷却
//   的部分；hasModelList 标记渠道是否声明了模型列表（未声明时无法计算可用集）。
function keyCoolDetail(ch, keyID) {
  const entries = ((STATE && STATE.cooling) || []).filter((c) => c.key_id === keyID && c.left_ms > 0);
  if (!entries.length) return null;
  const wholeEntry = entries.find((c) => !c.model);
  if (wholeEntry || ch.cooldown_scope !== "key_model") {
    const max = entries.reduce((a, b) => (b.left_ms > a.left_ms ? b : a));
    return { whole: true, left: Math.max(0, Math.round(max.left_ms / 1000)), cooled: [], available: [], hasModelList: false };
  }
  const cooled = entries
    .map((c) => ({ model: c.model, left: Math.max(0, Math.round(c.left_ms / 1000)) }))
    .sort((a, b) => a.model.localeCompare(b.model));
  const models = ch.models || [];
  const cooledSet = new Set(cooled.map((c) => c.model));
  return {
    whole: false,
    cooled,
    available: models.filter((m) => !cooledSet.has(m)),
    hasModelList: models.length > 0,
  };
}

// keyCoolHTML 渠道卡片上单个 key 的冷却明细：
//   - key 粒度渠道（或存在整体冷却条目）：沿用单个「冷却中 · 剩 X」徽标；
//   - (key, 模型) 粒度渠道：逐模型「冷却 m · 剩 X ×」徽标（× 只解除该模型），
//     并列出该 key 当前未被冷却、可正常路由的模型（可用模型）。
function keyCoolHTML(ch, keyID, cooling) {
  const d = keyCoolDetail(ch, keyID);
  if (!d) return "";
  if (d.whole) return coolingBadge(cooling, keyID);
  const chips = d.cooled.map((c) =>
    `<span class="badge warn" title="该 (key, 模型) 因上游 429 处于冷却中，转发此模型时网关会跳过该 key；其余模型不受影响">冷却 ${esc(c.model)} · 剩 ${fmtLeft(c.left)}
      <button class="btn small" data-act="clearcoolmodel" data-key="${esc(keyID)}" data-model="${esc(c.model)}" title="只解除该 (key, 模型) 的冷却">×</button></span>`
  ).join("");
  let avail = "";
  if (d.hasModelList) {
    avail = `<span class="pname" title="该 key 当前未被冷却、可正常路由的模型">可用模型 ${d.available.length ? "：" + esc(d.available.join("、")) : "：无（全部冷却中）"}</span>`;
  }
  return chips + avail;
}

// fmtLeft 冷却剩余时间：秒 → 1h2m3s 样式（不足 1 小时只显示分秒）。
function fmtLeft(sec) {
  if (sec < 60) return sec + "s";
  const m = Math.floor(sec / 60), s = sec % 60;
  if (m < 60) return `${m}m${s ? s + "s" : ""}`;
  const h = Math.floor(m / 60);
  return `${h}h${m % 60}m`;
}
function poolByURL(url) {
  const u = String(url || "").replace(/\/+$/, "");
  return ((STATE && STATE.proxy_pools) || []).find((p) => String(p.pool_url || "").replace(/\/+$/, "") === u);
}
function poolLabel(p) { return p ? `${p.name}（${p.pool_url}）` : ""; }

// 池下拉选项；当前绑定的池不在列表中时补一项（如池已被删除）
function poolOptions(curId) {
  const pools = (STATE && STATE.proxy_pools) || [];
  let opts = pools.map((p) =>
    `<option value="${esc(p.id)}" ${p.id === curId ? "selected" : ""}>${esc(poolLabel(p))}</option>`
  ).join("");
  if (curId && !pools.some((p) => p.id === curId)) {
    opts += `<option value="${esc(curId)}" selected>（未知池 ${esc(curId)}，请在代理池页检查）</option>`;
  }
  return opts;
}

function proxyDesc(p) {
  if (p.kind === "static") return p.url || "static";
  if (p.kind === "ipv6pool") {
    const pool = poolById(p.pool_id) || poolByURL(p.pool_url);
    const parts = [pool ? `池 ${poolLabel(pool)}` : (p.pool_url ? `池 ${p.pool_url}` : `池 ${p.pool_id || "(未绑定)"}`)];
    if (p.lease_id) parts.push(`租约 ${p.lease_id}`);
    else parts.push("租约(自动)");
    if (p.share) parts.push("跨渠道复用(同渠道各用IP/跨渠道可共用)");
    parts.push("网络错误/连续5xx自动换IP");
    if (p.rotate_interval_sec) parts.push(`${p.rotate_interval_sec}s换IP`);
    if (p.rotate_requests) parts.push(`${p.rotate_requests}次换IP`);
    return parts.join(", ");
  }
  return "直连";
}

// keyProxyDesc key 行的代理描述：key 自身配置 > 渠道级代理（继承）> 直连。
function keyProxyDesc(k, ch) {
  if (k.proxy && k.proxy.kind) return proxyDesc(k.proxy);
  if (k.proxy) return "直连（key 覆盖）";
  if (ch && ch.proxy && ch.proxy.kind) return "渠道代理: " + proxyDesc(ch.proxy);
  return "直连";
}

// ---- 渠道编辑器 ----
$("#addChannelBtn").addEventListener("click", () => openChannelEditor({
  id: "", name: "", group: "", base_url: "", models_url: "", endpoint_type: "chat", models: [], headers: {},
  rewrite_reasoning: false, cooldown_scope: "key", auto_probe: false, enabled: true, keys: [],
}));

function openChannelEditor(ch) {
  editChannel = ch;
  $("#channelModalTitle").textContent = ch.id ? "编辑渠道：" + ch.name : "新建渠道";
  $("#chName").value = ch.name || "";
  $("#chGroup").value = ch.group || "";
  $("#chBaseURL").value = ch.base_url || "";
  $("#chModelsURL").value = ch.models_url || "";
  $("#chEndpointType").value = ch.endpoint_type || "chat";
  $("#chModels").value = (ch.models || []).join("\n");
  $("#chEnabled").checked = !!ch.enabled;
  $("#chRewrite").checked = !!ch.rewrite_reasoning;
  $("#chCooldownScope").value = ch.cooldown_scope === "key_model" ? "key_model" : "key";
  $("#chSchedule").value = ch.schedule || "";
  $("#chAutoProbe").checked = !!ch.auto_probe;
  renderChannelProxy(ch.proxy || null);
  renderHeaderRows(ch.headers || {});
  renderKeyBlocks(ch.keys || []);
  renderModelChips();
  renderCoolingKeys(ch);
  $("#channelErr").textContent = "";
  $("#channelModal").classList.remove("hidden");
}

// ---- 渠道级代理块（复用 key 代理选择 UI，无测试/换IP按钮） ----
function renderChannelProxy(proxy) {
  const wrap = $("#chChannelProxy");
  wrap.innerHTML = "";
  wrap.appendChild(proxyBlock(proxy));
}

// proxyBlock 渠道级代理配置块：无代理 + 固定代理 + IPv6 代理池。
function proxyBlock(p) {
  const div = document.createElement("div");
  div.className = "keyblock";
  const kind = (p && p.kind) || "";
  const curPoolId = (p && p.pool_id) || (p && (poolByURL(p.pool_url) || {}).id) || "";
  div.innerHTML = `
    <div class="proxybox">
      <div class="row" style="margin:4px 0">
        <label class="inline"><input type="radio" class="cpk" name="cpk-ch" value="" ${!kind ? "checked" : ""}> 无（key 未配置代理时直连）</label>
        <label class="inline"><input type="radio" class="cpk" name="cpk-ch" value="static" ${kind === "static" ? "checked" : ""}> 固定代理</label>
        <label class="inline"><input type="radio" class="cpk" name="cpk-ch" value="ipv6pool" ${kind === "ipv6pool" ? "checked" : ""}> IPv6 代理池</label>
      </div>
      <div class="cp-static ${kind === "static" ? "" : "hidden"}">
        <label>代理 URL <input class="cp-url" placeholder="http://user:pass@host:port 或 socks5://user:pass@host:port" value="${esc(p && p.url || "")}"></label>
      </div>
      <div class="cp-pool ${kind === "ipv6pool" ? "" : "hidden"}">
        <div class="grid3">
          <label>代理池
            <select class="cp-poolid">
              <option value="">（选择代理池…）</option>
              ${poolOptions(curPoolId)}
            </select>
          </label>
          <label class="inline" style="align-self:end;margin-bottom:10px" title="同「池+BaseURL」分组的 key 共用同一租约/IP；不勾选时渠道内每个 key 各自独立租约/IP"><input type="checkbox" class="cp-share" ${p && p.share ? "checked" : ""}> 跨渠道复用</label>
          <label class="inline" style="align-self:end;margin-bottom:10px"><input type="checkbox" class="cp-persist" ${p && p.persistent ? "checked" : ""}> 常驻租约（免空闲回收）</label>
        </div>
        <div class="row" style="margin:4px 0">
          <label class="inline">每 N 次请求换IP（0=关闭）<input class="cp-rotreq" type="number" min="0" value="${(p && p.rotate_requests) || 0}" style="width:90px"></label>
          <label class="inline">每 N 秒换IP（0=关闭）<input class="cp-rotsec" type="number" min="0" value="${(p && p.rotate_interval_sec) || 0}" style="width:90px"></label>
        </div>
      </div>
    </div>`;
  const sync = () => {
    const val = (div.querySelector("input.cpk:checked") || {}).value || "";
    div.querySelector(".cp-static").classList.toggle("hidden", val !== "static");
    div.querySelector(".cp-pool").classList.toggle("hidden", val !== "ipv6pool");
  };
  div.querySelectorAll("input.cpk").forEach((r) => r.addEventListener("change", sync));
  return div;
}

// collectChannelProxy 读取渠道级代理块；未启用任何代理时返回 null。
function collectChannelProxy() {
  const wrap = $("#chChannelProxy");
  const kind = (wrap.querySelector("input.cpk:checked") || {}).value || "";
  if (kind === "static") {
    return { kind: "static", url: wrap.querySelector(".cp-url").value.trim() };
  }
  if (kind === "ipv6pool") {
    return {
      kind: "ipv6pool",
      pool_id: wrap.querySelector(".cp-poolid").value,
      share: wrap.querySelector(".cp-share").checked,
      persistent: wrap.querySelector(".cp-persist").checked,
      rotate_interval_sec: parseInt(wrap.querySelector(".cp-rotsec").value, 10) || 0,
      rotate_requests: parseInt(wrap.querySelector(".cp-rotreq").value, 10) || 0,
    };
  }
  return null;
}

// renderCoolingKeys 编辑弹窗底部：该渠道冷却中的 key / (key, 模型) 一键解除。
// (key, 模型) 粒度渠道逐模型列出；解除时只清对应条目（整体冷却仍全清）。
function renderCoolingKeys(ch) {
  const wrap = $("#chCoolingKeys");
  const items = [];
  for (const k of ch.keys || []) {
    const d = keyCoolDetail(ch, k.id);
    if (!d) continue;
    if (d.whole) items.push({ key: k, model: "", left: d.left });
    else for (const c of d.cooled) items.push({ key: k, model: c.model, left: c.left });
  }
  if (!items.length) {
    wrap.innerHTML = '<span class="muted" style="font-size:12px">（无冷却中的 key）</span>';
    return;
  }
  wrap.innerHTML = items.map((it) => {
    const scope = it.model ? `（${esc(it.model)}）` : "";
    return `<span class="chip">${esc(it.key.name || it.key.id)}${scope} 剩 ${fmtLeft(it.left)}
      <button class="btn small" data-clearkey="${esc(it.key.id)}" data-clearmodel="${esc(it.model)}">解除</button></span>`;
  }).join("");
  wrap.querySelectorAll("[data-clearkey]").forEach((b) => b.addEventListener("click", async () => {
    try {
      const model = b.dataset.clearmodel || "";
      const r = model
        ? await api("POST", "/admin/api/cooling/clear-model", { key_id: b.dataset.clearkey, model })
        : await api("POST", "/admin/api/cooling/clear", { key_id: b.dataset.clearkey, channel_id: ch.id || "" });
      toast(r.cleared > 0 ? "已解除冷却" : "该冷却已过期");
      await loadState();
      renderCoolingKeys(STATE.channels.find((c) => c.id === ch.id) || ch);
    } catch (e) { toast(e.message, true); }
  }));
}

// 可用模型 chips 展示（跟随左侧文本框实时变化）；freeSet 非空时为对应模型标注「免费」
let freeModelSet = new Set();
function renderModelChips() {
  const el = $("#chModelList");
  const models = $("#chModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
  el.innerHTML = models.map((m) =>
    `<span class="chip">${esc(m)}${freeModelSet.has(m) ? '<i class="badge free">免费</i>' : ""}</span>`
  ).join("") || '<span class="muted" style="font-size:12px">（暂无模型：可手工填写，或保存后点「从上游拉取模型列表」）</span>';
}
$("#chModels").addEventListener("input", () => { renderModelChips(); freeModelSet = new Set(); });

// ---- 弹窗通用交互：Esc 关闭、点击遮罩关闭 ----
// 模态框统一由 .modal 容器承载；除内容卡片外的区域即遮罩。
function closeModal(el) { if (el) el.classList.add("hidden"); }
$$(".modal").forEach((m) => {
  m.addEventListener("click", (e) => { if (e.target === m) closeModal(m); });
});
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  const open = $$(".modal").filter((m) => !m.classList.contains("hidden"));
  if (open.length) closeModal(open[open.length - 1]); // 只关最上层
});
$$('[data-close="channelModal"]').forEach((b) => b.addEventListener("click", () => $("#channelModal").classList.add("hidden")));

// 自定义请求头
function renderHeaderRows(headers) {
  const wrap = $("#chHeaders");
  wrap.innerHTML = "";
  const entries = Object.entries(headers || {});
  if (!entries.length) entries.push(["", ""]);
  entries.forEach(([k, v]) => wrap.appendChild(headerRow(k, v)));
}
function headerRow(name, value) {
  const div = document.createElement("div");
  div.className = "row";
  div.innerHTML = `<input placeholder="头名（如 http-referer）" value="${esc(name)}" style="max-width:260px">
    <input placeholder="值（空=不发送）" value="${esc(value)}">
    <button class="btn small danger">删除</button>`;
  div.querySelector(".danger").addEventListener("click", () => div.remove());
  return div;
}
$("#addHeaderBtn").addEventListener("click", () => $("#chHeaders").appendChild(headerRow("", "")));

// key 块
function renderKeyBlocks(keys) {
  const wrap = $("#chKeys");
  wrap.innerHTML = "";
  keys.forEach((k) => wrap.appendChild(keyBlock(k, keyInheritInfo(k))));
  if (!keys.length) wrap.appendChild(keyBlock({ name: "", api_key: "", enabled: true }));
}

// keyInheritInfo 「跟随渠道」时展示当前实际生效的代理描述。
function keyInheritInfo(k) {
  if (k.proxy) return null;
  const p = editChannel && editChannel.proxy;
  if (p && p.kind) return { desc: keyProxyDesc(k, editChannel) };
  return { desc: "直连（渠道未设置代理）" };
}
// key 块序号：radio 组名必须块间唯一——同名 radio 在整个文档内互斥，
// 多 key 渠道会互相取消选中导致保存/测试读不到代理类型
let keyBlockSeq = 0;

function keyBlock(k, inheritInfo) {
  const div = document.createElement("div");
  div.className = "keyblock";
  div.dataset.keyId = k.id || ""; // 已保存 key 的真实 ID：测试/换IP/释放租约依赖
  // proxy == null 时 key 未单独配置代理（跟随渠道级设置）；
  // proxy 为空规格（kind 空对象）表示 key 显式直连，覆盖渠道级代理
  const followsChannel = !k.proxy;
  const kind = followsChannel ? "inherit" : ((k.proxy.kind) || "direct");
  // 当前绑定的池：新格式按 pool_id，旧格式按 pool_url 匹配池实体（后端已自动迁移）
  const curPoolId = (k.proxy && k.proxy.pool_id) || (k.proxy && (poolByURL(k.proxy.pool_url) || {}).id) || "";
  const pkName = "pk-" + (++keyBlockSeq);
  const inheritLabel = inheritInfo && inheritInfo.desc
    ? `<span class="muted" style="font-size:12px;align-self:center">（当前生效：${esc(inheritInfo.desc)}）</span>`
    : "";
  div.innerHTML = `
    <div class="head">
      <input class="kb-name" placeholder="名称" value="${esc(k.name || "")}">
      <input class="kb-key" placeholder="上游 API Key（无需鉴权的渠道可留空）" value="${esc(k.api_key || "")}" style="flex:1">
      <label class="inline"><input type="checkbox" class="kb-enabled" ${k.enabled ? "checked" : ""}> 启用</label>
      <button class="btn small danger kb-del">删除</button>
    </div>
    <div class="proxybox">
      <div class="row" style="margin:4px 0">
        <label class="inline" title="未单独配置代理的 key 使用渠道级代理（渠道级未设置时直连）；渠道级为代理池时每个 key 独立租约/出口 IP"><input type="radio" class="pk" name="${pkName}" value="inherit" ${followsChannel ? "checked" : ""}> 跟随渠道</label>
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="direct" ${kind === "direct" ? "checked" : ""}> 直连</label>
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="static" ${kind === "static" ? "checked" : ""}> 固定代理</label>
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="ipv6pool" ${kind === "ipv6pool" ? "checked" : ""}> IPv6 代理池</label>
        <span class="spacer"></span>
        ${inheritLabel}
        <button class="btn small kb-test">测试</button>
        <button class="btn small kb-rotate">换IP</button>
        <button class="btn small kb-release">释放租约</button>
      </div>
      <div class="kb-static ${kind === "static" ? "" : "hidden"}">
        <label>代理 URL <input class="kb-url" placeholder="http://user:pass@host:port 或 socks5://user:pass@host:port" value="${esc(k.proxy && k.proxy.url || "")}"></label>
      </div>
      <div class="kb-pool ${kind === "ipv6pool" ? "" : "hidden"}">
        <div class="grid3">
          <label>代理池
            <select class="kb-poolid">
              <option value="">（选择代理池…）</option>
              ${poolOptions(curPoolId)}
            </select>
          </label>
          <label>租约 ID（空=自动 gw-keyID） <input class="kb-leaseid" value="${esc(k.proxy && k.proxy.lease_id || "")}"></label>
          <span class="muted" style="font-size:12px;align-self:end;margin-bottom:10px" title="网络/代理错误，或连续 5xx 超过阈值（ROTATE_AFTER_5XX，默认 3）时自动换出口 IP">自动换IP：网络错误或连续 5xx 超阈值</span>
        </div>
        <div class="grid3">
          <label>每 N 次请求换IP（0=关闭） <input class="kb-rotreq" type="number" min="0" value="${(k.proxy && k.proxy.rotate_requests) || 0}"></label>
          <label class="inline" style="align-self:end;margin-bottom:10px"><input type="checkbox" class="kb-persist" ${k.proxy && k.proxy.persistent ? "checked" : ""}> 常驻租约（免空闲回收）</label>
          <label class="inline" style="align-self:end;margin-bottom:10px" title="同「池+BaseURL」分组的 key 共用同一租约/IP"><input type="checkbox" class="kb-share" ${k.proxy && k.proxy.share ? "checked" : ""}> 跨渠道复用</label>
        </div>
        <div class="row" style="margin:4px 0">
          <label class="inline">每 N 秒换IP（0=关闭）<input class="kb-rotsec" type="number" min="0" value="${(k.proxy && k.proxy.rotate_interval_sec) || 0}" style="width:90px"></label>
          <span class="spacer"></span>
          <span class="muted" style="font-size:12px">池的连接信息在「<a href="#" class="swap-tab" data-tab="leases">代理池</a>」页配置</span>
        </div>
      </div>
    </div>`;
  div.querySelector(".kb-del").addEventListener("click", () => div.remove());
  // 池选择下拉的「管理代理池」跳转
  div.querySelectorAll(".swap-tab").forEach((a) => a.addEventListener("click", (e) => {
    e.preventDefault();
    const btn = document.querySelector(`.tab[data-tab="${a.dataset.tab}"]`);
    if (btn) btn.click();
  }));
  const syncProxyKind = () => {
    const val = (div.querySelector("input.pk:checked") || {}).value || "inherit";
    div.querySelector(".kb-static").classList.toggle("hidden", val !== "static");
    div.querySelector(".kb-pool").classList.toggle("hidden", val !== "ipv6pool");
  };
  div.querySelectorAll("input.pk").forEach((r) => r.addEventListener("change", syncProxyKind));

  // 池操作（换IP/释放租约）：作用于已保存租约，须先保存
  const needSaved = () => {
    if (!editChannel.id) { toast("请先保存渠道，再执行池操作", true); return false; }
    const keyID = div.dataset.keyId || "";
    if (!keyID) { toast("请先保存渠道以生成 key ID", true); return false; }
    return true;
  };
  // 测试：以页面当前填写内容为准（支持未保存的渠道与修改，无需先保存）。
  // 只发送被点击 key 的配置（keys[0]），后端直接按内联定义发起请求。
  div.querySelector(".kb-test").addEventListener("click", async () => {
    const ch = collectChannelForm();
    const k = collectKeyForm(div);
    if (!k.enabled) { toast("请先勾选「启用」再测试该 key", true); return; }
    ch.keys = [k];
    const model = $("#chModels").value.split("\n")[0].trim() || "";
    try {
      const r = await api("POST", "/admin/api/testkey", { channel: ch, model });
      if (r.ok) toast(`测试成功 ${r.status}（${r.latency_ms}ms，经 ${r.proxy}）`);
      else {
        let msg = r.error || r.snippet || r.status;
        if (r.rotated && r.prior) msg = `换 IP 重试仍失败: ${msg}；首次: ${r.prior.error || r.prior.snippet || r.prior.status}`;
        toast("测试失败: " + msg, true);
      }
    } catch (e) { toast(e.message, true); }
  });
  div.querySelector(".kb-rotate").addEventListener("click", async () => {
    if (!needSaved()) return;
    try {
      const lease = await api("POST", "/admin/api/pool/rotate", { channel_id: editChannel.id, key_id: div.dataset.keyId });
      toast("已换 IP: " + (lease.ipv6 || "(未知)"));
    } catch (e) { toast(e.message, true); }
  });
  div.querySelector(".kb-release").addEventListener("click", async () => {
    if (!needSaved()) return;
    try {
      await api("POST", "/admin/api/pool/release", { channel_id: editChannel.id, key_id: div.dataset.keyId });
      toast("租约已释放");
    } catch (e) { toast(e.message, true); }
  });
  return div;
}
$("#addKeyBtn").addEventListener("click", () => $("#chKeys").appendChild(keyBlock({ name: "", api_key: "", enabled: true }, keyInheritInfo({}))));

// ---- key 批量导入：每行一个，支持 "key"、"名称|key"（也兼容 "名称:key"、
// "名称----key"、"key|备注"——名称取不像 key 的那一侧），# 开头为注释 ----
const BULK_NAME = "导入key";
// cleanKeyText 清理复制粘贴带入的杂质：零宽字符（肉眼不可见、trim 去不掉，
// 存进 key 后上游必然鉴权失败）、首尾成对引号（从 JSON/代码里复制）。
function cleanKeyText(s) {
  return s.replace(/[\u200B-\u200D\uFEFF\u00AD]/g, "").trim()
    .replace(/^["'`“”‘’]+/, "").replace(/["'`“”‘’]+$/, "").trim();
}
// keyHasJunk key 里出现即必然损坏的字符：空白（含全角空格）或非 ASCII 可打印
// 字符。与后端 validateAPIKey 对齐，导入时拦下并在提示里指明行号。
function keyHasJunk(s) { return /\s/.test(s) || /[^\x21-\x7E]/.test(s); }
// looksLikeKey 粗判一段文本像不像 API key（纯 ASCII 可打印、无空白、够长）。
// 仅用于分隔歧义时判断哪侧是 key（如 "sk-xxx|备注" 的 key 在前）。
function looksLikeKey(s) { return /^[\x21-\x7E]{6,}$/.test(s); }
$("#toggleBulkBtn").addEventListener("click", () => {
  const box = $("#bulkImportBox");
  box.classList.toggle("hidden");
  if (!box.classList.contains("hidden")) $("#bulkKeysInput").focus();
});
$("#bulkImportBtn").addEventListener("click", () => {
  const rawLines = $("#bulkKeysInput").value.split("\n");
  let imported = 0;
  const bad = [];
  for (let i = 0; i < rawLines.length; i++) {
    const line = cleanKeyText(rawLines[i]);
    if (!line || line.startsWith("#")) continue;
    // 全角分隔符与 Excel 复制的制表符归一成 "|" 再切名称；4 个以上连字符
    // 也视作分隔符（"名称----key"）——真实 key 里"-"都是单个出现，4 连不会误切
    const norm = line.replace(/：/g, ":").replace(/｜/g, "|").replace(/\t+/g, "|").replace(/-{4,}/g, "|");
    let name = "", key = norm;
    const pipe = norm.indexOf("|");
    const colonSp = norm.match(/^(.+?)\s*:\s+(\S+)$/); // "名称: key"（冒号后必须跟空白）
    if (pipe >= 0) {
      name = norm.slice(0, pipe).trim();
      key = norm.slice(pipe + 1).trim();
      // "sk-xxx|备注"：名称写在 key 后面时，两侧交换
      if (looksLikeKey(name) && !looksLikeKey(key)) { const t = name; name = key; key = t; }
    } else if (colonSp) {
      name = colonSp[1].trim();
      key = colonSp[2].trim();
    } else {
      const c = norm.lastIndexOf(":");
      if (c > 0) {
        const left = norm.slice(0, c).trim(), right = norm.slice(c + 1).trim();
        // key 本身可能含 ":"（如 id:secret）：仅当左像名称、右像 key 才切，
        // 否则整行视为 key——宁可保守不误切
        if (!looksLikeKey(left) && looksLikeKey(right)) { name = left; key = right; }
      }
    }
    // 尾随/开头残留的分隔符（"key：" 这类粘贴）不是 key 的一部分
    key = cleanKeyText(key).replace(/^[|:]+|[|:]+$/g, "");
    if (!key) { bad.push(`第 ${i + 1} 行没解析出 key`); continue; }
    if (keyHasJunk(key)) { bad.push(`第 ${i + 1} 行 key 含空白或非 ASCII 字符（复制带入？）`); continue; }
    $("#chKeys").appendChild(keyBlock({
      name: cleanKeyText(name) || `${BULK_NAME}${imported + 1}`,
      api_key: key,
      enabled: true,
    }));
    imported++;
  }
  const badMsg = bad.slice(0, 3).join("；") + (bad.length > 3 ? ` 等共 ${bad.length} 行` : "");
  if (!imported) {
    toast(bad.length ? `没有可导入的 key：${badMsg}` : "没有可导入的 key（每行一个，# 开头为注释）", true);
    return;
  }
  $("#bulkKeysInput").value = "";
  $("#bulkImportBox").classList.add("hidden");
  if (bad.length) toast(`已导入 ${imported} 个 key；${bad.length} 行有问题已跳过：${badMsg}`, true);
  else toast(`已导入 ${imported} 个 key，请点「保存」写入配置`);
});

// 从上游拉取模型列表（用渠道 key 鉴权）：dry-run 只取候选清单，
// 弹出勾选面板，用户勾选要启用的模型后点「确定」写入左侧列表
let fetchedCandidates = [];        // 本次拉取的候选（用于确定时区分手工项）
let fetchedFreeSet = new Set();

async function fetchModelsDryRun() {
  if (!editChannel.id) throw new Error("请先保存渠道（生成 key 后）再拉取模型列表");
  const r = await api("POST", `/admin/api/channels/${encodeURIComponent(editChannel.id)}/fetch-models`, {});
  return { fetched: r.fetched || r.models || [], free: r.free_models || [], key: r.key_used || "?" };
}

function fetchSetCheckbox(v) { v.checked = true; }
function fetchClearCheckbox(v) { v.checked = false; }

$("#fetchModelsBtn").addEventListener("click", async () => {
  const btn = $("#fetchModelsBtn");
  btn.disabled = true;
  btn.textContent = "拉取中…";
  $("#channelErr").textContent = "";
  try {
    const { fetched, free, key } = await fetchModelsDryRun();
    if (!fetched.length) throw new Error("上游返回的模型列表为空");
    fetchedCandidates = fetched;
    fetchedFreeSet = new Set(free);
    const existing = new Set($("#chModels").value.split("\n").map((s) => s.trim()).filter(Boolean));
    const list = $("#fetchList");
    list.innerHTML = fetched.map((m) =>
      `<label title="${esc(m)}"><input type="checkbox" value="${esc(m)}" ${existing.has(m) ? "checked" : ""}> ${esc(m)}${fetchedFreeSet.has(m) ? '<i class="badge free">免费</i>' : ""}</label>`
    ).join("");
    $("#fetchCount").textContent = fetched.length;
    $("#fetchPanel").classList.remove("hidden");
    toast(`已拉取 ${fetched.length} 个模型（key: ${key}），勾选要启用的后点「确定」`);
  } catch (e) {
    $("#channelErr").textContent = "拉取失败: " + e.message;
    toast("拉取失败: " + e.message, true);
  } finally {
    btn.disabled = false;
    btn.textContent = "⤓ 从上游拉取模型列表";
  }
});

$("#fetchAllBtn").addEventListener("click", () => $$("#fetchList input[type=checkbox]").forEach(fetchSetCheckbox));
$("#fetchNoneBtn").addEventListener("click", () => $$("#fetchList input[type=checkbox]").forEach(fetchClearCheckbox));

// 确定：勾选模型 + 手工添加的非候选模型 → 写入左侧列表
$("#fetchApplyBtn").addEventListener("click", () => {
  const chosen = $$("#fetchList input[type=checkbox]:checked").map((c) => c.value);
  const candSet = new Set(fetchedCandidates);
  const manual = $("#chModels").value.split("\n").map((s) => s.trim()).filter(Boolean).filter((m) => !candSet.has(m));
  const merged = [...new Set([...chosen, ...manual])];
  $("#chModels").value = merged.join("\n");
  freeModelSet = fetchedFreeSet;
  renderModelChips();
  $("#fetchPanel").classList.add("hidden");
  toast(`已启用 ${chosen.length} 个模型，请点「保存」写入配置`);
});

// collectKeyForm 从单个 key 块读取配置（不做校验）。
// proxy=null 表示跟随渠道级代理；{kind:"none"} 表示 key 显式直连（覆盖渠道级）。
function collectKeyForm(div) {
  const kind = (div.querySelector("input.pk:checked") || {}).value || "inherit";
  const k = {
    id: div.dataset.keyId || "",
    name: div.querySelector(".kb-name").value.trim(),
    api_key: cleanKeyText(div.querySelector(".kb-key").value),
    enabled: div.querySelector(".kb-enabled").checked,
    proxy: null,
  };
  if (kind === "direct") {
    k.proxy = { kind: "none" };
  } else if (kind === "static") {
    k.proxy = { kind: "static", url: div.querySelector(".kb-url").value.trim() };
  } else if (kind === "ipv6pool") {
    k.proxy = {
      kind: "ipv6pool",
      pool_id: div.querySelector(".kb-poolid").value,
      lease_id: div.querySelector(".kb-leaseid").value.trim(),
      persistent: div.querySelector(".kb-persist").checked,
      share: div.querySelector(".kb-share").checked,
      rotate_interval_sec: parseInt(div.querySelector(".kb-rotsec").value, 10) || 0,
      rotate_requests: parseInt(div.querySelector(".kb-rotreq").value, 10) || 0,
    };
  }
  return k;
}

// collectChannelForm 从编辑弹窗当前页面内容构建渠道对象（不做校验）。
// 测试与保存共用：测试「以页面填写内容为准」，而非已保存配置。
// model_pins（内部渠道固定）：已保存配置 + 弹窗内分区草稿合并——草稿只在
// 用户改动过的模型上覆盖，其余模型原样保留（探测产物同样跟随）。
function collectChannelForm() {
  const headers = {};
  $$("#chHeaders .row").forEach((row) => {
    const inputs = row.querySelectorAll("input");
    const name = inputs[0].value.trim(), value = inputs[1].value;
    if (name) headers[name] = value;
  });
  const models = $("#chModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
  const keys = $$("#chKeys .keyblock").map(collectKeyForm);
  const chProxy = collectChannelProxy();
  // 内部渠道固定不在此编辑（独立「渠道固定」页）：保存渠道时原样携带，
  // 避免整体替换渠道时清空已配置的 model_pins
  const modelPins = {};
  for (const [m, p] of Object.entries(editChannel.model_pins || {})) {
    if (p) modelPins[m] = JSON.parse(JSON.stringify(p));
  }
  return {
    id: editChannel.id || "",
    name: $("#chName").value.trim(),
    group: $("#chGroup").value.trim(),
    base_url: $("#chBaseURL").value.trim(),
    models_url: $("#chModelsURL").value.trim(),
    endpoint_type: $("#chEndpointType").value,
    models,
    headers,
    rewrite_reasoning: $("#chRewrite").checked,
    cooldown_scope: $("#chCooldownScope").value,
    schedule: $("#chSchedule").value,
    auto_probe: $("#chAutoProbe").checked,
    proxy: chProxy,
    model_pins: Object.keys(modelPins).length ? modelPins : undefined,
    enabled: $("#chEnabled").checked,
    keys,
  };
}

// 保存渠道
$("#channelSaveBtn").addEventListener("click", async () => {
  const ch = collectChannelForm();
  try {
    const saved = await api("PUT", "/admin/api/channels", ch);
    editChannel = saved;
    // 回填 key ID，便于后续池操作
    const savedKeys = saved.keys || [];
    $$("#chKeys .keyblock").forEach((div, i) => { div.dataset.keyId = savedKeys[i] ? savedKeys[i].id : ""; });
    toast("已保存");
    await loadState();
  } catch (e) {
    $("#channelErr").textContent = e.message;
  }
});

// ---- 通用密钥 ----
function renderGWKeys() {
  const tbody = $("#gwKeyTable tbody");
  const keys = (STATE && STATE.gateway_keys) || [];
  tbody.innerHTML = keys.map((k) => `<tr data-id="${esc(k.id)}">
    <td>${esc(k.name)}</td>
    <td><code class="gwkey">${esc(k.key)}</code> <button class="btn small" data-act="copy">复制</button></td>
    <td><label class="inline"><input type="checkbox" data-act="toggle" ${k.enabled ? "checked" : ""}> ${k.enabled ? "启用" : "停用"}</label></td>
    <td class="muted">${esc(fmtTimeShort(k.created_at))}</td>
    <td><button class="btn small danger" data-act="del">删除</button></td>
  </tr>`).join("");

  tbody.querySelectorAll('[data-act="copy"]').forEach((b) => b.addEventListener("click", () => {
    navigator.clipboard.writeText(b.parentElement.querySelector("code").textContent).then(() => toast("已复制"));
  }));
  tbody.querySelectorAll('[data-act="toggle"]').forEach((c) => c.addEventListener("change", async () => {
    const tr = c.closest("tr");
    const k = keys.find((x) => x.id === tr.dataset.id);
    try { await api("PUT", "/admin/api/gwkeys", { id: k.id, name: k.name, key: k.key, enabled: c.checked }); await loadState(); }
    catch (e) { toast(e.message, true); }
  }));
  tbody.querySelectorAll('[data-act="del"]').forEach((b) => b.addEventListener("click", async () => {
    const id = b.closest("tr").dataset.id;
    if (!confirm("删除该通用密钥？使用它的下游将立即 401。")) return;
    try { await api("DELETE", "/admin/api/gwkeys/" + id); toast("已删除"); await loadState(); }
    catch (e) { toast(e.message, true); }
  }));
}

$("#addGWKeyBtn").addEventListener("click", async () => {
  const name = prompt("密钥名称（如 sub2api、cline-desktop）:");
  if (!name) return;
  try {
    const r = await api("PUT", "/admin/api/gwkeys", { name, key: "", enabled: true });
    await loadState();
    toast("已生成密钥: " + (r.key ? r.key.key : ""));
  } catch (e) { toast(e.message, true); }
});

// ---- 请求日志 / 错误日志（分页 + 行展开 + 客户端过滤） ----
// 分页状态；页码、每页条数由后端 page/page_size 控制，自动刷新保持当前页。
// 每行可点击展开全部字段详情（记录 id 上的展开状态内存记忆，翻页/自动刷新保持）。
const logsState = { page: 1, size: 50, pages: 1, total: 0, recs: [], expanded: {}, q: "" };
const errsState = { page: 1, size: 50, pages: 1, total: 0, recs: [], expanded: {}, q: "" };
let errorsTimer = null;
const LOGS_EMPTY = "暂无请求记录（记录 /v1/* 网关接口请求，以及后台渠道测试发出的上游请求）。";
const ERRS_EMPTY = "暂无错误记录（失败请求单独记录在此，正常请求不会挤占）。";

function pagerInfo(st) {
  if (!st.total) return "共 0 条";
  return `第 ${st.page} / ${st.pages} 页 · 共 ${st.total} 条`;
}

// toBeijing 把 RFC3339 时间转成北京时间（UTC+8）的 ISO 字符串展示：
// 无论服务器/浏览器位于哪个时区，WebUI 里的时间一律显示北京时间。
// 后端下发带偏移的时间戳，先转瞬间再 +8h，用 UTC getter 输出，不依赖
// 浏览器本地时区与 Intl/ICU 支持。
function toBeijing(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const b = new Date(d.getTime() + 8 * 3600 * 1000);
  return b.toISOString(); // 2026-09-15T17:37:14.688Z
}
// fmtTimeFull 完整北京时间（含毫秒）。
function fmtTimeFull(t) { return toBeijing(t).replace("T", " ").replace(/\.\d+Z$/, ""); }
// fmtTimeShort 北京时间 YYYY-MM-DD HH:MM:SS（表格列内）。
function fmtTimeShort(t) { return toBeijing(t).replace("T", " ").slice(0, 19); }

// logSearchText 把一条记录的全部字段拍平成搜索文本（当前页全字段不区分大小写过滤）。
function logSearchText(r) {
  return [
    r.time, r.method, r.path, r.status, r.duration_ms, r.channel, r.key,
    r.model, r.prompt_tokens, r.completion_tokens, r.user, r.client_ip, r.error, r.id,
  ].filter((v) => v != null && v !== "").join(" ").toLowerCase();
}

// renderLogRows 渲染日志行（请求/错误两张表共用）。每行首列是展开开关：
// 点击行展开该条记录的全部字段详情。q 非空时对当前页做客户端过滤。
function renderLogRows(tbodySel, st, emptyTip) {
  const tbody = $(tbodySel);
  const q = (st.q || "").trim().toLowerCase();
  const all = st.recs || [];
  const recs = q ? all.filter((r) => logSearchText(r).includes(q)) : all;
  if (!all.length) {
    tbody.innerHTML = `<tr><td colspan="12" class="muted">${emptyTip}</td></tr>`;
    return;
  }
  if (!recs.length) {
    tbody.innerHTML = `<tr><td colspan="12" class="muted">当前页没有匹配过滤条件的日志（本页共 ${all.length} 条）。</td></tr>`;
    return;
  }
  tbody.innerHTML = recs.map((r) => {
    const id = r.id || "";
    const open = !!(id && st.expanded[id]);
    const err = r.error || "";
    const errCell = err
      ? `<td class="err" title="${esc(err)}">${esc(err)}</td>`
      : `<td class="err"></td>`;
    return `<tr class="${open ? "tog-open" : ""}" data-id="${esc(id)}">
      <td class="tw tr-toggle" title="点击展开/收起全部字段">${open ? "▾" : "▸"}</td>
      <td class="muted nowrap" title="${esc(r.time || "")}">${esc(fmtTimeShort(r.time))}</td>
      <td class="muted" title="${esc(r.path || "")}">${esc(r.path || "")}</td>
      <td><span class="badge ${r.status && r.status < 400 ? "on" : "off"}">${r.status || "ERR"}</span></td>
      <td class="nowrap">${r.duration_ms}ms</td>
      <td title="${esc(r.channel || "")}">${esc(r.channel || "")}</td>
      <td class="muted" title="${esc(r.key || "")}">${esc(r.key || "")}</td>
      <td title="${esc(r.model || "")}">${esc(r.model || "")}</td>
      <td class="muted nowrap">${r.prompt_tokens || 0} / ${r.completion_tokens || 0}</td>
      <td title="${esc(r.user || "")}">${esc(r.user || "")}</td>
      <td class="nowrap" title="${esc(r.client_ip || "")}">${esc(r.client_ip || "")}</td>
      ${errCell}
    </tr>
    ${open ? logDetailRow(id, r) : ""}`;
  }).join("");
  // 展开/收起：点击行任意处切换（展开区是独立行，选中复制不受影响）
  tbody.querySelectorAll("tr[data-id]").forEach((tr) => {
    tr.addEventListener("click", () => {
      const id = tr.dataset.id;
      if (st.expanded[id]) delete st.expanded[id]; else st.expanded[id] = true;
      renderLogRows(tbodySel, st, emptyTip);
    });
  });
}

// logDetailRow 展开区：该记录全字段（含 Method/BytesOut/完整时间/完整错误），
// 行内标题即列名，与摘要列一一对应，方便对照查看。
function logDetailRow(id, r) {
  const f = (k, v) => `<div class="f"><span class="k">${k}</span><span class="v">${esc(v ?? "")}</span></div>`;
  return `<tr class="log-detail-row"><td colspan="12"><div class="log-detail">
    <div class="g">
      ${f("ID", id)}
      ${f("时间", fmtTimeFull(r.time))}
      ${f("方法", r.method)}
      ${f("接口", r.path)}
      ${f("状态", r.status != null ? r.status : "")}
      ${f("耗时", r.duration_ms != null ? r.duration_ms + "ms" : "")}
      ${f("输出字节", r.bytes_out != null ? r.bytes_out + " B" : "")}
      ${f("渠道", r.channel)}
      ${f("key", r.key)}
      ${f("模型", r.model)}
      ${f("Tokens 入/出", (r.prompt_tokens || 0) + " / " + (r.completion_tokens || 0))}
      ${f("下游", r.user)}
      ${f("出口", r.client_ip)}
    </div>
    ${r.error ? `<div class="e">${esc(r.error)}</div>` : ""}
  </div></td></tr>`;
}

// fetchLogPage 拉取一页；当前页超出总页数时回退到最后一页重取一次。
// 记录缓存到状态对象（st.recs），过滤/展开/自动刷新共用。
async function fetchLogPage(url, st, pagerSel, tbodySel, emptyTip) {
  try {
    const data = await api("GET", `${url}?page=${st.page}&page_size=${st.size}`);
    st.total = data.total || 0;
    st.pages = Math.max(1, data.total_pages || 1);
    st.recs = data.records || [];
    if (st.page > st.pages) {
      st.page = st.pages;
      return fetchLogPage(url, st, pagerSel, tbodySel, emptyTip);
    }
    renderLogRows(tbodySel, st, emptyTip);
    $(pagerSel).textContent = pagerInfo(st);
    return true;
  } catch (e) { toast(e.message, true); return false; }
}

function syncPagerBtns(prefix, st) {
  $(`#${prefix}Prev`).disabled = st.page <= 1;
  $(`#${prefix}Next`).disabled = st.page >= st.pages;
}

async function refreshLogs() {
  await fetchLogPage("/admin/api/requests", logsState, "#logsPagerInfo", "#logTable tbody", LOGS_EMPTY);
  syncPagerBtns("logs", logsState);
}

function wirePager(prefix, st, refresh) {
  $(`#${prefix}Refresh`).addEventListener("click", refresh);
  $(`#${prefix}Prev`).addEventListener("click", () => {
    if (st.page > 1) { st.page--; refresh(); }
  });
  $(`#${prefix}Next`).addEventListener("click", () => {
    if (st.page < st.pages) { st.page++; refresh(); }
  });
  $(`#${prefix}PageSize`).addEventListener("change", (e) => {
    st.size = parseInt(e.target.value, 10) || 100;
    st.page = 1;
    refresh();
  });
  $(`#${prefix}Auto`).addEventListener("change", (e) => {
    if (prefix === "logs") {
      if (logsTimer) clearInterval(logsTimer);
      if (e.target.checked) logsTimer = setInterval(refresh, 5000);
    } else {
      if (errorsTimer) clearInterval(errorsTimer);
      if (e.target.checked) errorsTimer = setInterval(refresh, 5000);
    }
  });
}

wirePager("logs", logsState, refreshLogs);

async function refreshErrors() {
  await fetchLogPage("/admin/api/errors", errsState, "#errorsPagerInfo", "#errTable tbody", ERRS_EMPTY);
  syncPagerBtns("errors", errsState);
}
wirePager("errors", errsState, refreshErrors);

// 客户端过滤：输入即时过滤当前页已加载的记录；自动刷新只重新拉页、不清空搜索。
$("#logsSearch").addEventListener("input", () => { logsState.q = $("#logsSearch").value; renderLogRows("#logTable tbody", logsState, LOGS_EMPTY); });
$("#errorsSearch").addEventListener("input", () => { errsState.q = $("#errorsSearch").value; renderLogRows("#errTable tbody", errsState, ERRS_EMPTY); });

// 清空日志：只清内存环形缓冲，不影响用量统计
async function clearLogs(scope, st, refresh, label) {
  if (!confirm(`确定清空${label}？清空后不可恢复。`)) return;
  try {
    await api("POST", "/admin/api/logs/clear", { scope });
    st.page = 1;
    await refresh();
    toast(`${label}已清空`);
  } catch (e) { toast(e.message, true); }
}
$("#logsClear").addEventListener("click", () => clearLogs("requests", logsState, refreshLogs, "请求日志"));
$("#errorsClear").addEventListener("click", () => clearLogs("errors", errsState, refreshErrors, "错误日志"));

// ---- 渠道测试 ----
// 对指定渠道按模型逐个发起真实对话请求（后端按渠道 key 顺序故障转移），
// 结果逐行写入表格。单渠道串行执行，避免并发触发上游限流。
let testRunning = false;
let testAbort = false;

function testChannelSel() {
  const sel = $("#testChannel");
  const chans = (STATE && STATE.channels) || [];
  const cur = sel.value;
  sel.innerHTML = chans.map((c) =>
    `<option value="${esc(c.id)}">${esc(c.name)}${c.enabled ? "" : "（已停用）"}（key ${c.keys ? c.keys.length : 0}）</option>`
  ).join("") || '<option value="">（无渠道）</option>';
  if (chans.some((c) => c.id === cur)) sel.value = cur;
}

// testModelChips 渲染「已启用模型」chips：已在测试清单中的高亮（✓）并可点击移除，
// 未在清单中的点击加入——同一 chip 点击即切换加入/移除。
function testModelChips() {
  const wrap = $("#testModelChips");
  const ch = ((STATE && STATE.channels) || []).find((c) => c.id === $("#testChannel").value);
  const models = (ch && ch.models) || [];
  const chosen = new Set($("#testModels").value.split("\n").map((s) => s.trim()).filter(Boolean));
  wrap.innerHTML = models.length
    ? models.map((m) => {
        const has = chosen.has(m);
        return `<span class="chip clickable ${has ? "chip-on" : ""}" data-model="${esc(m)}"
          title="${has ? "点击从测试清单移除" : "点击加入测试清单"}">${has ? "✓" : "+"} ${esc(m)}</span>`;
      }).join("")
    : '<span class="muted" style="font-size:12px">渠道未配置启用模型</span>';
  wrap.querySelectorAll(".chip").forEach((chip) => chip.addEventListener("click", () => {
    const m = chip.dataset.model;
    const lines = $("#testModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
    $("#testModels").value = lines.includes(m)
      ? lines.filter((l) => l !== m).join("\n")  // 再点一次：从清单移除
      : [...lines, m].join("\n");                // 首次点击：加入清单
    testModelChips(); // 刷新 ✓/移除态
  }));
}

// 手工编辑测试清单文本框时同步 chips 的选中态（避免高亮与内容不一致）
$("#testModels").addEventListener("input", testModelChips);

function refreshTestTab() {
  testChannelSel();
  testModelChips();
}

$("#testChannel").addEventListener("change", testModelChips);
$("#testRefreshBtn").addEventListener("click", async () => {
  try { await loadState(); refreshTestTab(); toast("已刷新"); }
  catch (e) { toast(e.message, true); }
});

function testRow(res) {
  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td>${esc(res.model)}</td>
    <td class="t-status"></td>
    <td class="muted">${esc((res.key || "(未命名)"))}</td>
    <td class="t-code"></td>
    <td>${res.latency_ms != null ? res.latency_ms + "ms" : ""}</td>
    <td class="muted">${esc(res.proxy || "")}</td>
    <td class="t-snippet"></td>`;
  renderTestStatus(tr.querySelector(".t-status"), tr.querySelector(".t-code"), tr.querySelector(".t-snippet"), res);
  if (res.rotated && res.prior) {
    tr.title = `换 IP 后重试的结果；首次错误：${res.prior.error || res.prior.snippet || "HTTP " + (res.prior.status || 0)}`;
  }
  return tr;
}

function renderTestStatus(statusEl, codeEl, snippetEl, res) {
  if (res.running) {
    statusEl.innerHTML = '<span class="badge info">测试中…</span>';
    return;
  }
  statusEl.innerHTML = res.ok ? '<span class="badge on">通过</span>' : '<span class="badge off">失败</span>';
  if (res.rotated) {
    statusEl.innerHTML += ' <span class="muted" title="首次网络失败后已自动换出口 IP 重试">（换IP重试）</span>';
  }
  codeEl.innerHTML = res.status ? `<span class="badge ${res.status < 400 ? "on" : "off"}">${res.status}</span>` : '<span class="muted">—</span>';
  snippetEl.innerHTML = res.ok
    ? `<span class="muted">${esc(res.snippet || "")}</span>`
    : `<span class="err">${esc(res.error || res.snippet || "未知错误")}</span>`;
}

function testSummaryLine(pass, fail) {
  const el = $("#testSummary");
  el.classList.remove("hidden");
  el.innerHTML = fail === 0
    ? `<span class="badge on">全部通过</span><span class="muted">共 ${pass} 项</span>`
    : `<span class="badge off">失败 ${fail}</span><span class="muted">通过 ${pass} / 共 ${pass + fail} 项</span>`;
}

$("#testRunBtn").addEventListener("click", async () => {
  if (testRunning) { testAbort = true; return; }
  const ch = ((STATE && STATE.channels) || []).find((c) => c.id === $("#testChannel").value);
  if (!ch) { toast("没有可测试的渠道", true); return; }
  let models = $("#testModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
  if (!models.length) {
    models = (ch.models || []).slice();
    if (!models.length) { toast("渠道未配置启用模型，请在输入框指定要测试的模型", true); return; }
  }
  const onlyFirst = $("#testOnlyFirst").checked;
  if (onlyFirst && models.length > 3) {
    models = models.slice(0, 3);
    toast(`已按「仅测前 3 个模型」截取：${models.join("、")}`);
  }

  testRunning = true;
  testAbort = false;
  const btn = $("#testRunBtn");
  btn.textContent = "■ 停止";
  btn.classList.add("danger");
  $("#testTable tbody").innerHTML = "";
  $("#testSummary").classList.add("hidden");

  let pass = 0, fail = 0;
  for (const m of models) {
    if (testAbort) break;
    const placeholder = testRow({ model: m, running: true, key: "…", proxy: "…", latency_ms: null });
    $("#testTable tbody").appendChild(placeholder);
    try {
      const r = await api("POST", `/admin/api/channels/${encodeURIComponent(ch.id)}/test-model`, {
        models: m,
        first_only: $("#testScope").value === "first",
      });
      const results = r.results || [];
      if (!results.length) {
        placeholder.remove();
        testRowAndCount({ model: m, ok: false, error: "渠道没有启用的 key", latency_ms: null });
        fail++;
        continue;
      }
      // 替换占位行：失败的 key 逐行展示，最后一行是最终结论
      placeholder.remove();
      for (const res of results) {
        const row = testRow({ ...res, model: res.model || m, latency_ms: res.latency_ms ?? null });
        $("#testTable tbody").appendChild(row);
      }
      const last = results[results.length - 1];
      if (last.ok) pass++; else fail++;
    } catch (e) {
      placeholder.remove();
      testRowAndCount({ model: m, ok: false, error: e.message, latency_ms: null });
      fail++;
    }
  }
  testSummaryLine(pass, fail);
  testRunning = false;
  testAbort = false;
  btn.textContent = "▶ 运行测试";
  btn.classList.remove("danger");
});

function testRowAndCount(res) {
  $("#testTable tbody").appendChild(testRow(res));
}

// ---- 用量 ----
// 数字单位格式化：超过 1K 用 K、超过 1M 用 M（B/T 类推），使用单位时保留
// 两位小数；千以下保持原值。悬浮可见精确数值。
// 单位采用计数惯用法 K/M/B/T（B=billion）；G 是 SI 词头，留给字节类量纲。
function fmtNum(n) {
  n = Number(n) || 0;
  const abs = Math.abs(n);
  if (abs >= 1e12) return (n / 1e12).toFixed(2) + "T";
  if (abs >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(2) + "K";
  return String(n);
}

async function refreshUsage() {
  const win = $("#usageWindow").value;
  try {
    const u = await api("GET", "/admin/api/usage?window=" + win);
    $("#usageCards").innerHTML = `
      <div class="card"><div class="v" title="${u.requests}">${fmtNum(u.requests)}</div><div class="k">请求</div></div>
      <div class="card"><div class="v" title="${u.errors}">${fmtNum(u.errors)}</div><div class="k">错误</div></div>
      <div class="card"><div class="v" title="${u.prompt_tokens}">${fmtNum(u.prompt_tokens)}</div><div class="k">输入 tokens</div></div>
      <div class="card"><div class="v" title="${u.completion_tokens}">${fmtNum(u.completion_tokens)}</div><div class="k">输出 tokens</div></div>
      <div class="card"><div class="v" title="${u.total_tokens}">${fmtNum(u.total_tokens)}</div><div class="k">总 tokens</div></div>`;
    const table = (title, rows) => {
      if (!rows || !rows.length) return "";
      return `<h4>${title}</h4><table class="tbl"><thead><tr><th>名称</th><th>请求</th><th>错误</th><th>输入</th><th>输出</th><th>合计</th></tr></thead><tbody>` +
        rows.map((r) => `<tr><td>${esc(r.name)}</td><td title="${r.requests}">${fmtNum(r.requests)}</td><td title="${r.errors}">${fmtNum(r.errors)}</td><td title="${r.prompt_tokens}">${fmtNum(r.prompt_tokens)}</td><td title="${r.completion_tokens}">${fmtNum(r.completion_tokens)}</td><td title="${r.total_tokens}">${fmtNum(r.total_tokens)}</td></tr>`).join("") +
        `</tbody></table>`;
    };
    $("#usageTables").innerHTML =
      table("按下游密钥", u.by_user) + table("按渠道", u.by_channel) + table("按模型", u.by_model) + table("按上游 key", u.by_key);
  } catch (e) { toast(e.message, true); }
}
$("#usageWindow").addEventListener("change", refreshUsage);

// ---- 本地代理池 ----
async function refreshLeases() {
  try {
    const data = await api("GET", "/admin/api/pool/leases");
    const q = ($("#leaseSearch").value || "").trim().toLowerCase();
    let leases = data.leases || [];
    const total = leases.length;
    const pname = (l) => { const p = poolByURL(l.pool_url); return p ? p.name : l.pool_url; };
    if (q) {
      leases = leases.filter((l) =>
        [pname(l), l.pool_url, l.lease_id, l.ipv6, (l.groups || []).join(" ")]
          .join(" ").toLowerCase().includes(q));
    }
    const tbody = $("#leaseTable tbody");
    if (!leases.length) {
      tbody.innerHTML = `<tr><td colspan="9" class="muted">${q ? `无匹配「${esc(q)}」的租约（共 ${total} 条）。` : "当前没有租约。发起一次请求或点击「测试」后自动申请。"}</td></tr>`;
      return;
    }
    tbody.innerHTML = leases.map((l) => `<tr data-pool="${esc(l.pool_url)}" data-lease="${esc(l.lease_id)}">
      <td class="muted">${esc(pname(l))}</td>
      <td><code>${esc(l.lease_id)}</code>${l.shared ? ' <span class="badge info">共享</span>' : ""}</td>
      <td class="muted">${esc((l.groups || []).join("、"))}</td>
      <td>${esc(l.ipv6)}</td>
      <td>${l.multiplex ? "复用基础端口" : esc(l.port)}</td>
      <td>${l.multiplex ? "multiplex" : "per_ipv6"}</td>
      <td>${l.requests}</td>
      <td class="muted">${esc(fmtTimeShort(l.last_rotate))}</td>
      <td>
        <button class="btn small" data-act="lrotate">换IP</button>
        <button class="btn small danger" data-act="lrelease">释放</button>
      </td>
    </tr>`).join("");
    tbody.querySelectorAll('[data-act="lrotate"]').forEach((b) => b.addEventListener("click", async () => {
      const tr = b.closest("tr");
      try {
        const lease = await api("POST", "/admin/api/pool/rotate", { pool_url: tr.dataset.pool, lease_id: tr.dataset.lease });
        toast("已换 IP: " + (lease.ipv6 || "(未知)"));
        refreshLeases();
      } catch (e) { toast(e.message, true); }
    }));
    tbody.querySelectorAll('[data-act="lrelease"]').forEach((b) => b.addEventListener("click", async () => {
      const tr = b.closest("tr");
      if (!confirm(`释放租约 ${tr.dataset.lease}？共享租约的所有使用方将换到新出口（下次请求自动重新申请）。`)) return;
      try {
        await api("POST", "/admin/api/pool/release", { pool_url: tr.dataset.pool, lease_id: tr.dataset.lease });
        toast("已释放");
        refreshLeases();
      } catch (e) { toast(e.message, true); }
    }));
  } catch (e) { toast(e.message, true); }
}
$("#leasesRefresh").addEventListener("click", refreshLeases);
$("#leaseSearch").addEventListener("input", refreshLeases);
let leasesTimer = null;
$("#leasesAuto").addEventListener("change", (e) => {
  if (leasesTimer) clearInterval(leasesTimer);
  if (e.target.checked) leasesTimer = setInterval(refreshLeases, 5000);
});

// ---- 代理池管理 ----
let editPool = null; // 正在编辑的代理池（深拷贝）

function poolUsedByCount(poolId) {
  return ((STATE && STATE.channels) || []).reduce((n, ch) =>
    n + (ch.keys || []).filter((k) => k.proxy && k.proxy.kind === "ipv6pool" && k.proxy.pool_id === poolId).length, 0);
}

function renderPools() {
  const tbody = $("#poolTable tbody");
  const pools = (STATE && STATE.proxy_pools) || [];
  if (!pools.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">还没有代理池。点「新建代理池」配置连接信息（管理端、Token、SOCKS5 地址），再到渠道的 key 中选择使用。</td></tr>`;
    return;
  }
  tbody.innerHTML = pools.map((p) => `<tr data-id="${esc(p.id)}">
    <td><b>${esc(p.name)}</b></td>
    <td class="muted">${esc(p.pool_url)}</td>
    <td class="muted">${p.pool_token ? "已设置" : "—"}</td>
    <td class="muted">${esc(p.socks_host || "（默认池管理端）")}</td>
    <td>${poolUsedByCount(p.id)} 个 key</td>
    <td>
      <button class="btn small" data-act="ptest">测试</button>
      <button class="btn small" data-act="pedit">编辑</button>
      <button class="btn small danger" data-act="pdel">删除</button>
    </td>
  </tr>`).join("");
  tbody.querySelectorAll('[data-act="ptest"]').forEach((b) => b.addEventListener("click", async () => {
    const p = pools.find((x) => x.id === b.closest("tr").dataset.id);
    try {
      const st = await api("POST", "/admin/api/pool/test", { pool_id: p.id });
      toast(`测试成功：状态 ${st.status || "ok"}，租约 ${st.lease_count || 0}/${st.max_leases || "?"}`);
    } catch (e) { toast("测试失败: " + e.message, true); }
  }));
  tbody.querySelectorAll('[data-act="pedit"]').forEach((b) => b.addEventListener("click", () => {
    openPoolEditor(JSON.parse(JSON.stringify(pools.find((x) => x.id === b.closest("tr").dataset.id))));
  }));
  tbody.querySelectorAll('[data-act="pdel"]').forEach((b) => b.addEventListener("click", async () => {
    const p = pools.find((x) => x.id === b.closest("tr").dataset.id);
    const used = poolUsedByCount(p.id);
    if (!confirm(`删除代理池「${p.name}」？${used ? `当前有 ${used} 个 key 引用它，删除会被拒绝。` : "其遗留租约将被释放。"}`)) return;
    try {
      await api("DELETE", "/admin/api/pools/" + encodeURIComponent(p.id));
      toast("已删除");
      await loadState();
    } catch (e) { toast(e.message, true); }
  }));
}

function openPoolEditor(p) {
  editPool = p;
  $("#poolModalTitle").textContent = p.id ? "编辑代理池：" + p.name : "新建代理池";
  $("#plName").value = p.name || "";
  $("#plURL").value = p.pool_url || "";
  $("#plToken").value = p.pool_token || "";
  $("#plSocks").value = p.socks_host || "";
  $("#poolErr").textContent = "";
  $("#poolModal").classList.remove("hidden");
}

$("#poolAddBtn").addEventListener("click", () => openPoolEditor({}));
$$('[data-close="poolModal"]').forEach((b) => b.addEventListener("click", () => $("#poolModal").classList.add("hidden")));
$("#poolSaveBtn").addEventListener("click", async () => {
  const p = {
    id: editPool.id || "",
    name: $("#plName").value.trim(),
    pool_url: $("#plURL").value.trim(),
    pool_token: $("#plToken").value.trim(),
    socks_host: $("#plSocks").value.trim(),
  };
  try {
    await api("PUT", "/admin/api/pools", p);
    toast("已保存代理池");
    $("#poolModal").classList.add("hidden");
    await loadState();
  } catch (e) { $("#poolErr").textContent = e.message; }
});

// ---- 启动 ----
(async function init() {
  if (!TOKEN) { showLogin(); return; }
  try {
    showApp();
    await loadState();
  } catch {
    logout();
  }
})();
