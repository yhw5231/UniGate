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
  if (btn.dataset.tab === "test") refreshTestTab();
  if (btn.dataset.tab === "usage") refreshUsage();
  if (btn.dataset.tab === "leases") { renderPools(); refreshLeases(); }
}));

// ---- 状态加载 ----
async function loadState() {
  STATE = await api("GET", "/admin/api/state");
  renderChannels();
  renderGWKeys();
  renderPools();
  if (!$("#tab-leases").classList.contains("hidden")) refreshLeases();
}

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
    const keyLines = keys.map((k) => {
      const p = k.proxy && k.proxy.kind ? proxyDesc(k.proxy) : "直连";
      return `<div class="key-line">
        <span class="badge ${k.enabled ? "on" : "off"}">${k.enabled ? "启用" : "停用"}</span>
        <span>${esc(k.name || "(未命名)")}</span>
        <span class="pname">代理: ${esc(p)}</span>
      </div>`;
    }).join("");
    const modelLine = (ch.models || []).length
      ? `<div class="key-line"><span class="pname">模型 ${ch.models.length} 个：${esc(ch.models.slice(0, 4).join("、"))}${ch.models.length > 4 ? " …" : ""}</span></div>`
      : "";
    return `<div class="channel-card" data-id="${esc(ch.id)}">
      <div class="head">
        <span class="badge ${ch.enabled ? "on" : "off"}">${ch.enabled ? "启用" : "停用"}</span>
        <span class="name">${esc(ch.name)}</span>
        ${ch.group ? `<span class="badge group">${esc(ch.group)}</span>` : ""}
        <span class="muted">${esc(ch.base_url)}</span>
        ${ch.endpoint_type === "responses" ? '<span class="badge info">responses</span>' : ""}
        ${ch.rewrite_reasoning ? '<span class="badge info">reasoning改写</span>' : ""}
        ${ch.cooldown_scope === "key_model" ? '<span class="badge info">按(Key,模型)冷却</span>' : ""}
        <span class="spacer"></span>
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
  wrap.querySelectorAll('[data-act="del"]').forEach((b) => b.addEventListener("click", async () => {
    const id = b.closest(".channel-card").dataset.id;
    const ch = chans.find((c) => c.id === id);
    if (!confirm(`删除渠道「${ch.name}」？其绑定的代理池租约将被释放。`)) return;
    try { await api("DELETE", "/admin/api/channels/" + id); toast("已删除"); await loadState(); }
    catch (e) { toast(e.message, true); }
  }));
}
$("#channelSearch").addEventListener("input", renderChannels);
$("#channelGroupFilter").addEventListener("change", renderChannels);

// 代理池查找：按 ID（新格式引用）或 URL（旧内联格式）→ 池实体
function poolById(id) { return ((STATE && STATE.proxy_pools) || []).find((p) => p.id === id); }
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
    if (p.rotate_on_net_err) parts.push("网络失败换IP");
    if (p.rotate_statuses && p.rotate_statuses.length) parts.push(`状态码${p.rotate_statuses.join("/")}换IP`);
    if (p.rotate_interval_sec) parts.push(`${p.rotate_interval_sec}s换IP`);
    if (p.rotate_requests) parts.push(`${p.rotate_requests}次换IP`);
    return parts.join(", ");
  }
  return "直连";
}

// ---- 渠道编辑器 ----
$("#addChannelBtn").addEventListener("click", () => openChannelEditor({
  id: "", name: "", group: "", base_url: "", models_url: "", endpoint_type: "chat", models: [], headers: {},
  rewrite_reasoning: false, cooldown_scope: "key", enabled: true, keys: [],
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
  renderHeaderRows(ch.headers || {});
  renderKeyBlocks(ch.keys || []);
  renderModelChips();
  $("#channelErr").textContent = "";
  $("#channelModal").classList.remove("hidden");
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
  keys.forEach((k) => wrap.appendChild(keyBlock(k)));
  if (!keys.length) wrap.appendChild(keyBlock({ name: "", api_key: "", enabled: true }));
}
// key 块序号：radio 组名必须块间唯一——同名 radio 在整个文档内互斥，
// 多 key 渠道会互相取消选中导致保存/测试读不到代理类型
let keyBlockSeq = 0;

function keyBlock(k) {
  const div = document.createElement("div");
  div.className = "keyblock";
  div.dataset.keyId = k.id || ""; // 已保存 key 的真实 ID：测试/换IP/释放租约依赖
  const kind = (k.proxy && k.proxy.kind) || "";
  // 当前绑定的池：新格式按 pool_id，旧格式按 pool_url 匹配池实体（后端已自动迁移）
  const curPoolId = (k.proxy && k.proxy.pool_id) || (k.proxy && (poolByURL(k.proxy.pool_url) || {}).id) || "";
  const pkName = "pk-" + (++keyBlockSeq);
  div.innerHTML = `
    <div class="head">
      <input class="kb-name" placeholder="名称" value="${esc(k.name || "")}">
      <input class="kb-key" placeholder="上游 API Key（无需鉴权的渠道可留空）" value="${esc(k.api_key || "")}" style="flex:1">
      <label class="inline"><input type="checkbox" class="kb-enabled" ${k.enabled ? "checked" : ""}> 启用</label>
      <button class="btn small danger kb-del">删除</button>
    </div>
    <div class="proxybox">
      <div class="row" style="margin:4px 0">
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="" ${!kind ? "checked" : ""}> 直连</label>
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="static" ${kind === "static" ? "checked" : ""}> 固定代理</label>
        <label class="inline"><input type="radio" class="pk" name="${pkName}" value="ipv6pool" ${kind === "ipv6pool" ? "checked" : ""}> IPv6 代理池</label>
        <span class="spacer"></span>
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
          <label>换IP状态码（逗号分隔，如 403,429） <input class="kb-rotstatus" value="${esc((k.proxy && k.proxy.rotate_statuses || []).join(","))}"></label>
        </div>
        <div class="grid3">
          <label>每 N 次请求换IP（0=关闭） <input class="kb-rotreq" type="number" min="0" value="${(k.proxy && k.proxy.rotate_requests) || 0}"></label>
          <label class="inline" style="align-self:end;margin-bottom:10px"><input type="checkbox" class="kb-persist" ${k.proxy && k.proxy.persistent ? "checked" : ""}> 常驻租约（免空闲回收）</label>
          <label class="inline" style="align-self:end;margin-bottom:10px" title="同「池+BaseURL」分组的 key 共用同一租约/IP"><input type="checkbox" class="kb-share" ${k.proxy && k.proxy.share ? "checked" : ""}> 跨渠道复用</label>
        </div>
        <div class="row" style="margin:4px 0">
          <label class="inline"><input type="checkbox" class="kb-rotnet" ${k.proxy && k.proxy.rotate_on_net_err ? "checked" : ""}> 网络失败自动换IP</label>
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
    const val = (div.querySelector("input.pk:checked") || {}).value || "";
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
      else toast("测试失败: " + (r.error || r.snippet || r.status), true);
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
$("#addKeyBtn").addEventListener("click", () => $("#chKeys").appendChild(keyBlock({ name: "", api_key: "", enabled: true })));

// ---- key 批量导入：每行一个，支持 "key" 或 "名称|key"（也兼容 "名称:key"），# 开头为注释 ----
const BULK_NAME = "导入key";
$("#toggleBulkBtn").addEventListener("click", () => {
  const box = $("#bulkImportBox");
  box.classList.toggle("hidden");
  if (!box.classList.contains("hidden")) $("#bulkKeysInput").focus();
});
$("#bulkImportBtn").addEventListener("click", () => {
  const lines = $("#bulkKeysInput").value.split("\n").map((s) => s.trim()).filter(Boolean);
  let imported = 0;
  for (const line of lines) {
    if (line.startsWith("#")) continue;
    let name = "", key = line;
    // 用分隔符切分名称与 key（允许分隔符两侧空白；key 本身不含 | 或 :）
    const m = line.match(/^(.+?)\s*[|:]\s*(\S+)$/);
    if (m) { name = m[1].trim(); key = m[2].trim(); }
    if (!key) continue;
    $("#chKeys").appendChild(keyBlock({
      name: name || `${BULK_NAME}${imported + 1}`,
      api_key: key,
      enabled: true,
    }));
    imported++;
  }
  if (!imported) { toast("没有可导入的 key（每行一个，# 开头为注释）", true); return; }
  $("#bulkKeysInput").value = "";
  $("#bulkImportBox").classList.add("hidden");
  toast(`已导入 ${imported} 个 key，请点「保存」写入配置`);
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
function collectKeyForm(div) {
  const kind = (div.querySelector("input.pk:checked") || {}).value || "";
  const k = {
    id: div.dataset.keyId || "",
    name: div.querySelector(".kb-name").value.trim(),
    api_key: div.querySelector(".kb-key").value.trim(),
    enabled: div.querySelector(".kb-enabled").checked,
    proxy: null,
  };
  if (kind === "static") {
    k.proxy = { kind: "static", url: div.querySelector(".kb-url").value.trim() };
  } else if (kind === "ipv6pool") {
    const statuses = div.querySelector(".kb-rotstatus").value.split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => !isNaN(n));
    k.proxy = {
      kind: "ipv6pool",
      pool_id: div.querySelector(".kb-poolid").value,
      lease_id: div.querySelector(".kb-leaseid").value.trim(),
      persistent: div.querySelector(".kb-persist").checked,
      share: div.querySelector(".kb-share").checked,
      rotate_on_net_err: div.querySelector(".kb-rotnet").checked,
      rotate_statuses: statuses,
      rotate_interval_sec: parseInt(div.querySelector(".kb-rotsec").value, 10) || 0,
      rotate_requests: parseInt(div.querySelector(".kb-rotreq").value, 10) || 0,
    };
  }
  return k;
}

// collectChannelForm 从编辑弹窗当前页面内容构建渠道对象（不做校验）。
// 测试与保存共用：测试「以页面填写内容为准」，而非已保存配置。
function collectChannelForm() {
  const headers = {};
  $$("#chHeaders .row").forEach((row) => {
    const inputs = row.querySelectorAll("input");
    const name = inputs[0].value.trim(), value = inputs[1].value;
    if (name) headers[name] = value;
  });
  const models = $("#chModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
  const keys = $$("#chKeys .keyblock").map(collectKeyForm);
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
    <td class="muted">${esc((k.created_at || "").replace("T", " ").slice(0, 19))}</td>
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

// ---- 请求日志 ----
async function refreshLogs() {
  try {
    const data = await api("GET", "/admin/api/requests?limit=200");
    const tbody = $("#logTable tbody");
    const recs = data.records || [];
    if (!recs.length) {
      tbody.innerHTML = `<tr><td colspan="11" class="muted">暂无请求记录（记录 /v1/* 网关接口请求，以及后台渠道测试发出的上游请求）。</td></tr>`;
      return;
    }
    tbody.innerHTML = recs.map((r) => `<tr>
      <td class="muted">${esc((r.time || "").replace("T", " ").slice(2, 19))}</td>
      <td class="muted">${esc(r.path || "")}</td>
      <td><span class="badge ${r.status && r.status < 400 ? "on" : "off"}">${r.status || "ERR"}</span></td>
      <td>${r.duration_ms}ms</td>
      <td>${esc(r.channel || "")}</td>
      <td>${esc(r.key || "")}</td>
      <td>${esc(r.model || "")}</td>
      <td class="muted">${r.prompt_tokens || 0} / ${r.completion_tokens || 0}</td>
      <td>${esc(r.user || "")}</td>
      <td>${esc(r.client_ip || "")}</td>
      <td class="err">${esc(r.error || "")}</td>
    </tr>`).join("");
  } catch (e) { toast(e.message, true); }
}
$("#logsRefresh").addEventListener("click", refreshLogs);
$("#logsAuto").addEventListener("change", (e) => {
  if (logsTimer) clearInterval(logsTimer);
  if (e.target.checked) logsTimer = setInterval(refreshLogs, 5000);
});

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

function testModelChips() {
  const wrap = $("#testModelChips");
  const ch = ((STATE && STATE.channels) || []).find((c) => c.id === $("#testChannel").value);
  const models = (ch && ch.models) || [];
  wrap.innerHTML = models.length
    ? models.map((m) => `<span class="chip clickable" data-model="${esc(m)}" title="点击加入测试清单">+ ${esc(m)}</span>`).join("")
    : '<span class="muted" style="font-size:12px">渠道未配置启用模型</span>';
  wrap.querySelectorAll(".chip").forEach((chip) => chip.addEventListener("click", () => {
    const m = chip.dataset.model;
    const lines = $("#testModels").value.split("\n").map((s) => s.trim()).filter(Boolean);
    if (!lines.includes(m)) {
      lines.push(m);
      $("#testModels").value = lines.join("\n");
    }
  }));
}

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
  return tr;
}

function renderTestStatus(statusEl, codeEl, snippetEl, res) {
  if (res.running) {
    statusEl.innerHTML = '<span class="badge info">测试中…</span>';
    return;
  }
  statusEl.innerHTML = res.ok ? '<span class="badge on">通过</span>' : '<span class="badge off">失败</span>';
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
async function refreshUsage() {
  const win = $("#usageWindow").value;
  try {
    const u = await api("GET", "/admin/api/usage?window=" + win);
    const fmt = (n) => Number(n).toLocaleString();
    $("#usageCards").innerHTML = `
      <div class="card"><div class="v">${fmt(u.requests)}</div><div class="k">请求</div></div>
      <div class="card"><div class="v">${fmt(u.errors)}</div><div class="k">错误</div></div>
      <div class="card"><div class="v">${fmt(u.prompt_tokens)}</div><div class="k">输入 tokens</div></div>
      <div class="card"><div class="v">${fmt(u.completion_tokens)}</div><div class="k">输出 tokens</div></div>
      <div class="card"><div class="v">${fmt(u.total_tokens)}</div><div class="k">总 tokens</div></div>`;
    const table = (title, rows) => {
      if (!rows || !rows.length) return "";
      return `<h4>${title}</h4><table class="tbl"><thead><tr><th>名称</th><th>请求</th><th>错误</th><th>输入</th><th>输出</th><th>合计</th></tr></thead><tbody>` +
        rows.map((r) => `<tr><td>${esc(r.name)}</td><td>${fmt(r.requests)}</td><td>${fmt(r.errors)}</td><td>${fmt(r.prompt_tokens)}</td><td>${fmt(r.completion_tokens)}</td><td>${fmt(r.total_tokens)}</td></tr>`).join("") +
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
      <td class="muted">${esc((l.last_rotate || "").replace("T", " ").slice(0, 19))}</td>
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
