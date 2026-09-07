// Admin REST API：供 WebUI 与脚本管理网关（渠道、上游 key、下游通用 key、
// 代理池操作、请求日志、用量统计）。全部端点要求管理员登录 token。
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// adminAPIHandler 分发 /admin/api/* 请求（Go 1.22 路由模式），整体要求管理员 token。
func adminAPIHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /admin/api/state", handleAdminState)
	mux.HandleFunc("PUT /admin/api/channels", handleAdminPutChannel)
	mux.HandleFunc("DELETE /admin/api/channels/{id}", handleAdminDeleteChannel)
	mux.HandleFunc("PUT /admin/api/pools", handleAdminPutPool)
	mux.HandleFunc("DELETE /admin/api/pools/{id}", handleAdminDeletePool)
	mux.HandleFunc("PUT /admin/api/gwkeys", handleAdminPutGWKey)
	mux.HandleFunc("DELETE /admin/api/gwkeys/{id}", handleAdminDeleteGWKey)
	mux.HandleFunc("POST /admin/api/pool/test", handleAdminPoolTest)
	mux.HandleFunc("POST /admin/api/pool/rotate", handleAdminPoolRotate)
	mux.HandleFunc("POST /admin/api/pool/release", handleAdminPoolRelease)
	mux.HandleFunc("GET /admin/api/pool/leases", handleAdminPoolLeases)
	mux.HandleFunc("POST /admin/api/testkey", handleAdminTestKey)
	mux.HandleFunc("POST /admin/api/channels/{id}/test-model", handleAdminTestModel)
	mux.HandleFunc("POST /admin/api/channels/{id}/fetch-models", handleAdminFetchModels)
	mux.HandleFunc("GET /admin/api/requests", handleAdminRequests)
	mux.HandleFunc("GET /admin/api/usage", handleAdminUsage)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := requireAdmin(w, r)
		if !ok {
			return
		}
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), adminUserKey{}, user)))
	})
}

// adminUserKey 上下文键：adminAPIHandler 鉴权通过后注入的登录用户名。
type adminUserKey struct{}

// adminUserFrom 取当前登录管理员用户名（未注入时为空）。
func adminUserFrom(ctx context.Context) string {
	user, _ := ctx.Value(adminUserKey{}).(string)
	return user
}

// requireAdmin 校验管理员 token。
func requireAdmin(w http.ResponseWriter, r *http.Request) (string, bool) {
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing admin token", "unauthorized")
		return "", false
	}
	user, err := verifyToken(token)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "invalid or expired token", "unauthorized")
		return "", false
	}
	if !isAdmin(user) {
		writeJSONError(w, http.StatusForbidden, "admin access required", "forbidden")
		return "", false
	}
	return user, true
}

// handleAdminState 返回 WebUI 所需的全部状态：渠道、下游 key、池租约缓存、总览。
func handleAdminState(w http.ResponseWriter, r *http.Request) {
	snap := store.Snapshot()
	type channelInfo struct {
		ID         string   `json:"id"`
		Name       string   `json:"name"`
		Group      string   `json:"group,omitempty"`
		Enabled    bool     `json:"enabled"`
		KeysTotal  int      `json:"keys_total"`
		KeysOn     int      `json:"keys_enabled"`
		HasPoolKey bool     `json:"has_pool_key"`
		Models     []string `json:"models,omitempty"`
	}
	chans := make([]channelInfo, 0, len(snap.Channels))
	for _, ch := range snap.Channels {
		ci := channelInfo{ID: ch.ID, Name: ch.Name, Group: ch.Group, Enabled: ch.Enabled, KeysTotal: len(ch.Keys)}
		for _, k := range ch.Keys {
			if k.Enabled {
				ci.KeysOn++
			}
			if k.Proxy != nil && k.Proxy.Kind == "ipv6pool" {
				ci.HasPoolKey = true
			}
		}
		ci.Models = ch.Models
		chans = append(chans, ci)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels":     snap.Channels,
		"gateway_keys": snap.GWKeys,
		"proxy_pools":  snap.ProxyPools,
		"leases":       leaseMgr.ListLeases(),
		"channel_info": chans,
	})
}

// handleAdminPutChannel 新增/整体更新渠道（含内嵌 keys），随后按全量配置
// Reconcile 回收不再使用的池租约。
func handleAdminPutChannel(w http.ResponseWriter, r *http.Request) {
	var ch Channel
	if err := json.NewDecoder(r.Body).Decode(&ch); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid channel json: "+err.Error(), "bad_request")
		return
	}
	if err := store.PutChannel(&ch); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	reconcileLeases()
	writeJSON(w, http.StatusOK, ch)
}

// reconcileLeases 按当前全部渠道配置构建在用 key 集合并回收孤儿租约。
func reconcileLeases() {
	live := map[string]map[string]livePoolKey{}
	for _, ch := range store.Snapshot().Channels {
		for _, k := range ch.Keys {
			if k.Proxy == nil || k.Proxy.Kind != "ipv6pool" {
				continue
			}
			spec, err := poolSpecReady(k.Proxy)
			if err != nil {
				log.Printf("reconcile: skip key %q: %v", k.Name, err)
				continue // 池不可解析（如已删除）→ 该 key 的分配会被当作孤儿回收
			}
			if live[spec.PoolURL] == nil {
				live[spec.PoolURL] = map[string]livePoolKey{}
			}
			live[spec.PoolURL][k.ID] = livePoolKey{Group: proxyGroup(ch, k), Shared: spec.Share}
		}
	}
	leaseMgr.Reconcile(live)
}

// handleAdminPutPool 新增/更新代理池（连接信息统一在此配置，渠道 key 只引用）。
func handleAdminPutPool(w http.ResponseWriter, r *http.Request) {
	var p ProxyPool
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid proxy pool json: "+err.Error(), "bad_request")
		return
	}
	if err := store.PutProxyPool(&p); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	log.Printf("admin saved proxy pool %q (%s)", p.Name, p.PoolURL)
	writeJSON(w, http.StatusOK, p)
}

// handleAdminDeletePool 删除代理池：仍被渠道 key 引用时拒绝；
// 删除成功后释放该池在网关持有的全部租约。
func handleAdminDeletePool(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	pool, deleted, usedBy := store.DeleteProxyPool(id)
	if usedBy != "" {
		writeJSONError(w, http.StatusBadRequest, "代理池仍被渠道使用（"+usedBy+"），请先解绑相关 key", "pool_in_use")
		return
	}
	if !deleted {
		writeJSONError(w, http.StatusNotFound, "proxy pool not found", "not_found")
		return
	}
	// 释放该池遗留的租约（best effort，逐个失败仅记日志）
	for _, l := range leaseMgr.ListLeases() {
		if l.PoolURL != pool.PoolURL {
			continue
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		if err := leaseMgr.ReleaseLease(ctx, l.PoolURL, l.LeaseID); err != nil {
			log.Printf("admin delete pool %q: release lease %s: %v", pool.Name, l.LeaseID, err)
		}
		cancel()
	}
	log.Printf("admin deleted proxy pool %q (%s)", pool.Name, pool.PoolURL)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// handleAdminDeleteChannel 删除渠道并释放其池租约。
func handleAdminDeleteChannel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := store.DeleteChannel(id); !ok {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return
	}
	reconcileLeases()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// handleAdminPutGWKey 新增/更新下游通用 key（Key 为空时自动生成）。
func handleAdminPutGWKey(w http.ResponseWriter, r *http.Request) {
	var k GWKey
	if err := json.NewDecoder(r.Body).Decode(&k); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid gwkey json: "+err.Error(), "bad_request")
		return
	}
	generated := false
	if strings.TrimSpace(k.Key) == "" {
		k.Key = newGWKeyValue()
		generated = true
	}
	if err := store.PutGWKey(&k); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": k, "generated": generated})
}

// handleAdminDeleteGWKey 删除下游 key。
func handleAdminDeleteGWKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := store.DeleteGWKey(id); !ok {
		writeJSONError(w, http.StatusNotFound, "gateway key not found", "not_found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": id})
}

// ---- 代理池操作 ----

// poolActionBody Admin 池操作的请求体。
// 指定 lease_id 时直接操作本地代理池中的该租约；否则按 channel_id+key_id 定位。
// pool_id 用于按代理池实体操作（测试连通性）；pool_url/pool_token 为旧格式直连参数。
type poolActionBody struct {
	PoolID    string `json:"pool_id"`
	PoolURL   string `json:"pool_url"`
	PoolToken string `json:"pool_token"`
	ChannelID string `json:"channel_id"`
	KeyID     string `json:"key_id"`
	LeaseID   string `json:"lease_id"`
}

// handleAdminPoolTest 测试池子连通性（返回池状态）。支持 pool_id（新格式，
// 从代理池实体取连接信息）或旧格式 pool_url/pool_token 直连。
func handleAdminPoolTest(w http.ResponseWriter, r *http.Request) {
	var body poolActionBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	poolURL, poolToken := strings.TrimSpace(body.PoolURL), strings.TrimSpace(body.PoolToken)
	if strings.TrimSpace(body.PoolID) != "" {
		pool, ok := store.proxyPoolByID(strings.TrimSpace(body.PoolID))
		if !ok {
			writeJSONError(w, http.StatusNotFound, "proxy pool not found", "not_found")
			return
		}
		poolURL, poolToken = pool.PoolURL, pool.PoolToken
	}
	if poolURL == "" {
		writeJSONError(w, http.StatusBadRequest, "pool_id or pool_url required", "bad_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	st, err := newPoolClient(poolURL, poolToken).Status(ctx)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "pool test failed: "+err.Error(), "pool_error")
		return
	}
	writeJSON(w, http.StatusOK, st)
}

// findKeySpec 取指定渠道 key 的代理配置（快照）。
func findKeySpec(channelID, keyID string) (*ProxySpec, string, bool) {
	_, k, ok := store.FindUpKey2(channelID, keyID)
	if !ok {
		return nil, "", false
	}
	if k.Proxy == nil || k.Proxy.Kind != "ipv6pool" {
		return nil, k.Name, false
	}
	return k.Proxy, k.Name, true
}

// handleAdminPoolRotate 手动换 IP（lease_id 直连本地代理池条目，或按 key 定位）。
func handleAdminPoolRotate(w http.ResponseWriter, r *http.Request) {
	var body poolActionBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	var (
		lease *PoolLease
		err   error
		desc  string
	)
	if lid := strings.TrimSpace(body.LeaseID); lid != "" {
		lease, err = leaseMgr.RotateCached(ctx, body.PoolURL, lid)
		desc = lid
	} else {
		var spec *ProxySpec
		var keyName string
		var ok bool
		spec, keyName, ok = findKeySpec(body.ChannelID, body.KeyID)
		if !ok {
			writeJSONError(w, http.StatusBadRequest, "key is not bound to an ipv6pool proxy", "bad_request")
			return
		}
		lease, err = leaseMgr.Rotate(ctx, spec, body.KeyID)
		if rspec, rerr := poolSpecReady(spec); rerr == nil {
			desc = keyName + " @ " + rspec.PoolURL
		} else {
			desc = keyName
		}
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "rotate failed: "+err.Error(), "pool_error")
		return
	}
	log.Printf("admin rotated lease %s: -> %s", desc, lease.IPv6)
	writeJSON(w, http.StatusOK, lease)
}

// handleAdminPoolRelease 手动释放租约（lease_id 直连本地代理池条目，或按 key 定位）。
func handleAdminPoolRelease(w http.ResponseWriter, r *http.Request) {
	var body poolActionBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	var err error
	var desc string
	if lid := strings.TrimSpace(body.LeaseID); lid != "" {
		err = leaseMgr.ReleaseLease(ctx, body.PoolURL, lid)
		desc = lid
	} else {
		var spec *ProxySpec
		var keyName string
		var ok bool
		spec, keyName, ok = findKeySpec(body.ChannelID, body.KeyID)
		if !ok {
			writeJSONError(w, http.StatusBadRequest, "key is not bound to an ipv6pool proxy", "bad_request")
			return
		}
		resolved, err := poolSpecReady(spec)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
			return
		}
		spec = resolved
		leaseID := leaseMgr.leaseIDForKey(spec, body.KeyID)
		err = leaseMgr.ReleaseLease(ctx, spec.PoolURL, leaseID)
		desc = keyName + " lease " + leaseID + " @ " + spec.PoolURL
	}
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "release failed: "+err.Error(), "pool_error")
		return
	}
	log.Printf("admin released lease %s", desc)
	writeJSON(w, http.StatusOK, map[string]any{"released": true})
}

// handleAdminPoolLeases 列出网关持有的池租约缓存。
func handleAdminPoolLeases(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"leases": leaseMgr.ListLeases()})
}

// findChannel 从快照中按 ID 查找渠道。
func findChannel(id string) *Channel {
	for _, c := range store.Snapshot().Channels {
		if c.ID == id {
			return c
		}
	}
	return nil
}

// handleAdminFetchModels 用渠道的 key 拉取上游模型列表。
// 默认（dry_run=1 或未带 replace）只返回候选列表供 WebUI 勾选启用，不写回渠道；
// replace=1 时全量替换写回渠道（兼容旧脚本）。返回拉取结果供 WebUI 展示。
func handleAdminFetchModels(w http.ResponseWriter, r *http.Request) {
	ch := findChannel(r.PathValue("id"))
	if ch == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), fetchModelsTimeout+10*time.Second)
	defer cancel()
	fetched, free, usedKey, err := fetchUpstreamModels(ctx, ch)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "fetch models failed: "+err.Error(), "upstream_error")
		return
	}

	// dry-run（默认）：不写回渠道，仅返回候选 + 当前已启用集合
	if r.URL.Query().Get("replace") != "1" {
		writeJSON(w, http.StatusOK, map[string]any{
			"channel_id":  ch.ID,
			"fetched":     fetched,
			"free_models": free,
			"key_used":    usedKey,
			"enabled":     ch.Models,
			"total":       len(fetched),
		})
		return
	}

	// 兼容旧行为：全量替换写回渠道
	ch.Models = normalizeModelList(fetched)
	if err := store.PutChannel(ch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "save channel: "+err.Error(), "internal")
		return
	}
	log.Printf("admin replaced models for channel %q via key %q: %d models (%d free)",
		ch.Name, usedKey, len(fetched), len(free))
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id":  ch.ID,
		"models":      ch.Models,
		"free_models": free,
		"key_used":    usedKey,
		"total":       len(ch.Models),
	})
}

// testTarget 指定渠道级测试的范围。
type testTarget struct {
	KeyID     string `json:"key_id"`     // 非空=只测该 key
	FirstOnly bool   `json:"first_only"` // 只用第一个启用的 key（不看后续）
	Models    string `json:"models"`     // 每行/逗号分隔；空=用渠道已启用模型（无则 "gpt-4o-mini"）
}

// testResult 一次测试请求的结果（HTTP 层不报错，全部通过 ok=false 表达失败）。
type testResult struct {
	OK        bool   `json:"ok"`
	ChannelID string `json:"channel_id"`
	Channel   string `json:"channel"`
	KeyID     string `json:"key_id"`
	Key       string `json:"key"`
	Model     string `json:"model"`
	Status    int    `json:"status,omitempty"`
	Proxy     string `json:"proxy,omitempty"`
	LatencyMS int64  `json:"latency_ms"`
	Snippet   string `json:"snippet,omitempty"`
	Error     string `json:"error,omitempty"`
}

// runTestOnce 用指定渠道 key 发一条最小测试请求（与客户端直连形态一致：
// 不带 max_tokens，标准 chat 请求体；responses 渠道由 doUpstreamRequest 自动转换）。
// 每次测试（含失败）都会写入请求记录，user 为发起测试的管理员。
func runTestOnce(ctx context.Context, ch *Channel, k *UpKey, model, msg, user string) testResult {
	res := testResult{ChannelID: ch.ID, Channel: ch.Name, KeyID: k.ID, Key: k.Name, Model: model}
	reqBody, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": msg}},
		"stream":   false,
	})
	cand := candidate{ch: ch, k: k}
	target := cand.chatTarget()
	begin := time.Now()
	var bytesOut int64
	defer func() { recordTestRequest(begin, target, user, ch.Name, k.Name, model, res, bytesOut) }()
	route, err := resolveProxy(&cand)
	if err != nil {
		res.Error = "proxy resolve failed: " + err.Error()
		return res
	}
	res.Proxy = route.describe()
	client := newUpstreamClient(route)
	start := time.Now()
	resp, err := doUpstreamRequest(ctx, client, &cand, reqBody, false, nil)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	bytesOut = int64(len(data))
	leaseMgr.RecordUse(cand.k.Proxy, cand.k.ID)
	res.Status = resp.StatusCode
	res.OK = resp.StatusCode >= 200 && resp.StatusCode < 300
	res.Snippet = truncate(string(data), 512)
	return res
}

// recordTestRequest 把一次渠道测试请求写入请求记录（与网关请求共用请求日志；
// 代理解析失败时无上游请求，以 status=0 记录并带错误信息）。不计入用量统计。
func recordTestRequest(start time.Time, target, user, channel, key, model string, res testResult, bytesOut int64) {
	if reqLog == nil {
		return
	}
	rec := RequestRecord{
		ID:         strconv.FormatInt(time.Now().UnixNano(), 36),
		Time:       start,
		Duration:   time.Since(start),
		DurationMs: time.Since(start).Milliseconds(),
		Method:     http.MethodPost,
		Path:       target,
		Status:     res.Status,
		BytesOut:   bytesOut,
		User:       user,
		Channel:    channel,
		Model:      model,
		Key:        maskKey(key + "@" + channel),
	}
	if res.Error != "" {
		rec.ErrMsg = res.Error
	} else if rec.Status >= 400 {
		rec.ErrMsg = truncate(res.Snippet, 200)
	}
	reqLog.Add(rec)
}

// parseTestModels 解析测试模型清单：支持逗号/空白/换行分隔，去重去空。
func parseTestModels(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	})
	out := make([]string, 0, len(fields))
	seen := map[string]bool{}
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" && !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	return out
}

// testKeyBody 单 key 测试请求。channel 非空时以内联定义（WebUI 页面当前
// 填写内容，keys 只带被测的那个 key）为准；否则按 channel_id+key_id 从存储读取。
type testKeyBody struct {
	ChannelID string   `json:"channel_id"`
	KeyID     string   `json:"key_id"`
	Model     string   `json:"model"`
	Message   string   `json:"message"`
	Channel   *Channel `json:"channel"`
}

// handleAdminTestKey 用指定渠道 key 发一条测试请求，验证上游与代理连通性。
// 请求体带 channel 时直接测试页面填写内容（支持未保存的渠道/修改，不落盘）；
// 否则按 channel_id+key_id 查存储。
func handleAdminTestKey(w http.ResponseWriter, r *http.Request) {
	var body testKeyBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	var ch *Channel
	var k *UpKey
	if body.Channel != nil {
		ich := body.Channel
		if len(ich.Keys) == 0 {
			writeJSONError(w, http.StatusBadRequest, "channel has no key to test", "bad_request")
			return
		}
		// 未保存的 key 赋固定临时 ID：池租约 ID（gw-<keyID>）幂等复用，
		// 渠道保存时由 Reconcile 作为孤儿分配回收
		if strings.TrimSpace(ich.Keys[0].ID) == "" {
			ich.Keys[0].ID = "preview"
		}
		if strings.TrimSpace(ich.Name) == "" {
			ich.Name = "（未保存）" // 测试不强制命名，日志展示用
		}
		if err := normalizeChannel(ich); err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
			return
		}
		ch, k = ich, ich.Keys[0]
	} else {
		var ok bool
		ch, k, ok = store.FindUpKey2(body.ChannelID, body.KeyID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, "key not found", "not_found")
			return
		}
	}
	model := strings.TrimSpace(body.Model)
	if model == "" {
		if len(ch.Models) > 0 {
			model = ch.Models[0]
		} else {
			model = "gpt-4o-mini"
		}
	}
	msg := body.Message
	if msg == "" {
		msg = "ping"
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, runTestOnce(ctx, ch, k, model, msg, adminUserFrom(r.Context())))
}

// handleAdminTestModel 渠道级测试：对指定模型集合，在渠道内按顺序挑选可用 key
// 发起真实对话请求（复用网关的故障转移语义），返回逐 (key, 模型) 结果供 WebUI 展示。
// key_id 非空时只测该 key。所有失败都以 ok=false 的结果返回，不用 HTTP 错误码。
func handleAdminTestModel(w http.ResponseWriter, r *http.Request) {
	var body testTarget
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ch := findChannel(r.PathValue("id"))
	if ch == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return
	}
	models := parseTestModels(body.Models)
	if len(models) == 0 {
		models = ch.Models
		if len(models) == 0 {
			models = []string{"gpt-4o-mini"}
		}
	}
	// 候选 key：按配置顺序（key_id 非空时仅该 key；first_only 时取第一个启用的）
	keys := make([]*UpKey, 0, len(ch.Keys))
	for _, k := range ch.Keys {
		if !k.Enabled {
			continue
		}
		if body.KeyID == "" || k.ID == body.KeyID {
			keys = append(keys, k)
		}
		if body.FirstOnly {
			break
		}
	}
	if len(keys) == 0 {
		writeJSONError(w, http.StatusBadRequest, "channel has no enabled key matching key_id", "bad_request")
		return
	}

	results := make([]testResult, 0, len(keys)*len(models))
	for _, m := range models {
		for _, k := range keys {
			ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
			res := runTestOnce(ctx, ch, k, m, "ping", adminUserFrom(r.Context()))
			cancel()
			results = append(results, res)
			if res.OK {
				break // 该模型已通过，无需继续其余 key
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id": ch.ID,
		"channel":    ch.Name,
		"models":     models,
		"results":    results,
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
