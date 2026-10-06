// Admin REST API：供 WebUI 与脚本管理网关（渠道、上游 key、下游通用 key、
// 代理池操作、请求日志、用量统计）。全部端点要求管理员登录 token。
package main

import (
	"context"
	"encoding/json"
	"errors"
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
	mux.HandleFunc("GET /admin/api/client-profiles", handleAdminClientProfiles)
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
	mux.HandleFunc("POST /admin/api/cooling/clear", handleAdminClearCooling)
	mux.HandleFunc("POST /admin/api/cooling/clear-model", handleAdminClearCoolingModel)
	mux.HandleFunc("POST /admin/api/cooling/clear-all", handleAdminClearCoolingAll)
	mux.HandleFunc("POST /admin/api/channels/{id}/cooling/clear-all", handleAdminClearChannelCoolingAll)
	mux.HandleFunc("POST /admin/api/channels/{id}/cooling/clear-model", handleAdminClearChannelCoolingModel)
	mux.HandleFunc("POST /admin/api/route/cooling/clear-model", handleAdminClearRouteModelCooling)
	mux.HandleFunc("GET /admin/api/route", handleAdminRoute)
	mux.HandleFunc("PUT /admin/api/channels/{id}/model-pin", handleAdminPutModelUpstreamPin)
	mux.HandleFunc("POST /admin/api/channels/{id}/probe-upstreams", handleAdminProbeUpstreams)
	mux.HandleFunc("POST /admin/api/channels/{id}/validate-upstreams", handleAdminValidateUpstreams)
	mux.HandleFunc("POST /admin/api/channels/{id}/fetch-models", handleAdminFetchModels)
	mux.HandleFunc("POST /admin/api/fetch-models", handleAdminFetchModelsInline)
	mux.HandleFunc("GET /admin/api/requests", handleAdminRequests)
	mux.HandleFunc("GET /admin/api/errors", handleAdminErrors)
	mux.HandleFunc("POST /admin/api/logs/clear", handleAdminClearLogs)
	mux.HandleFunc("GET /admin/api/usage", handleAdminUsage)
	mux.HandleFunc("PUT /admin/api/settings", handleAdminPutSettings)
	mux.HandleFunc("PUT /admin/api/account", handleAdminPutAccount)

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
	snap := store.View() // 只读视图：本接口只序列化展示，无需深拷贝
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
			if spec := k.effectiveProxy(ch); spec != nil && spec.Kind == "ipv6pool" {
				ci.HasPoolKey = true
			}
		}
		ci.Models = ch.Models
		chans = append(chans, ci)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channels":        snap.Channels,
		"gateway_keys":    snap.GWKeys,
		"proxy_pools":     snap.ProxyPools,
		"leases":          leaseMgr.ListLeases(),
		"channel_info":    chans,
		"cooling":         cool.CoolingList(),
		"settings":        store.Settings(),
		"policy":          currentPolicy(),
		"route":           routeStatusData(""),
		"client_profiles": clientProfileCatalog(),
	})
}

// handleAdminClientProfiles 返回内置客户端协议头预设清单（WebUI 下拉与外部
// 调用方都从这里取；含可信度、出处与中文说明，便于判断哪些是已核实的指纹）。
func handleAdminClientProfiles(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"profiles": clientProfileCatalog()})
}

// handleAdminPutSettings 更新路由策略设置（全量替换；字段缺省 = 恢复环境
// 变量默认值）。保存后立即生效，无需重启。
func handleAdminPutSettings(w http.ResponseWriter, r *http.Request) {
	var set GatewaySettings
	if err := json.NewDecoder(r.Body).Decode(&set); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid settings json: "+err.Error(), "bad_request")
		return
	}
	if err := store.PutSettings(&set); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	applySettings(store.Settings())
	writeJSON(w, http.StatusOK, map[string]any{"policy": currentPolicy()})
}

// handleAdminClearCooling 手动解除上游 key 的冷却：body {key_id, channel_id?}。
// key 级与 (key, model) 级冷却全部清除，返回清除条数。用于上游已恢复（如额度
// 重置）时立即恢复路由，不必等冷却到期或跑一次渠道测试。
func handleAdminClearCooling(w http.ResponseWriter, r *http.Request) {
	var body struct {
		KeyID     string `json:"key_id"`
		ChannelID string `json:"channel_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	keyID := strings.TrimSpace(body.KeyID)
	if keyID == "" {
		writeJSONError(w, http.StatusBadRequest, "key_id required", "bad_request")
		return
	}
	// 带渠道 ID 时校验 key 归属，避免对已删除 key 的误操作静默成功
	if chID := strings.TrimSpace(body.ChannelID); chID != "" {
		if _, _, ok := store.FindUpKey2(chID, keyID); !ok {
			writeJSONError(w, http.StatusNotFound, "key not found in channel", "not_found")
			return
		}
	}
	cleared := cool.ClearKey(keyID)
	writeJSON(w, http.StatusOK, map[string]any{"key_id": keyID, "cleared": cleared})
}

// handleAdminClearCoolingModel 按 (key, model) 粒度精确解除一条冷却：
// body {key_id, model}。路由页对 key_model 渠道的单模型解除使用。
func handleAdminClearCoolingModel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		KeyID string `json:"key_id"`
		Model string `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	keyID := strings.TrimSpace(body.KeyID)
	model := strings.TrimSpace(body.Model)
	if keyID == "" || model == "" {
		writeJSONError(w, http.StatusBadRequest, "key_id and model required", "bad_request")
		return
	}
	cleared := 0
	if cool.ClearModel(keyID, model) {
		cleared = 1
	}
	writeJSON(w, http.StatusOK, map[string]any{"key_id": keyID, "model": model, "cleared": cleared})
}

// handleAdminClearCoolingAll 一键清空全部冷却（所有 key、所有模型粒度），
// 返回清除条数。上游整体恢复后的快速恢复操作。
func handleAdminClearCoolingAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cool.ClearAll()})
}

// ---- 渠道级 / 路由模型级清除冷却 ----
// WebUI 的层级操作：渠道卡片「清除冷却」可整渠道全清或按模型清本渠道所有 key；
// 路由页模型分组「清除冷却」清该模型在全部渠道全部 key 上的冷却。

// decodeRequiredModel 解析 {model} 请求体：字段必须存在。空串是合法取值——
// 冷却键的 model 部分为空 = 按 key 跨模型共享条目（渠道粒度下），路由页
// 「对全部模型放行」分组同理。
func decodeRequiredModel(w http.ResponseWriter, r *http.Request) (string, bool) {
	var body struct {
		Model *string `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return "", false
	}
	if body.Model == nil {
		writeJSONError(w, http.StatusBadRequest, "model required", "bad_request")
		return "", false
	}
	return strings.TrimSpace(*body.Model), true
}

// channelCoolPairs 某渠道全部 key 在给定模型下应清除的冷却键，含两部分：
//   - 与网关转发/路由视图一致的候选冷却键（channelUpstreamRows：渠道映射出的
//     每个上游模型各自一条，key 粒度渠道折叠为空串共享条目）；
//   - 该模型名的字面量条目（渠道粒度从 key_model 切回 key 后残留的按模型条目，
//     渠道卡片仍会显示，按模型清除时一并清掉）。
func channelCoolPairs(ch *Channel, model string) []cooldownPair {
	routeModel := resolveModelAlias(model)
	pairs := []cooldownPair{}
	for _, k := range ch.Keys {
		for _, row := range channelUpstreamRows(ch, model, routeModel) {
			pairs = append(pairs, cooldownPair{k.ID, row.cool})
		}
		if model != "" {
			pairs = append(pairs, cooldownPair{k.ID, model})
		}
	}
	return pairs
}

// modelCoolPairs 某模型（路由页分组名，可为全局别名）在全部渠道的冷却键：
// 展开规则与 routeStatusData 完全一致（渠道模型匹配 + channelUpstreamRows），
// 即路由页该分组上显示的每一条冷却，清完该分组必然显示为全部可用。
func modelCoolPairs(model string) []cooldownPair {
	snap := store.View()
	routeModel := resolveModelAlias(model)
	pairs := []cooldownPair{}
	for _, ch := range snap.Channels {
		matcher := newModelMatcher(append(append([]string{}, ch.Models...), ch.modelMapKeys()...))
		if model != "" && !matcher.match(model) && !matcher.match(routeModel) {
			continue
		}
		for _, k := range ch.Keys {
			for _, row := range channelUpstreamRows(ch, model, routeModel) {
				pairs = append(pairs, cooldownPair{k.ID, row.cool})
			}
		}
	}
	return pairs
}

// handleAdminClearChannelCoolingAll 清除某渠道全部 key 的全部冷却（所有模型粒度）。
func handleAdminClearChannelCoolingAll(w http.ResponseWriter, r *http.Request) {
	ch := findChannel(r.PathValue("id"))
	if ch == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return
	}
	ids := make([]string, 0, len(ch.Keys))
	for _, k := range ch.Keys {
		ids = append(ids, k.ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_id": ch.ID, "cleared": cool.ClearKeys(ids)})
}

// handleAdminClearChannelCoolingModel 按模型清除某渠道所有 key 的冷却：
// body {model}（空串 = 按 key 跨模型共享条目）。
func handleAdminClearChannelCoolingModel(w http.ResponseWriter, r *http.Request) {
	ch := findChannel(r.PathValue("id"))
	if ch == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return
	}
	model, ok := decodeRequiredModel(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id": ch.ID, "model": model, "cleared": cool.ClearPairs(channelCoolPairs(ch, model)),
	})
}

// handleAdminClearRouteModelCooling 清除某模型在全部渠道、全部 key 上的冷却：
// body {model}（路由页模型分组名，可为全局别名；空串 = 「对全部模型放行」分组）。
func handleAdminClearRouteModelCooling(w http.ResponseWriter, r *http.Request) {
	model, ok := decodeRequiredModel(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model": model, "cleared": cool.ClearPairs(modelCoolPairs(model)),
	})
}

// handleAdminRoute 路由页视图：按模型聚合候选 (渠道, key) 与实时状态。
// 可带 ?model=xxx 只看单个模型。
func handleAdminRoute(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, routeStatusData(strings.TrimSpace(r.URL.Query().Get("model"))))
}

// ---- 上游内部渠道固定（upstreampin.go）----

// upstreamPinBody PUT /admin/api/channels/{id}/model-pin 的请求体。
// upstreams/exclude/sort 全空 = 清除该模型的固定配置（保留探测产物）。
type upstreamPinBody struct {
	Model     string   `json:"model"`
	Mode      string   `json:"mode,omitempty"`      // strict（默认）/ preferred
	Upstreams []string `json:"upstreams,omitempty"` // 固定的内部渠道有序表
	Exclude   []string `json:"exclude,omitempty"`   // 排除的内部渠道
	Sort      string   `json:"sort,omitempty"`      // cost / ttft / tps / none
}

// pinChannelForModel 取出渠道快照并校验支持内部渠道固定（chat 端点、模型非空）。
func pinChannelForModel(w http.ResponseWriter, channelID, model string) (*Channel, bool) {
	ch := findChannel(channelID)
	if ch == nil {
		writeJSONError(w, http.StatusNotFound, "channel not found", "not_found")
		return nil, false
	}
	if ch.EndpointType == endpointResponses {
		writeJSONError(w, http.StatusBadRequest, "responses 端点渠道不支持内部渠道固定（探测/注入只适用于 chat 端点）", "bad_request")
		return nil, false
	}
	if strings.TrimSpace(model) == "" {
		writeJSONError(w, http.StatusBadRequest, "model required", "bad_request")
		return nil, false
	}
	return ch, true
}

// pickChannelKey 指定 key 或第一个启用的 key（无则 nil）。
func pickChannelKey(ch *Channel, keyID string) *UpKey {
	if keyID != "" {
		return ch.keyByID(keyID)
	}
	for _, k := range ch.Keys {
		if k.Enabled {
			return k
		}
	}
	return nil
}

// handleAdminPutModelUpstreamPin 保存/清除渠道上某模型的内部渠道固定配置：
// body {model, mode, upstreams, exclude, sort}（upstreams/exclude/sort 全空 = 清除，
// 探测产物保留）。保存立即生效（路由每请求实时取快照）。
func handleAdminPutModelUpstreamPin(w http.ResponseWriter, r *http.Request) {
	var body upstreamPinBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ch, ok := pinChannelForModel(w, r.PathValue("id"), body.Model)
	if !ok {
		return
	}
	model := strings.TrimSpace(body.Model)
	if ch.ModelPins == nil {
		ch.ModelPins = map[string]*ModelUpstreamPin{}
	}
	// 复用等价的已有键（历史配置里可能存的是上游原写法），避免同一模型两条固定配置
	key := pinKeyFor(ch, model)
	pin := ch.ModelPins[key]
	if pin == nil {
		pin = &ModelUpstreamPin{}
	}
	cleared := len(body.Upstreams) == 0 && len(body.Exclude) == 0 && strings.TrimSpace(body.Sort) == ""
	if cleared {
		// 清除固定配置，保留探测产物
		pin.Upstreams, pin.Exclude, pin.Sort = nil, nil, ""
	} else {
		pin.Mode = normalizeUpstreamPinMode(body.Mode)
		pin.Upstreams = normalizeModelList(body.Upstreams)
		pin.Exclude = normalizeModelList(body.Exclude)
		pin.Sort = normalizeSortValue(body.Sort)
	}
	if pin.normalize() {
		ch.ModelPins[key] = pin
	} else {
		delete(ch.ModelPins, key)
	}
	if err := store.PutChannel(ch); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	log.Printf("admin upstream pin %s: channel %q model %q mode=%s pinned=%v excluded=%v",
		map[bool]string{true: "cleared", false: "saved"}[cleared],
		ch.Name, model, pin.Mode, pin.Upstreams, pin.Exclude)
	writeJSON(w, http.StatusOK, map[string]any{"channel_id": ch.ID, "model": model, "pin": pin})
}

// handleAdminProbeUpstreams 探测渠道上某模型的内部渠道：
// body {model, key_id?}。发两条小请求——正常请求读 routing（管线类型/实际
// 服务渠道），only 钉到 __probe__ 的请求从上游报错里收割全部可用渠道；
// 结果写回渠道的 ModelPins 探测产物（固定配置原样保留）。
func handleAdminProbeUpstreams(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Model string `json:"model"`
		KeyID string `json:"key_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ch, ok := pinChannelForModel(w, r.PathValue("id"), body.Model)
	if !ok {
		return
	}
	k := pickChannelKey(ch, strings.TrimSpace(body.KeyID))
	if k == nil {
		writeJSONError(w, http.StatusBadRequest, "channel has no enabled key", "bad_request")
		return
	}
	model := strings.TrimSpace(body.Model)
	pin, err := probeChannelModel(ch, k, model)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "probe failed: "+err.Error(), "upstream_error")
		return
	}
	if ch.ModelPins == nil {
		ch.ModelPins = map[string]*ModelUpstreamPin{}
	}
	ch.ModelPins[pinKeyFor(ch, model)] = pin
	if err := store.PutChannel(ch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "save channel: "+err.Error(), "internal")
		return
	}
	log.Printf("admin probed upstreams: channel %q model %q pipeline=%s last=%s known=%v",
		ch.Name, model, pin.Pipeline, pin.LastProvider, pin.Known)
	// known 恒为数组：上游未点名可用渠道时前端不做 null/undefined 判断
	known := pin.Known
	if known == nil {
		known = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id":     ch.ID,
		"model":          model,
		"pipeline":       pin.Pipeline,
		"canonical_slug": pin.CanonicalSlug,
		"last_provider":  pin.LastProvider,
		"known":          known,
		"pin":            pin,
	})
}

// handleAdminValidateUpstreams 逐个测试内部渠道：body {model, key_id?,
// upstreams?}（缺省用探测到的 Known）。每个渠道发一条 only=[u] 的小请求，
// 按上游响应分类（ok/limited/bad/auth/unknown），结果随响应返回不落盘。
func handleAdminValidateUpstreams(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Model     string   `json:"model"`
		KeyID     string   `json:"key_id"`
		Upstreams []string `json:"upstreams,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ch, ok := pinChannelForModel(w, r.PathValue("id"), body.Model)
	if !ok {
		return
	}
	k := pickChannelKey(ch, strings.TrimSpace(body.KeyID))
	if k == nil {
		writeJSONError(w, http.StatusBadRequest, "channel has no enabled key", "bad_request")
		return
	}
	model := strings.TrimSpace(body.Model)
	pin := ch.upstreamPinFor(model)
	targets := normalizeModelList(body.Upstreams)
	if len(targets) == 0 && pin != nil {
		targets = pin.Known
	}
	if len(targets) == 0 {
		targets = pin.Upstreams
	}
	if len(targets) == 0 {
		writeJSONError(w, http.StatusBadRequest, "no known upstreams; run probe first or pass upstreams", "bad_request")
		return
	}
	pipeline := ""
	if pin != nil {
		pipeline = pin.Pipeline
	}
	results := make([]map[string]any, 0, len(targets))
	summary := map[string]int{}
	for _, u := range targets {
		res := validateUpstream(ch, k, model, pipeline, u)
		results = append(results, res)
		summary[res["status"].(string)]++
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id": ch.ID,
		"model":      model,
		"results":    results,
		"summary":    summary,
	})
}

// validateUpstream 用 only=[u] 的小请求测试单个内部渠道并分类结果。
func validateUpstream(ch *Channel, k *UpKey, model, pipeline, upstream string) map[string]any {
	reqBody := injectUpstreamPin(probeRequestBody(model, "hi", probeHarvestTokens), pipeline, upstream, nil, "", nil)
	if reqBody == nil {
		reqBody = probeRequestBody(model, "hi", probeHarvestTokens)
	}
	cand := newCandidate(ch, k, model)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.TestTimeout)
	defer cancel()
	start := time.Now()
	ans, err := sendProbeChat(ctx, &cand, reqBody)
	ms := time.Since(start).Milliseconds()
	res := map[string]any{"upstream": upstream, "ms": ms}
	if err != nil {
		res["status"] = "unknown"
		res["note"] = truncate(err.Error(), 200)
		return res
	}
	status, note := classifyUpstreamResponse(ans)
	res["status"] = status
	if note != "" {
		res["note"] = truncate(note, 200)
	}
	return res
}

// classifyUpstreamResponse 按（可能错误的）响应体分类内部渠道状态
// （对标 dsh-cline-pass 的 classifyUpstreamError）。
func classifyUpstreamResponse(body []byte) (status, note string) {
	var payload struct {
		Error   json.RawMessage `json:"error"`
		Choices []json.RawMessage `json:"choices"`
	}
	if json.Unmarshal(body, &payload) == nil && len(payload.Choices) > 0 {
		return "ok", ""
	}
	note = upstreamErrorText(payload.Error)
	if note == "" {
		note = strings.TrimSpace(string(body))
	}
	if note == "" {
		return "unknown", ""
	}
	lower := strings.ToLower(note)
	switch {
	case strings.Contains(lower, "empty response content"):
		return "ok", note
	case strings.Contains(lower, "429"), strings.Contains(lower, "rate-limited"),
		strings.Contains(lower, "rate limited"), strings.Contains(lower, "temporarily rate"):
		return "limited", note
	case strings.Contains(lower, "invalid_request"), strings.Contains(lower, "modelid"),
		strings.Contains(lower, "no allowed providers"), strings.Contains(lower, "no available providers"),
		strings.Contains(lower, "not found"), strings.Contains(lower, "unsupported"):
		return "bad", note
	case strings.Contains(lower, "unauthorized"), strings.Contains(lower, "re-authenticate"),
		strings.Contains(lower, "401"):
		return "auth", note
	}
	return "unknown", note
}

// upstreamErrorText 从 error 字段提取可读文本（字符串或 {message:...}）。
func upstreamErrorText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var obj struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &obj) == nil && obj.Message != "" {
		return obj.Message
	}
	return strings.TrimSpace(string(raw))
}

// handleAdminPutChannel 新增/整体更新渠道（含内嵌 keys），随后按全量配置
// Reconcile 回收不再使用的池租约。
func handleAdminPutChannel(w http.ResponseWriter, r *http.Request) {
	var ch Channel
	if err := json.NewDecoder(r.Body).Decode(&ch); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid channel json: "+err.Error(), "bad_request")
		return
	}
	old := findChannel(ch.ID) // nil = 新建渠道
	if err := store.PutChannel(&ch); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	reconcileLeases()
	// 新增模型/新增 key 尽早探测：部分上游的配额重置计时从首次调用起算，探测
	// 把它提前；范围与节流见 ChannelUpdated（按冷却粒度、入队限并发）
	probes.ChannelUpdated(old, &ch)
	writeJSON(w, http.StatusOK, ch)
}

// reconcileLeases 按当前全部渠道配置构建在用 key 集合并回收孤儿租约。
// 代理来源：key 自身配置优先，未配置时继承渠道级代理。
func reconcileLeases() {
	live := map[string]map[string]livePoolKey{}
	for _, ch := range store.View().Channels {
		for _, k := range ch.Keys {
			spec := k.effectiveProxy(ch)
			if spec == nil || spec.Kind != "ipv6pool" {
				continue
			}
			resolved, err := poolSpecReady(spec)
			if err != nil {
				log.Printf("reconcile: skip key %q: %v", k.Name, err)
				continue // 池不可解析（如已删除）→ 该 key 的分配会被当作孤儿回收
			}
			if live[resolved.PoolURL] == nil {
				live[resolved.PoolURL] = map[string]livePoolKey{}
			}
			live[resolved.PoolURL][k.ID] = livePoolKey{Group: proxyGroup(ch, k), Shared: resolved.Share}
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
// proxy 用于「编辑器里尚未保存的 key」：直接带内联代理配置 + 客户端生成的 key_id
// 操作（WebUI 新增/编辑渠道弹窗的「换IP / 释放租约」无需先保存）。
type poolActionBody struct {
	PoolID    string     `json:"pool_id"`
	PoolURL   string     `json:"pool_url"`
	PoolToken string     `json:"pool_token"`
	ChannelID string     `json:"channel_id"`
	KeyID     string     `json:"key_id"`
	LeaseID   string     `json:"lease_id"`
	Proxy     *ProxySpec `json:"proxy"`
	KeyName   string     `json:"key_name"`
}

// actionProxySpec 解析池操作的目标代理配置：请求体带内联 proxy 时直接用（编辑器
// 当前内容，支持未保存的渠道/key），否则按 channel_id+key_id 从存储快照取生效代理。
func actionProxySpec(body *poolActionBody) (spec *ProxySpec, keyName string, err error) {
	if body.Proxy != nil {
		if body.Proxy.Kind == "" {
			return nil, "", errors.New("key has no proxy configured")
		}
		if strings.TrimSpace(body.KeyID) == "" {
			return nil, "", errors.New("key id required for inline pool action")
		}
		name := strings.TrimSpace(body.KeyName)
		if name == "" {
			name = "（未保存）"
		}
		return body.Proxy, name, nil
	}
	spec, keyName, ok := findKeySpec(body.ChannelID, body.KeyID)
	if !ok {
		return nil, "", errors.New("key is not bound to an ipv6pool proxy")
	}
	return spec, keyName, nil
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
		pool, ok := store.ProxyPool(strings.TrimSpace(body.PoolID))
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

// findKeySpec 取指定渠道 key 生效的代理配置（快照；key 未配置时继承渠道级代理）。
func findKeySpec(channelID, keyID string) (*ProxySpec, string, bool) {
	ch, k, ok := store.FindUpKey2(channelID, keyID)
	if !ok {
		return nil, "", false
	}
	spec := k.effectiveProxy(ch)
	if spec == nil || spec.Kind != "ipv6pool" {
		return nil, k.Name, false
	}
	return spec, k.Name, true
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
		spec, keyName, err = actionProxySpec(&body)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
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
		spec, keyName, err = actionProxySpec(&body)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
			return
		}
		resolved, rerr := poolSpecReady(spec)
		if rerr != nil {
			writeJSONError(w, http.StatusBadRequest, rerr.Error(), "bad_request")
			return
		}
		leaseID := leaseMgr.leaseIDForKey(resolved, body.KeyID)
		err = leaseMgr.ReleaseLease(ctx, resolved.PoolURL, leaseID)
		desc = keyName + " lease " + leaseID + " @ " + resolved.PoolURL
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

// inlineChannel 校验并规范化请求体内联的渠道定义（WebUI 编辑器当前内容）：
// 未命名渠道给占位名，未保存的 key 给稳定占位 ID（池租约 ID 幂等复用，渠道保存时
// 由 Reconcile 作为孤儿分配回收），其余字段与保存时同一套归一化规则。
func inlineChannel(src *Channel) (*Channel, error) {
	if src == nil {
		return nil, errors.New("channel required")
	}
	ch := *src
	if strings.TrimSpace(ch.Name) == "" {
		ch.Name = "（未保存）"
	}
	if strings.TrimSpace(ch.BaseURL) == "" {
		return nil, errors.New("请先填写 Base URL")
	}
	keys := make([]*UpKey, 0, len(src.Keys))
	for i, k := range src.Keys {
		if k == nil {
			continue
		}
		kc := *k
		if strings.TrimSpace(kc.ID) == "" {
			// 首个 key 固定 "preview"（测试/池操作的租约 ID 幂等复用），其余 previewN
			kc.ID = "preview"
			if i > 0 {
				kc.ID = "preview" + strconv.Itoa(i+1)
			}
		}
		keys = append(keys, &kc)
	}
	ch.Keys = keys
	if err := normalizeChannel(&ch); err != nil {
		return nil, err
	}
	return &ch, nil
}

// fetchedModelsPayload 拉取结果的统一响应体：
//   - fetched：上游候选（**上游写法**，每个上游模型一条，同一对外名的多个写法各自
//     一条，WebUI 逐个勾选）；
//   - groups：候选按上游给的分组聚合（name/free/models/total，保持上游顺序；
//     Cline 的 recommended-models 会给出 recommended/free/clinePass/clineCloud）；
//   - free_models：免费候选（fetched 的子集，仅展示不落盘）；
//   - enabled / enabled_ids：渠道当前声明的模型（原文 / 对外名）；
//   - enabled_upstream：候选里当前已生效的上游写法（WebUI 预勾选；渠道未声明模型
//     时等于全部候选 = 全选）；
//   - stale：渠道当前声明、但上游本次未返回的模型（重建会移除，供提示）。
//
// 对外名与上游写法的关系是「多对一」：WebUI 用每个候选的 modelIdentity 显示对外名、
// 用 fetched 里的原文写回模型映射，从而对外只暴露一个名字、上游仍收到各自写法。
func fetchedModelsPayload(ch *Channel, fetched []fetchedModel, usedKey string) map[string]any {
	cands := fetchedCandidates(fetched)
	raws := fetchedRawModels(cands)
	enabledIDs := make([]string, 0, len(ch.Models))
	for _, m := range ch.Models {
		if id := exposedModelName(m); id != "" {
			enabledIDs = append(enabledIDs, id)
		}
	}
	enabledUpstream := enabledUpstreamModels(ch, cands)
	if enabledUpstream == nil {
		enabledUpstream = []string{}
	}
	return map[string]any{
		"channel_id":       ch.ID,
		"fetched":          raws,
		"groups":           fetchedModelGroups(cands),
		"free_models":      fetchedFreeModels(cands),
		"key_used":         usedKey,
		"enabled":          ch.Models,
		"enabled_ids":      enabledIDs,
		"enabled_upstream": enabledUpstream,
		"stale":            staleModels(ch.Models, raws),
		"total":            len(raws),
	}
}

// handleAdminFetchModelsInline 用「编辑器里当前填写的渠道内容」拉取上游模型列表，
// 不要求渠道已保存（WebUI 新增/编辑弹窗的「从上游拉取模型列表」，见 web/app.js）。
// 只读操作：不落盘，返回候选 + 免费标注 + 当前列表里上游已不返回的 stale。
func handleAdminFetchModelsInline(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Channel *Channel `json:"channel"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
		return
	}
	ch, err := inlineChannel(body.Channel)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fetchModelsTimeout+10*time.Second)
	defer cancel()
	fetched, usedKey, err := fetchUpstreamModels(ctx, ch)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "fetch models failed: "+err.Error(), "upstream_error")
		return
	}
	writeJSON(w, http.StatusOK, fetchedModelsPayload(ch, fetched, usedKey))
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
	fetched, usedKey, err := fetchUpstreamModels(ctx, ch)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "fetch models failed: "+err.Error(), "upstream_error")
		return
	}

	// dry-run（默认）：不写回渠道，仅返回候选 + 当前已启用集合 + 上游已不再
	// 返回的旧条目（stale，供 WebUI 提示「重建将移除这些模型」）
	if r.URL.Query().Get("replace") != "1" {
		writeJSON(w, http.StatusOK, fetchedModelsPayload(ch, fetched, usedKey))
		return
	}

	// 兼容旧行为：全量替换写回渠道。模型名归一化为对外名（上游报的
	// "cline-free/x:free" 存成 "x"），上游原写法记进 ModelMap 保证转发仍用
	// 上游认识的写法；同时丢弃已不在列表里的旧映射键（以上游为准）
	oldModels := ch.Models
	ch.Models = fetchedRawModels(fetchedCandidates(fetched))
	ch.normalizeDeclaredModels(true)
	if err := store.PutChannel(ch); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "save channel: "+err.Error(), "internal")
		return
	}
	log.Printf("admin replaced models for channel %q via key %q: %d models (%d free)",
		ch.Name, usedKey, len(ch.Models), len(fetchedFreeModels(fetchedCandidates(fetched))))
	// 新增模型尽早探测（替换只动模型列表，key 集合不变；同一套范围/节流/队列）
	oldCh := *ch
	oldCh.Models = oldModels
	probes.ChannelUpdated(&oldCh, ch)
	out := fetchedModelsPayload(ch, fetched, usedKey)
	out["models"] = ch.Models
	writeJSON(w, http.StatusOK, out)
}

// staleModels 返回渠道当前已启用、但上游本次未返回的模型（模型名归一化后
// 比较）：拉取重建时会移除这些条目，WebUI 据此提示用户。
func staleModels(enabled, fetched []string) []string {
	if len(enabled) == 0 {
		return nil
	}
	live := make(map[string]bool, len(fetched))
	for _, f := range fetched {
		if c := canonicalModel(f); c != "" {
			live[c] = true
		}
	}
	var out []string
	for _, m := range enabled {
		if c := canonicalModel(m); c != "" && live[c] {
			continue
		}
		out = append(out, m)
	}
	return out
}

// testTarget 指定渠道级测试的范围。scope:
//   - first（默认）：只测第一个启用的 key；
//   - all：全部启用的 key 逐个完整测试（不因某个 key 通过而跳过其余）；
//   - pick：只测 key_id 指定的 key。
// 旧字段 first_only=true 兼容为 first。Models 每行/逗号分隔；空=用渠道已启用
// 模型（无则 "gpt-4o-mini"）。
type testTarget struct {
	KeyID     string `json:"key_id"`     // scope=pick 时指定被测 key
	Scope     string `json:"scope"`      // first（默认）/ all / pick
	FirstOnly bool   `json:"first_only"` // 旧版兼容：true 等价 scope=first
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
	// Rotated 非空 = 网络失败自动换 IP 后重试，本条是重试结果（Prior 为首次结果）
	Rotated bool        `json:"rotated,omitempty"`
	Prior   *testResult `json:"prior,omitempty"`
	// proxyFailed 失败发生在代理解析/探测阶段（池基础设施问题，非"已连上
	// 出口后的网络错误"）——换 IP 无济于事，测试链路不重试。
	proxyFailed bool
	// 上游响应的 token 用量（仅用于请求日志的 Tokens 列，不回给 WebUI）
	promptTokens     int64
	completionTokens int64
}

// runTestOnce 用指定渠道 key 发一条最小测试请求（与客户端直连形态一致：
// 不带 max_tokens，标准 chat 请求体；responses 渠道由 doUpstreamRequest 自动转换）。
// 每次测试（含失败）都会写入请求记录，user 为发起测试的管理员。
func runTestOnce(ctx context.Context, ch *Channel, k *UpKey, model, msg, user string) testResult {
	res := testOnce(ctx, ch, k, model, msg, user)
	// 已连上出口后的网络错误（Status=0 且非代理解析失败）换 IP 重试一次，
	// 与网关真实转发语义一致；代理解析/探测失败是池基础设施问题，换 IP 无济于事
	if res.Status == 0 && !res.proxyFailed && isPoolKey(&candidate{ch: ch, k: k}) {
		rotateOnNetErr(&candidate{ch: ch, k: k})
		res2 := testOnce(ctx, ch, k, model, msg, user)
		res2.Rotated = true
		// Prior 必须指向首次结果的独立副本：直接 &res 会取到随后被 res=res2
		// 覆盖的同一变量地址，结构体自引用成环，json.Marshal 直接失败
		//（表现为 WebUI 测试报 "marshal failed"）。
		first := res
		res2.Prior = &first
		res = res2
	}
	if res.OK {
		// 测试是真实打通的请求：据此解除该 key 的存量冷却（key 级与 (key,model)
		// 级都清），避免「渠道测试通过、网关却因冷却继续 502」的错位
		cool.Clear(k.ID, "")
		cool.Clear(k.ID, model)
	}
	return res
}

func testOnce(ctx context.Context, ch *Channel, k *UpKey, model, msg, user string) testResult {
	res := testResult{ChannelID: ch.ID, Channel: ch.Name, KeyID: k.ID, Key: k.Name, Model: model}
	reqBody, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": msg}},
		"stream":   false,
	})
	cand := newCandidate(ch, k, model)
	target := cand.chatTarget()
	begin := time.Now()
	var bytesOut int64
	defer func() { recordTestRequest(begin, target, user, ch.Name, k.Name, model, res, bytesOut) }()
	// 测试链路带探测：池 SOCKS 出口不可达时秒级返回可读错误（真实转发不受影响）
	route, err := resolveProxyOpts(&cand, true)
	if err != nil {
		res.proxyFailed = true
		res.Error = "proxy resolve failed: " + err.Error()
		return res
	}
	res.Proxy = route.describe()
	client := newUpstreamClient(route)
	noteKeyCall(cand.k.ID, model) // 管理员测试也是一次真实调用：刷新 key 空闲计时
	start := time.Now()
	resp, err := doUpstreamRequest(ctx, client, &cand, target, reqBody, "", true, false, nil)
	res.LatencyMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Error = err.Error()
		return res
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	bytesOut = int64(len(data))
	leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
	res.Status = resp.StatusCode
	res.OK = resp.StatusCode >= 200 && resp.StatusCode < 300
	res.Snippet = truncate(string(data), 512)
	if hint := missingAuthHint(resp.StatusCode, k, ch, data); hint != "" {
		res.Error = hint
	}
	res.promptTokens, res.completionTokens = parseUsageFromBody(data)
	return res
}

// missingAuthHint 上游回 401/403 且被测 key 根本没有凭证时，把「上游说缺
// authorization 头」翻译成可执行的说明：这种 401 不是网关把认证弄丢了，而是
// 这个 key 本来就没有 API Key（渠道里常见残留的空白占位 key 行），或者凭证
// 配在别处（渠道自定义头）。其他情况返回空串，不改写上游原文。
func missingAuthHint(status int, k *UpKey, ch *Channel, body []byte) string {
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		return ""
	}
	if k == nil || strings.TrimSpace(k.APIKey) != "" || channelSendsAuthHeader(ch) {
		return ""
	}
	msg := "该 key 未配置 API Key，请求没有 Authorization 头（渠道自定义头里也没有）——上游需要鉴权时请在该 key 上填写 API Key"
	if s := strings.TrimSpace(string(body)); s != "" {
		msg += "；上游返回 " + strconv.Itoa(status) + "：" + truncate(s, 200)
	}
	return msg
}

// channelSendsAuthHeader 渠道生效头里是否配了非空的 Authorization（它会覆盖
// key 的 Bearer，见 applyCustomHeaders）。生效头 = 客户端预设基线 + 渠道自定义头。
func channelSendsAuthHeader(ch *Channel) bool {
	if ch == nil {
		return false
	}
	for name, value := range effectiveChannelHeaders(ch) {
		if strings.EqualFold(strings.TrimSpace(name), "Authorization") && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

// recordTestRequest 把一次渠道测试请求写入请求记录（与网关请求共用请求日志；
// 代理解析失败时无上游请求，以 status=0 记录并带错误信息）。Tokens 列取上游
// 响应里的真实用量。不计入用量统计。
func recordTestRequest(start time.Time, target, user, channel, key, model string, res testResult, bytesOut int64) {
	if reqLog == nil {
		return
	}
	rec := RequestRecord{
		ID:               strconv.FormatInt(time.Now().UnixNano(), 36),
		Time:             start,
		Duration:         time.Since(start),
		DurationMs:       time.Since(start).Milliseconds(),
		Method:           http.MethodPost,
		Path:             target,
		Status:           res.Status,
		BytesOut:         bytesOut,
		PromptTokens:     res.promptTokens,
		CompletionTokens: res.completionTokens,
		User:             user,
		Channel:          channel,
		Model:            model,
		Key:              key + "@" + channel,
	}
	if res.Error != "" {
		rec.ErrMsg = truncate(res.Error, errMsgMax)
	} else if rec.Status >= 400 {
		rec.ErrMsg = truncate(res.Snippet, 200)
	}
	recordRequest(rec)
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
		if len(body.Channel.Keys) == 0 {
			writeJSONError(w, http.StatusBadRequest, "channel has no key to test", "bad_request")
			return
		}
		// 未保存的渠道/修改直接按页面内容测试（不落盘）：内联定义走同一套归一化，
		// 未保存的 key 赋固定临时 ID（池租约 ID 幂等复用，保存时作为孤儿回收）
		ich, err := inlineChannel(body.Channel)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
			return
		}
		if len(ich.Keys) == 0 {
			writeJSONError(w, http.StatusBadRequest, "channel has no key to test", "bad_request")
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
	ctx, cancel := context.WithTimeout(r.Context(), cfg.TestTimeout)
	defer cancel()
	writeJSON(w, http.StatusOK, runTestOnce(ctx, ch, k, model, msg, adminUserFrom(r.Context())))
}

// placeholderUpKey 判断「占位空 key」：没有名称、没有 API Key、也没有单独的
// 代理配置。新建渠道时编辑器会自动生成这样一个空 key 行（保证渠道至少有一个
// key 可填），批量导入 key 时它不会被顶掉，于是排在被测 key 的最前面：按
// first 语义测它必然没有 Authorization 头，上游回 401「缺 authorization」，
// 看起来像「渠道没正确携带认证」，实际是这一行本来就没有凭证。
func placeholderUpKey(k *UpKey) bool {
	return k != nil && strings.TrimSpace(k.Name) == "" && strings.TrimSpace(k.APIKey) == "" && k.Proxy == nil
}

// selectTestKeys 按测试范围挑选被测 key：all = 全部启用的 key；pick = key_id
// 指定的 key（未给 key_id 时调用方已退化为 first）；first（默认）= 第一个启用
// 的 key。
//
// first / all 会跳过占位空 key（见 placeholderUpKey）：它们排在配置最前面时会
// 成为 first 语义的被测对象，测试必然 401「缺 Authorization」，掩盖真正可用的
// key。跳过条数由 skipped 返回，供 WebUI 提示。无鉴权渠道可以只用一个空白 key
// 行表达「该渠道可路由」，因此唯一的启用 key 不跳过；全部启用 key 都是空白占位
// 时同样不跳过（否则这类渠道会变成「没有可测 key」）。pick 是用户显式指定，
// 不跳过——照测并给出可执行的说明（见 missingAuthHint）。
func selectTestKeys(ch *Channel, scope, keyID string) (keys []*UpKey, skipped int) {
	enabled := make([]*UpKey, 0, len(ch.Keys))
	for _, k := range ch.Keys {
		if k != nil && k.Enabled {
			enabled = append(enabled, k)
		}
	}
	if scope == "pick" {
		for _, k := range enabled {
			if k.ID == keyID {
				return []*UpKey{k}, 0
			}
		}
		return nil, 0
	}
	if len(enabled) > 1 {
		real := make([]*UpKey, 0, len(enabled))
		for _, k := range enabled {
			if placeholderUpKey(k) {
				skipped++
				continue
			}
			real = append(real, k)
		}
		if len(real) > 0 {
			enabled = real
		} else {
			skipped = 0 // 全是空白占位：照常测试（无鉴权渠道的极端形态）
		}
	}
	if scope == "all" {
		return enabled, skipped
	}
	if len(enabled) == 0 { // first
		return nil, skipped
	}
	return enabled[:1], skipped
}

// handleAdminTestModel 渠道级测试：对指定模型集合，按 scope 挑选 key 发起真实
// 对话请求（复用网关请求链路与自动换 IP 重试），返回逐 (key, 模型) 结果供
// WebUI 展示。scope=first（默认）只测第一个启用的 key；scope=all 对全部启用
// 的 key 逐个完整测试（不做故障转移截断）；scope=pick 只测 key_id 指定的 key。
// 所有失败都以 ok=false 的结果返回，不用 HTTP 错误码。
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
	// key 选择：scope=all 取全部启用的 key；scope=pick 只取指定 key（未给
	// key_id 时退化为 first）；默认（空 / first / 旧字段 first_only）只取
	// 第一个启用的 key。占位空 key（见 selectTestKeys）不计入被测对象。
	scope := body.Scope
	if scope != "all" && scope != "pick" {
		scope = "first"
	}
	if scope == "pick" && body.KeyID == "" {
		scope = "first"
	}
	keys, skippedKeys := selectTestKeys(ch, scope, body.KeyID)
	if len(keys) == 0 {
		writeJSONError(w, http.StatusBadRequest, "channel has no enabled key matching key_id", "bad_request")
		return
	}

	results := make([]testResult, 0, len(keys)*len(models))
	for _, m := range models {
		for _, k := range keys {
			ctx, cancel := context.WithTimeout(r.Context(), cfg.TestTimeout)
			res := runTestOnce(ctx, ch, k, m, "ping", adminUserFrom(r.Context()))
			cancel()
			results = append(results, res)
			if scope != "all" && res.OK {
				break // 非全量模式：该模型已通过，无需继续其余 key
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"channel_id": ch.ID,
		"channel":    ch.Name,
		"models":     models,
		"results":    results,
		// 被跳过的空白占位 key 条数：WebUI 据此提示「不是没带认证，而是这些
		// 空行没有凭证」，避免把占位行当成本次测试的失败项
		"skipped_keys": skippedKeys,
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
