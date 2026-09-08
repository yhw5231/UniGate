// Admin API 测试：登录换 token、渠道/下游 key CRUD、鉴权边界、key 测试端点。
package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func adminToken(t *testing.T) string {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"admin","password":"admin"}`))
	req.Header.Set("Content-Type", "application/json")
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Token
}

func adminReq(method, path, body, token string) *http.Request {
	var rd *strings.Reader
	if body == "" {
		rd = strings.NewReader("")
	} else {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestAdminAPIAuthRequired(t *testing.T) {
	setupGateway(t)
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodGet, "/admin/api/state", "", ""))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr.Code)
	}
	// 伪造 token
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodGet, "/admin/api/state", "", "fake"))
	if rr2.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr2.Code)
	}
}

func TestAdminChannelCRUD(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	// PUT channel
	body := `{"name":"Cline","base_url":"https://api.cline.bot/api/v1","enabled":true,
		"rewrite_reasoning":true,
		"keys":[{"name":"acc1","api_key":"sk-a","enabled":true}]}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/channels", body, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("put channel status=%d body=%s", rr.Code, rr.Body.String())
	}
	var ch Channel
	_ = json.Unmarshal(rr.Body.Bytes(), &ch)
	if ch.ID == "" || len(ch.Keys) != 1 || ch.Keys[0].ID == "" {
		t.Fatalf("channel not saved properly: %+v", ch)
	}

	// state 可见
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodGet, "/admin/api/state", "", tok))
	var st struct {
		Channels []*Channel `json:"channels"`
	}
	_ = json.Unmarshal(rr2.Body.Bytes(), &st)
	if len(st.Channels) != 1 || st.Channels[0].Name != "Cline" {
		t.Fatalf("state channels: %+v", st.Channels)
	}

	// DELETE
	rr3 := httptest.NewRecorder()
	rootHandler(rr3, adminReq(http.MethodDelete, "/admin/api/channels/"+ch.ID, "", tok))
	if rr3.Code != http.StatusOK {
		t.Fatalf("delete status=%d", rr3.Code)
	}
	rr4 := httptest.NewRecorder()
	rootHandler(rr4, adminReq(http.MethodGet, "/admin/api/state", "", tok))
	_ = json.Unmarshal(rr4.Body.Bytes(), &st)
	if len(st.Channels) != 0 {
		t.Fatalf("expected empty after delete")
	}
}

func TestAdminGWKeyFlow(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	// Key 为空 → 自动生成
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/gwkeys", `{"name":"sub2api","enabled":true}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("put gwkey status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Key       GWKey `json:"key"`
		Generated bool  `json:"generated"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if !resp.Generated || !strings.HasPrefix(resp.Key.Key, "sk-gw-") {
		t.Fatalf("expected generated sk-gw key, got %+v", resp)
	}

	// 生成的 key 能通过下游鉴权
	rr2 := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+resp.Key.Key)
	rootHandler(rr2, req)
	if rr2.Code == http.StatusUnauthorized {
		t.Fatal("generated key should authenticate")
	}

	// 删除
	rr3 := httptest.NewRecorder()
	rootHandler(rr3, adminReq(http.MethodDelete, "/admin/api/gwkeys/"+resp.Key.ID, "", tok))
	if rr3.Code != http.StatusOK {
		t.Fatalf("delete gwkey status=%d", rr3.Code)
	}

	// 删除后 401
	rr4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req4.Header.Set("Authorization", "Bearer "+resp.Key.Key)
	rootHandler(rr4, req4)
	if rr4.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401 after delete", rr4.Code)
	}
}

func TestAdminTestKeyEndpoint(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey",
		`{"channel_id":"`+chid+`","key_id":"`+kid+`","model":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		OK     bool   `json:"ok"`
		Status int    `json:"status"`
		Proxy  string `json:"proxy"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if !out.OK || out.Status != 200 || out.Proxy != "direct" {
		t.Fatalf("testkey result: %+v", out)
	}
	if up.count() != 1 || up.lastAuth() != "Bearer sk-1" {
		t.Fatalf("upstream not called correctly: calls=%d auth=%q", up.count(), up.lastAuth())
	}
}

// TestAdminTestKeyWritesRequestLog：测试请求也要进请求记录（reqLog），
// user 记为发起测试的管理员；路径为上游对话端点，成功状态为 200。
// TestTestKeySuccessClearsCooldown：渠道测试成功 = 真实请求已打通该 key，
// 应解除其存量冷却（含按 key 共享与按 (key,model) 两种粒度），
// 避免「渠道测试全部通过、网关却因冷却继续 502」的错位。
func TestTestKeySuccessClearsCooldown(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID
	chid := store.Snapshot().Channels[0].ID

	// 模拟历史上该 key 触发过 429/网络错误：两种粒度都冷却中
	cool.Mark(kid, "", time.Minute)   // key 级共享冷却
	cool.Mark(kid, "m1", time.Minute) // (key, model) 冷却
	if !cool.IsCooling(kid, "") || !cool.IsCooling(kid, "m1") {
		t.Fatal("precondition: key should be cooling")
	}

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey",
		`{"channel_id":"`+chid+`","key_id":"`+kid+`","model":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if cool.IsCooling(kid, "") || cool.IsCooling(kid, "m1") {
		t.Fatal("successful test must clear cooldown for the tested key")
	}
}

// TestAdminSettingsEndpoint：PUT /admin/api/settings 全量替换设置并即时生效；
// 字段缺省 = 恢复环境变量默认；校验在 state 接口回读。
func TestAdminSettingsEndpoint(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	cd := 600
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/settings",
		`{"rate_limit_cooldown_sec":600,"rotate_after_5xx":5,"max_route_tries":2}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if p := currentPolicy(); p.RateLimitCooldown != 600*time.Second || p.RotateAfter5xx != 5 || p.MaxRouteTries != 2 {
		t.Fatalf("policy after PUT: %+v", p)
	}

	// state 回读：settings 为显式值、policy 为生效值
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodGet, "/admin/api/state", "", tok))
	var st struct {
		Settings GatewaySettings `json:"settings"`
		Policy   RoutePolicy     `json:"policy"`
	}
	_ = json.Unmarshal(rr2.Body.Bytes(), &st)
	if st.Settings.RateLimitCooldownSec == nil || *st.Settings.RateLimitCooldownSec != cd {
		t.Fatalf("state settings: %+v", st.Settings)
	}
	if st.Policy.RateLimitCooldown != 600*time.Second {
		t.Fatalf("state policy: %+v", st.Policy)
	}

	// 非法值 → 400
	rr3 := httptest.NewRecorder()
	rootHandler(rr3, adminReq(http.MethodPut, "/admin/api/settings", `{"rotate_after_5xx":-1}`, tok))
	if rr3.Code != http.StatusBadRequest {
		t.Fatalf("negative value: status=%d want 400", rr3.Code)
	}

	// 字段缺省 = 恢复环境变量默认（RATE_LIMIT_COOLDOWN 在 setupGateway 中为 60s）
	rr4 := httptest.NewRecorder()
	rootHandler(rr4, adminReq(http.MethodPut, "/admin/api/settings", `{}`, tok))
	if rr4.Code != http.StatusOK {
		t.Fatalf("reset: status=%d", rr4.Code)
	}
	if p := currentPolicy(); p.RateLimitCooldown != 60*time.Second || p.RotateAfter5xx != 3 || p.MaxRouteTries != 0 {
		t.Fatalf("policy after reset should fall back to env defaults: %+v", p)
	}
}

func TestAdminTestKeyWritesRequestLog(t *testing.T) {
	setupGateway(t)
	initStats()
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey",
		`{"channel_id":"`+chid+`","key_id":"`+kid+`","model":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	recs := reqLog.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("reqLog has %d records, want 1", len(recs))
	}
	rec := recs[0]
	if !strings.HasSuffix(rec.Path, "/chat/completions") {
		t.Fatalf("path = %q, want upstream chat endpoint", rec.Path)
	}
	if rec.Status != http.StatusOK || rec.User != "admin" || rec.Model != "m1" {
		t.Fatalf("record = status:%d user:%q model:%q", rec.Status, rec.User, rec.Model)
	}
	if rec.Channel != "c" || rec.Key == "" {
		t.Fatalf("record = channel:%q key:%q", rec.Channel, rec.Key)
	}
	if rec.DurationMs < 0 || rec.Time.IsZero() {
		t.Fatalf("record time/duration invalid: %+v", rec)
	}

	// 失败的测试请求也要记录（上游 500）
	up500 := newUpstream(t, http.StatusInternalServerError, `{"error":"boom"}`)
	mustPutChannel(t, &Channel{Name: "c2", BaseURL: up500.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})
	var ch2 *Channel
	for _, c := range store.Snapshot().Channels {
		if c.Name == "c2" {
			ch2 = c
		}
	}
	if ch2 == nil {
		t.Fatal("channel c2 not found")
	}
	rr = httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey",
		`{"channel_id":"`+ch2.ID+`","key_id":"`+ch2.Keys[0].ID+`","model":"m2"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	recs = reqLog.Snapshot()
	if len(recs) != 2 {
		t.Fatalf("reqLog has %d records, want 2", len(recs))
	}
	if recs[0].Status != http.StatusInternalServerError || recs[0].ErrMsg == "" {
		t.Fatalf("failed test record = status:%d err:%q", recs[0].Status, recs[0].ErrMsg)
	}
}

func TestAdminTestKeyRequestFormat(t *testing.T) {
	// 测试请求必须是最小标准 OpenAI chat 请求：不带 max_tokens（新版模型已移除该参数），
	// 带 Accept: application/json 与明确 UA（避免 Go 默认 UA 被上游拒绝）。
	setupGateway(t)
	tok := adminToken(t)
	var mu sync.Mutex
	var gotBody map[string]any
	var gotAccept, gotUA, gotPath string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(b, &gotBody)
		gotAccept = r.Header.Get("Accept")
		gotUA = r.Header.Get("User-Agent")
		gotPath = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey",
		`{"channel_id":"`+chid+`","key_id":"`+kid+`","model":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	body, accept, ua, path := gotBody, gotAccept, gotUA, gotPath
	mu.Unlock()
	if path != "/chat/completions" {
		t.Fatalf("path = %s, want /chat/completions", path)
	}
	if accept != "application/json" {
		t.Fatalf("Accept = %q, want application/json", accept)
	}
	if ua == "" || ua == "Go-http-client/1.1" {
		t.Fatalf("User-Agent = %q, want explicit UA", ua)
	}
	if _, has := body["max_tokens"]; has {
		t.Fatalf("test body must not carry max_tokens: %v", body)
	}
	if body["stream"] != false {
		t.Fatalf("stream should be false: %v", body)
	}
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages: %v", body["messages"])
	}
}

// TestAdminTestModelEndpoint：渠道级测试端点——逐 (key, 模型) 故障转移、
// first_only、指定模型、responses 渠道请求体转换。
func TestAdminTestModelEndpoint(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	// k1 返回 500（触发转移），k2 返回 200
	fail := newUpstream(t, http.StatusInternalServerError, `{"error":"boom"}`)
	okSrv := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: okSrv.URL, Models: []string{"m1", "m2"}, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true, BaseURL: fail.URL},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
		}})
	chid := store.Snapshot().Channels[0].ID

	// 全部启用 key：m1 在 k1 失败后应由 k2 成功
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/test-model",
		`{}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Results []struct {
			OK    bool   `json:"ok"`
			Key   string `json:"key"`
			Model string `json:"model"`
		} `json:"results"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	// 每个模型 2 条（k1 失败 + k2 成功），共 4 条；最终每模型都成功
	if len(out.Results) != 4 {
		t.Fatalf("expected 4 results (2 models × failover), got %d: %+v", len(out.Results), out.Results)
	}
	for i, m := range []string{"m1", "m1", "m2", "m2"} {
		if out.Results[i].Model != m {
			t.Fatalf("result[%d].model=%q want %q", i, out.Results[i].Model, m)
		}
	}
	if out.Results[0].OK || out.Results[0].Key != "k1" {
		t.Fatalf("result[0] should be k1 failure: %+v", out.Results[0])
	}
	if !out.Results[1].OK || out.Results[1].Key != "k2" {
		t.Fatalf("result[1] should be k2 success: %+v", out.Results[1])
	}
	if !out.Results[3].OK {
		t.Fatalf("result[3] should succeed: %+v", out.Results[3])
	}
	if fail.count() != 2 || okSrv.count() != 2 {
		t.Fatalf("upstream calls: fail=%d ok=%d, want 2/2", fail.count(), okSrv.count())
	}

	// first_only：只用 k1（必失败）
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/test-model",
		`{"first_only":true}`, tok))
	var out2 struct {
		Results []struct {
			OK bool `json:"ok"`
		} `json:"results"`
	}
	_ = json.Unmarshal(rr2.Body.Bytes(), &out2)
	if len(out2.Results) != 2 || out2.Results[0].OK {
		t.Fatalf("first_only should only use k1 and fail: %+v", out2.Results)
	}
	if okSrv.count() != 2 {
		t.Fatalf("first_only must not touch k2: ok=%d", okSrv.count())
	}
}

// TestAdminTestModelResponsesChannel：responses 渠道的测试请求应转换为
// Responses API 格式（POST /responses，input 数组）。
func TestAdminTestModelResponsesChannel(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	var mu sync.Mutex
	var gotPath string
	var gotBody map[string]any
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotPath = r.URL.Path
		_ = json.Unmarshal(b, &gotBody)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"pong"}]}]}`))
	}))
	defer up.Close()
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, EndpointType: "responses", Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/test-model",
		`{"models":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Results []struct {
			OK bool `json:"ok"`
		} `json:"results"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if len(out.Results) != 1 || !out.Results[0].OK {
		t.Fatalf("responses channel test should pass: %+v", out.Results)
	}
	mu.Lock()
	path, body := gotPath, gotBody
	mu.Unlock()
	if path != "/v1/responses" {
		t.Fatalf("path = %s, want /v1/responses", path)
	}
	if _, ok := body["input"].([]any); !ok {
		t.Fatalf("responses body should carry input array: %v", body)
	}
}

func TestVersionEndpoint(t *testing.T) {
	// /api/version 公开返回版本号（dev 或 -ldflags 注入值），无需鉴权
	rr := httptest.NewRecorder()
	rootHandler(rr, httptest.NewRequest(http.MethodGet, "/api/version", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	v, ok := out["version"]
	if !ok || v == "" || v != displayVersion() {
		t.Fatalf("version mismatch: got %q want %q", v, displayVersion())
	}
}

func TestWebUIStaticServed(t *testing.T) {
	setupGateway(t)
	rr := httptest.NewRecorder()
	rootHandler(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "UniGate") {
		t.Fatalf("index served: status=%d", rr.Code)
	}
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, httptest.NewRequest(http.MethodGet, "/app.js", nil))
	if rr2.Code != http.StatusOK {
		t.Fatalf("app.js status=%d", rr2.Code)
	}
	// SPA 回落
	rr3 := httptest.NewRecorder()
	rootHandler(rr3, httptest.NewRequest(http.MethodGet, "/some/spa/route", nil))
	if rr3.Code != http.StatusOK {
		t.Fatalf("spa fallback status=%d", rr3.Code)
	}
}

// TestSharedLeaseCrossChannelViaAdminAPI：跨渠道复用全流程（走 Admin API）——
// 渠道 A 两个 key + 渠道 B 一个 key（同池、Share 开）→ 只需 2 个租约；
// 删除渠道 B后租约数不变（A 仍各占一个）；删除渠道 A 后全部回收。
func TestSharedLeaseCrossChannelViaAdminAPI(t *testing.T) {
	setupGateway(t)
	pool, poolSrv := startFakePool(t)
	tok := adminToken(t)

	mkKey := func(name string) *UpKey {
		return &UpKey{Name: name, APIKey: "sk-" + name, Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL, Share: true}}
	}
	ctx := context.Background()

	// 渠道 A：A1、A2（同组）
	chA := &Channel{Name: "A", BaseURL: "https://a.example.com/v1", Enabled: true,
		Keys: []*UpKey{mkKey("A1"), mkKey("A2")}}
	if err := store.PutChannel(chA); err != nil {
		t.Fatalf("PutChannel A: %v", err)
	}
	snapA := store.Snapshot().Channels[0]
	if _, err := leaseMgr.Ensure(ctx, snapA.Keys[0].Proxy, snapA.Keys[0].ID, snapA.BaseURL); err != nil {
		t.Fatalf("Ensure A1: %v", err)
	}
	if _, err := leaseMgr.Ensure(ctx, snapA.Keys[1].Proxy, snapA.Keys[1].ID, snapA.BaseURL); err != nil {
		t.Fatalf("Ensure A2: %v", err)
	}

	// 渠道 B：B1（异组，应复用 A1/A2 之一的租约——具体落在哪个租约取决于
	// 本地缓存的遍历顺序，断言不指定具体哪个）
	chB := &Channel{Name: "B", BaseURL: "https://b.example.com/v1", Enabled: true,
		Keys: []*UpKey{mkKey("B1")}}
	if err := store.PutChannel(chB); err != nil {
		t.Fatalf("PutChannel B: %v", err)
	}
	snapB := store.Snapshot().Channels[1]
	rB1, err := leaseMgr.Ensure(ctx, snapB.Keys[0].Proxy, snapB.Keys[0].ID, snapB.BaseURL)
	if err != nil {
		t.Fatalf("Ensure B1: %v", err)
	}
	rA1, _ := leaseMgr.Ensure(ctx, snapA.Keys[0].Proxy, snapA.Keys[0].ID, snapA.BaseURL)
	rA2, _ := leaseMgr.Ensure(ctx, snapA.Keys[1].Proxy, snapA.Keys[1].ID, snapA.BaseURL)
	if rB1.Addr != rA1.Addr && rB1.Addr != rA2.Addr {
		t.Fatalf("B1 should share A1/A2's IP, got %s (A1=%s A2=%s)", rB1.Addr, rA1.Addr, rA2.Addr)
	}
	pool.mu.Lock()
	if n := len(pool.leases); n != 2 {
		pool.mu.Unlock()
		t.Fatalf("expected 2 leases for 3 keys across 2 channels, got %d", n)
	}
	pool.mu.Unlock()

	// 通过 Admin API 删除渠道 B：B1 的分配回收，但 gw-A1 仍被 A1 引用 → 租约保留
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodDelete, "/admin/api/channels/"+chB.ID, "", tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("delete B: %d", rr.Code)
	}
	pool.mu.Lock()
	if n := len(pool.leases); n != 2 {
		pool.mu.Unlock()
		t.Fatalf("channel B removal must not release leases still used by A, got %d", n)
	}
	pool.mu.Unlock()

	// 删除渠道 A：全部回收
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodDelete, "/admin/api/channels/"+chA.ID, "", tok))
	if rr2.Code != http.StatusOK {
		t.Fatalf("delete A: %d", rr2.Code)
	}
	pool.mu.Lock()
	if n := len(pool.leases); n != 0 {
		pool.mu.Unlock()
		t.Fatalf("expected all leases released, got %d", n)
	}
	pool.mu.Unlock()
}

// TestPoolRotateByLeaseID：按 lease_id 直接操作本地代理池条目。
func TestPoolRotateByLeaseID(t *testing.T) {
	setupGateway(t)
	pool, poolSrv := startFakePool(t)
	tok := adminToken(t)
	socksAddr := startFakeSocks5(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
				SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})

	// 先产生一次真实请求，使租约进入本地缓存（经 SOCKS5 stub 隧道）
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("precondition: forwardChat status=%d body=%s", rr.Code, rr.Body.String())
	}

	// lease 列表应有 1 条，取其 lease_id
	infos := leaseMgr.ListLeases()
	if len(infos) != 1 {
		t.Fatalf("expected 1 cached lease, got %d", len(infos))
	}

	rr2 := httptest.NewRecorder()
	body := `{"pool_url":"` + poolSrv.URL + `","lease_id":"` + infos[0].LeaseID + `"}`
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/pool/rotate", body, tok))
	if rr2.Code != http.StatusOK {
		t.Fatalf("rotate by lease_id status=%d body=%s", rr2.Code, rr2.Body.String())
	}
	pool.mu.Lock()
	n := pool.rotateN
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected pool rotate called once, got %d", n)
	}

	rr3 := httptest.NewRecorder()
	rootHandler(rr3, adminReq(http.MethodPost, "/admin/api/pool/release", body, tok))
	if rr3.Code != http.StatusOK {
		t.Fatalf("release by lease_id status=%d body=%s", rr3.Code, rr3.Body.String())
	}
	if infos := leaseMgr.ListLeases(); len(infos) != 0 {
		t.Fatalf("expected empty local pool after release, got %d", len(infos))
	}
}

// TestAdminTestKeyInlineChannel：编辑器「测试」以页面填写内容为准——
// 渠道未保存时也能直接用内联配置测试（请求体带 channel），且不落盘。
func TestAdminTestKeyInlineChannel(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)

	// 不保存任何渠道，直接内联（新建渠道未保存场景）
	inline := `{"channel":{"name":"草稿渠道","base_url":"` + up.URL + `","enabled":true,
		"keys":[{"name":"k1","api_key":"sk-inline","enabled":true}]},"model":"m1"}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey", inline, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		OK     bool   `json:"ok"`
		Status int    `json:"status"`
		Proxy  string `json:"proxy"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if !out.OK || out.Status != 200 || out.Proxy != "direct" {
		t.Fatalf("inline testkey result: %+v", out)
	}
	if up.count() != 1 || up.lastAuth() != "Bearer sk-inline" {
		t.Fatalf("upstream not called with inline key: calls=%d auth=%q", up.count(), up.lastAuth())
	}
	// 内联测试不得写入渠道配置
	if n := len(store.Snapshot().Channels); n != 0 {
		t.Fatalf("inline test must not persist channels, got %d", n)
	}
}

// TestAdminTestKeyInlineOverridesSaved：渠道已保存但页面改了内容未保存时，
// 测试必须使用页面内容（API Key 以页面为准），而不是存储里的旧配置。
func TestAdminTestKeyInlineOverridesSaved(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-saved", Enabled: true}}})
	snap := store.Snapshot()
	kid, chid := snap.Channels[0].Keys[0].ID, snap.Channels[0].ID

	// 页面把 API Key 改成 sk-page（未保存），带上原 key ID
	inline := `{"channel":{"id":"` + chid + `","name":"c","base_url":"` + up.URL + `","enabled":true,
		"keys":[{"id":"` + kid + `","name":"k1","api_key":"sk-page","enabled":true}]},"model":"m1"}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey", inline, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if up.lastAuth() != "Bearer sk-page" {
		t.Fatalf("test must use page content: auth=%q", up.lastAuth())
	}
	// 存储中的配置不受测试影响
	if k := store.Snapshot().Channels[0].Keys[0].APIKey; k != "sk-saved" {
		t.Fatalf("saved key mutated: %q", k)
	}
}

// TestAdminTestKeyInlineIPv6Pool：内联渠道 key 绑定代理池（按 pool_id 引用）时，
// 未保存的渠道也能通过页面内容测试：申请租约 gw-preview 并经 SOCKS5 访问上游。
func TestAdminTestKeyInlineIPv6Pool(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	socksAddr := startFakeSocks5(t)
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	p := &ProxyPool{Name: "p", PoolURL: poolSrv.URL, PoolToken: "tok",
		SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}
	if err := store.PutProxyPool(p); err != nil {
		t.Fatalf("PutProxyPool: %v", err)
	}

	inline := `{"channel":{"name":"草稿渠道","base_url":"` + up.URL + `","enabled":true,
		"keys":[{"name":"k1","api_key":"sk-inline","enabled":true,
			"proxy":{"kind":"ipv6pool","pool_id":"` + p.ID + `"}}]},"model":"m1"}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey", inline, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		OK    bool   `json:"ok"`
		Proxy string `json:"proxy"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if !out.OK || !strings.HasPrefix(out.Proxy, "socks5://") {
		t.Fatalf("inline pool test result: ok=%v proxy=%q err=%q", out.OK, out.Proxy, out.Error)
	}
	if up.count() != 1 {
		t.Fatalf("upstream calls = %d", up.count())
	}
	// 未保存 key 使用固定临时 ID（gw-preview）申请租约，重复测试幂等复用
	pool.mu.Lock()
	_, ok := pool.leases["gw-preview"]
	pool.mu.Unlock()
	if !ok {
		t.Fatalf("expected gw-preview lease on pool, leases: %v", pool.leases)
	}
}

// TestAdminTestKeyDeadSocksFastFail：渠道编辑页「测试」的池 SOCKS 出口挂死
// （accept 后不响应）时，测试须秒级失败并给出可读错误，而不是挂到整体超时
// （WebUI 长时间无响应，经反代时被判定 504）。
func TestAdminTestKeyDeadSocksFastFail(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	_, poolSrv := startFakePool(t) // 池管理端正常（模拟「代理池页测试可用」）

	// 挂死的"SOCKS"端口：accept 后不读不回
	deadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deadLn.Close() })
	go func() {
		for {
			c, err := deadLn.Accept()
			if err != nil {
				return
			}
			_ = c // 永不响应
		}
	}()
	deadAddr := deadLn.Addr().String()

	p := &ProxyPool{Name: "p", PoolURL: poolSrv.URL, SocksHost: deadAddr}
	if err := store.PutProxyPool(p); err != nil {
		t.Fatalf("PutProxyPool: %v", err)
	}

	inline := `{"channel":{"name":"ch","base_url":"` + up.URL + `","enabled":true,
		"keys":[{"name":"k1","api_key":"sk-1","enabled":true,
			"proxy":{"kind":"ipv6pool","pool_id":"` + p.ID + `"}}]},"model":"m1"}`
	start := time.Now()
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/testkey", inline, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.OK {
		t.Fatal("dead socks test must fail")
	}
	if !strings.Contains(out.Error, "socks proxy") {
		t.Fatalf("expected readable socks error, got: %q", out.Error)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("test must fail fast on dead socks, took %v", d)
	}
	if up.count() != 0 {
		t.Fatalf("upstream must not be reached via dead socks, calls=%d", up.count())
	}
}
