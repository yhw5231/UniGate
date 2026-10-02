// 模型名称处理测试：归一化、通配/正则模式、渠道名称映射、全局别名、
// 请求体 model 改写（JSON 与 multipart），以及端到端路由行为
//（下游写法与上游写法不同时仍能命中渠道并按上游名转发）。
package main

import (
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCanonicalModel(t *testing.T) {
	cases := []struct{ in, want string }{
		{"DeepSeek-V4.1-Flash", "deepseek-v4.1-flash"},
		{"cline-free/deepseek-v4.1-flash", "deepseek-v4.1-flash"},
		{"cline-free/deepseek-v4.1-flash:free", "deepseek-v4.1-flash"},
		{"gpt-4o:thinking", "gpt-4o"},
		{"gpt-4o-free", "gpt-4o"},
		{"  gpt-4o  ", "gpt-4o"},
		{"cn:deepseek-v4", "cn:deepseek-v4"}, // 冒号后非装饰词：保留（分组名）
		{"gpt-4o:", "gpt-4o"},                // 冒号后为空：视为装饰
		{"", ""},
	}
	for _, c := range cases {
		if got := canonicalModel(c.in); got != c.want {
			t.Errorf("canonicalModel(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestModelMatches(t *testing.T) {
	yes := []struct{ model, pattern string }{
		{"gpt-4o", "gpt-4o"},
		{"GPT-4O", "gpt-4o"},
		{"cline-free/deepseek-v4.1-flash:free", "deepseek-v4.1-flash"}, // 上游写法 ↔ 下游写法
		{"deepseek-v4.1-flash", "cline-free/deepseek-v4.1-flash"},      // 反向
		{"gpt-4o-free", "gpt-4o"},                                      // 声明基础名覆盖免费变体
		{"claude-3-5-sonnet", "claude-*"},
		{"anthropic/claude-3-5-sonnet:free", "claude-*"}, // 通配匹配归一化名
		{"gpt-4o", "gpt-4?"},
		{"gpt-4o-mini", "re:^gpt-4.*$"},
		{"openai/gpt-4o", "re:^gpt-4.*"},
	}
	for _, c := range yes {
		if !modelMatches(c.model, c.pattern) {
			t.Errorf("modelMatches(%q, %q) = false, want true", c.model, c.pattern)
		}
	}
	no := []struct{ model, pattern string }{
		{"gpt-4o", "gpt-4o-mini"},
		{"gpt-4o", "claude-*"},
		{"gpt-4o", "re:^claude"},
		{"", "gpt-4o"},
		{"gpt-4o", ""},
		{"gpt-4o", "gpt-*mini"},
		{"gpt-4o", "re:^(unclosed"}, // 非法正则：不匹配也不 panic
	}
	for _, c := range no {
		if modelMatches(c.model, c.pattern) {
			t.Errorf("modelMatches(%q, %q) = true, want false", c.model, c.pattern)
		}
	}
}

func TestChannelAllowsModelPatterns(t *testing.T) {
	ch := &Channel{Models: []string{"deepseek/deepseek-v4-flash", "claude-*", "re:^gpt-4.*$"}}
	for _, m := range []string{
		"deepseek/deepseek-v4-flash",
		"deepseek-v4-flash",      // 归一化匹配（去供应商前缀）
		"deepseek-v4-flash:free", // 归一化匹配（去装饰后缀）
		"claude-3-5-sonnet",
		"gpt-4o-mini",
	} {
		if !ch.allowsModel(m) {
			t.Errorf("allowsModel(%q) = false, want true (models=%v)", m, ch.Models)
		}
	}
	for _, m := range []string{"gemini-pro", "deepseek-v3"} {
		if ch.allowsModel(m) {
			t.Errorf("allowsModel(%q) = true, want false", m)
		}
	}
	// 未声明模型列表 = 放行全部
	open := &Channel{}
	if !open.allowsModel("anything") {
		t.Error("channel without model list must allow any model")
	}
}

// firstUpstream 单值解析结果（多上游映射取首个）——断言辅助。
func firstUpstream(ch *Channel, model string) string {
	if v := ch.upstreamModelsFor(model); len(v) > 0 {
		return v[0]
	}
	return ""
}

func TestChannelUpstreamModelFor(t *testing.T) {
	ch := &Channel{
		Models:   []string{"deepseek/deepseek-v4-flash", "claude-*"},
		ModelMap: ModelMap{"my-gpt": {"gpt-4o-2024-11-20"}, "alias": {"real-model"}},
	}
	// 声明列表原文写法（上游 /models 的写法才是上游认识的名字）
	if got := firstUpstream(ch, "deepseek-v4-flash"); got != "deepseek/deepseek-v4-flash" {
		t.Errorf("declared spelling: got %q", got)
	}
	// 名称映射优先
	if got := firstUpstream(ch, "my-gpt"); got != "gpt-4o-2024-11-20" {
		t.Errorf("model_map: got %q", got)
	}
	// 归一化等价的映射键也能命中
	if got := firstUpstream(ch, "ALIAS"); got != "real-model" {
		t.Errorf("canonical mapping key: got %q", got)
	}
	// 通配命中的条目不是具体名字：不改写，按请求名发送
	if got := firstUpstream(ch, "claude-3-5-sonnet"); got != "" {
		t.Errorf("wildcard match must not rewrite upstream model, got %q", got)
	}
	// 未命中：原样发送
	if got := firstUpstream(ch, "unknown-model"); got != "" {
		t.Errorf("unmatched model: got %q", got)
	}
	// 仅大小写不同：保持下游写法（不猜测上游的大小写偏好）
	caseOnly := &Channel{Models: []string{"GPT-4O"}}
	if got := firstUpstream(caseOnly, "gpt-4o"); got != "" {
		t.Errorf("case-only difference must not rewrite, got %q", got)
	}
}

func TestRewriteBodyModel(t *testing.T) {
	// JSON：只改 model，其余字段保留
	in := []byte(`{"model":"old","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	out, ct := rewriteBodyModel(in, "application/json", "new")
	if ct != "application/json" {
		t.Fatalf("content type changed: %q", ct)
	}
	var obj map[string]any
	if err := json.Unmarshal(out, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if obj["model"] != "new" || obj["stream"] != true {
		t.Fatalf("rewritten body: %s", out)
	}
	if _, ok := obj["messages"]; !ok {
		t.Fatalf("messages dropped: %s", out)
	}
	// 非法 JSON：原样返回，不影响转发
	bad := []byte("not json")
	if got, _ := rewriteBodyModel(bad, "application/json", "new"); string(got) != "not json" {
		t.Fatalf("invalid json must pass through, got %q", got)
	}
	// 已经是目标名：字节原样透传
	if got, _ := rewriteBodyModel(in, "application/json", "old"); string(got) != string(in) {
		t.Fatalf("same model must pass through unchanged")
	}
}

func TestRewriteMultipartModel(t *testing.T) {
	var buf strings.Builder
	w := multipart.NewWriter(&buf)
	_ = w.WriteField("model", "old-model")
	_ = w.WriteField("prompt", "a cat")
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	ct := w.FormDataContentType()
	out, newCT, ok := rewriteMultipartModel([]byte(buf.String()), ct, "new-model")
	if !ok {
		t.Fatal("multipart rewrite failed")
	}
	if newCT == ct {
		t.Fatal("boundary must be regenerated")
	}
	_, params, err := mime.ParseMediaType(newCT)
	if err != nil {
		t.Fatalf("parse new content type: %v", err)
	}
	rd := multipart.NewReader(strings.NewReader(string(out)), params["boundary"])
	fields := map[string]string{}
	for {
		p, err := rd.NextPart()
		if err != nil {
			break
		}
		b, _ := io.ReadAll(p)
		fields[p.FormName()] = string(b)
	}
	if fields["model"] != "new-model" || fields["prompt"] != "a cat" {
		t.Fatalf("rewritten form: %+v", fields)
	}
}

func TestGWKeyAllowsModel(t *testing.T) {
	k := &GWKey{Models: []string{"gpt-4o", "claude-*"}}
	for _, m := range []string{"gpt-4o", "GPT-4O", "claude-3-5-sonnet"} {
		if !gwKeyAllowsModel(k, m) {
			t.Errorf("gwKeyAllowsModel(%q) = false, want true", m)
		}
	}
	if gwKeyAllowsModel(k, "gemini-pro") {
		t.Error("gwKeyAllowsModel(gemini-pro) = true, want false")
	}
	if !gwKeyAllowsModel(&GWKey{}, "anything") {
		t.Error("empty model list must allow any model")
	}
	if !gwKeyAllowsModel(nil, "anything") {
		t.Error("nil key must allow any model")
	}
}

// TestRouteNormalizesModelName 端到端：渠道声明上游写法（带供应商前缀/装饰后缀），
// 下游用简化写法请求，应命中同一渠道，并按上游写法改写请求体的 model。
func TestRouteNormalizesModelName(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	var gotModel string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotModel = extractModel(body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"cline-free/deepseek-v4.1-flash:free"},
		Keys:   []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})

	rr := httptest.NewRecorder()
	req := chatRequest("deepseek-v4.1-flash")
	req.Header.Set("Authorization", "Bearer "+testGWKey(t, "norm"))
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if gotModel != "cline-free/deepseek-v4.1-flash:free" {
		t.Fatalf("upstream model = %q, want 上游 /models 的原文写法", gotModel)
	}
}

// TestRouteAppliesChannelModelMap 渠道名称映射：下游名 → 上游名（改写请求体）。
func TestRouteAppliesChannelModelMap(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	var gotModel string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotModel = extractModel(body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models:   []string{"gpt-4o"},
		ModelMap: ModelMap{"my-gpt": {"gpt-4o-2024-11-20"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	// 用映射名调用：渠道模型列表里没有 my-gpt，靠映射打通需要显式声明；
	// 这里验证「声明 gpt-4o + 映射 my-gpt→上游名」时下游用 gpt-4o 调用的改写
	rr := httptest.NewRecorder()
	req := chatRequest("gpt-4o")
	req.Header.Set("Authorization", "Bearer "+testGWKey(t, "map"))
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if gotModel != "gpt-4o" {
		t.Fatalf("模型名与声明一致时不应改写，got %q", gotModel)
	}
}

// TestRouteModelAlias 全局别名：下游用别名调用，路由到目标模型并按渠道映射改写上游名。
func TestRouteModelAlias(t *testing.T) {
	setupGateway(t)
	set := GatewaySettings{ModelAliases: map[string]string{"my-gpt": "gpt-4o"}}
	if err := store.PutSettings(&set); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())

	var mu sync.Mutex
	var gotModel string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotModel = extractModel(body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models:   []string{"gpt-4o"},
		ModelMap: ModelMap{"gpt-4o": {"gpt-4o-2024-11-20"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})

	authKey := testGWKey(t, "alias")
	rr := httptest.NewRecorder()
	req := chatRequest("my-gpt")
	req.Header.Set("Authorization", "Bearer "+authKey)
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	// 别名解析 my-gpt → gpt-4o，再按渠道映射改写为上游名
	if gotModel != "gpt-4o-2024-11-20" {
		t.Fatalf("upstream model = %q, want gpt-4o-2024-11-20", gotModel)
	}

	// /v1/models 暴露别名与规范名
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+authKey)
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("models status=%d body=%s", rr.Code, rr.Body.String())
	}
	for _, want := range []string{`"gpt-4o"`, `"my-gpt"`} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Fatalf("/v1/models missing %s: %s", want, rr.Body.String())
		}
	}
}

// testGWKey 建一个下游通用 key 并返回其明文值。
func testGWKey(t *testing.T, name string) string {
	t.Helper()
	key := &GWKey{Name: name, Key: "sk-gw-" + name, Enabled: true}
	if err := store.PutGWKey(key); err != nil {
		t.Fatalf("PutGWKey: %v", err)
	}
	return key.Key
}

// TestGatewayKeyModelRestriction 下游 key 模型白名单：不在名单内的模型返回 403。
func TestGatewayKeyModelRestriction(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"pong"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"gpt-4o", "claude-3-5-sonnet"},
		Keys:   []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	key := &GWKey{Name: "restricted", Key: "sk-gw-restricted", Models: []string{"gpt-4o"}, Enabled: true}
	if err := store.PutGWKey(key); err != nil {
		t.Fatalf("PutGWKey(models): %v", err)
	}

	call := func(model string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		req := chatRequest(model)
		req.Header.Set("Authorization", "Bearer "+key.Key)
		rootHandler(rr, req)
		return rr
	}
	if rr := call("gpt-4o"); rr.Code != http.StatusOK {
		t.Fatalf("allowed model status=%d body=%s", rr.Code, rr.Body.String())
	}
	rr := call("claude-3-5-sonnet")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("disallowed model status=%d (want 403) body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "model_not_allowed") {
		t.Fatalf("403 body must carry model_not_allowed: %s", rr.Body.String())
	}
	if up.count() != 1 {
		t.Fatalf("upstream calls=%d, 被拒请求不应打到上游", up.count())
	}

	// /v1/models 按白名单过滤
	srr := httptest.NewRecorder()
	sreq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	sreq.Header.Set("Authorization", "Bearer "+key.Key)
	rootHandler(srr, sreq)
	if strings.Contains(srr.Body.String(), "claude-3-5-sonnet") {
		t.Fatalf("/v1/models must hide disallowed models: %s", srr.Body.String())
	}
	if !strings.Contains(srr.Body.String(), "gpt-4o") {
		t.Fatalf("/v1/models must keep allowed models: %s", srr.Body.String())
	}
}

// TestRouteStatusDataModelGrouping 路由页视图：模型分组按对外名（归一化后）列出，
// 声明里写上游原写法（供应商前缀）时也只出现干净的分组名，不出现带前缀的重复分组。
func TestRouteStatusDataModelGrouping(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "a", BaseURL: "https://a.example/v1", Enabled: true,
		Models: []string{"deepseek/deepseek-v4-flash", "claude-*"},
		Keys:   []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	mustPutChannel(t, &Channel{Name: "b", BaseURL: "https://b.example/v1", Enabled: true,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})

	// 声明列表归一化：上游原写法进了模型映射（发往上游仍用它自己的写法）
	if got := store.View().Channels[0].Models; len(got) != 2 || got[0] != "deepseek-v4-flash" {
		t.Fatalf("declared models should be canonical: %v", got)
	}
	if got := store.View().Channels[0].ModelMap["deepseek-v4-flash"]; len(got) != 1 || got[0] != "deepseek/deepseek-v4-flash" {
		t.Fatalf("upstream spelling should be kept in model_map: %+v", store.View().Channels[0].ModelMap)
	}

	data := routeStatusData("")
	byModel := map[string]*RouteModelGroup{}
	for i := range data.Models {
		byModel[data.Models[i].Model] = &data.Models[i]
	}
	g := byModel["deepseek-v4-flash"]
	if g == nil || len(g.Keys) != 2 {
		t.Fatalf("canonical model group: %+v (all=%v)", g, byModel)
	}
	if _, dup := byModel["deepseek/deepseek-v4-flash"]; dup {
		t.Fatalf("带前缀的写法不应另成分组: %v", byModel)
	}
	// 该分组发往上游的名字仍是上游原写法
	if g.Keys[0].Upstream != "deepseek/deepseek-v4-flash" {
		t.Fatalf("upstream model should keep the declared spelling: %+v", g.Keys[0])
	}
	// 通配命中的渠道
	wild := byModel["claude-*"]
	if wild == nil || len(wild.Keys) != 2 {
		t.Fatalf("wildcard group: %+v", wild)
	}
	// 单模型查询：带前缀的写法按归一化落到同一分组
	single := routeStatusData("deepseek/deepseek-v4-flash")
	if len(single.Models) != 1 || single.Models[0].Model != "deepseek-v4-flash" || len(single.Models[0].Keys) != 2 {
		t.Fatalf("normalized single-model view: %+v", single.Models)
	}
}

// TestUpstreamPinCanonicalLookup 固定配置按归一化名查找（下游写法与记录名不同）。
func TestUpstreamPinCanonicalLookup(t *testing.T) {
	ch := &Channel{ModelPins: map[string]*ModelUpstreamPin{
		"cline-free/deepseek-v4.1-flash:free": {Upstreams: []string{"p1"}},
	}}
	if pin := ch.upstreamPinFor("deepseek-v4.1-flash"); pin == nil || len(pin.Upstreams) != 1 {
		t.Fatalf("canonical pin lookup failed: %+v", pin)
	}
	if pin := ch.upstreamPinFor("other"); pin != nil {
		t.Fatalf("unrelated model must not match a pin: %+v", pin)
	}
}

// TestStaleModels 拉取重建的差异计算：上游已不返回的旧条目。
func TestStaleModels(t *testing.T) {
	got := staleModels(
		[]string{"a", "cline-free/b:free", "c"},
		[]string{"a", "b", "d"},
	)
	if strings.Join(got, ",") != "c" {
		t.Fatalf("stale = %v, want [c]（b 归一化后等价，不算 stale）", got)
	}
	if n := len(staleModels(nil, []string{"x"})); n != 0 {
		t.Fatalf("empty enabled list must produce no stale entries")
	}
}

// TestBreakerCooldownEscalation 熔断：连续失败达阈值触发冷却并按指数退避升级，
// 成功即清零并解除冷却。
func TestBreakerCooldownEscalation(t *testing.T) {
	setupGateway(t)
	pol := BreakerPolicy{Enabled: true, Threshold: 3, BaseCooldown: time.Minute, MaxCooldown: 10 * time.Minute, Multiplier: 2}

	if d, cooled := cool.RecordFailure("k1", "", pol); cooled {
		t.Fatalf("第 1 次失败不应触发冷却（d=%s）", d)
	}
	if d, cooled := cool.RecordFailure("k1", "", pol); cooled {
		t.Fatalf("第 2 次失败不应触发冷却（d=%s）", d)
	}
	d, cooled := cool.RecordFailure("k1", "", pol)
	if !cooled || d != time.Minute {
		t.Fatalf("第 3 次失败应冷却 1m，got %s cooled=%v", d, cooled)
	}
	if !cool.IsCooling("k1", "") {
		t.Fatal("key must be cooling after threshold")
	}
	// 冷却到期后（模拟）下一次失败立即再触发，且时长翻倍
	cool.mu.Lock()
	cool.until[cooldownPair{"k1", ""}] = cooldownEntry{until: time.Now().Add(-time.Second), failures: 3, level: 1}
	cool.mu.Unlock()
	d, cooled = cool.RecordFailure("k1", "", pol)
	if !cooled || d != 2*time.Minute {
		t.Fatalf("第 4 次失败应冷却 2m（指数退避），got %s cooled=%v", d, cooled)
	}
	// 成功：清零并解除冷却
	cool.RecordSuccess("k1", "")
	if cool.IsCooling("k1", "") {
		t.Fatal("success must clear cooldown")
	}
	if d, cooled := cool.RecordFailure("k1", "", pol); cooled {
		t.Fatalf("成功清零后重新计数，首次失败不应冷却（d=%s）", d)
	}
	// 关闭熔断：不记录
	if _, cooled := cool.RecordFailure("k2", "", BreakerPolicy{Enabled: false, Threshold: 1}); cooled {
		t.Fatal("disabled breaker must not cool")
	}
	// 上限：层级很大时封顶
	if got := breakerCooldown(pol, 20); got != 10*time.Minute {
		t.Fatalf("cooldown must cap at max, got %s", got)
	}
}

// TestBreakerCooldownPersistence 熔断状态（失败计数/层级）随 cooldowns.json 持久化。
func TestBreakerCooldownPersistence(t *testing.T) {
	setupGateway(t)
	path := t.TempDir() + "/cooldowns.json"
	if err := cool.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	pol := BreakerPolicy{Enabled: true, Threshold: 2, BaseCooldown: time.Minute, MaxCooldown: time.Hour, Multiplier: 2}
	cool.RecordFailure("k1", "m1", pol) // 计数 1（未冷却）
	if _, cooled := cool.RecordFailure("k1", "m1", pol); !cooled {
		t.Fatal("第 2 次失败应触发冷却")
	}
	cool.RecordFailure("k2", "", pol) // 只有计数，无冷却

	restored := newCooldowns()
	if err := restored.SetPersistPath(path); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !restored.IsCooling("k1", "m1") {
		t.Fatal("冷却条目应随文件恢复")
	}
	// k2 无冷却但失败计数应保留：下一次失败立即触发
	if _, cooled := restored.RecordFailure("k2", "", pol); !cooled {
		t.Fatal("恢复后的失败计数必须保留（第 2 次失败立即触发冷却）")
	}
	if restored.IsCooling("k2", "") == false {
		t.Fatal("触发后应处于冷却")
	}
}

// TestRouteBreakerCoolsAfterRepeated5xx 端到端：同一 key 连续 5xx 达阈值后进入冷却，
// 后续请求跳过该 key（无其它候选时返回 502 且带冷却说明）。
func TestRouteBreakerCoolsAfterRepeated5xx(t *testing.T) {
	setupGateway(t)
	set := GatewaySettings{
		BreakerThreshold:  intPtr(2),
		BreakerBaseSec:    intPtr(60),
		BreakerMaxSec:     intPtr(600),
		BreakerMultiplier: floatPtr(2),
	}
	if err := store.PutSettings(&set); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())

	up := newUpstream(t, http.StatusInternalServerError, `{"error":"boom"}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"gpt-4o"},
		Keys:   []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})

	// 第 1 次失败：只记失败，不冷却
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("gpt-4o"), nil, false, "gpt-4o")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("第一次 5xx 应 502，got %d", rr.Code)
	}
	if cool.IsCooling(store.Snapshot().Channels[0].Keys[0].ID, "") {
		t.Fatal("第 1 次失败不应冷却")
	}
	// 第 2 次失败：达到阈值（2）→ 冷却
	rr = httptest.NewRecorder()
	forwardChat(rr, chatRequest("gpt-4o"), nil, false, "gpt-4o")
	if !cool.IsCooling(store.Snapshot().Channels[0].Keys[0].ID, "") {
		t.Fatal("连续 5xx 达阈值后 key 应进入冷却")
	}
	calls := up.count()
	// 第 3 次请求：候选全在冷却 → 穿透试探一次（仍打上游），失败后 502 并提示冷却
	rr = httptest.NewRecorder()
	forwardChat(rr, chatRequest("gpt-4o"), nil, false, "gpt-4o")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "cooldown") && !strings.Contains(rr.Body.String(), "cool") {
		t.Fatalf("错误信息应说明冷却：%s", rr.Body.String())
	}
	if up.count() <= calls {
		t.Fatalf("穿透试探应真实打上游一次：calls=%d", up.count())
	}
	// 熔断冷却在路由页可见
	data := routeStatusData("gpt-4o")
	cooling := 0
	for _, g := range data.Models {
		for _, k := range g.Keys {
			if k.Status == routeStatusCooling {
				cooling++
			}
		}
	}
	if cooling == 0 {
		t.Fatal("路由页应显示该 key 冷却中")
	}
}

func intPtr(v int) *int           { return &v }
func floatPtr(v float64) *float64 { return &v }
