// 模型名映射（一个下游名 → 上游多个模型）测试：
//   - 上游多个模型（如 cn:x / global:x）都对外叫同一个下游名时，每个上游模型各成
//     一个候选，转发时分别改写 model，冷却按上游模型分别计算；
//   - 渠道映射优先于全局别名；
//   - 映射键（下游名）即「声明支持」：可用它调用、出现在 /v1/models 与路由视图；
//   - ModelMap 的 JSON 兼容旧格式（单值字符串）与新格式（数组）。
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// modelMapUpstream 按请求体里的 model 返回不同状态，并记录每个模型收到的次数。
func modelMapUpstream(t *testing.T, statusFor func(model string) int) (*httptest.Server, func(model string) int) {
	t.Helper()
	var mu sync.Mutex
	calls := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m := extractModel(body)
		mu.Lock()
		calls[m]++
		mu.Unlock()
		st := http.StatusOK
		if statusFor != nil {
			st = statusFor(m)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		if st == http.StatusOK {
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"pong"}}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	}))
	t.Cleanup(srv.Close)
	count := func(model string) int {
		mu.Lock()
		defer mu.Unlock()
		return calls[model]
	}
	return srv, count
}

// TestModelMapFanOutPerUpstreamCooldown 一个下游名映射到两个上游模型：
// 第一个上游模型 429 只冷却它自己，请求由第二个上游模型接住；下一次请求跳过
// 冷却中的那个，不再重复冲击（冷却按上游模型分别计算，不受渠道 key 级粒度影响）。
func TestModelMapFanOutPerUpstreamCooldown(t *testing.T) {
	setupGateway(t)
	srv, count := modelMapUpstream(t, func(model string) int {
		if model == "cn:x" {
			return http.StatusTooManyRequests
		}
		return http.StatusOK
	})
	ch := &Channel{Name: "c", BaseURL: srv.URL, Enabled: true,
		Models:   []string{"cn:x", "global:x"},
		ModelMap: ModelMap{"x": {"cn:x", "global:x"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}}
	mustPutChannel(t, ch)
	keyID := store.View().Channels[0].Keys[0].ID
	auth := testGWKey(t, "fanout")

	// 第一次：cn:x 撞 429 → 故障转移到 global:x
	rr := httptest.NewRecorder()
	req := chatRequest("x")
	req.Header.Set("Authorization", "Bearer "+auth)
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := count("cn:x"); got != 1 {
		t.Fatalf("cn:x calls=%d, want 1", got)
	}
	if got := count("global:x"); got != 1 {
		t.Fatalf("global:x calls=%d, want 1", got)
	}
	// 冷却按上游模型粒度：只有 cn:x 被冷却，key 级与 global:x 不受影响
	if _, ok := cool.CoolingKey(keyID, "cn:x"); !ok {
		t.Fatalf("cn:x 应处于冷却中，cooldowns=%+v", cool.CoolingList())
	}
	if _, ok := cool.CoolingKey(keyID, ""); ok {
		t.Fatal("映射候选不应按 key 跨模型共享冷却")
	}
	if _, ok := cool.CoolingKey(keyID, "global:x"); ok {
		t.Fatal("global:x 不应被 cn:x 的 429 牵连")
	}

	// 第二次：cn:x 冷却中直接跳过，只打 global:x
	rr = httptest.NewRecorder()
	req = chatRequest("x")
	req.Header.Set("Authorization", "Bearer "+auth)
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("second status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := count("cn:x"); got != 1 {
		t.Fatalf("cn:x calls=%d, want still 1 (冷却中应跳过)", got)
	}
	if got := count("global:x"); got != 2 {
		t.Fatalf("global:x calls=%d, want 2", got)
	}
}

// TestChannelMapOverridesGlobalAlias 渠道映射优先于全局别名：全局别名 my-flash→gpt-4o，
// 渠道对 my-flash 配了映射时按渠道映射发往上游，不落到全局别名的目标模型上。
func TestChannelMapOverridesGlobalAlias(t *testing.T) {
	setupGateway(t)
	set := GatewaySettings{ModelAliases: map[string]string{"my-flash": "gpt-4o"}}
	if err := store.PutSettings(&set); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())

	srv, count := modelMapUpstream(t, nil)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: srv.URL, Enabled: true,
		Models:   []string{"cn:gpt-4o"},
		ModelMap: ModelMap{"my-flash": {"cn:gpt-4o"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})

	rr := httptest.NewRecorder()
	req := chatRequest("my-flash")
	req.Header.Set("Authorization", "Bearer "+testGWKey(t, "aliasmap"))
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := count("cn:gpt-4o"); got != 1 {
		t.Fatalf("cn:gpt-4o calls=%d, want 1 (渠道映射应优先于全局别名)", got)
	}
	if got := count("gpt-4o"); got != 0 {
		t.Fatalf("gpt-4o calls=%d, want 0（不应落到全局别名的目标）", got)
	}
}

// TestModelMapKeyExposedAsModel 映射键（下游名）算「声明支持」：无需写进 Models
// 即可用该名调用，并出现在 /v1/models 与路由视图分组里。
func TestModelMapKeyExposedAsModel(t *testing.T) {
	setupGateway(t)
	srv, _ := modelMapUpstream(t, nil)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: srv.URL, Enabled: true,
		Models:   []string{"cn:x", "global:x"},
		ModelMap: ModelMap{"x": {"cn:x", "global:x"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	auth := testGWKey(t, "expose")

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+auth)
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("models status=%d body=%s", rr.Code, rr.Body.String())
	}
	for _, want := range []string{`"x"`, `"cn:x"`, `"global:x"`} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Fatalf("/v1/models missing %s: %s", want, rr.Body.String())
		}
	}
}

// TestRouteViewExpandsMappedUpstreams 路由视图：映射到多个上游模型的下游名分组下，
// 同一 (渠道, key) 展开为多行，各自标注 upstream_model 并各自检查冷却。
func TestRouteViewExpandsMappedUpstreams(t *testing.T) {
	setupGateway(t)
	srv, _ := modelMapUpstream(t, nil)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: srv.URL, Enabled: true,
		Models:   []string{"cn:x", "global:x"},
		ModelMap: ModelMap{"x": {"cn:x", "global:x"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	keyID := store.View().Channels[0].Keys[0].ID
	cool.Mark(keyID, "cn:x", time.Minute)

	data := routeStatusData("x")
	if len(data.Models) != 1 {
		t.Fatalf("models=%d, want 1", len(data.Models))
	}
	g := data.Models[0]
	if g.Model != "x" || len(g.Keys) != 2 {
		t.Fatalf("group=%q keys=%d, want model x with 2 rows", g.Model, len(g.Keys))
	}
	byUpstream := map[string]RouteKeyStatus{}
	for _, k := range g.Keys {
		byUpstream[k.Upstream] = k
	}
	cn, ok := byUpstream["cn:x"]
	if !ok {
		t.Fatalf("缺少 cn:x 行: %+v", g.Keys)
	}
	if cn.Status != routeStatusCooling || cn.CoolModel != "cn:x" {
		t.Fatalf("cn:x row = %+v, want cooling with cool_model cn:x", cn)
	}
	gl, ok := byUpstream["global:x"]
	if !ok {
		t.Fatalf("缺少 global:x 行: %+v", g.Keys)
	}
	if gl.Status != routeStatusOK || gl.CoolModel != "global:x" {
		t.Fatalf("global:x row = %+v, want ok with cool_model global:x", gl)
	}

	// 上游模型分组（cn:x / global:x）与映射键分组（x）都在
	full := routeStatusData("")
	got := map[string]bool{}
	for _, m := range full.Models {
		got[m.Model] = true
	}
	for _, want := range []string{"x", "cn:x", "global:x"} {
		if !got[want] {
			t.Fatalf("路由视图缺少分组 %q: %+v", want, got)
		}
	}
}

// TestModelMapJSONCompat 旧格式（单值字符串）与新格式（数组）都能解析；
// 序列化时单值写字符串、多值写数组。
func TestModelMapJSONCompat(t *testing.T) {
	var old struct {
		M ModelMap `json:"model_map"`
	}
	if err := json.Unmarshal([]byte(`{"model_map":{"a":"up-a","b":["up-b1","up-b2"]}}`), &old); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(old.M["a"]) != 1 || old.M["a"][0] != "up-a" {
		t.Fatalf("旧格式单值解析失败: %+v", old.M)
	}
	if len(old.M["b"]) != 2 || old.M["b"][1] != "up-b2" {
		t.Fatalf("数组格式解析失败: %+v", old.M)
	}

	out, err := json.Marshal(ModelMap{"a": {"up-a"}, "b": {"up-b1", "up-b2"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"a":"up-a"`) {
		t.Fatalf("单值应写字符串: %s", s)
	}
	if !strings.Contains(s, `"b":["up-b1","up-b2"]`) {
		t.Fatalf("多值应写数组: %s", s)
	}

	// normalizeChannel 清理：去空白、丢空目标、丢「映射到自身」
	ch := &Channel{Name: "c", BaseURL: "http://x", ModelMap: ModelMap{
		" a ":  {" up-a ", "", "up-a"},
		"self": {"self"},
		"  ":   {"up-c"},
	}}
	if err := normalizeChannel(ch); err != nil {
		t.Fatalf("normalizeChannel: %v", err)
	}
	if len(ch.ModelMap) != 1 || len(ch.ModelMap["a"]) != 1 || ch.ModelMap["a"][0] != "up-a" {
		t.Fatalf("normalizeModelMap 结果异常: %+v", ch.ModelMap)
	}
}

// TestModelMapPersistRoundtrip 多上游映射落盘后重新加载不丢（自定义 JSON 编解码
// 与 atomic 发布的 cloneJSON 往返都要正确）。
func TestModelMapPersistRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway.json")
	s := newGatewayStore(path)
	if err := s.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := s.PutChannel(&Channel{Name: "c", BaseURL: "http://up", Enabled: true,
		Models:   []string{"cn:x", "global:x"},
		ModelMap: ModelMap{"x": {"cn:x", "global:x"}, "y": {"up-y"}},
		Keys:     []*UpKey{{Name: "k", APIKey: "sk-test", Enabled: true}}}); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	reloaded := newGatewayStore(path)
	if err := reloaded.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := reloaded.View().Channels[0].ModelMap
	if len(got["x"]) != 2 || got["x"][0] != "cn:x" || got["x"][1] != "global:x" {
		t.Fatalf("多值映射未正确往返: %+v", got)
	}
	if len(got["y"]) != 1 || got["y"][0] != "up-y" {
		t.Fatalf("单值映射未正确往返: %+v", got)
	}
}
