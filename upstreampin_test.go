// 上游内部渠道固定（upstreampin.go）测试：请求体注入、候选展开、管线解析、
// 探测收割、逐渠道验证、持久化与路由集成。
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// scriptedUpstream 记录收到的请求体并按序回放脚本响应。
type scriptedUpstream struct {
	mu     sync.Mutex
	bodies []string
	// respond 第 i 次调用（0 起）返回 (状态码, 响应体)；nil 则统一 200 {}
	respond func(i int, body string) (int, string)
	srv     *httptest.Server
	URL     string
}

func newScriptedUpstream(t *testing.T, respond func(i int, body string) (int, string)) *scriptedUpstream {
	t.Helper()
	u := &scriptedUpstream{respond: respond}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := readAllBody(r)
		u.mu.Lock()
		i := len(u.bodies)
		u.bodies = append(u.bodies, b)
		fn := u.respond
		u.mu.Unlock()
		st, body := 200, "{}"
		if fn != nil {
			st, body = fn(i, b)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		_, _ = w.Write([]byte(body))
	}))
	u.URL = u.srv.URL
	t.Cleanup(u.srv.Close)
	return u
}

func (u *scriptedUpstream) calls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.bodies)
}

func (u *scriptedUpstream) body(i int) string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if i < len(u.bodies) {
		return u.bodies[i]
	}
	return ""
}

func readAllBody(r *http.Request) (string, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 2048)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err.Error() == "EOF" {
				return string(buf), nil
			}
			return string(buf), err
		}
	}
}

// ---- 注入 ----

func TestInjectUpstreamPinDirectStrict(t *testing.T) {
	raw := []byte(`{"model":"m","messages":[],"stream":true,"temperature":0.7}`)
	out := injectUpstreamPin(raw, pipelineDirect, "alibaba", nil, "", nil)
	if out == nil {
		t.Fatal("inject failed")
	}
	var obj map[string]any
	if err := json.Unmarshal(out, &obj); err != nil {
		t.Fatal(err)
	}
	prv, _ := obj["provider"].(map[string]any)
	if prv == nil || fmt.Sprint(prv["only"]) != "[alibaba]" {
		t.Fatalf("provider.only = %v, want [alibaba] (%s)", prv, out)
	}
	if _, ok := obj["providerOptions"]; ok {
		t.Fatalf("direct 管线不应注入 providerOptions: %s", out)
	}
	// 其余字段原样保留
	if obj["temperature"] != 0.7 || obj["stream"] != true {
		t.Fatalf("unrelated fields lost: %s", out)
	}
}

func TestInjectUpstreamPinPlannerStrict(t *testing.T) {
	raw := []byte(`{"model":"m","messages":[]}`)
	out := injectUpstreamPin(raw, pipelinePlanner, "baseten", nil, "", nil)
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	po, _ := obj["providerOptions"].(map[string]any)
	gw, _ := po["gateway"].(map[string]any)
	if gw == nil || fmt.Sprint(gw["only"]) != "[baseten]" {
		t.Fatalf("gateway.only = %v (%s)", gw, out)
	}
	if _, ok := obj["provider"]; ok {
		t.Fatalf("planner 管线不应注入顶层 provider: %s", out)
	}
}

func TestInjectUpstreamPinUnknownPipelineBoth(t *testing.T) {
	raw := []byte(`{"model":"m"}`)
	out := injectUpstreamPin(raw, "", "alibaba", nil, "", nil)
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	if _, ok := obj["provider"]; !ok {
		t.Fatalf("未知管线应同时注入 provider: %s", out)
	}
	po, _ := obj["providerOptions"].(map[string]any)
	if po == nil || po["gateway"] == nil {
		t.Fatalf("未知管线应同时注入 providerOptions.gateway: %s", out)
	}
}

func TestInjectUpstreamPinPreferredOrderAndAllowlist(t *testing.T) {
	raw := []byte(`{"model":"m"}`)
	out := injectUpstreamPin(raw, pipelineDirect, "", []string{"alibaba", "baseten"}, "", []string{"alibaba", "baseten", "gl"})
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	prv, _ := obj["provider"].(map[string]any)
	if prv == nil || fmt.Sprint(prv["order"]) != "[alibaba baseten]" {
		t.Fatalf("provider.order = %v (%s)", prv, out)
	}
	if fmt.Sprint(prv["only"]) != "[alibaba baseten gl]" {
		t.Fatalf("provider.only(allowlist) = %v (%s)", prv["only"], out)
	}
}

func TestInjectUpstreamPinSortMapping(t *testing.T) {
	raw := []byte(`{"model":"m"}`)
	out := injectUpstreamPin(raw, pipelineDirect, "", nil, "cost", nil)
	var obj map[string]any
	_ = json.Unmarshal(out, &obj)
	prv := obj["provider"].(map[string]any)
	if prv["sort"] != "price" {
		t.Fatalf("direct sort = %v, want price", prv["sort"])
	}
	out = injectUpstreamPin(raw, pipelinePlanner, "", nil, "cost", nil)
	_ = json.Unmarshal(out, &obj)
	gw := obj["providerOptions"].(map[string]any)["gateway"].(map[string]any)
	if gw["sort"] != "cost" {
		t.Fatalf("planner sort = %v, want cost", gw["sort"])
	}
	if v := normalizeSortValue("none"); v != "" {
		t.Fatalf("none should clear sort, got %q", v)
	}
}

// ---- 候选展开 ----

func TestBuildUpstreamPinAttempts(t *testing.T) {
	setupGateway(t)
	raw := []byte(`{"model":"m"}`)
	ch := &Channel{Name: "cp", BaseURL: "http://up", Enabled: true}

	// 无固定 → nil（原样转发）
	if att := buildUpstreamPinAttempts(ch, "m", raw); att != nil {
		t.Fatalf("no pin: %+v", att)
	}
	// strict 多渠道 → 每渠道一个候选，only=[u]
	ch.ModelPins = map[string]*ModelUpstreamPin{"m": {Upstreams: []string{"a", "b"}, Mode: upstreamPinStrict}}
	att := buildUpstreamPinAttempts(ch, "m", raw)
	if len(att) != 2 || att[0].upstream != "a" || att[1].upstream != "b" {
		t.Fatalf("strict attempts: %+v", att)
	}
	for i, want := range []string{"[a]", "[b]"} {
		var obj map[string]any
		_ = json.Unmarshal(att[i].body, &obj)
		prv := obj["provider"].(map[string]any)
		if fmt.Sprint(prv["only"]) != want {
			t.Fatalf("attempt %d only = %v, want %s", i, prv["only"], want)
		}
	}
	// 排除项跳过
	ch.ModelPins["m"].Exclude = []string{"a"}
	att = buildUpstreamPinAttempts(ch, "m", raw)
	if len(att) != 1 || att[0].upstream != "b" {
		t.Fatalf("strict with exclude: %+v", att)
	}
	// preferred → 单候选 order
	ch.ModelPins["m"] = &ModelUpstreamPin{Upstreams: []string{"a", "b"}, Mode: upstreamPinPreferred}
	att = buildUpstreamPinAttempts(ch, "m", raw)
	if len(att) != 1 || att[0].upstream != "" {
		t.Fatalf("preferred attempts: %+v", att)
	}
	var obj map[string]any
	_ = json.Unmarshal(att[0].body, &obj)
	prv := obj["provider"].(map[string]any)
	if fmt.Sprint(prv["order"]) != "[a b]" {
		t.Fatalf("preferred order = %v (%s)", prv["order"], att[0].body)
	}
	// responses 端点渠道不支持
	ch.EndpointType = endpointResponses
	ch.ModelPins["m"] = &ModelUpstreamPin{Upstreams: []string{"a"}}
	if att := buildUpstreamPinAttempts(ch, "m", raw); att != nil {
		t.Fatalf("responses channel must not pin: %+v", att)
	}
}

// chatBody 构造转发用的原始请求体（固定注入在 forwardChat 内基于它展开）。
func chatBody(model string) []byte {
	return []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":false}`, model))
}

// ---- 路由集成 ----

// TestUpstreamPinStrictFailover strict 固定多渠道：第一个内部渠道 5xx 后，
// 同 key 立即用下一个固定渠道独占重试（请求体 only 逐个切换）。
func TestUpstreamPinStrictFailover(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, func(i int, body string) (int, string) {
		if i == 0 {
			return http.StatusInternalServerError, "boom"
		}
		return http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`
	})
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"m"},
		ModelPins: map[string]*ModelUpstreamPin{"m": {Upstreams: []string{"prov-a", "prov-b"}, Mode: upstreamPinStrict}},
		Keys:      []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	raw := chatBody("m")
	rr := httptest.NewRecorder()
	forwardChat(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(raw))), raw, false, "m")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if up.calls() != 2 {
		t.Fatalf("calls=%d, want 2", up.calls())
	}
	var first, second map[string]any
	_ = json.Unmarshal([]byte(up.body(0)), &first)
	_ = json.Unmarshal([]byte(up.body(1)), &second)
	if fmt.Sprint(first["provider"].(map[string]any)["only"]) != "[prov-a]" {
		t.Fatalf("first attempt only = %v", first["provider"])
	}
	if fmt.Sprint(second["provider"].(map[string]any)["only"]) != "[prov-b]" {
		t.Fatalf("second attempt only = %v", second["provider"])
	}
}

// TestUpstreamPinPreferredOrder preferred：单候选注入完整 order，上游自选。
func TestUpstreamPinPreferredOrder(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, nil)
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"m"},
		ModelPins: map[string]*ModelUpstreamPin{"m": {Upstreams: []string{"prov-a", "prov-b"}, Mode: upstreamPinPreferred,
			Known: []string{"prov-a", "prov-b", "prov-c"}, Exclude: []string{"prov-c"}}},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	raw := chatBody("m")
	rr := httptest.NewRecorder()
	forwardChat(rr, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(raw))), raw, false, "m")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if up.calls() != 1 {
		t.Fatalf("calls=%d, want 1（preferred 单候选）", up.calls())
	}
	var body map[string]any
	_ = json.Unmarshal([]byte(up.body(0)), &body)
	prv := body["provider"].(map[string]any)
	if fmt.Sprint(prv["order"]) != "[prov-a prov-b]" {
		t.Fatalf("order = %v", prv["order"])
	}
	if fmt.Sprint(prv["only"]) != "[prov-a prov-b]" {
		t.Fatalf("only(allowlist=Known-Exclude) = %v", prv["only"])
	}
}

// TestUpstreamPinModelScoped 未固定的模型不受影响；固定只作用于配置的模型。
func TestUpstreamPinModelScoped(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, nil)
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"m1", "m2"},
		ModelPins: map[string]*ModelUpstreamPin{"m1": {Upstreams: []string{"prov-a"}, Mode: upstreamPinStrict}},
		Keys:      []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	raw := chatBody("m2")
	forwardChat(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(raw))), raw, false, "m2")
	var body map[string]any
	_ = json.Unmarshal([]byte(up.body(0)), &body)
	if _, has := body["provider"]; has {
		t.Fatalf("未固定模型不应注入: %s", up.body(0))
	}
}

// ---- 探测（planner 管线）----

func TestProbeUpstreamsPlannerPipeline(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, func(i int, body string) (int, string) {
		if strings.Contains(body, "__probe__") {
			return http.StatusBadRequest, `{"error":{"message":"Invalid provider __probe__. Available providers are: Alibaba, Baseten, GL."}}`
		}
		return http.StatusOK, `{"choices":[{"message":{"content":"OK","provider_metadata":{"gateway":{"routing":{"finalProvider":"alibaba","canonicalSlug":"glm-5.3","fallbacksAvailable":["Baseten"]}}}}}]}`
	})
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"glm-5.3"},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	chID := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chID+"/probe-upstreams", `{"model":"glm-5.3"}`, adminToken(t)))
	if rr.Code != http.StatusOK {
		t.Fatalf("probe status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Pipeline     string   `json:"pipeline"`
		LastProvider string   `json:"last_provider"`
		Known        []string `json:"known"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Pipeline != pipelinePlanner || out.LastProvider != "alibaba" {
		t.Fatalf("pipeline=%q last=%q", out.Pipeline, out.LastProvider)
	}
	if strings.Join(out.Known, ",") != "alibaba,baseten,gl" {
		t.Fatalf("known = %v", out.Known)
	}
	// 探测产物写回渠道
	snap := store.Snapshot().Channels[0]
	pin := snap.ModelPins["glm-5.3"]
	if pin == nil || pin.Pipeline != pipelinePlanner || len(pin.Known) != 3 || pin.LastProvider != "alibaba" || pin.CanonicalSlug != "glm-5.3" {
		t.Fatalf("persisted pin: %+v", pin)
	}
}

// ---- 探测（direct 管线）----

func TestProbeUpstreamsDirectPipeline(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, func(i int, body string) (int, string) {
		if strings.Contains(body, "__probe__") {
			return http.StatusBadRequest, `{"error":{"metadata":{"available_providers":["Alibaba","Baseten"]}}}`
		}
		return http.StatusOK, `{"provider":"Alibaba","model":"z-ai/glm-5.3","choices":[{"message":{"content":"OK"}}]}`
	})
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"z-ai/glm-5.3"},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	chID := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chID+"/probe-upstreams", `{"model":"z-ai/glm-5.3"}`, adminToken(t)))
	if rr.Code != http.StatusOK {
		t.Fatalf("probe status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Pipeline     string `json:"pipeline"`
		LastProvider string `json:"last_provider"`
		Known        []string `json:"known"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.Pipeline != pipelineDirect || out.LastProvider != "alibaba" {
		t.Fatalf("pipeline=%q last=%q", out.Pipeline, out.LastProvider)
	}
	if strings.Join(out.Known, ",") != "alibaba,baseten" {
		t.Fatalf("known = %v", out.Known)
	}
	// direct 管线的收割请求应带顶层 provider.only
	var probeBody map[string]any
	_ = json.Unmarshal([]byte(up.body(1)), &probeBody)
	prv := probeBody["provider"].(map[string]any)
	if fmt.Sprint(prv["only"]) != "[__probe__]" {
		t.Fatalf("harvest provider.only = %v", prv["only"])
	}
}

// ---- 逐渠道验证 ----

func TestValidateUpstreamsEndpoint(t *testing.T) {
	setupGateway(t)
	up := newScriptedUpstream(t, func(i int, body string) (int, string) {
		var obj map[string]any
		_ = json.Unmarshal([]byte(body), &obj)
		only := obj["providerOptions"].(map[string]any)["gateway"].(map[string]any)["only"].([]any)
		switch only[0].(string) {
		case "alibaba":
			return http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`
		case "baseten":
			return http.StatusTooManyRequests, `{"error":{"message":"temporarily rate limited"}}`
		default:
			return http.StatusOK, `{"error":{"message":"no available providers for this model"}}`
		}
	})
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: up.URL, Enabled: true, Models: []string{"m"},
		ModelPins: map[string]*ModelUpstreamPin{"m": {Pipeline: pipelinePlanner,
			Known: []string{"alibaba", "baseten", "gl"}}},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	chID := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chID+"/validate-upstreams", `{"model":"m"}`, adminToken(t)))
	if rr.Code != http.StatusOK {
		t.Fatalf("validate status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Results []struct {
			Upstream string `json:"upstream"`
			Status   string `json:"status"`
		} `json:"results"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range out.Results {
		got[r.Upstream] = r.Status
	}
	if got["alibaba"] != "ok" || got["baseten"] != "limited" || got["gl"] != "bad" {
		t.Fatalf("statuses = %v", got)
	}
}

// ---- 保存固定配置 ----

func TestModelUpstreamPinEndpoint(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: "http://up", Enabled: true,
		Keys: []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})
	chID := store.Snapshot().Channels[0].ID

	// 保存
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/channels/"+chID+"/model-pin",
		`{"model":"m","mode":"preferred","upstreams":["a","b"],"exclude":["c"],"sort":"cost"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("put status=%d body=%s", rr.Code, rr.Body.String())
	}
	pin := store.Snapshot().Channels[0].ModelPins["m"]
	if pin == nil || pin.Mode != upstreamPinPreferred || strings.Join(pin.Upstreams, ",") != "a,b" || pin.Sort != "cost" {
		t.Fatalf("saved pin: %+v", pin)
	}

	// 清除（无探测产物 → 条目删除；有探测产物则保留）
	rr = httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/channels/"+chID+"/model-pin", `{"model":"m"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("clear status=%d", rr.Code)
	}
	if pin := store.Snapshot().Channels[0].ModelPins["m"]; pin != nil {
		t.Fatalf("cleared pin should be gone (no probe artifacts): %+v", pin)
	}

	// responses 渠道拒绝
	mustPutChannel(t, &Channel{Name: "resp", BaseURL: "http://up2", Enabled: true, EndpointType: endpointResponses,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})
	respID := store.Snapshot().Channels[1].ID
	rr = httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/channels/"+respID+"/model-pin", `{"model":"m","upstreams":["a"]}`, tok))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("responses channel pin status=%d want 400", rr.Code)
	}
}

// ---- 持久化 ----

func TestModelUpstreamPinPersist(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "cp", BaseURL: "http://up", Enabled: true,
		ModelPins: map[string]*ModelUpstreamPin{
			"m":     {Upstreams: []string{"a", "b"}, Mode: upstreamPinStrict, Known: []string{"a", "b", "c"}, Pipeline: pipelinePlanner},
			"empty": {Mode: "preferred"}, // 全空条目在 normalize 时删除
		},
		Keys: []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})
	snap := store.Snapshot().Channels[0]
	if _, ok := snap.ModelPins["empty"]; ok {
		t.Fatal("空条目应被 normalize 删除")
	}
	pin := snap.ModelPins["m"]
	if pin == nil || pin.Mode != upstreamPinStrict || len(pin.Known) != 3 {
		t.Fatalf("pin: %+v", pin)
	}
	// 重新加载（PutChannel 落盘 → load）
	if err := store.load(); err != nil {
		t.Fatal(err)
	}
	pin = store.Snapshot().Channels[0].ModelPins["m"]
	if pin == nil || strings.Join(pin.Upstreams, ",") != "a,b" {
		t.Fatalf("reloaded pin: %+v", pin)
	}
}

// ---- 解析 ----

func TestParseGatewayRoutingEnvelope(t *testing.T) {
	r := parseGatewayRouting([]byte(`{"data":{"provider":"Alibaba","model":"z-ai/glm","choices":[{"message":{"content":"x"}}]}}`))
	if r.pipeline != pipelineDirect || r.finalProvider != "alibaba" || r.canonicalSlug != "z-ai/glm" {
		t.Fatalf("direct envelope: %+v", r)
	}
	r = parseGatewayRouting([]byte(`{"choices":[{"message":{"provider_metadata":{"gateway":{"routing":{"finalProvider":"alibaba","fallbacksAvailable":["Baseten","GL"]}}}}}]}`))
	if r.pipeline != pipelinePlanner || r.finalProvider != "alibaba" || len(r.fallbacks) != 2 {
		t.Fatalf("planner: %+v", r)
	}
	r = parseGatewayRouting([]byte(`not json`))
	if r.pipeline != "" {
		t.Fatalf("garbage: %+v", r)
	}
}

func TestExtractAvailableProviders(t *testing.T) {
	got := extractAvailableProviders([]byte(`upstream error. Available providers are: alibaba, baseten and gl.`), pipelinePlanner)
	if strings.Join(got, ",") != "alibaba,baseten,gl" {
		t.Fatalf("planner extract: %v", got)
	}
	got = extractAvailableProviders([]byte(`{"error":{"metadata":{"available_providers":["Alibaba Cloud","Baseten"]}}}`), pipelineDirect)
	if strings.Join(got, ",") != "alibaba-cloud,baseten" {
		t.Fatalf("direct extract: %v", got)
	}
}

func TestClassifyUpstreamResponse(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"choices":[{"message":{"content":"ok"}}]}`, "ok"},
		{`{"error":{"message":"Rate limited, try again"}}`, "limited"},
		{`{"error":"unauthorized"}`, "auth"},
		{`{"error":{"message":"No available providers for model"}}`, "bad"},
		{`{"error":{"message":"weird failure"}}`, "unknown"},
		{`{"error":{"message":"empty response content"}}`, "ok"},
	}
	for _, c := range cases {
		if got, _ := classifyUpstreamResponse([]byte(c.body)); got != c.want {
			t.Fatalf("%s → %q, want %q", c.body, got, c.want)
		}
	}
}
