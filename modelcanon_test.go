// 模型名归一化（对标 go-gateway 的 modelIdentity/source_model 语义）：
// 上游报什么写法都对外暴露干净的名字（小写、去供应商前缀与变体后缀），
// 发往上游时仍用上游自己的写法（渠道模型映射记录）。
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// TestModelIdentityAndExposedName 归一化与对外名：模式不参与归一化。
func TestModelIdentityAndExposedName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"cline-free/deepseek-v4.1-flash:free", "deepseek-v4.1-flash"},
		{"DeepSeek-V4.1-Flash", "deepseek-v4.1-flash"},
		{"z-ai/glm-5.3", "glm-5.3"},
		{"gpt-4o", "gpt-4o"},
		{"x:free", "x"},
		{"x-free", "x"},
		{"cn:deepseek-v4.1-flash", "cn:deepseek-v4.1-flash"}, // 冒号后不是装饰词：整名保留
		{"  spaced-model  ", "spaced-model"},
		{"claude-*", "claude-*"},
	}
	for _, c := range cases {
		if got := modelIdentity(c.in); got != c.want {
			t.Errorf("modelIdentity(%q) = %q, want %q", c.in, got, c.want)
		}
		if got := exposedModelName(c.in); got != c.want {
			t.Errorf("exposedModelName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	// 通配/正则模式原样保留：归一化会把 "re:^gpt-4/.*$" 截断成 ".*$"
	for _, pat := range []string{"claude-*", "gpt-4?", "re:^gpt-4/.*$"} {
		if got := exposedModelName(pat); got != pat {
			t.Errorf("exposedModelName(%q) = %q, want unchanged", pat, got)
		}
	}
	// 归一化结果为空的名字退回原样（"cn:" 的冒号后为空串 = 装饰词，截断后只剩 "cn"）
	if got := modelIdentity("cn:"); got != "cn" {
		t.Errorf("modelIdentity(\"cn:\") = %q, want cn", got)
	}
}

// canonChat 通过转发链路发一次对话请求（显式传请求体，与网关实际路径一致）。
func canonChat(t *testing.T, model string) *httptest.ResponseRecorder {
	t.Helper()
	body := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],"stream":false}`)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	forwardChat(rr, req, body, false, model)
	if rr.Code != http.StatusOK {
		t.Fatalf("forward %q: status=%d body=%s", model, rr.Code, rr.Body.String())
	}
	return rr
}

// TestDeclaredModelsCanonicalizedWithUpstreamSpelling 保存时声明列表归一化，
// 上游原写法进模型映射；下游用干净名或原写法调用，上游都收到它自己的写法。
func TestDeclaredModelsCanonicalizedWithUpstreamSpelling(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	var models []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(body, &in)
		mu.Lock()
		models = append(models, in.Model)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"cline-free/DeepSeek-V4.1-Flash:free", "gpt-4o", "claude-*"},
		Keys:   []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})

	ch := store.Snapshot().Channels[0]
	if strings.Join(ch.Models, ",") != "deepseek-v4.1-flash,gpt-4o,claude-*" {
		t.Fatalf("声明列表应归一化为对外名: %v", ch.Models)
	}
	if got := ch.ModelMap["deepseek-v4.1-flash"]; len(got) != 1 || got[0] != "cline-free/DeepSeek-V4.1-Flash:free" {
		t.Fatalf("上游原写法应进模型映射: %+v", ch.ModelMap)
	}
	if _, ok := ch.ModelMap["gpt-4o"]; ok {
		t.Fatalf("与对外名相同的写法不该建映射: %+v", ch.ModelMap)
	}

	// 下游用干净名调用：上游收到上游原写法（含大小写）
	canonChat(t, "deepseek-v4.1-flash")
	// 下游用上游原写法调用：同样命中，上游写法不变
	canonChat(t, "cline-free/DeepSeek-V4.1-Flash:free")
	mu.Lock()
	defer mu.Unlock()
	if len(models) != 2 || models[0] != "cline-free/DeepSeek-V4.1-Flash:free" || models[1] != models[0] {
		t.Fatalf("上游收到的模型名应为上游原写法: %v", models)
	}
}

// TestSameIdentityMultipleUpstreamSpellings 同一对外名在上游的多个写法各成一个候选：
// 请求按顺序故障转移、冷却各自独立计算，对外只暴露一个名字、路由页逐行标注上游名。
func TestSameIdentityMultipleUpstreamSpellings(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	var models []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in struct {
			Model string `json:"model"`
		}
		_ = json.Unmarshal(body, &in)
		mu.Lock()
		models = append(models, in.Model)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if in.Model == "x" {
			w.WriteHeader(http.StatusTooManyRequests) // 第一个候选被限流 → 换下一个
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"x", "x:free"},
		Keys:   []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})

	ch := store.Snapshot().Channels[0]
	// 对外只留一个名字，两个上游写法都在映射里（各自一个候选）
	if strings.Join(ch.Models, ",") != "x" {
		t.Fatalf("对外名应去重为一个: %v", ch.Models)
	}
	if got := ch.ModelMap["x"]; strings.Join(got, ",") != "x,x:free" {
		t.Fatalf("上游写法应各成一个候选: %+v", ch.ModelMap)
	}
	// 路由视图：一个分组两行，各自的冷却键与上游名
	rs := routeStatusData("x")
	if len(rs.Models) != 1 || len(rs.Models[0].Keys) != 2 {
		t.Fatalf("路由视图应为一个分组两行: %+v", rs.Models)
	}
	if rs.Models[0].Keys[0].Upstream != "x" || rs.Models[0].Keys[0].CoolModel != "x" ||
		rs.Models[0].Keys[1].Upstream != "x:free" || rs.Models[0].Keys[1].CoolModel != "x:free" {
		t.Fatalf("路由行应分别标注上游名与冷却键: %+v", rs.Models[0].Keys)
	}

	// 请求：x 被限流 → 自动换 x:free（两个候选分别尝试、分别冷却）
	canonChat(t, "x")
	mu.Lock()
	if strings.Join(models, ",") != "x,x:free" {
		mu.Unlock()
		t.Fatalf("两个上游写法应各发一次: %v", models)
	}
	mu.Unlock()
	// 第二次请求：x 已冷却 → 直接打 x:free（x 不再被尝试）
	canonChat(t, "x")
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(models, ",") != "x,x:free,x:free" {
		t.Fatalf("冷却应按上游写法分别计算: %v", models)
	}
}

// TestFetchedCandidates 拉取候选：每个上游写法各占一条（同一对外名不合并，
// 因为上游的不同写法是不同的上游模型，要能分别勾选），字面去重、保序、带免费标记。
func TestFetchedCandidates(t *testing.T) {
	cands := fetchedCandidates(
		[]string{"cline-free/DeepSeek-V4.1-Flash:free", "deepseek-v4.1-flash", "gpt-4o", "z-ai/glm-5.3", "gpt-4o"},
		[]string{"cline-free/DeepSeek-V4.1-Flash:free"},
	)
	if len(cands) != 4 {
		t.Fatalf("candidates = %+v", cands)
	}
	if cands[0].Raw != "cline-free/DeepSeek-V4.1-Flash:free" || cands[0].Name != "deepseek-v4.1-flash" || !cands[0].Free {
		t.Fatalf("first candidate: %+v", cands[0])
	}
	if cands[1].Raw != "deepseek-v4.1-flash" || cands[1].Name != "deepseek-v4.1-flash" || cands[1].Free {
		t.Fatalf("second candidate（同一对外名的另一个上游写法）: %+v", cands[1])
	}
	if strings.Join(fetchedRawModels(cands), ",") != "cline-free/DeepSeek-V4.1-Flash:free,deepseek-v4.1-flash,gpt-4o,z-ai/glm-5.3" {
		t.Fatalf("raws = %v", fetchedRawModels(cands))
	}
	if strings.Join(fetchedFreeModels(cands), ",") != "cline-free/DeepSeek-V4.1-Flash:free" {
		t.Fatalf("free = %v", fetchedFreeModels(cands))
	}
}

// TestEnabledUpstreamModels 预勾选集合：当前真正发往上游的名字（映射目标优先），
// 未声明模型的渠道全选，通配覆盖的候选也算已启用。
func TestEnabledUpstreamModels(t *testing.T) {
	cands := fetchedCandidates([]string{"x", "x:free", "claude-3-5-sonnet", "gpt-4o"}, nil)
	// 未声明任何模型（放行全部）→ 全选
	if got := enabledUpstreamModels(&Channel{}, cands); strings.Join(got, ",") != "x,x:free,claude-3-5-sonnet,gpt-4o" {
		t.Fatalf("allow-all should check every candidate: %v", got)
	}
	// 声明了 x，且映射把 x 指到 x:free → 只有 x:free 生效（x 本身不是候选）
	ch := &Channel{Models: []string{"x"}, ModelMap: ModelMap{"x": {"x:free"}}}
	if got := enabledUpstreamModels(ch, cands); strings.Join(got, ",") != "x:free" {
		t.Fatalf("mapped target should be the only enabled candidate: %v", got)
	}
	// 声明列表里写了两个写法 → 两个都是候选
	ch2 := &Channel{Models: []string{"x", "x:free"}}
	if got := enabledUpstreamModels(ch2, cands); strings.Join(got, ",") != "x,x:free" {
		t.Fatalf("both spellings declared: %v", got)
	}
	// 通配覆盖的候选也算已启用（避免重建时静默丢掉通配覆盖的模型）
	ch3 := &Channel{Models: []string{"claude-*"}}
	if got := enabledUpstreamModels(ch3, cands); strings.Join(got, ",") != "claude-3-5-sonnet" {
		t.Fatalf("pattern-covered candidate: %v", got)
	}
}

// TestFetchModelsInlineCanonicalPayload 编辑器内联拉取：返回对外名 + 上游原写法 +
// 已启用集合（归一化），供 WebUI 勾选与写回映射。
func TestFetchModelsInlineCanonicalPayload(t *testing.T) {
	setupGateway(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[
			{"id":"cline-free/DeepSeek-V4.1-Flash:free","pricing":{"prompt":"0","completion":"0"}},
			{"id":"z-ai/glm-5.3"},
			{"id":"handmade"}]}`))
	}))
	defer up.Close()

	tok := adminToken(t)
	body := `{"channel":{"base_url":"` + up.URL + `","models":["handmade","Old-Prefixed/Model-A"],
		"keys":[{"name":"k","api_key":"sk-f","enabled":true}]}}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/fetch-models", body, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Fetched         []string `json:"fetched"`
		FreeModels      []string `json:"free_models"`
		Enabled         []string `json:"enabled"`
		EnabledIDs      []string `json:"enabled_ids"`
		EnabledUpstream []string `json:"enabled_upstream"`
		Stale           []string `json:"stale"`
		Total           int      `json:"total"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 候选是**上游写法**（每个上游模型一条，供逐个勾选）
	if strings.Join(out.Fetched, ",") != "cline-free/DeepSeek-V4.1-Flash:free,z-ai/glm-5.3,handmade" || out.Total != 3 {
		t.Fatalf("fetched = %v total=%d", out.Fetched, out.Total)
	}
	if strings.Join(out.FreeModels, ",") != "cline-free/DeepSeek-V4.1-Flash:free" {
		t.Fatalf("free_models = %v", out.FreeModels)
	}
	// 已启用集合：对外名（enabled_ids）与上游写法（enabled_upstream，供预勾选）
	if strings.Join(out.EnabledIDs, ",") != "handmade,model-a" {
		t.Fatalf("enabled_ids = %v", out.EnabledIDs)
	}
	if strings.Join(out.EnabledUpstream, ",") != "handmade" {
		t.Fatalf("enabled_upstream = %v", out.EnabledUpstream)
	}
	// stale 用归一化比较（Old-Prefixed/Model-A 上游未返回）
	if strings.Join(out.Stale, ",") != "model-a" {
		t.Fatalf("stale = %v", out.Stale)
	}
}

// TestFetchModelsReplaceWritesCanonicalModelsAndMapping ?replace=1 全量替换：列表存
// 对外名、上游写法进映射，且丢弃已不在列表里的旧映射键。
func TestFetchModelsReplaceWritesCanonicalModelsAndMapping(t *testing.T) {
	setupGateway(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"z-ai/glm-5.3"},{"id":"gpt-4o"}]}`))
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models:   []string{"old-model"},
		ModelMap: ModelMap{"old-model": {"legacy/old-model"}, "keep": {"up-keep"}},
		Keys:     []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/fetch-models?replace=1", "", adminToken(t)))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	ch := store.Snapshot().Channels[0]
	if strings.Join(ch.Models, ",") != "glm-5.3,gpt-4o" {
		t.Fatalf("models = %v", ch.Models)
	}
	if got := ch.ModelMap["glm-5.3"]; len(got) != 1 || got[0] != "z-ai/glm-5.3" {
		t.Fatalf("model_map = %+v", ch.ModelMap)
	}
	if _, ok := ch.ModelMap["old-model"]; ok {
		t.Fatalf("已不在列表里的映射键应被丢弃: %+v", ch.ModelMap)
	}
	if _, ok := ch.ModelMap["keep"]; ok {
		t.Fatalf("同样不在新列表里的映射键一并丢弃（以上游为准）: %+v", ch.ModelMap)
	}
}

// TestNormalizeDeclaredModelsIdempotent 归一化幂等：保存两次不会给同一个名字再加一个
//「自身候选」（否则映射越存越长、请求越试越多），多写法的候选也不会丢。
func TestNormalizeDeclaredModelsIdempotent(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: "http://127.0.0.1:1", Enabled: true,
		Models: []string{"z-ai/glm-5.3", "x", "x:free", "gpt-4o"},
		Keys:   []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})

	first := store.Snapshot().Channels[0]
	if strings.Join(first.Models, ",") != "glm-5.3,x,gpt-4o" {
		t.Fatalf("models = %v", first.Models)
	}
	if strings.Join(first.ModelMap["glm-5.3"], ",") != "z-ai/glm-5.3" ||
		strings.Join(first.ModelMap["x"], ",") != "x,x:free" {
		t.Fatalf("model_map = %+v", first.ModelMap)
	}

	// 原样再保存一次（WebUI 打开即保存的路径）：结果必须完全一致
	again := *first
	mustPutChannel(t, &again)
	second := store.Snapshot().Channels[0]
	if !sameModelList(first.Models, second.Models) || !sameModelMap(first.ModelMap, second.ModelMap) {
		t.Fatalf("归一化不幂等: models %v → %v, map %+v → %+v",
			first.Models, second.Models, first.ModelMap, second.ModelMap)
	}
}

// TestLegacyConfigMigratesDeclaredModels 历史配置（列表里存的是上游原写法）加载时
// 迁移为对外名 + 映射并写回磁盘；/v1/models 暴露干净名字。
func TestLegacyConfigMigratesDeclaredModels(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.json")
	legacy := `{"channels":[{"id":"c1","name":"c","base_url":"http://127.0.0.1:1","enabled":true,
		"models":["cline-free/DeepSeek-V4.1-Flash:free","gpt-4o"],
		"keys":[{"id":"k1","name":"k","api_key":"sk-1","enabled":true}]}],
		"gateway_keys":[{"id":"g1","name":"down","key":"sk-gw","enabled":true}]}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatalf("write legacy config: %v", err)
	}

	s := newGatewayStore(path)
	if err := s.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	ch := s.View().Channels[0]
	if strings.Join(ch.Models, ",") != "deepseek-v4.1-flash,gpt-4o" {
		t.Fatalf("迁移后的模型列表: %v", ch.Models)
	}
	if got := ch.ModelMap["deepseek-v4.1-flash"]; len(got) != 1 || got[0] != "cline-free/DeepSeek-V4.1-Flash:free" {
		t.Fatalf("迁移后的模型映射: %+v", ch.ModelMap)
	}
	// 迁移写回磁盘：再加载一次仍是归一化结果
	reloaded := newGatewayStore(path)
	if err := reloaded.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := reloaded.View().Channels[0].Models; strings.Join(got, ",") != "deepseek-v4.1-flash,gpt-4o" {
		t.Fatalf("迁移未写回: %v", got)
	}
}

// TestGatewayModelsExposesCanonicalNames /v1/models 对外只给干净名字：历史配置里
// 存着上游原写法（含映射键写法）时也一样。
func TestGatewayModelsExposesCanonicalNames(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: "http://127.0.0.1:1", Enabled: true,
		Models:   []string{"cline-free/DeepSeek-V4.1-Flash:free", "claude-*"},
		ModelMap: ModelMap{"cline-free/Y:free": {"up-y"}},
		Keys:     []*UpKey{{Name: "k", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testGWKey(t, "canon"))
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"deepseek-v4.1-flash"`) || !strings.Contains(body, `"y"`) {
		t.Fatalf("/v1/models 应暴露归一化名: %s", body)
	}
	if strings.Contains(body, "cline-free/") {
		t.Fatalf("/v1/models 不应暴露上游前缀: %s", body)
	}
}
