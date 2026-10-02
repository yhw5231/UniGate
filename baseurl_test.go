// Base URL（OpenAI 兼容前缀）自适应补全 /v1 的单元测试与内联拉取模型端点测试。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNormalizeBaseURL：Base URL 默认自适应补全版本段——裸站点/自定义前缀补 /v1，
// 已含版本段或已指向具体端点时保持原样。
func TestNormalizeBaseURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		// 裸站点：补 /v1
		{"https://api.deepseek.com", "https://api.deepseek.com/v1"},
		{"https://api.deepseek.com/", "https://api.deepseek.com/v1"},
		{"https://api.deepseek.com///", "https://api.deepseek.com/v1"},
		{"  https://api.openai.com  ", "https://api.openai.com/v1"},
		{"http://127.0.0.1:8080", "http://127.0.0.1:8080/v1"},
		// 自定义前缀（无版本段）：在其后补 /v1
		{"https://host/openai", "https://host/openai/v1"},
		// 已有版本段：原样
		{"https://api.cline.bot/api/v1", "https://api.cline.bot/api/v1"},
		{"https://openrouter.ai/api/v1/", "https://openrouter.ai/api/v1"},
		{"https://host/v1", "https://host/v1"},
		{"https://host/v4", "https://host/v4"},
		{"https://host/v1beta", "https://host/v1beta"},
		{"https://host/v1/models", "https://host/v1/models"}, // 已指端点
		{"https://host/api/v2/openai", "https://host/api/v2/openai"},
		// 已指具体端点：原样（不再补版本段）
		{"https://host/chat/completions", "https://host/chat/completions"},
		{"https://host/v1/chat/completions", "https://host/v1/chat/completions"},
		{"https://host/responses", "https://host/responses"},
		{"https://host/models", "https://host/models"},
		{"https://host/embeddings", "https://host/embeddings"},
		{"https://host/images/generations", "https://host/images/generations"},
		// 带查询串（Azure 风格）：不改写
		{"https://h/openai/deployments/d?api-version=2024-02-01", "https://h/openai/deployments/d?api-version=2024-02-01"},
	}
	for _, c := range cases {
		if got := normalizeBaseURL(c.in); got != c.want {
			t.Errorf("normalizeBaseURL(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestChannelURLsAutoVersion：渠道端点推导同样自适应——只填站点根即可工作，
// 且不会出现 /v1/v1。
func TestChannelURLsAutoVersion(t *testing.T) {
	ch := &Channel{Name: "c", BaseURL: "https://api.deepseek.com"}
	if got := ch.chatURL(); got != "https://api.deepseek.com/v1/chat/completions" {
		t.Fatalf("chatURL = %q", got)
	}
	if got := ch.modelsURL(); got != "https://api.deepseek.com/v1/models" {
		t.Fatalf("modelsURL = %q", got)
	}
	ch.EndpointType = endpointResponses
	if got := ch.endpointURL(); got != "https://api.deepseek.com/v1/responses" {
		t.Fatalf("responsesURL = %q", got)
	}

	// 已含版本段：不重复补
	ch2 := &Channel{Name: "c", BaseURL: "https://api.cline.bot/api/v1"}
	if got := ch2.chatURL(); got != "https://api.cline.bot/api/v1/chat/completions" {
		t.Fatalf("chatURL = %q", got)
	}
	if got := ch2.modelsURL(); got != "https://api.cline.bot/api/v1/models" {
		t.Fatalf("modelsURL = %q", got)
	}

	// 显式模型列表端点优先
	ch3 := &Channel{Name: "c", BaseURL: "https://h", ModelsURL: "https://h/custom/list"}
	if got := ch3.modelsURL(); got != "https://h/custom/list" {
		t.Fatalf("modelsURL = %q", got)
	}

	// key 级 BaseURL 覆盖同样自适应
	ch4 := &Channel{Name: "c", BaseURL: "https://a", Keys: []*UpKey{{Name: "k", APIKey: "sk-1", BaseURL: "https://b"}}}
	cand := candidate{ch: ch4, k: ch4.Keys[0]}
	if got := cand.chatTarget(); got != "https://b/v1/chat/completions" {
		t.Fatalf("chatTarget = %q", got)
	}
	if got := cand.modelsTarget(); got != "https://b/v1/models" {
		t.Fatalf("modelsTarget = %q", got)
	}
	if got := cand.targetFor("/embeddings"); got != "https://b/v1/embeddings" {
		t.Fatalf("targetFor = %q", got)
	}
}

// TestChannelBaseURLPersistedNormalized：保存渠道时 BaseURL 落盘为补全后的形态。
func TestChannelBaseURLPersistedNormalized(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: "https://api.deepseek.com/", Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true, BaseURL: "https://api.x.com"}}})
	ch := store.Snapshot().Channels[0]
	if ch.BaseURL != "https://api.deepseek.com/v1" {
		t.Fatalf("channel base_url = %q", ch.BaseURL)
	}
	if got := ch.Keys[0].BaseURL; got != "https://api.x.com/v1" {
		t.Fatalf("key base_url = %q", got)
	}
}

// TestAdminFetchModelsInlineUsesFormContent：未保存的渠道也能按编辑器当前内容拉取
// 模型列表（内联定义，不落盘）。
func TestAdminFetchModelsInlineUsesFormContent(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	var gotPath, gotAuth string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"m-a"},{"id":"m-b","pricing":{"prompt":"0","completion":"0"}}]}`))
	}))
	defer up.Close()

	// 渠道尚未保存：只有编辑器里的表单内容（无 id、无名称）
	body := `{"channel":{"base_url":"` + up.URL + `","models":["handmade"],"keys":[{"name":"k1","api_key":"sk-form","enabled":true}]}}`
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/fetch-models", body, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Fetched    []string `json:"fetched"`
		FreeModels []string `json:"free_models"`
		Enabled    []string `json:"enabled"`
		Stale      []string `json:"stale"`
		KeyUsed    string   `json:"key_used"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if strings.Join(out.Fetched, ",") != "m-a,m-b" || out.KeyUsed != "k1" {
		t.Fatalf("inline fetch result: %+v", out)
	}
	if strings.Join(out.FreeModels, ",") != "m-b" {
		t.Fatalf("free models: %v", out.FreeModels)
	}
	if strings.Join(out.Enabled, ",") != "handmade" {
		t.Fatalf("enabled from form: %v", out.Enabled)
	}
	if strings.Join(out.Stale, ",") != "handmade" {
		t.Fatalf("stale from form: %v", out.Stale)
	}
	// 表单内容里的 Base URL 同样自适应补版本段
	if gotPath != "/v1/models" {
		t.Fatalf("upstream path = %q, want /v1/models", gotPath)
	}
	if gotAuth != "Bearer sk-form" {
		t.Fatalf("upstream auth = %q", gotAuth)
	}
	// 未保存：不落盘
	if got := len(store.Snapshot().Channels); got != 0 {
		t.Fatalf("inline fetch must not persist channel, got %d channels", got)
	}
}

// TestAdminFetchModelsInlineValidation：缺 Base URL / 缺渠道体时给出可读错误。
func TestAdminFetchModelsInlineValidation(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/fetch-models", `{"channel":{"keys":[{"api_key":"sk-1","enabled":true}]}}`, tok))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Base URL") {
		t.Fatalf("error should mention Base URL: %s", rr.Body.String())
	}

	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/fetch-models", `{}`, tok))
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 body=%s", rr2.Code, rr2.Body.String())
	}
}
