// 新功能测试：渠道分组、用 key 拉取上游模型列表（免费标记仅随响应展示不落盘）、
// 请求日志只记录大模型网关接口。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// ---- parseModelsPayload：价格解析与免费标记 ----

func TestParseModelsPayloadPricing(t *testing.T) {
	body := `{"data":[
		{"id":"m-free","pricing":{"prompt":"0","completion":"0"}},
		{"id":"m-free-num","pricing":{"prompt":0,"completion":0}},
		{"id":"m-paid","pricing":{"prompt":"0.0000015","completion":"0.000002"}},
		{"id":"m-half","pricing":{"prompt":"0","completion":"0.5"}},
		{"id":"m-noprice"},
		{"id":"m-io","pricing":{"input":0,"output":0}}
	]}`
	models, free, err := parseModelsPayload([]byte(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	wantModels := []string{"m-free", "m-free-num", "m-paid", "m-half", "m-noprice", "m-io"}
	if strings.Join(models, ",") != strings.Join(wantModels, ",") {
		t.Fatalf("models=%v want %v", models, wantModels)
	}
	wantFree := []string{"m-free", "m-free-num", "m-io"}
	if strings.Join(free, ",") != strings.Join(wantFree, ",") {
		t.Fatalf("free=%v want %v", free, wantFree)
	}
}

func TestParseModelsPayloadInvalid(t *testing.T) {
	if _, _, err := parseModelsPayload([]byte(`not json`)); err == nil {
		t.Fatal("expected error for invalid json")
	}
	models, free, err := parseModelsPayload([]byte(`{"data":[]}`))
	if err != nil || models != nil || free != nil {
		t.Fatalf("empty data: %v %v %v", models, free, err)
	}
}

// 非标准 /models 响应形态的兼容：裸数组对象、裸字符串数组、name/model 回退
func TestParseModelsPayloadAlternateShapes(t *testing.T) {
	// 裸对象数组（无 data 包装）
	models, _, err := parseModelsPayload([]byte(`[{"id":"a"},{"name":"n1"},{"model":"m1"},{}]`))
	if err != nil {
		t.Fatalf("bare object array: %v", err)
	}
	if strings.Join(models, ",") != "a,n1,m1" {
		t.Fatalf("bare object array models: %v", models)
	}
	// 裸字符串数组
	models, _, err = parseModelsPayload([]byte(`["x","y"]`))
	if err != nil || strings.Join(models, ",") != "x,y" {
		t.Fatalf("bare string array: %v %v", models, err)
	}
	// id 为数字（部分上游）应跳过而非报错
	models, _, err = parseModelsPayload([]byte(`{"data":[{"id":123},{"name":"ok"}]}`))
	if err != nil || strings.Join(models, ",") != "ok" {
		t.Fatalf("numeric id skipped: %v %v", models, err)
	}
	// 完全不是模型列表的响应
	if _, _, err = parseModelsPayload([]byte(`{"error":{"message":"nope"}}`)); err == nil {
		t.Fatal("expected error for non-model response")
	}
}

// ---- 拉取模型端点：key 鉴权 + 写回可用模型（免费标记不落盘） ----

func TestAdminFetchModelsSetsAvailableModels(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	calls := map[string]int{}
	auths := map[string]string{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		auths[r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/models") {
			_, _ = w.Write([]byte(`{"data":[
				{"id":"free-model","pricing":{"prompt":"0","completion":"0"}},
				{"id":"paid-model","pricing":{"prompt":"0.001","completion":"0.002"}},
				{"id":"plain-model"}]}`))
		} else {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"pong"}}]}`))
		}
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", Group: "主力", BaseURL: up.URL, Enabled: true,
		Models: []string{"handmade"},
		Keys:   []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	chid := store.Snapshot().Channels[0].ID
	tok := adminToken(t)

	// 默认 dry-run：返回候选与 free 标注，不写回渠道（供 WebUI 勾选启用）
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/fetch-models", "", tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("fetch-models status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Fetched    []string `json:"fetched"`
		FreeModels []string `json:"free_models"`
		Enabled    []string `json:"enabled"`
		Total      int      `json:"total"`
		KeyUsed    string   `json:"key_used"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &out)
	if out.Total != 3 || out.KeyUsed != "k1" {
		t.Fatalf("fetch result: %+v", out)
	}
	if len(out.Fetched) != 3 || out.Fetched[0] != "free-model" {
		t.Fatalf("fetched candidates: %v", out.Fetched)
	}
	// 免费清单随响应返回（供展示），渠道配置中不落盘
	if strings.Join(out.FreeModels, ",") != "free-model" {
		t.Fatalf("response free_models: %v", out.FreeModels)
	}
	// enabled 返回当前已启用集合（供前端默认勾选）
	if strings.Join(out.Enabled, ",") != "handmade" {
		t.Fatalf("response enabled: %v", out.Enabled)
	}
	mu.Lock()
	if auths["/models"] != "Bearer sk-test" {
		t.Fatalf("models fetch should send Bearer key, got %q", auths["/models"])
	}
	mu.Unlock()

	// dry-run 不写回渠道：仍只有手工模型
	ch := store.Snapshot().Channels[0]
	if len(ch.Models) != 1 || ch.Models[0] != "handmade" {
		t.Fatalf("dry-run should not modify channel models: %v", ch.Models)
	}
	// 分组持久化
	if ch.Group != "主力" {
		t.Fatalf("group lost: %q", ch.Group)
	}

	// replace=1 全量替换
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/fetch-models?replace=1", "", tok))
	if rr2.Code != http.StatusOK {
		t.Fatalf("fetch replace status=%d", rr2.Code)
	}
	ch = store.Snapshot().Channels[0]
	if len(ch.Models) != 3 || ch.Models[0] != "free-model" {
		t.Fatalf("replace mode models: %v", ch.Models)
	}

	// /v1/models 输出保持 OpenAI 兼容形态（不添加 free 等额外字段，需下游 gw key 鉴权）
	if err := store.PutGWKey(&GWKey{Name: "down", Key: "sk-gw-test", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewRecorder()
	mreq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	mreq.Header.Set("Authorization", "Bearer sk-gw-test")
	rootHandler(gw, mreq)
	if gw.Code != http.StatusOK {
		t.Fatalf("/v1/models status=%d body=%s", gw.Code, gw.Body.String())
	}
	var ml modelsList
	_ = json.Unmarshal(gw.Body.Bytes(), &ml)
	if len(ml.Data) != 3 {
		t.Fatalf("models: %+v", ml.Data)
	}
	ids := map[string]bool{}
	for _, e := range ml.Data {
		ids[e.ID] = true
	}
	if !ids["free-model"] || !ids["paid-model"] || !ids["plain-model"] {
		t.Fatalf("models: %+v", ml.Data)
	}
	if strings.Contains(gw.Body.String(), `"free"`) {
		t.Fatalf("/v1/models should not emit free field: %s", gw.Body.String())
	}
}

func TestAdminFetchModelsChannelNotFound(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/nope/fetch-models", "", tok))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status=%d want 404", rr.Code)
	}
}

// 上游 /models 返回 401 时，错误信息应包含目标 URL（便于用户排查鉴权/端点问题）
func TestAdminFetchModelsErrorIncludesURL(t *testing.T) {
	setupGateway(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad key"}`, http.StatusUnauthorized)
	}))
	defer up.Close()

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-bad", Enabled: true}}})
	chid := store.Snapshot().Channels[0].ID
	tok := adminToken(t)

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/fetch-models", "", tok))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, up.URL) {
		t.Fatalf("error should include target url %s: %s", up.URL, body)
	}
}

// ---- 渠道分组归一化 ----

func TestChannelGroupNormalized(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "g", BaseURL: "https://x.example.com", Group: "  付费组  ", Enabled: true})
	ch := store.Snapshot().Channels[0]
	if ch.Group != "付费组" {
		t.Fatalf("group not trimmed: %q", ch.Group)
	}
}

// ---- 请求日志只记录大模型网关接口 ----

func TestRequestLogRecordsLLMOnly(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":11,"completion_tokens":7}}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	if err := store.PutGWKey(&GWKey{Name: "down", Key: "sk-gw-test", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	// 与 main.go 一致的全局包装，使 token 计量生效
	wrapped := statsMiddleware(rootHandler)

	// 1) 大模型请求 → 记录（含模型与 token）
	rr := httptest.NewRecorder()
	chat := chatRequest("m1")
	chat.Header.Set("Authorization", "Bearer sk-gw-test")
	wrapped(rr, chat)
	if rr.Code != http.StatusOK {
		t.Fatalf("chat status=%d body=%s", rr.Code, rr.Body.String())
	}

	// 2) /v1/models 等大模型接口 → 也记录
	mreq := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	mreq.Header.Set("Authorization", "Bearer sk-gw-test")
	wrapped(httptest.NewRecorder(), mreq)

	// 3) 系统后台请求 → 不记录
	adminToken(t)                                                                      // /login
	rootHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil)) // WebUI 静态页
	rootHandler(httptest.NewRecorder(), adminReq(http.MethodGet, "/admin/api/state", "", adminToken(t)))

	recs := reqLog.Snapshot()
	if len(recs) != 2 {
		t.Fatalf("reqLog should contain only LLM gateway requests, got %d: %+v", len(recs), recs)
	}
	paths := map[string]bool{}
	var chatRec *RequestRecord
	for i := range recs {
		paths[recs[i].Path] = true
		if recs[i].Path == "/v1/chat/completions" {
			chatRec = &recs[i]
		}
	}
	if !paths["/v1/chat/completions"] || !paths["/v1/models"] {
		t.Fatalf("expected chat+models records, got paths %v", paths)
	}
	if chatRec == nil || chatRec.Model != "m1" || chatRec.PromptTokens != 11 || chatRec.CompletionTokens != 7 {
		t.Fatalf("chat record fields: %+v", chatRec)
	}
	if chatRec.Key == "" || strings.Contains(chatRec.Key, "sk-gw-test") {
		t.Fatalf("key 列应为「名称@渠道」而非原始 key 值: %q", chatRec.Key)
	}
}
