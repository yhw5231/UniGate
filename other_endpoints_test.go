// 非对话端点（embeddings / 图片生成 / 视频生成）转发测试：路径推导、multipart
// 透传、模型过滤、responses 渠道不转换、故障转移与用量记账。
package main

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// otherUpstream 记录收到的路径、鉴权头、Content-Type 与请求体。
type otherUpstream struct {
	mu          sync.Mutex
	calls       int
	paths       []string
	auths       []string
	contentType []string
	bodies      [][]byte
	status      int
	respBody    string
	srv         *httptest.Server
	URL         string
}

func newOtherUpstream(t *testing.T, status int, respBody string) *otherUpstream {
	t.Helper()
	u := &otherUpstream{status: status, respBody: respBody}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.calls++
		u.paths = append(u.paths, r.URL.Path)
		u.auths = append(u.auths, r.Header.Get("Authorization"))
		u.contentType = append(u.contentType, r.Header.Get("Content-Type"))
		u.bodies = append(u.bodies, body)
		u.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(u.status)
		_, _ = io.WriteString(w, u.respBody)
	}))
	u.URL = u.srv.URL
	t.Cleanup(u.srv.Close)
	return u
}

func (u *otherUpstream) last() (path, auth, ct string, body []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	i := len(u.paths) - 1
	if i < 0 {
		return "", "", "", nil
	}
	return u.paths[i], u.auths[i], u.contentType[i], u.bodies[i]
}

// otherRequest 构造指向非对话端点的网关请求。
func otherRequest(path, contentType, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

// multipartBody 构造含 model 表单字段与图片文件字段的 multipart 体，
// 返回 body 与带 boundary 的 Content-Type。
func multipartBody(t *testing.T, fields map[string]string, fileName string) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			t.Fatalf("WriteField(%s): %v", k, err)
		}
	}
	if fileName != "" {
		fw, err := mw.CreateFormFile("image", fileName)
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		_, _ = io.WriteString(fw, "fake-image-bytes")
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("multipart close: %v", err)
	}
	return buf.String(), mw.FormDataContentType()
}

// gwOtherHandler 把 statsMiddleware 包在 gatewayOther 上（与生产挂载一致，
// 用量/日志中间件生效）。
func gwOtherHandler(suffix string) http.HandlerFunc {
	return statsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		gatewayOther(w, r, suffix)
	})
}

func embeddingsResp(usage bool) string {
	suffix := ""
	if usage {
		suffix = `,"usage":{"prompt_tokens":15,"total_tokens":15}`
	}
	return `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}]` + suffix + `}`
}

// TestEmbeddingsForward：embeddings 请求按 model 路由到渠道，上游收到
// /embeddings 路径 + Bearer 鉴权，响应原样透传。
func TestEmbeddingsForward(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, embeddingsResp(true))
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	body := `{"model":"text-embedding-3-small","input":"hello"}`
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json", body))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, auth, ct, got := up.last()
	if path != "/v1/embeddings" {
		t.Fatalf("upstream path=%q, want /v1/embeddings", path)
	}
	if auth != "Bearer sk-1" {
		t.Fatalf("upstream auth=%q", auth)
	}
	if !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("upstream content-type=%q", ct)
	}
	if string(got) != body {
		t.Fatalf("upstream body mismatch: got %q want %q", got, body)
	}
	if !strings.Contains(rr.Body.String(), `"embedding":[0.1,0.2,0.3]`) {
		t.Fatalf("downstream body not passthrough: %q", rr.Body.String())
	}
}

// TestOtherEndpointModelFilter：渠道未声明该模型时与 chat 一致返回 502。
func TestOtherEndpointModelFilter(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, embeddingsResp(false))
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Models: []string{"gpt-4o"}, Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json",
		`{"model":"text-embedding-3-small","input":"hi"}`))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502, body=%s", rr.Code, rr.Body.String())
	}
	if up.calls != 0 {
		t.Fatalf("upstream should not be hit, calls=%d", up.calls)
	}
}

// TestImageGenerationsForward：图片生成转发到 /images/generations。
func TestImageGenerationsForward(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, `{"created":1,"data":[{"b64_json":"AAA"}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/images/generations")(rr, otherRequest("/v1/images/generations", "application/json",
		`{"model":"dall-e-3","prompt":"a cat","size":"1024x1024"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, auth, _, _ := up.last()
	if path != "/v1/images/generations" {
		t.Fatalf("upstream path=%q, want /v1/images/generations", path)
	}
	if auth != "Bearer sk-1" {
		t.Fatalf("upstream auth=%q", auth)
	}
	if !strings.Contains(rr.Body.String(), `"b64_json":"AAA"`) {
		t.Fatalf("downstream body not passthrough: %q", rr.Body.String())
	}
}

// TestImageEditsMultipartForward：edits 的 multipart 请求体与 Content-Type
//（含 boundary）原样透传上游，model 从表单字段提取并正确路由。
func TestImageEditsMultipartForward(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, `{"created":1,"data":[{"url":"https://x/y.png"}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	body, ctype := multipartBody(t, map[string]string{"model": "dall-e-2", "prompt": "make it red"}, "cat.png")
	rr := httptest.NewRecorder()
	gwOtherHandler("/images/edits")(rr, otherRequest("/v1/images/edits", ctype, body))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, _, ct, got := up.last()
	if path != "/v1/images/edits" {
		t.Fatalf("upstream path=%q, want /v1/images/edits", path)
	}
	if ct != ctype {
		t.Fatalf("upstream content-type=%q, want original %q", ct, ctype)
	}
	if string(got) != body {
		t.Fatalf("upstream multipart body not byte-identical: got %d bytes want %d", len(got), len(body))
	}
}

// TestVideoGenerationsForward：视频生成转发到 /videos/generations。
func TestVideoGenerationsForward(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, `{"task_id":"task-123"}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/videos/generations")(rr, otherRequest("/v1/videos/generations", "application/json",
		`{"model":"kling-v1","prompt":"a whale"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, _, _, _ := up.last()
	if path != "/v1/videos/generations" {
		t.Fatalf("upstream path=%q, want /v1/videos/generations", path)
	}
	if !strings.Contains(rr.Body.String(), `"task_id":"task-123"`) {
		t.Fatalf("downstream body not passthrough: %q", rr.Body.String())
	}
}

// TestOtherEndpointSkipsResponsesTranslation：EndpointType=responses 的渠道上，
// 非对话端点既不做请求体转换，也不打 /responses，只按后缀拼接。
func TestOtherEndpointSkipsResponsesTranslation(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, embeddingsResp(false))
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, EndpointType: "responses", Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	body := `{"model":"text-embedding-3-small","input":"hi"}`
	rr := httptest.NewRecorder()
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json", body))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, _, _, got := up.last()
	if path != "/v1/embeddings" {
		t.Fatalf("upstream path=%q, want /v1/embeddings (not /responses)", path)
	}
	if string(got) != body {
		t.Fatalf("upstream body was translated: got %q want %q", got, body)
	}
}

// TestOtherEndpointNoRewrite：Rewrite=true 的渠道上，非对话响应原样透传
//（不做 reasoning -> reasoning_content 改写）。
func TestOtherEndpointNoRewrite(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusOK, `{"created":1,"data":[{"url":"u"}],"reasoning":"think"}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Rewrite: true, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/images/generations")(rr, otherRequest("/v1/images/generations", "application/json",
		`{"model":"dall-e-3","prompt":"a cat"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Body.String(); !strings.Contains(got, `"reasoning":"think"`) {
		t.Fatalf("response was rewritten: %q", got)
	}
}

// TestOtherEndpointFailoverOn429：非对话端点同样走故障转移——首 key 429
// 记冷却并换下一 key，成功响应来自第二个 key。
func TestOtherEndpointFailoverOn429(t *testing.T) {
	setupGateway(t)
	up1 := newOtherUpstream(t, http.StatusTooManyRequests, `{"error":{"message":"rate limited"}}`)
	up2 := newOtherUpstream(t, http.StatusOK, embeddingsResp(false))
	k1 := &UpKey{Name: "k1", APIKey: "sk-1", Enabled: true}
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.URL, Enabled: true,
		Keys: []*UpKey{
			k1,
			{Name: "k2", APIKey: "sk-2", BaseURL: up2.URL, Enabled: true},
		}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json",
		`{"model":"text-embedding-3-small","input":"hi"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	path, auth, _, _ := up2.last()
	if path != "/v1/embeddings" || auth != "Bearer sk-2" {
		t.Fatalf("second key not served: path=%q auth=%q", path, auth)
	}
	// 首 key 已按 key 粒度（默认冷却域，跨模型共享）记冷却
	if !cool.IsCooling(k1.ID, "") {
		t.Fatalf("key k1 cooldown not recorded")
	}
}

// TestOtherEndpointAll429：全部候选 429 时与 chat 一致返回 429 + rate_limited。
func TestOtherEndpointAll429(t *testing.T) {
	setupGateway(t)
	up := newOtherUpstream(t, http.StatusTooManyRequests, `{"error":{"message":"rate limited"}}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json",
		`{"model":"text-embedding-3-small","input":"hi"}`))

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "rate_limited") {
		t.Fatalf("error code not rate_limited: %q", rr.Body.String())
	}
}

// TestEmbeddingsUsageRecorded：embeddings 响应的 usage.prompt_tokens 计入
// 请求日志与用量记账（与 chat 同一链路）。
func TestEmbeddingsUsageRecorded(t *testing.T) {
	setupGateway(t)
	initStats()
	up := newOtherUpstream(t, http.StatusOK, embeddingsResp(true))
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	gwOtherHandler("/embeddings")(rr, otherRequest("/v1/embeddings", "application/json",
		`{"model":"text-embedding-3-small","input":"hello"}`))

	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d, body=%s", rr.Code, rr.Body.String())
	}
	recs := reqLog.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("reqLog has %d records, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Model != "text-embedding-3-small" {
		t.Fatalf("record model=%q", rec.Model)
	}
	if rec.PromptTokens != 15 {
		t.Fatalf("record prompt_tokens=%d, want 15", rec.PromptTokens)
	}
	if !strings.HasPrefix(rec.Path, "/v1/embeddings") {
		t.Fatalf("record path=%q", rec.Path)
	}
}