// 对下游暴露的 OpenAI 兼容端点：/v1/chat/completions 与 /v1/models。
// 下游用通用 key（gw keys）鉴权；请求体不改写、原样转发上游，
// 响应按渠道配置可选做 reasoning -> reasoning_content 改写。
package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// handleGateway 路由下游请求（已通过 GW key 鉴权，user = "gw:" + keyID）。
func handleGateway(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/chat/completions", "/chat/completions"):
		gatewayChat(w, r)
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/embeddings", "/embeddings"):
		gatewayOther(w, r, "/embeddings")
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/images/generations", "/images/generations"):
		gatewayOther(w, r, "/images/generations")
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/images/edits", "/images/edits"):
		gatewayOther(w, r, "/images/edits")
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/images/variations", "/images/variations"):
		gatewayOther(w, r, "/images/variations")
	case r.Method == http.MethodPost && matchesPath(r.URL.Path, "/v1/videos/generations", "/videos/generations"):
		gatewayOther(w, r, "/videos/generations")
	case r.Method == http.MethodGet && matchesPath(r.URL.Path, "/v1/models", "/models", "/v1/models/", "/models/"):
		gatewayModels(w, r)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown gateway endpoint", "not_found")
	}
}

func matchesPath(path string, candidates ...string) bool {
	p := strings.TrimSuffix(path, "/")
	if p == "" {
		p = "/"
	}
	for _, c := range candidates {
		if strings.TrimSuffix(c, "/") == p {
			return true
		}
	}
	return false
}

// gatewayChat 处理 chat/completions 转发。
func gatewayChat(w http.ResponseWriter, r *http.Request) {
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "read request body failed: "+err.Error(), "bad_request")
		return
	}
	stream := isStreamRequest(rawBody)
	model := extractModel(rawBody)

	if rs := reqStatsFrom(r.Context()); rs != nil {
		rs.model = model
		// 错误记录需要请求内容：gatewayChat 是唯一完整读到请求体的地方
		rs.requestBody = rawBody
	}

	cand := forwardChat(w, r, rawBody, stream, model)
	if cand == nil {
		return
	}
	if rs := reqStatsFrom(r.Context()); rs != nil {
		rs.key = cand.k.Name + "@" + cand.ch.Name
		rs.channel = cand.ch.Name
	}
}

// gatewayOther 处理 embeddings / 图片生成 / 视频生成等非对话端点的转发。
// 与 chat 共用同一套渠道路由/故障转移/冷却逻辑（按请求体的 model 字段匹配
// 渠道模型列表），上游端点由渠道 BaseURL（或 key BaseURL）拼接对应后缀得到，
// 不做 Responses 转换与 reasoning 改写；multipart 请求体（图片 edits /
// variations）连同 Content-Type（含 boundary）原样透传。
func gatewayOther(w http.ResponseWriter, r *http.Request, suffix string) {
	rawBody, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "read request body failed: "+err.Error(), "bad_request")
		return
	}
	ct := r.Header.Get("Content-Type")
	model := extractModelAny(rawBody, ct)
	// multipart 的 boundary 必须保留：只透传 multipart 类型的原始 Content-Type，
	// 普通 JSON 请求继续用网关默认的 application/json
	upstreamCT := ""
	if strings.HasPrefix(ct, "multipart/") {
		upstreamCT = ct
	}

	if rs := reqStatsFrom(r.Context()); rs != nil {
		rs.model = model
		// 错误记录需要请求内容：gatewayChat 是唯一完整读到请求体的地方，
		// 非对话端点在这里同样记下（含 multipart 表单体）
		rs.requestBody = rawBody
	}

	cand := forwardOther(w, r, rawBody, upstreamCT, model, suffix)
	if cand == nil {
		return
	}
	if rs := reqStatsFrom(r.Context()); rs != nil {
		rs.key = cand.k.Name + "@" + cand.ch.Name
		rs.channel = cand.ch.Name
	}
}

// modelsEntry /v1/models 单条模型。
type modelsEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// gatewayModels 聚合所有启用渠道的模型列表（去重）。
// 渠道声明了模型列表（静态配置或拉取结果）时直接使用；否则尝试拉取其 models
// 端点（失败不阻塞其它渠道）。
func gatewayModels(w http.ResponseWriter, r *http.Request) {
	snap := store.View() // 只读视图，零拷贝
	seen := map[string]bool{}
	var ids []string
	addModel := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		ids = append(ids, id)
	}

	type fetchJob struct {
		url    string
		header map[string]string
		apiKey string // 渠道第一个启用 key，用于 Bearer 鉴权
	}
	var jobs []fetchJob
	results := make(chan []string, len(snap.Channels))

	for _, ch := range snap.Channels {
		if !ch.Enabled {
			continue
		}
		if len(ch.Models) > 0 {
			for _, m := range ch.Models {
				addModel(m)
			}
			continue
		}
		u := ch.modelsURL()
		if u == "" {
			continue
		}
		apiKey := ""
		for _, k := range ch.Keys {
			if k.Enabled {
				apiKey = k.APIKey
				break
			}
		}
		jobs = append(jobs, fetchJob{url: u, header: ch.Headers, apiKey: apiKey})
	}

	// 并发拉取各渠道动态模型列表（5s 超时）
	for _, j := range jobs {
		go func(j fetchJob) {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			results <- fetchChannelModels(ctx, j.url, j.header, j.apiKey)
		}(j)
	}
	for range jobs {
		select {
		case list := <-results:
			for _, m := range list {
				addModel(m)
			}
		case <-r.Context().Done():
			return
		}
	}

	list := modelsList{Object: "list"}
	for _, id := range ids {
		owner := id
		if i := strings.Index(id, "/"); i > 0 {
			owner = id[:i]
		}
		list.Data = append(list.Data, modelsEntry{ID: id, Object: "model", OwnedBy: owner})
	}
	writeJSON(w, http.StatusOK, list)
}

// modelsList OpenAI models 响应。
type modelsList struct {
	Object string        `json:"object"`
	Data   []modelsEntry `json:"data"`
}

// fetchChannelModels 拉取单个渠道的模型列表；apiKey 非空时带 Bearer 鉴权，
// 同时发送渠道自定义头。首个 id 字符串数组字段被接受。失败返回空列表（聚合接口尽力而为）。
func fetchChannelModels(ctx context.Context, url string, headers map[string]string, apiKey string) []string {
	client := &http.Client{Timeout: 6 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	for name, value := range headers {
		if value != "" {
			req.Header.Set(name, value)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil
	}
	var out []string
	for _, m := range parsed.Data {
		if m.ID != "" {
			out = append(out, m.ID)
		}
	}
	return out
}

// ---- 下游鉴权 ----

// authorizeGW 校验下游请求的 Bearer key（gw keys）。
// GW_KEY_AUTH=false 时跳过校验（内网使用）。返回 (keyName, ok)。
func authorizeGW(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !cfg.GWKeyAuth {
		return "", true
	}
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing gateway key", "unauthorized")
		return "", false
	}
	for _, k := range store.View().GWKeys { // 只读视图（热路径，零拷贝）
		if k.Enabled && k.Key == token {
			return k.Name, true
		}
	}
	writeJSONError(w, http.StatusUnauthorized, "invalid gateway key", "unauthorized")
	return "", false
}
