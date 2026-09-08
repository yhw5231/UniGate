// 路由引擎：把下游 OpenAI 兼容请求按渠道顺序 + key 顺序转发到上游，
// 失败时自动故障转移到下一个 key/渠道。
//
// 候选顺序：渠道在配置中的顺序即优先级；同一渠道内按 key 顺序。
// 每次请求最多尝试 cfg.MaxRouteTries 个候选；命中冷却的候选直接跳过。
// 冷却键粒度为渠道级开关 cooldown_scope："key"（默认，含旧配置空值）按 key 跨模型
// 共享冷却（一个模型触发故障即冷停该 key 的所有模型）；"key_model" 按 (keyID, model) 独立冷却。
// 故障分类与冷却：
//   - 网络错误            → NET_ERR_COOLDOWN（默认 15s），可配置自动换出口 IP
//   - 429                 → Retry-After（缺省 RATE_LIMIT_COOLDOWN，默认 1h）
//   - 401/403（鉴权失败） → AUTH_FAIL_COOLDOWN（默认 10m）
//   - 5xx                 → SERVER_ERR_COOLDOWN（默认 30s）
//   - 其他（含上游 400）  → 原样透传给下游（上游的业务语义不动）
package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

// candidate 一个可路由的 (渠道, key) 组合。
type candidate struct {
	ch *Channel
	k  *UpKey
}

// cooldownModel 返回冷却键中 model 部分的取值：渠道粒度为 "key_model" 时按
// (key, model) 独立冷却；默认 "" / "key"（含旧配置空值）按 key 跨模型共享冷却。
func (c *candidate) cooldownModel(model string) string {
	if c.ch.CooldownScope == cooldownScopeKeyModel {
		return model
	}
	return ""
}

// chatTarget 返回该候选的上游对话端点（key BaseURL 优先；按渠道端点类型
// 选择 /chat/completions 或 OpenAI Responses API 的 /responses）。
func (c *candidate) chatTarget() string {
	if u := c.k.BaseURL; u != "" {
		base := trimSlash(u)
		if c.ch.EndpointType == endpointResponses {
			return responsesURLOf(base)
		}
		if endsWithChatCompletions(base) {
			return base
		}
		return base + "/chat/completions"
	}
	return c.ch.endpointURL()
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func endsWithChatCompletions(s string) bool {
	const suffix = "/chat/completions"
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// buildCandidates 按优先级构建候选列表（深拷贝自 store）。
func buildCandidates(model string) []candidate {
	snap := store.Snapshot()
	var out []candidate
	for _, ch := range snap.Channels {
		if !ch.Enabled {
			continue
		}
		if !ch.allowsModel(model) {
			continue
		}
		for _, k := range ch.Keys {
			if k.Enabled {
				out = append(out, candidate{ch: ch, k: k})
			}
		}
	}
	return out
}

// resolveProxy 解析候选的代理路由（ipv6pool 懒解析：首次用时向池申请租约并缓存）。
// probe=true 时对池 SOCKS 出口做快速可达性探测（测试链路用，快速失败给可读错误）。
func resolveProxy(cand *candidate) (*ProxyRoute, error) {
	return resolveProxyOpts(cand, false)
}

func resolveProxyOpts(cand *candidate, probe bool) (*ProxyRoute, error) {
	spec := cand.k.Proxy
	if spec == nil || spec.Kind == "" {
		return nil, nil
	}
	switch spec.Kind {
	case "static":
		return parseProxyURL(spec.URL)
	case "ipv6pool":
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if probe {
			return leaseMgr.EnsureProbed(ctx, spec, cand.k.ID, proxyGroup(cand.ch, cand.k))
		}
		return leaseMgr.Ensure(ctx, spec, cand.k.ID, proxyGroup(cand.ch, cand.k))
	default:
		return nil, nil
	}
}

// maybeRotate 上游返回指定状态码时更换 ipv6pool 出口 IP（best effort）。
func maybeRotate(cand *candidate, status int) {
	spec := cand.k.Proxy
	if spec == nil || spec.Kind != "ipv6pool" {
		return
	}
	for _, s := range spec.RotateStatuses {
		if s != status {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		_, err := leaseMgr.Rotate(ctx, spec, cand.k.ID)
		cancel()
		if err != nil {
			log.Printf("rotate lease for key %s after status %d: %v", cand.k.Name, status, err)
		}
		return
	}
}

// rotateOnNetErr 网络失败时按配置换 IP。
func rotateOnNetErr(cand *candidate) {
	spec := cand.k.Proxy
	if spec == nil || spec.Kind != "ipv6pool" || !spec.RotateOnNetErr {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, err := leaseMgr.Rotate(ctx, spec, cand.k.ID)
	cancel()
	if err != nil {
		log.Printf("rotate lease for key %s after network error: %v", cand.k.Name, err)
	}
}

// applyChannelHeaders 把渠道级自定义头写入请求（头名原样保留，空值跳过）。
func applyChannelHeaders(h http.Header, headers map[string]string) {
	for name, value := range headers {
		if value == "" {
			continue
		}
		h[name] = []string{value}
	}
}

// forwardChat 执行故障转移转发，把最终响应写给下游 w。
// 返回实际服务的候选（用于统计），无可用上游时返回 nil。
func forwardChat(w http.ResponseWriter, r *http.Request, rawBody []byte, stream bool, model string) *candidate {
	cands := buildCandidates(model)
	if len(cands) == 0 {
		writeJSONError(w, http.StatusBadGateway, "no enabled upstream key available", "no_upstream")
		return nil
	}

	maxTries := cfg.MaxRouteTries
	if maxTries <= 0 {
		maxTries = len(cands)
	}
	if maxTries > len(cands) {
		maxTries = len(cands)
	}

	var (
		served      *candidate
		lastErr     string
		rateLimited bool
	)
	attempts := 0
	for _, cand := range cands {
		if attempts >= maxTries {
			break
		}
		cm := cand.cooldownModel(model)
		if cool.IsCooling(cand.k.ID, cm) {
			continue
		}
		attempts++

		route, err := resolveProxy(&cand)
		if err != nil {
			lastErr = "resolve proxy: " + err.Error()
			log.Printf("route: key %s proxy resolve failed: %v", cand.k.Name, err)
			cool.Mark(cand.k.ID, cm, cfg.NetErrCooldown)
			continue
		}

		client := newUpstreamClient(route)
		resp, err := doUpstreamRequest(r.Context(), client, &cand, rawBody, stream, r.Header)
		if err != nil {
			lastErr = "upstream: " + err.Error()
			cool.Mark(cand.k.ID, cm, cfg.NetErrCooldown)
			rotateOnNetErr(&cand)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			rateLimited = true
			d := retryAfterDuration(resp.Header.Get("Retry-After"), cfg.RateLimitCooldown)
			cool.Mark(cand.k.ID, cm, d)
			leaseMgr.RecordUse(cand.k.Proxy, cand.k.ID)
			maybeRotate(&cand, resp.StatusCode)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			lastErr = "upstream 429"
			continue
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			cool.Mark(cand.k.ID, cm, cfg.AuthFailCooldown)
			maybeRotate(&cand, resp.StatusCode)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			lastErr = "upstream auth failed"
			continue
		case resp.StatusCode >= 500:
			cool.Mark(cand.k.ID, cm, cfg.ServerErrCooldown)
			maybeRotate(&cand, resp.StatusCode)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			lastErr = "upstream 5xx"
			continue
		}

		// 成功拿到可透传的响应：改写（可选）后回给下游
		leaseMgr.RecordUse(cand.k.Proxy, cand.k.ID)
		serveUpstreamResponse(w, resp, stream, cand.ch.Rewrite, cand.ch.EndpointType)
		served = &cand
		break
	}

	if served != nil {
		return served
	}

	if rateLimited {
		// 按各候选渠道的冷却粒度构建冷却键（渠道可能混用两种粒度）
		pairs := make([]cooldownPair, 0, len(cands))
		for _, c := range cands {
			pairs = append(pairs, cooldownPair{c.k.ID, c.cooldownModel(model)})
		}
		if d, ok := cool.EarliestRetry(pairs); ok {
			secs := int64(d.Seconds()) + 1
			w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
			writeJSONError(w, http.StatusTooManyRequests,
				"all upstream keys rate limited for model "+model, "rate_limited")
			return nil
		}
	}
	msg := "all upstream attempts failed"
	if lastErr != "" {
		msg += ": " + lastErr
	}
	writeJSONError(w, http.StatusBadGateway, msg, "upstream_error")
	return nil
}

// doUpstreamRequest 构造并发送上游请求：注入渠道头 + Bearer key，
// 透传下游的 UA/Accept（无则按流式/非流式给默认值），保证请求与标准
// OpenAI 客户端直连形态一致。
// 渠道端点类型为 responses 时，先把 chat/completions 请求体转换为 Responses API 格式。
func doUpstreamRequest(ctx context.Context, client *http.Client, cand *candidate, rawBody []byte, stream bool, srcHeader http.Header) (*http.Response, error) {
	body := rawBody
	if cand.ch.EndpointType == endpointResponses {
		body = chatToResponsesRequest(rawBody)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cand.chatTarget(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cand.k.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cand.k.APIKey)
	}
	// 与客户端直连一致：透传下游 UA，缺省给出明确 UA（避免 Go 默认 UA 被上游/CDN 拒绝）
	if srcHeader != nil {
		if ua := srcHeader.Get("User-Agent"); ua != "" {
			req.Header.Set("User-Agent", ua)
		}
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "unigate/"+displayVersion())
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	applyChannelHeaders(req.Header, cand.ch.Headers)
	return client.Do(req)
}
