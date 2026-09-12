// 路由引擎：把下游 OpenAI 兼容请求按渠道顺序 + key 顺序转发到上游，
// 失败时自动故障转移到下一个 key/渠道。
//
// 候选顺序：渠道在配置中的顺序即优先级；同一渠道内按 key 顺序。
// 每次请求最多尝试 cfg.MaxRouteTries 个候选；命中冷却的候选直接跳过。
// 冷却键粒度为渠道级开关 cooldown_scope："key"（默认，含旧配置空值）按 key 跨模型
// 共享冷却；"key_model" 按 (keyID, model) 独立冷却。
// 故障分类与处理：
//   - 429                  → 唯一记冷却的故障：Retry-After 优先，缺省 RATE_LIMIT_COOLDOWN（默认 1h）
//   - 5xx                  → 只换 key 不冷却；按 key 记连续次数（正常请求清零），连续超过
//     ROTATE_AFTER_5XX（默认 3）自动换出口 IP（ipv6pool key 生效）
//   - 网络/代理错误         → 只换 key 不冷却；ipv6pool 候选换出口 IP 后同 key 立即
//     重试一次（坏出口自愈），仍失败再继续下一个 key
//   - 401/403              → 只换 key，不冷却、不换出口
//   - 其他（含上游 400）   → 原样透传给下游（上游的业务语义不动）
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
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
// 代理来源：key 自身配置优先，未单独配置时继承渠道级代理（同设置不同 IP）。
func resolveProxy(cand *candidate) (*ProxyRoute, error) {
	return resolveProxyOpts(cand, false)
}

func resolveProxyOpts(cand *candidate, probe bool) (*ProxyRoute, error) {
	spec := cand.k.effectiveProxy(cand.ch)
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

// rotateExit 更换 ipv6pool 出口 IP（best effort），reason 用于日志定位。
// 渠道级代理池（key 继承）同样生效。返回换 IP 失败的错误（非池代理返回 nil）。
func rotateExit(cand *candidate, reason string) error {
	spec := cand.k.effectiveProxy(cand.ch)
	if spec == nil || spec.Kind != "ipv6pool" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, err := leaseMgr.Rotate(ctx, spec, cand.k.ID)
	cancel()
	if err != nil {
		log.Printf("rotate lease for key %s (%s): %v", cand.k.Name, reason, err)
	}
	return err
}

// rotateOnNetErr 网络/代理失败时换 IP（ipv6pool key 均生效，无开关）。
func rotateOnNetErr(cand *candidate) error {
	return rotateExit(cand, "network error")
}

// isPoolKey 判断候选是否使用 ipv6pool 代理（key 级配置或继承渠道级）。
func isPoolKey(cand *candidate) bool {
	spec := cand.k.effectiveProxy(cand.ch)
	return spec != nil && spec.Kind == "ipv6pool"
}

// applyCustomHeaders 把渠道级自定义头写入上游请求：同名覆盖、无同名新增，
// 空值跳过。头名统一经 http.Header.Set 规范化——直接写 map 会保留原始大小写，
// 非规范名与网关自身设置的规范名并存成重复头（而非覆盖），HTTP/2 上游还会
// 因头名含大写被拒。Host 是特例：它不由头表下发（HTTP/1 由 req.Host 决定，
// HTTP/2 走 :authority），配置同名头时转入 req.Host 生效。
func applyCustomHeaders(req *http.Request, headers map[string]string) {
	for name, value := range headers {
		if name == "" || value == "" {
			continue
		}
		req.Header.Set(name, value)
	}
	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
		req.Header.Del("Host")
	}
}

// forwardChat 执行故障转移转发，把最终响应写给下游 w。
// 返回实际服务的候选（用于统计），无可用上游时返回 nil。
// 每个候选的尝试结果（含跳过原因）都会追加到请求日志的错误信息，便于在
// WebUI 上直接回答「为什么 502」：哪个 key 因什么失败、冷却了多久。
func forwardChat(w http.ResponseWriter, r *http.Request, rawBody []byte, stream bool, model string) *candidate {
	cands := buildCandidates(model)
	if len(cands) == 0 {
		writeJSONError(w, http.StatusBadGateway, "no enabled upstream key available", "no_upstream")
		return nil
	}

	// 路由策略：WebUI 设置优先，环境变量为默认
	pol := currentPolicy()
	maxTries := pol.MaxRouteTries
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
		attempts    int
		cooled      int
		trace       []attemptTrace
	)
	recordTrace := func(cand *candidate, event, detail string) {
		trace = append(trace, attemptTrace{
			Key:   maskKey(cand.k.Name + "@" + cand.ch.Name),
			Model: model,
			Event: event,
			Err:   detail,
		})
	}
	// 全部候选都在冷却时不硬 502：对最早到期的 key 穿透试探一次。冷却防的是
	// 反复冲击限流上游，但上游可能已恢复（并发型 429 恢复极快、短窗口限流
	// 数十秒即过），试探成功即解除冷却自愈——否则会一直表现为「渠道测试可用
	// 而网关持续 502」，只能靠手工测试解冻。
	pierceKey, pierceModel := "", ""
	if pierce := earliestCooldown(cands, model); pierce != nil {
		pierceKey, pierceModel = pierce.k.ID, pierce.cooldownModel(model)
	}
	pierced := false
	// egressRetried 记录本轮请求中已做过「换出口重试」的 key（每 key 最多一次）
	egressRetried := map[string]bool{}
	for i := 0; i < len(cands); {
		cand := cands[i]
		isRetry := egressRetried[cand.k.ID]
		if !isRetry && attempts >= maxTries {
			break
		}
		cm := cand.cooldownModel(model)
		if !isRetry && cool.IsCooling(cand.k.ID, cm) {
			if cand.k.ID != pierceKey || cm != pierceModel {
				cooled++
				recordTrace(&cand, "cooldown_skipped", "")
				i++
				continue
			}
			// 穿透：其余候选全在冷却，对此最早到期 key 做一次真实尝试
			pierced = true
			recordTrace(&cand, "cooldown_pierced", "")
		}
		if !isRetry {
			attempts++
		}

		route, err := resolveProxy(&cand)
		if err != nil {
			lastErr = "resolve proxy: " + err.Error()
			recordTrace(&cand, "proxy_error", lastErr)
			log.Printf("route: key %s proxy resolve failed: %v", cand.k.Name, err)
			i++
			continue
		}

		client := newUpstreamClient(route)
		resp, err := doUpstreamRequest(r.Context(), client, &cand, rawBody, stream, r.Header)
		if err != nil {
			// 下游已断开（超时/取消）导致上游请求被中止：不是上游故障，
			// 不换 IP，也不必再试其余候选（都会立刻以同样方式失败）
			if r.Context().Err() != nil {
				lastErr = "client canceled before upstream response"
				recordTrace(&cand, "client_canceled", lastErr)
				break
			}
			// 网络错误只换 key，不冷却（瞬断不该把健康 key 冷停）
			lastErr = "network: " + err.Error()
			recordTrace(&cand, "network_error", lastErr)
			// ipv6pool 候选：换出口后同 key 立即重试一次——SOCKS CONNECT 被拒
			// （rep 0x05）等出口级故障常只影响单个出口 IP，新出口可能立即可用；
			// 不重试的话单 key 渠道本次请求直接 502，要等下一次请求才用上新 IP。
			// 与测试链路 runTestOnce 的换 IP 重试语义一致；重试不计入 attempts
			//（MaxRouteTries 限制的是候选数），重试仍失败则继续下一个候选。
			if isPoolKey(&cand) && !isRetry && rotateOnNetErr(&cand) == nil {
				egressRetried[cand.k.ID] = true
				recordTrace(&cand, "egress_retry", "")
				continue // 不推进 i：同一候选换出口后原地重试
			}
			i++
			continue
		}

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			// 429 是唯一记冷却的故障：上游明确说"额度/频率受限"，冷却等待重试
			rateLimited = true
			d := retryAfterDuration(resp.Header.Get("Retry-After"), pol.RateLimitCooldown)
			cool.Mark(cand.k.ID, cm, d)
			leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
			lastErr = "upstream " + rejectReason(resp)
			recordTrace(&cand, "rejected_429", lastErr)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			i++
			continue
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			// 鉴权失败只换 key，不冷却、不换出口
			lastErr = "upstream " + rejectReason(resp)
			recordTrace(&cand, "rejected_auth", lastErr)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			i++
			continue
		case resp.StatusCode >= 500:
			// 上游 5xx 只换 key，不冷却；但按 key 记连续次数（成功请求清零），
			// 连续超过阈值（默认 3，即第 4 次起）说明当前出口大概率被上游
			// 封禁/降级，自动换出口 IP
			n := streaks.Inc(cand.k.ID)
			lastErr = "upstream " + rejectReason(resp)
			recordTrace(&cand, "rejected_5xx", lastErr)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			// 关闭 body（连接归还连接池）之后再换 IP：换 IP 会作废旧出口的
			// 空闲隧道，太早调用会漏掉当前这条刚用完的连接
			if pol.RotateAfter5xx > 0 && n > pol.RotateAfter5xx {
				streaks.Reset(cand.k.ID)
				rotateExit(&cand, fmt.Sprintf("%d consecutive 5xx", n))
			}
			i++
			continue
		}

		// 正常拿到响应（2xx/3xx/4xx 业务语义）：清零该 key 的 5xx 连续计数
		streaks.Reset(cand.k.ID)

		// 穿透成功：该 key 的存量冷却与事实相悖，立即解除（后续请求恢复正常路由）
		if pierced {
			cool.Clear(cand.k.ID, cand.cooldownModel(model))
		}

		// 成功拿到可透传的响应：改写（可选）后回给下游
		leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
		if resp.StatusCode >= 400 {
			// 上游 4xx/5xx 业务错误原样透传，但把响应体片段记入请求日志：
			// 不记的话错误日志只有 "Bad Request" 状态文本，回答不了
			//「上游为什么 400/404」（无效 key、模型不存在等具体原因）。
			peekUpstreamError(r, resp)
		}
		serveUpstreamResponse(w, resp, stream, cand.ch.Rewrite, cand.ch.EndpointType)
		served = &cand
		break
	}

	if served != nil {
		return served
	}

	// 无候选成功：把诊断信息写进请求日志（渠道/key/逐 key 原因），再回给下游
	msg := describeRouteFailure(model, len(cands), attempts, cooled, lastErr, trace)
	// 429 余波（全部冷却或本轮撞过 429）：附上最早可重试时间，下游可据此退避
	if attempts == 0 || rateLimited {
		pairs := make([]cooldownPair, 0, len(cands))
		for _, c := range cands {
			pairs = append(pairs, cooldownPair{c.k.ID, c.cooldownModel(model)})
		}
		if d, ok := cool.EarliestRetry(pairs); ok {
			secs := int64(d.Seconds()) + 1
			w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
			msg += fmt.Sprintf("; earliest retry in %ds", secs)
		}
	}
	setReqErrMsg(r, msg)

	if rateLimited {
		writeJSONError(w, http.StatusTooManyRequests, msg, "rate_limited")
		return nil
	}
	writeJSONError(w, http.StatusBadGateway, msg, "upstream_error")
	return nil
}

// attemptTrace 一次候选尝试的轨迹（key 脱敏、事件、失败原因）。
type attemptTrace struct {
	Key   string `json:"key"`
	Model string `json:"model,omitempty"`
	Event string `json:"event"`
	Err   string `json:"error,omitempty"`
}

// formatTrace 把逐 key 尝试轨迹压成一行紧凑文本，如
// "sk-1****c1 rejected_429(upstream 429: rate limited); sk-2****c2 network_error(...)"。
// 事件名保留原始英文标识，与日志/测试断言一致，避免翻译漂移。
func formatTrace(trace []attemptTrace) string {
	var parts []string
	for _, t := range trace {
		p := t.Key + " " + t.Event
		if t.Err != "" {
			p += "(" + t.Err + ")"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "; ")
}

// describeRouteFailure 汇总一轮故障转移的结果：可用 key 总数、尝试数、
// 冷却跳过数、最后失败原因与逐 key 尝试轨迹，供下游错误体与请求日志使用。
// 轨迹完整跟随（不再截断），保证「哪个 key 因什么失败」在错误信息里可查。
func describeRouteFailure(model string, total, attempted, cooled int, lastErr string, trace []attemptTrace) string {
	var b strings.Builder
	fmt.Fprintf(&b, "model %q: %d upstream key(s) unavailable", model, total)
	if attempted > 0 {
		fmt.Fprintf(&b, " (%d attempted", attempted)
		if cooled > 0 {
			fmt.Fprintf(&b, ", %d skipped by cooldown", cooled)
		}
		b.WriteString(")")
		if lastErr != "" {
			fmt.Fprintf(&b, ": %s", lastErr)
		}
	} else {
		// 一次都没尝试：全部 key 处于故障冷却中（典型为上一轮 429 的余波）
		fmt.Fprintf(&b, " (all %d in failure cooldown, nothing attempted", total)
		if len(trace) > 0 {
			var keys []string
			for _, t := range trace {
				keys = append(keys, t.Key)
			}
			fmt.Fprintf(&b, ": %s", strings.Join(keys, ", "))
		}
		b.WriteString(")")
	}
	if len(trace) > 0 {
		b.WriteString("; attempts: ")
		b.WriteString(formatTrace(trace))
	}
	return b.String()
}

// earliestCooldown 在「全部候选都处于冷却」时返回最早到期的候选（穿透试探
// 用）；存在任一未冷却候选时返回 nil——此时正常故障转移即可，不做穿透。
func earliestCooldown(cands []candidate, model string) *candidate {
	var best *candidate
	var bestUntil time.Time
	for i := range cands {
		cm := cands[i].cooldownModel(model)
		until, ok := cool.CoolingKey(cands[i].k.ID, cm)
		if !ok {
			return nil
		}
		if best == nil || until.Before(bestUntil) {
			best = &cands[i]
			bestUntil = until
		}
	}
	return best
}

// rejectReason 读取上游拒绝（4xx/5xx）响应体的开头作为失败原因
// （如限流描述、Cloudflare 拦截页、上游错误详情），供日志与错误体定位。
// 调用方负责随后 drain + Close body。
func rejectReason(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	s := strings.TrimSpace(string(body))
	if s == "" {
		return strconv.Itoa(resp.StatusCode)
	}
	return fmt.Sprintf("%d: %s", resp.StatusCode, truncate(s, 200))
}

// peekUpstreamError 预读上游错误响应（透传分支）开头一段响应体，作为失败
// 原因写入请求日志；预读的字节重新拼回 Body，透传给下游的内容不受影响。
func peekUpstreamError(r *http.Request, resp *http.Response) {
	prefix, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if len(prefix) == 0 {
		return
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(prefix), resp.Body), resp.Body}
	setReqErrMsg(r, fmt.Sprintf("upstream %d: %s", resp.StatusCode, truncate(strings.TrimSpace(string(prefix)), 200)))
}

// doUpstreamRequest 构造并发送上游请求：注入渠道头 + Bearer key，
// 透传下游的 UA/Accept（无则按流式/非流式给默认值），保证请求与标准
// OpenAI 客户端直连形态一致。渠道自定义头最后写入：同名覆盖网关默认头
//（含 Authorization / Content-Type / UA / Accept），无同名新增。
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
	applyCustomHeaders(req, cand.ch.Headers)
	return client.Do(req)
}
