// 上游内部渠道固定：把上游网关（如 Cline Pass）模型背后内置的多个上游渠道
// （provider）钉到固定集合，不让上游随机路由。语义对齐 dsh-cline-pass 的
// per-model pin（其路由行为参考 MIT 许可的 cline-pass-switcher 实测结论）：
//
//   - direct 管线（OpenRouter 型）：顶层 `provider.only/order/sort`；
//   - planner 管线（Vercel AI Gateway 型）：`providerOptions.gateway.only/order/sort`；
//   - 管线未知时两种写法都注入，各管线会忽略自己不认识的字段；
//   - 上游不认 exclude/ignore 字段，排除语义由「Known 集合减去排除项」的
//     allowlist（only）表达（Known 未探测时无法构造，仅 strict 固定可用）；
//   - strict（默认）把固定列表展开为逐个内部渠道的独占尝试（only=[u]，
//     失败由路由引擎故障转移到下一个内部渠道/key）；preferred 单候选注入
//     完整 order 优先序，由上游按序自选。
//
// 探测：先发正常小请求从响应的 provider_metadata.gateway.routing 读管线类型
// 与实际服务的渠道；再把 only 钉到不可能的渠道（__probe__）——上游在花费
// token 前报错并点名全部可用渠道，从中收割渠道清单。探测产物持久化在渠道
// 的 ModelPins 里供 WebUI 勾选，可随时重建。
package main

import (
	"context"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"time"
)

// openrouterSort OpenRouter（direct 管线）的排序指标名（planner 直接用原名）。
var openrouterSort = map[string]string{"cost": "price", "ttft": "latency", "tps": "throughput"}

// normalizeSortValue 归一化排序配置：空串/none = 不排序（none 是 dsh-cline-pass
// 约定的「清除」值）；其他非空值小写后原样传递（写错由上游 400 报出，不静默吞掉）。
func normalizeSortValue(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	if s == "" || s == "none" {
		return ""
	}
	return s
}

// pinAttempt 一个候选的固定注入结果。
type pinAttempt struct {
	body     []byte // 注入后的请求体（nil = 原样转发）
	upstream string // 本候选独占的内部渠道（trace 展示用；整体注入候选为空）
}

// withoutStrings 从 list 中剔除 exclude 里出现的项（保序去重不在此处理）。
func withoutStrings(list []string, exclude ...string) []string {
	if len(exclude) == 0 {
		return list
	}
	ex := map[string]bool{}
	for _, e := range exclude {
		ex[e] = true
	}
	out := make([]string, 0, len(list))
	for _, v := range list {
		if !ex[v] {
			out = append(out, v)
		}
	}
	return out
}

// buildUpstreamPinAttempts 把渠道上该模型的固定配置展开为路由候选请求体列表；
// 无固定配置（或 rawBody 无法注入）返回 nil，调用方原样转发。
//
//   - strict（默认）且固定列表非空：列表里每个内部渠道一个候选（only=[u]，
//     列表顺序即尝试顺序，排除项跳过）；
//   - 其余情况（preferred，或 strict 无固定列表但配了排除/sort）：单候选
//     整体注入 order/allowlist/sort。
func buildUpstreamPinAttempts(ch *Channel, model string, rawBody []byte) []pinAttempt {
	pin := ch.upstreamPinFor(model)
	if pin == nil {
		return nil
	}
	if ch.EndpointType == endpointResponses {
		// Responses API 请求体由 chatToResponsesRequest 重新构造，会丢弃
		// provider/providerOptions 字段——注入无效，按未固定处理
		return nil
	}
	sortVal := normalizeSortValue(pin.Sort)
	allow := allowlistOf(pin)

	if pin.Mode == upstreamPinStrict && len(pin.Upstreams) > 0 {
		out := make([]pinAttempt, 0, len(pin.Upstreams))
		for _, u := range withoutStrings(pin.Upstreams, pin.Exclude...) {
			body := injectUpstreamPin(rawBody, pin.Pipeline, u, nil, "", nil)
			if body == nil {
				return nil // 请求体不可解析：固定失效，原样转发
			}
			out = append(out, pinAttempt{body: body, upstream: u})
		}
		if len(out) == 0 {
			return nil // 固定列表全被排除：按未固定处理
		}
		return out
	}
	// preferred 或（strict 无固定列表）：单候选整体注入
	body := injectUpstreamPin(rawBody, pin.Pipeline, "", withoutStrings(pin.Upstreams, pin.Exclude...), sortVal, allow)
	if body == nil {
		return nil
	}
	return []pinAttempt{{body: body}}
}

// allowlistOf 计算 allowlist（only 集合）：探测到的 Known 减去排除项。
// 上游不认 exclude 字段，排除语义只能这样表达；Known 未探测（空）时返回
// nil——无法可靠构造允许集合，宁可不限制（WebUI 会提示先探测）。
func allowlistOf(pin *ModelUpstreamPin) []string {
	if len(pin.Known) == 0 || len(pin.Exclude) == 0 {
		return nil
	}
	excl := map[string]bool{}
	for _, e := range pin.Exclude {
		excl[e] = true
	}
	allow := make([]string, 0, len(pin.Known))
	for _, k := range pin.Known {
		if !excl[k] {
			allow = append(allow, k)
		}
	}
	if len(allow) == 0 {
		return nil
	}
	return allow
}

// injectUpstreamPin 把内部渠道固定写进 chat/completions 请求体。
//
//	upstream 非空：strict 独占尝试，注入 only=[upstream]（order/allow/sort 忽略）；
//	upstream 为空：整体注入——order 优先序（可空）、allow allowlist（可空，
//	映射为 only）、sortVal 排序指标（可空）。
//
// pipeline 为空（未探测）时两种管线写法都注入，各管线忽略不认识的字段。
// 请求体不可解析时返回 nil。请求体里已有的 provider/providerOptions.gateway
// 字段被网关注入值覆盖（同名合并，其余键保留）。
func injectUpstreamPin(rawBody []byte, pipeline, upstream string, order []string, sortVal string, allow []string) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &obj); err != nil {
		return nil
	}
	gw := map[string]any{}
	prv := map[string]any{}
	fill := func(m map[string]any) {
		if upstream != "" {
			m["only"] = []string{upstream}
			return
		}
		if len(order) > 0 {
			m["order"] = order
		}
		if len(allow) > 0 {
			m["only"] = allow
		}
	}
	if pipeline == pipelinePlanner || pipeline == "" {
		fill(gw)
		if sortVal != "" {
			gw["sort"] = sortVal
		}
	}
	if pipeline == pipelineDirect || pipeline == "" {
		fill(prv)
		if sortVal != "" {
			if mapped := openrouterSort[sortVal]; mapped != "" {
				prv["sort"] = mapped
			} else {
				prv["sort"] = sortVal
			}
		}
	}
	if len(gw) > 0 {
		merged := map[string]any{}
		if raw, ok := obj["providerOptions"]; ok {
			var po struct {
				Gateway map[string]any `json:"gateway"`
			}
			if json.Unmarshal(raw, &po) == nil && po.Gateway != nil {
				merged = po.Gateway
			}
		}
		for k, v := range gw {
			merged[k] = v
		}
		raw, err := json.Marshal(map[string]any{"gateway": merged})
		if err != nil {
			return nil
		}
		obj["providerOptions"] = raw
	}
	if len(prv) > 0 {
		merged := map[string]any{}
		if raw, ok := obj["provider"]; ok {
			_ = json.Unmarshal(raw, &merged)
		}
		for k, v := range prv {
			merged[k] = v
		}
		raw, err := json.Marshal(merged)
		if err != nil {
			return nil
		}
		obj["provider"] = raw
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	return out
}

// ---- 探测 ----

// pinRouting 一次响应读出的路由事实。
type pinRouting struct {
	pipeline      string // direct / planner / ""（无路由信息）
	canonicalSlug string
	finalProvider string
	fallbacks     []string
}

// gatewayEnvelope OpenAI 形态的响应（可能包 {data:...} 信封）。
type gatewayEnvelope struct {
	Data    json.RawMessage           `json:"data"`
	Choices []gatewayEnvelopeChoice   `json:"choices"`
	Provider string                    `json:"provider"`
	Model    string                    `json:"model"`
}

type gatewayEnvelopeChoice struct {
	Message struct {
		ProviderMetadata struct {
			Gateway struct {
				Routing struct {
					FinalProvider      string   `json:"finalProvider"`
					CanonicalSlug      string   `json:"canonicalSlug"`
					FallbacksAvailable []string `json:"fallbacksAvailable"`
				} `json:"routing"`
			} `json:"gateway"`
		} `json:"provider_metadata"`
	} `json:"message"`
}

// parseGatewayRouting 从 chat/completions 响应体解析上游路由信息（对标
// dsh-cline-pass 的 parseRouting）：planner 管线的路由挂在
// choices[0].message.provider_metadata.gateway.routing；direct 管线用顶层
// provider 字符串。{data:{choices:...}} 信封自动展开。
func parseGatewayRouting(body []byte) pinRouting {
	var env gatewayEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return pinRouting{}
	}
	provider, model, choices := env.Provider, env.Model, env.Choices
	if len(env.Data) > 0 {
		// {data:{...choices...}} 信封：展开后重新解析
		var inner gatewayEnvelope
		if json.Unmarshal(env.Data, &inner) == nil && len(inner.Choices) > 0 {
			provider, model, choices = inner.Provider, inner.Model, inner.Choices
		}
	}
	var r pinRouting
	if len(choices) > 0 {
		rt := choices[0].Message.ProviderMetadata.Gateway.Routing
		r.finalProvider = strings.TrimSpace(rt.FinalProvider)
		r.canonicalSlug = strings.TrimSpace(rt.CanonicalSlug)
		r.fallbacks = toSlugs(rt.FallbacksAvailable)
	}
	switch {
	case r.finalProvider != "":
		r.pipeline = pipelinePlanner
	default:
		if p := slugifyProvider(provider); p != "" {
			// direct（OpenRouter）管线：顶层 provider 字符串即实际服务的渠道
			r.pipeline = pipelineDirect
			r.finalProvider = p
			if r.canonicalSlug == "" && strings.Contains(model, "/") {
				r.canonicalSlug = model
			}
		}
	}
	return r
}

// availableProvidersRe planner 报错文本中的可用渠道清单。
var availableProvidersRe = regexp.MustCompile(`Available providers are:\s*([^.]+)`)

// providerListSplitRe 清单分隔：逗号或 " and "（部分网关用 and 连接末两项）。
var providerListSplitRe = regexp.MustCompile(`\s*,\s*|\s+and\s+`)

// probeChannelModel 探测渠道上某模型的内部渠道：识别管线、收割可用渠道
// 清单、记录实际服务的渠道。返回更新后的固定配置（含探测产物，固定配置
// 原样保留），由调用方写回渠道。
func probeChannelModel(ch *Channel, k *UpKey, model string) (*ModelUpstreamPin, error) {
	pin := ch.upstreamPinFor(model)
	if pin == nil {
		pin = &ModelUpstreamPin{}
	}
	cand := candidate{ch: ch, k: k}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.TestTimeout)
	defer cancel()

	// 1) 正常小请求：读 routing（管线 / 实际服务的渠道 / 备选列表）
	ans, err := sendProbeChat(ctx, &cand, probeRequestBody(model, "Reply with the word OK"))
	if err != nil {
		return nil, err
	}
	routing := parseGatewayRouting(ans)

	// 2) 收割：only 钉到 __probe__，报错文本/JSON 点名可用渠道全集
	harvested, herr := harvestKnownProviders(ctx, &cand, model, routing.pipeline)
	if herr != nil {
		// 收割失败不否定探测本身：保留 routing 结论
		harvested = nil
	}

	pin.Pipeline = routing.pipeline
	pin.LastProvider = routing.finalProvider
	pin.CanonicalSlug = routing.canonicalSlug
	pin.Known = mergeUnique(harvested, routing.fallbacks, pin.Known)
	pin.ProbedAt = time.Now().Unix()
	pin.normalize()
	return pin, nil
}

// harvestKnownProviders 把 only 钉到 __probe__ 触发上游「渠道不存在」报错，
// 从报错里提取可用渠道全集。收割失败（上游不点名、网络错误）返回 nil。
func harvestKnownProviders(ctx context.Context, cand *candidate, model, pipeline string) ([]string, error) {
	ans, err := sendProbeChat(ctx, cand, injectUpstreamPin(probeRequestBody(model, "hi"), pipeline, "__probe__", nil, "", nil))
	if err != nil {
		return nil, err
	}
	return extractAvailableProviders(ans, pipeline), nil
}

// extractAvailableProviders 从（错误）响应体提取可用内部渠道列表：
// planner 从错误文本 "Available providers are: a, b, c" 提取；direct 从
// 错误 JSON 的 error.metadata.available_providers 提取。管线未知时两种都试。
func extractAvailableProviders(body []byte, pipeline string) []string {
	text := string(body)
	if pipeline == pipelinePlanner || pipeline == "" {
		if m := availableProvidersRe.FindStringSubmatch(text); m != nil {
			if slugs := toSlugs(providerListSplitRe.Split(m[1], -1)); len(slugs) > 0 {
				return slugs
			}
		}
	}
	if pipeline == pipelineDirect || pipeline == "" {
		var e struct {
			Error struct {
				Metadata struct {
					AvailableProviders []string `json:"available_providers"`
				} `json:"metadata"`
			} `json:"error"`
		}
		if start := strings.Index(text, "{"); start >= 0 && json.Unmarshal([]byte(text[start:]), &e) == nil {
			if slugs := toSlugs(e.Error.Metadata.AvailableProviders); len(slugs) > 0 {
				return slugs
			}
		}
	}
	return nil
}

// ---- 探测/验证的请求辅助（与 WebUI 测试链路同形态）----

// probeRequestBody 最小对话请求体。
func probeRequestBody(model, msg string) []byte {
	b, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": msg}},
		"stream":   false,
	})
	return b
}

// sendProbeChat 用指定候选发一条非流式请求（渠道头、key、代理与真实转发
// 一致），返回响应体（上限 16KB）。状态码不作为错误处理——探测要读错误体。
func sendProbeChat(ctx context.Context, cand *candidate, body []byte) ([]byte, error) {
	route, err := resolveProxy(cand)
	if err != nil {
		return nil, err
	}
	client := newUpstreamClient(route)
	resp, err := doUpstreamRequest(ctx, client, cand, body, false, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	if err != nil {
		return nil, err
	}
	leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
	return data, nil
}

// toSlugs 清理渠道名列表：小写、连字符化、去空去重保序。
func toSlugs(in []string) []string {
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for _, v := range in {
		s := slugifyProvider(v)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// slugifyProvider 渠道显示名 → slug（小写、空白转连字符，仅保留 slug 安全
// 字符），对标 dsh-cline-pass 的 slugify——错误文本混入的标点不产生脏渠道名。
func slugifyProvider(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	s = strings.Join(strings.Fields(s), "-")
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '.', r == '_':
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), "-")
}

// mergeUnique 多列表合并去重保序（先到先得，不排序——列表顺序即置信顺序）。
func mergeUnique(lists ...[]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, l := range lists {
		for _, v := range l {
			if v == "" || seen[v] {
				continue
			}
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
