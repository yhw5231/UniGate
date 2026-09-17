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
// 探测（对标 dsh-cline-pass 的 probe/harvest）：先发正常小请求从响应的
// provider_metadata.gateway.routing 读管线类型与实际服务的渠道——真实网关把
// 路由块挂在响应顶层（message/choice 级是一并兼容的变体）；再把 only 钉到
// 不可能的渠道（__probe__）——上游在花费 token 前报错并点名全部可用渠道，
// 从中收割渠道清单（每个收割请求只带当前管线那一种写法，管线未知时按
// planner → direct 各发一次干净请求）；direct 管线另从 OpenRouter 公开目录
// 补充该模型的全部渠道。探测产物持久化在渠道的 ModelPins 里供 WebUI 勾选，
// 可随时重建。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	tier0         []string // 规划器 tier-0 考虑过的渠道（planningReasoning）
}

// gatewayRoutingBody provider_metadata.gateway.routing 的路由块（planner 管线）。
type gatewayRoutingBody struct {
	FinalProvider      string   `json:"finalProvider"`
	CanonicalSlug      string   `json:"canonicalSlug"`
	FallbacksAvailable []string `json:"fallbacksAvailable"`
	PlanningReasoning  string   `json:"planningReasoning"`
}

// gatewayRoutingMeta provider_metadata 的 gateway 路由容器。
type gatewayRoutingMeta struct {
	Gateway struct {
		Routing gatewayRoutingBody `json:"routing"`
	} `json:"gateway"`
}

// gatewayEnvelope OpenAI 形态的响应（可能包 {data:...} 信封）。真实 Cline Pass
// 网关把 planner 路由块挂在响应顶层 provider_metadata；message/choice 级是
// 部分网关的变体，一并兼容（对标 parseRouting 的 message ?? payload 两级查找）。
type gatewayEnvelope struct {
	Data             json.RawMessage         `json:"data"`
	Choices          []gatewayEnvelopeChoice `json:"choices"`
	Provider         string                  `json:"provider"`
	Model            string                  `json:"model"`
	ProviderMetadata *gatewayRoutingMeta     `json:"provider_metadata"`
}

type gatewayEnvelopeChoice struct {
	Message struct {
		ProviderMetadata *gatewayRoutingMeta `json:"provider_metadata"`
	} `json:"message"`
	ProviderMetadata *gatewayRoutingMeta `json:"provider_metadata"`
}

// parseGatewayRouting 从 chat/completions 响应体解析上游路由信息（对标
// dsh-cline-pass 的 parseRouting）：planner 管线的路由挂在
// provider_metadata.gateway.routing——依次读 choices[0].message、choices[0]、
// 响应顶层；direct 管线用顶层 provider 字符串。{data:{choices:...}} 信封自动展开。
func parseGatewayRouting(body []byte) pinRouting {
	var env gatewayEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return pinRouting{}
	}
	provider, model, choices, meta := env.Provider, env.Model, env.Choices, env.ProviderMetadata
	if len(env.Data) > 0 {
		// {data:{...choices...}} 信封：展开后重新解析
		var inner gatewayEnvelope
		if json.Unmarshal(env.Data, &inner) == nil && len(inner.Choices) > 0 {
			provider, model, choices, meta = inner.Provider, inner.Model, inner.Choices, inner.ProviderMetadata
		}
	}
	var r pinRouting
	if rt := routingOf(choices, meta); rt != nil {
		r.finalProvider = strings.TrimSpace(rt.FinalProvider)
		r.canonicalSlug = strings.TrimSpace(rt.CanonicalSlug)
		r.fallbacks = toSlugs(rt.FallbacksAvailable)
		r.tier0 = parseTier0(rt.PlanningReasoning)
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

// routingOf 依次从 choices[0].message / choices[0] / 响应顶层取路由块。
func routingOf(choices []gatewayEnvelopeChoice, top *gatewayRoutingMeta) *gatewayRoutingBody {
	if len(choices) > 0 {
		if m := choices[0].Message.ProviderMetadata; m != nil {
			return &m.Gateway.Routing
		}
		if c := choices[0].ProviderMetadata; c != nil {
			return &c.Gateway.Routing
		}
	}
	if top != nil {
		return &top.Gateway.Routing
	}
	return nil
}

// tier0Re 规划器推理句里的 tier-0 竞争渠道（对标 parseTier0）。
var tier0Re = regexp.MustCompile(`([\w-]+) won tier 0 over ([^."]+)`)

// parseTier0 从 planningReasoning 提取规划器 tier-0 考虑过的渠道：
// "alibaba won tier 0 over baseten and novita" → [alibaba baseten novita]。
// 被 tier-0 考虑过的渠道都是确认存在的内部渠道，可并入 Known。
func parseTier0(plan string) []string {
	m := tier0Re.FindStringSubmatch(plan)
	if m == nil {
		return nil
	}
	return toSlugs(append([]string{m[1]}, providerListSplitRe.Split(m[2], -1)...))
}

// availableProvidersRe planner 报错文本中的可用渠道清单。
var availableProvidersRe = regexp.MustCompile(`Available providers are:\s*([^.]+)`)

// providerListSplitRe 清单分隔：逗号或 " and "（部分网关用 and 连接末两项）。
var providerListSplitRe = regexp.MustCompile(`\s*,\s*|\s+and\s+`)

// plannerSlugRe planner 清单 token 的干净 slug 形态（对标 dsh-cline-pass 的
// ^[a-z0-9][a-z0-9-]*$ 过滤）。
var plannerSlugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

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

	// 1) 正常小请求：读 routing（管线 / 实际服务的渠道 / 备选列表 / tier-0）
	ans, err := sendProbeChat(ctx, &cand, probeRequestBody(model, "Reply with the word OK", probeAskTokens))
	if err != nil {
		return nil, err
	}
	if msg, failed := probeErrorMessage(ans); failed {
		// 上游明确报错（模型不存在/限流/鉴权失败）：探测到此为止，把上游的
		// 话报给人看，而不是静默产出一份空探测产物
		return nil, fmt.Errorf("upstream: %s", truncate(msg, 300))
	}
	routing := parseGatewayRouting(ans)

	// 2) 收割：only 钉到 __probe__，报错文本/JSON 点名可用渠道全集
	harvested, herr := harvestKnownProviders(ctx, &cand, model, routing.pipeline)
	if herr != nil {
		// 收割失败不否定探测本身：保留 routing 结论
		harvested = nil
	}

	// 3) direct 管线的补充来源：OpenRouter 公开目录里该模型的全部渠道
	var endpoints []string
	if routing.pipeline != pipelinePlanner && routing.canonicalSlug != "" {
		endpoints = openRouterProviderSlugs(ctx, &cand, routing.canonicalSlug)
	}

	// 合并顺序即置信顺序（对标 probe 的 mergeUpstreams 顺序）；tier-0 与
	// 上次探测产物追加在后
	var known []string
	switch routing.pipeline {
	case pipelineDirect:
		known = mergeUnique(routing.fallbacks, harvested, endpoints)
	default:
		known = mergeUnique(harvested, routing.fallbacks, endpoints)
	}
	pin.Pipeline = routing.pipeline
	pin.LastProvider = routing.finalProvider
	pin.CanonicalSlug = routing.canonicalSlug
	pin.Known = mergeUnique(known, routing.tier0, pin.Known)
	pin.ProbedAt = time.Now().Unix()
	pin.normalize()
	return pin, nil
}

// probeErrorMessage 判定响应体是否为上游错误（有 error 且无 choices），
// 是则返回可读文本。对标 probe() 的失败分支：模型不存在/限流/鉴权失败时
// 探测直接失败，而不是带着空结果"成功"返回。
func probeErrorMessage(body []byte) (string, bool) {
	var payload struct {
		Error   json.RawMessage   `json:"error"`
		Choices []json.RawMessage `json:"choices"`
	}
	if json.Unmarshal(body, &payload) != nil || len(payload.Choices) > 0 || len(payload.Error) == 0 {
		return "", false
	}
	if msg := upstreamErrorText(payload.Error); msg != "" {
		return msg, true
	}
	return "", false
}

// harvestKnownProviders 把 only 钉到 __probe__ 触发上游「渠道不存在」报错，
// 从报错里提取可用渠道全集。每个收割请求只带一种管线写法——管线已知发
// 一次；管线未知按 planner → direct 各发一次干净请求（对标 harvest() 的
// 单写法形态；两种写法混进同一请求会改变上游的路由判定/报错形态，反而
// 收不到清单）。全部请求都失败才返回错误；只是没点到清单则返回 nil。
func harvestKnownProviders(ctx context.Context, cand *candidate, model, pipeline string) ([]string, error) {
	pipelines := []string{pipeline}
	if pipeline == "" {
		pipelines = []string{pipelinePlanner, pipelineDirect}
	}
	var merged []string
	var firstErr error
	for _, p := range pipelines {
		body := injectUpstreamPin(probeRequestBody(model, "hi", probeHarvestTokens), p, "__probe__", nil, "", nil)
		ans, err := sendProbeChat(ctx, cand, body)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		merged = mergeUnique(merged, extractAvailableProviders(ans, p))
		if len(merged) > 0 {
			return merged, nil
		}
	}
	if len(merged) > 0 {
		return merged, nil
	}
	return nil, firstErr
}

// extractAvailableProviders 从（错误）响应体提取可用内部渠道列表：
// planner 从错误文本 "Available providers are: a, b, c" 提取；direct 从
// 错误 JSON 的 error.metadata.available_providers 提取。管线未知时两种都试。
func extractAvailableProviders(body []byte, pipeline string) []string {
	text := string(body)
	if pipeline == pipelinePlanner || pipeline == "" {
		if m := availableProvidersRe.FindStringSubmatch(text); m != nil {
			if slugs := toPlannerSlugs(providerListSplitRe.Split(m[1], -1)); len(slugs) > 0 {
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

// toPlannerSlugs 报错句子里的渠道 token → slug：slugify 后只保留纯
// [a-z0-9-] 形态。句子是从（JSON）报错体里截出来的，截取范围会混入
// `","type":"invalid_request_error"` 之类碎片，不过滤会变成脏渠道名。
func toPlannerSlugs(tokens []string) []string {
	out := make([]string, 0, len(tokens))
	seen := map[string]bool{}
	for _, tok := range tokens {
		s := slugifyProvider(tok)
		if s == "" || seen[s] || !plannerSlugRe.MatchString(s) {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// ---- 探测/验证的请求辅助（与 WebUI 测试链路同形态）----

// 探测/收割请求的 max_tokens（对标 probe=256 / harvest=16）：不带 max_tokens
// 时推理模型可能把无上限的生成烧在探测请求上。
const (
	probeAskTokens     = 256
	probeHarvestTokens = 16
)

// probeRequestBody 最小对话请求体（maxTokens>0 时带上）。
func probeRequestBody(model, msg string, maxTokens int) []byte {
	b := map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": msg}},
		"stream":   false,
	}
	if maxTokens > 0 {
		b["max_tokens"] = maxTokens
	}
	out, _ := json.Marshal(b)
	return out
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

// mergeUnique 多列表合并去重保序（先到先得，不排序——列表顺序即置信顺序），
// 上限 25（对标 mergeUpstreams，防异常上游灌入超长清单）。
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
			if len(out) >= maxKnownUpstreams {
				return out
			}
		}
	}
	return out
}

// ---- direct 管线的 OpenRouter 目录补充来源（对标 openRouterEndpoints）----

// maxKnownUpstreams 单模型 Known 清单上限。
const maxKnownUpstreams = 25

// openRouterBaseURL OpenRouter 公开 API（var 仅为测试可替换；公开接口无需鉴权）。
var openRouterBaseURL = "https://openrouter.ai/api/v1"

// openRouterFetchBudget OpenRouter 目录请求的独立预算（受探测总超时约束）。
const openRouterFetchBudget = 30 * time.Second

// openRouterProviderSlugs 拉取 OpenRouter 上该模型的全部渠道 slug。网关的
// canonicalSlug 与 OpenRouter id 可能连字符不同（zai/… vs z-ai/…），先直取，
// 失败再按去符号形态模糊匹配解析一次。任何失败都静默跳过——这只是
// direct 管线渠道清单的补充来源，不影响探测结论。
func openRouterProviderSlugs(ctx context.Context, cand *candidate, canonicalSlug string) []string {
	ctx, cancel := context.WithTimeout(ctx, openRouterFetchBudget)
	defer cancel()
	if slugs := fetchOpenRouterEndpoints(ctx, cand, canonicalSlug); len(slugs) > 0 {
		return slugs
	}
	real := resolveOpenRouterSlug(ctx, cand, canonicalSlug)
	if real == "" || real == canonicalSlug {
		return nil
	}
	return fetchOpenRouterEndpoints(ctx, cand, real)
}

// openRouterSlugPath 拼路径用的 slug 清洗：OpenRouter id 含 org/ 段，斜杠要
// 保留；其余字符只留 id 安全字符，杜绝 `?`/`#`/`..` 之类的路径注入。
func openRouterSlugPath(slug string) string {
	if strings.Contains(slug, "..") {
		return ""
	}
	var b strings.Builder
	for _, r := range slug {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '.', r == '_', r == '/':
			b.WriteRune(r)
		}
	}
	return strings.Trim(b.String(), "/")
}

// fetchOpenRouterEndpoints GET /models/{slug}/endpoints，聚合渠道 slug
// （endpoint tag 的 org 段优先，退化用 provider_name）。
func fetchOpenRouterEndpoints(ctx context.Context, cand *candidate, slug string) []string {
	slug = openRouterSlugPath(slug)
	if slug == "" {
		return nil
	}
	route, err := resolveProxy(cand)
	if err != nil {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		openRouterBaseURL+"/models/"+slug+"/endpoints", nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	resp, err := newUpstreamClient(route).Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil
	}
	var detail struct {
		Data struct {
			Endpoints []struct {
				Tag          string `json:"tag"`
				ProviderName string `json:"provider_name"`
			} `json:"endpoints"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &detail) != nil {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, e := range detail.Data.Endpoints {
		s := slugifyProvider(strings.SplitN(e.Tag, "/", 2)[0])
		if s == "" {
			s = slugifyProvider(e.ProviderName)
		}
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// resolveOpenRouterSlug 在 OpenRouter 模型目录里按去符号形态模糊匹配 slug。
func resolveOpenRouterSlug(ctx context.Context, cand *candidate, slug string) string {
	route, err := resolveProxy(cand)
	if err != nil {
		return ""
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, openRouterBaseURL+"/models", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Accept", "application/json")
	resp, err := newUpstreamClient(route).Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return ""
	}
	var catalog struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &catalog) != nil {
		return ""
	}
	normalize := func(v string) string {
		return strings.Map(func(r rune) rune {
			if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
				return r
			}
			if r >= 'A' && r <= 'Z' {
				return r + ('a' - 'A')
			}
			return -1
		}, v)
	}
	want := normalize(slug)
	for _, m := range catalog.Data {
		if m.ID == slug {
			return m.ID
		}
	}
	for _, m := range catalog.Data {
		if normalize(m.ID) == want && want != "" {
			return m.ID
		}
	}
	return ""
}
