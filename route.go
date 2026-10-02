// 路由引擎：把下游 OpenAI 兼容请求按渠道顺序 + key 顺序转发到上游，
// 失败时自动故障转移到下一个 key/渠道。
//
// 候选顺序：渠道在配置中的顺序即优先级；同一渠道内按 key 顺序。
// 每次请求最多尝试 cfg.MaxRouteTries 个候选；命中冷却的候选直接跳过。
// 冷却键粒度为渠道级开关 cooldown_scope："key"（默认，含旧配置空值）按 key 跨模型
// 共享冷却；"key_model" 按 (keyID, model) 独立冷却。
// 模型名称处理（modelname.go）：路由用别名解析后的模型名，渠道匹配支持通配/正则
// 与归一化等价（大小写/供应商前缀/变体后缀），发往上游前按渠道名称映射改写
// 请求体的 model 字段。
// 故障分类与处理：
//   - 429                  → 记冷却：冷却时长优先取上游明确的到期时间
//     （Retry-After 头 > 响应体 "Try again in 14h 23m" 类文本/时间戳），
//     上游没给明确时间才用 RATE_LIMIT_COOLDOWN（默认 1h）
//   - 5xx                  → 换 key；按 key 记连续次数（正常请求清零），连续超过
//     ROTATE_AFTER_5XX（默认 3）自动换出口 IP（ipv6pool key 生效），
//     并按熔断策略累计失败次数触发冷却（见下）
//   - 网络/代理错误         → 换 key；ipv6pool 候选换出口 IP 后同 key 立即
//     重试一次（坏出口自愈），仍失败再继续下一个 key；同样计入熔断
//   - 401/403              → 只换 key，不冷却、不换出口（熔断不计：鉴权失败
//     不是上游容量/健康信号，冷却无助于恢复）
//   - 其他（含上游 400）   → 原样透传给下游（上游的业务语义不动）
//
// 熔断（连续失败冷却，对标 go-gateway 的 breaker）：网络/代理错误与 5xx 连续
// 失败达阈值（默认 3）即按指数退避冷却该 (key, model)（基础 30s × 倍数^(触发
// 次数-1)，上限 15m）；任意一次成功即清零计数并解除冷却。全部候选都在冷却时
// 仍会对最早到期者做一次穿透试探，成功自愈。
//
// 流式保活：上游排队首包慢或流中途静默时，每 KEEPALIVE_INTERVAL（默认 15s）
// 向下游写一帧 SSE 注释心跳，保证下游反代/客户端不因空闲超时掐断连接
// （否则表现为「渠道测试可用、下游挂满 60s 记 client canceled」）。首帧心跳
// 会提前提交 200+event-stream 响应头，之后路由失败改用流内 data: {"error":...}
// 帧表达；快速失败（429/5xx 秒级返回）通常发生在首帧心跳之前，JSON 语义不变。
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// chEntry 参与构建候选的渠道 + 配置序（同优先级内稳定排序依据）。
type chEntry struct {
	ch  *Channel
	idx int
}

// candidate 一个可路由的 (渠道, key) 组合。
type candidate struct {
	ch *Channel
	k  *UpKey
	// upstreamModel 非空 = 发往上游前把请求体的 model 字段改写为该名称
	//（渠道名称映射 / 渠道声明列表的原文写法，见 modelname.go）；空 = 原样发送。
	upstreamModel string
	// coolModel 非空 = 冷却键中 model 部分的固定取值：渠道映射把下游模型映射到
	// 多个上游模型时，每个上游模型各自成一个候选并各自独立冷却（cn:/global: 变体
	// 互不牵连）。空 = 按渠道冷却粒度规则（cooldownModelFor）。
	coolModel string
	// pinBody 非空 = 渠道固定注入后的请求体（发往上游时优先于原始 body）；
	// pinName 为该候选固定的内部渠道（provider slug），空 = 未固定（自动路由）。
	pinBody []byte
	pinName string
}

// requestBody 返回发往上游的请求体：渠道固定注入体优先。
func (c *candidate) requestBody(raw []byte) []byte {
	if c.pinBody != nil {
		return c.pinBody
	}
	return raw
}

// cooldownModel 返回冷却键中 model 部分的取值：渠道映射出的候选按上游模型独立
// 冷却（coolModel，如 cn:x 与 global:y 互不牵连）；其余按渠道粒度——
// 渠道粒度为 "key_model" 时按 (key, model) 独立冷却，默认 "" / "key"（含旧配置
// 空值）按 key 跨模型共享冷却。
func (c *candidate) cooldownModel(model string) string {
	if c.coolModel != "" {
		return c.coolModel
	}
	return c.ch.cooldownModelFor(model)
}

// chatTarget 返回该候选的上游对话端点（key BaseURL 优先；按渠道端点类型
// 选择 /chat/completions 或 OpenAI Responses API 的 /responses）。
func (c *candidate) chatTarget() string {
	if u := c.k.BaseURL; u != "" {
		base := normalizeBaseURL(u)
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

// targetFor 返回该候选的非对话端点（embeddings/图片/视频等，suffix 如
// "/embeddings"）：key BaseURL 优先，其次渠道 BaseURL；BaseURL 已以该路径
// 结尾（配置了完整端点地址）时原样使用，否则拼接。不做 Responses 转换与
// 版本段推导——非对话接口不与 chatTarget 的端点类型逻辑混用。
func (c *candidate) targetFor(suffix string) string {
	base := normalizeBaseURL(c.k.BaseURL)
	if base == "" {
		base = normalizeBaseURL(c.ch.BaseURL)
	}
	if base == "" {
		return ""
	}
	if strings.HasSuffix(base, suffix) {
		return base
	}
	return base + suffix
}

func endsWithChatCompletions(s string) bool {
	const suffix = "/chat/completions"
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// failoverSameChannel 该渠道的失败转移模式：默认（"" / same_channel）在一个
// key 失败后继续尝试同渠道的下一个 key；force_channel 则跳过本渠道剩余 key，
// 直接进入下一渠道（「强制切换渠道」）。
func (ch *Channel) failoverSameChannel() bool {
	return ch.FailoverMode != "force_channel"
}

// buildCandidates 按优先级构建候选列表（深拷贝自 store）。
// rawModel 为下游请求的模型名，model 为「路由模型名」——已完成别名解析
//（见 resolveModelAlias），渠道匹配、冷却键、（key,模型）活动计时都用它；
// 每个候选另行解析发往上游的模型名（渠道 ModelMap 映射 / 声明列表原文，
// 见 modelname.go）。渠道级映射优先于全局别名：渠道对下游原名配了映射时按
// 渠道映射发往上游；一个下游名映射到多个上游模型时，每个上游模型各成一个候选。
// 渠道顺序：先按渠道优先级（priority，默认 0，越小越靠前）排序，同一优先级内
// 若存在设置了权重（weight>0）的渠道，按权重比例做平滑加权轮询决定每请求的
// 渠道先后；否则保持配置顺序。渠道内 key 顺序由调度模式决定
//（渠道 schedule，未配置跟随全局默认）：
//   - failover（默认）：按 key 顺序，靠前的 key 用满才轮到后面；
//   - round_robin：每个渠道把 key 列表从递增游标处旋转一轮（保序），
//     请求在账号间轮流分配，均摊用量。游标按渠道 ID 记忆（内存态，重启归零）。
//
// 渠道配置了模型固定（ModelPins，见 upstreampin.go）时，每个 (渠道, key)
// 按固定列表展开为多个候选（每个候选注入对应的内部渠道固定请求体），
// strict 模式按固定顺序逐个内部渠道独占尝试。
func buildCandidates(rawModel, model string, rawBody []byte) []candidate {
	snap := store.View() // 只读视图：零拷贝（每请求热路径，不做全量配置深拷贝）
	def := currentPolicy().DefaultSchedule
	var avail []chEntry
	for i, ch := range snap.Channels {
		if !ch.Enabled || !(ch.allowsModel(model) || ch.allowsModel(rawModel)) {
			continue
		}
		avail = append(avail, chEntry{ch: ch, idx: i})
	}
	if len(avail) == 0 {
		return nil
	}
	// 先按优先级升序稳定排序（同优先级保持配置顺序），再按权重重排
	sort.SliceStable(avail, func(a, b int) bool { return avail[a].ch.Priority < avail[b].ch.Priority })
	ordered := orderByPriorityWeight(avail)

	var out []candidate
	for _, e := range ordered {
		ch := e.ch
		keys := ch.Keys
		if ch.effectiveSchedule(def) == scheduleRoundRobin {
			keys = rotateKeysRR(ch.ID, keys)
		}
		pins := buildUpstreamPinAttempts(ch, model, rawBody)
		for _, k := range keys {
			if !k.Enabled {
				continue
			}
			for _, cand := range newCandidates(ch, k, rawModel, model) {
				if pins == nil {
					out = append(out, cand)
					continue
				}
				// 有固定配置：按固定列表展开（strict 每个内部渠道一个候选）
				for _, pa := range pins {
					pinned := cand
					pinned.pinBody, pinned.pinName = pa.body, pa.upstream
					out = append(out, pinned)
				}
			}
		}
	}
	return out
}

// wrrState 平滑加权轮询状态（渠道 ID → 当前权值；内存态，重启归零）。
var wrrState struct {
	sync.Mutex
	cur map[string]int
}

// orderByPriorityWeight 按 (优先级, 权重) 确定渠道顺序：优先级分组已由调用方
// 排序完成，这里对每个同优先级组内存在权重（>0）渠道的组做平滑加权轮询重排，
// 其余组保持原顺序（纯故障转移）。
func orderByPriorityWeight(entries []chEntry) []chEntry {
	out := make([]chEntry, 0, len(entries))
	for i := 0; i < len(entries); {
		j := i
		for j < len(entries) && entries[j].ch.Priority == entries[i].ch.Priority {
			j++
		}
		group := entries[i:j]
		hasWeight := false
		for _, e := range group {
			if e.ch.Weight > 0 {
				hasWeight = true
				break
			}
		}
		if hasWeight {
			group = smoothWRREntries(group) // 同优先级组内按权重轮流优先
		}
		out = append(out, group...)
		i = j
	}
	return out
}

// smoothWRREntries 对渠道组做一轮平滑加权轮询（Nginx 算法）：每请求把各渠道
// 的当前权值 += 权重，取当前权值最大的渠道放到本轮候选首位，选中者 -= 总权重。
// 长期看各渠道被选为先手的比例 = 权重比（weight=0 的渠道不获得额外分配）；
// 首位之后的渠道保持原顺序（故障转移沿该顺序继续）。
func smoothWRREntries(entries []chEntry) []chEntry {
	if len(entries) <= 1 {
		return entries
	}
	total := 0
	for _, e := range entries {
		total += e.ch.Weight
	}
	if total <= 0 {
		return entries
	}
	wrrState.Lock()
	defer wrrState.Unlock()
	if wrrState.cur == nil {
		wrrState.cur = map[string]int{}
	}
	best := 0
	for i := range entries {
		id := entries[i].ch.ID
		cur := wrrState.cur[id] + entries[i].ch.Weight
		wrrState.cur[id] = cur
		if cur > wrrState.cur[entries[best].ch.ID] {
			best = i
		}
	}
	wrrState.cur[entries[best].ch.ID] -= total
	out := make([]chEntry, 0, len(entries))
	out = append(out, entries[best])
	for i := range entries {
		if i != best {
			out = append(out, entries[i])
		}
	}
	return out
}

// nextCandidateIndex 失败后的候选推进：same_channel（默认）只前进一个候选
//（同渠道内继续尝试下一个 key）；force_channel 跳过本渠道剩余全部候选，直接
// 进入下一渠道的候选段（「强制切换渠道」）。
func nextCandidateIndex(cands []candidate, i int) int {
	if cands[i].ch.failoverSameChannel() {
		return i + 1
	}
	j := i + 1
	for j < len(cands) && cands[j].ch.ID == cands[i].ch.ID {
		j++
	}
	return j
}

// rrCursors 轮询游标：渠道 ID → 该渠道 key 列表的下一起点（内存态）。
var rrCursors struct {
	sync.Mutex
	m map[string]int
}

// rotateKeysRR 把 key 列表从游标处旋转一轮（保序）并推进游标。
// 游标按「全部 key（含停用）」的索引记忆：停用 key 只是路由时跳过，
// 不打乱轮询节奏（避免每次请求都从同一个 key 开始）。空列表原样返回。
func rotateKeysRR(chID string, keys []*UpKey) []*UpKey {
	if len(keys) == 0 {
		return keys
	}
	rrCursors.Lock()
	if rrCursors.m == nil {
		rrCursors.m = map[string]int{}
	}
	start := rrCursors.m[chID] % len(keys)
	rrCursors.m[chID] = (start + 1) % len(keys)
	rrCursors.Unlock()
	out := make([]*UpKey, 0, len(keys))
	out = append(out, keys[start:]...)
	out = append(out, keys[:start]...)
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

// forwardMode 转发模式：modeChat 为对话端点（含 responses 适配）、
// modeOther 为 embeddings/图片/视频等非对话端点（上游路径按后缀拼接、
// 不做 Responses 转换与 reasoning 改写、无流式）。
type forwardMode int

const (
	forwardModeChat forwardMode = iota
	forwardModeOther
)

// forwardChat 执行 chat/completions 的故障转移转发（见 forwardRequest）。
func forwardChat(w http.ResponseWriter, r *http.Request, rawBody []byte, stream bool, model string) *candidate {
	return forwardRequest(w, r, rawBody, "", stream, model, forwardModeChat, "")
}

// forwardOther 执行非对话端点（embeddings/图片/视频）的故障转移转发：
// contentType 非空（multipart 请求）时连同 boundary 原样透传上游，
// suffix 为上游路径后缀（如 "/embeddings"）。
func forwardOther(w http.ResponseWriter, r *http.Request, rawBody []byte, contentType string, model, suffix string) *candidate {
	return forwardRequest(w, r, rawBody, contentType, false, model, forwardModeOther, suffix)
}

// forwardRequest 执行故障转移转发，把最终响应写给下游 w。
// 返回实际服务的候选（用于统计），无可用上游时返回 nil。
// model 为下游请求的模型名（未解析别名/映射）；路由与冷却用别名解析后的
// 「路由模型名」，上游请求体按渠道映射改写（名称处理见 modelname.go）。
// 每个候选的尝试结果（含跳过原因）都会追加到请求日志的错误信息，便于在
// WebUI 上直接回答「为什么 502」：哪个 key 因什么失败、冷却了多久。
func forwardRequest(w http.ResponseWriter, r *http.Request, rawBody []byte, contentType string, stream bool, model string, mode forwardMode, suffix string) *candidate {
	isChat := mode == forwardModeChat
	if !isChat {
		stream = false // 非对话端点（embeddings/图片/视频）无流式
	}
	// 名称处理：全局别名解析（下游请求名 → 规范模型名），路由/冷却/活动计时
	// 都以解析后的名字为准，上游请求体由渠道映射决定（candidate.upstreamModel）；
	// 渠道级映射优先于全局别名（渠道对下游原名配了映射就按渠道映射发往上游）
	routeModel := resolveModelAlias(model)
	cands := buildCandidates(model, routeModel, rawBody)
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
	// 流式保活：等待上游/故障转移期间持续给下游发心跳，下游空闲超时不触发，
	// 网关就能安心等上游出包（keepalive.go）。非流式无法在 JSON 前面垫注释帧，
	// 不启用。
	var (
		sink http.ResponseWriter = w
		keep *streamKeeper
	)
	if stream {
		if keep = newStreamKeeper(w, pol.KeepaliveInterval); keep != nil {
			keep.start()
			defer keep.stop()
			sink = keep
		}
	}
	recordTrace := func(cand *candidate, event, detail string) {
		name := cand.k.Name + "@" + cand.ch.Name
		if cand.pinName != "" {
			name += "[" + cand.pinName + "]"
		}
		trace = append(trace, attemptTrace{
			Key:   name,
			Model: routeModel,
			Event: event,
			Err:   detail,
		})
	}
	// 全部候选都在冷却时不硬 502：对最早到期的 key 穿透试探一次。冷却防的是
	// 反复冲击限流上游，但上游可能已恢复（并发型 429 恢复极快、短窗口限流
	// 数十秒即过），试探成功即解除冷却自愈——否则会一直表现为「渠道测试可用
	// 而网关持续 502」，只能靠手工测试解冻。
	pierceKey, pierceModel := "", ""
	if pierce := earliestCooldown(cands, routeModel); pierce != nil {
		pierceKey, pierceModel = pierce.k.ID, pierce.cooldownModel(routeModel)
	}
	// 本候选是否为「穿透试探」的一次真实尝试：成功即解除存量冷却（下方
	// RecordSuccess 一并处理），失败照常继续故障转移
	pierced := false
	// egressRetried 记录本轮请求中已做过「换出口重试」的 key（每 key 最多一次）
	egressRetried := map[string]bool{}
	for i := 0; i < len(cands); {
		cand := cands[i]
		isRetry := egressRetried[cand.k.ID]
		if !isRetry && attempts >= maxTries {
			break
		}
		cm := cand.cooldownModel(routeModel)
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
			cool.RecordFailure(cand.k.ID, cm, pol.Breaker)
			i = nextCandidateIndex(cands, i)
			continue
		}

		client := newUpstreamClient(route)
		// 记录 key 活动（空闲探测的计时依据）：任何真实发出的上游请求都算调用
		noteKeyCall(cand.k.ID, routeModel)
		target := cand.chatTarget()
		if !isChat {
			target = cand.targetFor(suffix)
		}
		resp, err := doUpstreamRequest(r.Context(), client, &cand, target, rawBody, contentType, isChat, stream, r.Header)
		if err != nil {
			// 下游已断开（超时/取消）导致上游请求被中止：不是上游故障，
			// 不换 IP，也不必再试其余候选（都会立刻以同样方式失败）
			if r.Context().Err() != nil {
				lastErr = "client canceled before upstream response"
				recordTrace(&cand, "client_canceled", lastErr)
				break
			}
			// 网络错误只换 key，不冷却（瞬断不该把健康 key 冷停）；但连续
			// 失败达熔断阈值即按指数退避冷却该 key（成功请求清零计数）
			lastErr = "network: " + err.Error()
			recordTrace(&cand, "network_error", lastErr)
			cool.RecordFailure(cand.k.ID, cm, pol.Breaker)
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
			i = nextCandidateIndex(cands, i)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			// 429 是唯一记冷却的故障：上游明确说"额度/频率受限"，冷却等待重试。
			// 冷却时长优先级：上游明确的到期时间（Retry-After 头 > 错误体文本/
			// 时间戳）> 配置的固定 CD——如免费额度按日重置的上游会在错误体写
			// "Try again in 14h 23m"，按固定 CD 提前重试只会反复撞 429。
			rateLimited = true
			prefix, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			d := retryAfterDuration(resp.Header.Get("Retry-After"), 0)
			if d == 0 {
				if d2, ok := bodyRetryAfter(string(prefix)); ok {
					d = d2
				}
			}
			// explicit：时长来自上游明确到期时间（启动探测对这类账号不再试探，
			// 等它自然到期；只有兜底冷却的账号才值得重启后重新核对状态）
			explicit := d != 0
			if !explicit {
				d = pol.RateLimitCooldown
			}
			if explicit {
				cool.MarkExplicit(cand.k.ID, cm, d)
			} else {
				cool.Mark(cand.k.ID, cm, d)
			}
			leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
			lastErr = "upstream " + formatRejectReason(resp.StatusCode, prefix)
			recordTrace(&cand, "rejected_429", lastErr)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			i = nextCandidateIndex(cands, i)
			continue
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			// 鉴权失败只换 key，不冷却、不换出口
			lastErr = "upstream " + rejectReason(resp)
			recordTrace(&cand, "rejected_auth", lastErr)
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			i = nextCandidateIndex(cands, i)
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
			// 连续 5xx 同样计入熔断：上游整体故障时把 key 冷却一段时间，
			// 停止无谓冲击（阈值/时长见设置页熔断项）
			cool.RecordFailure(cand.k.ID, cm, pol.Breaker)
			// 关闭 body（连接归还连接池）之后再换 IP：换 IP 会作废旧出口的
			// 空闲隧道，太早调用会漏掉当前这条刚用完的连接
			if pol.RotateAfter5xx > 0 && n > pol.RotateAfter5xx {
				streaks.Reset(cand.k.ID)
				rotateExit(&cand, fmt.Sprintf("%d consecutive 5xx", n))
			}
			i = nextCandidateIndex(cands, i)
			continue
		}

		// 正常拿到响应（2xx/3xx/4xx 业务语义）：清零该 key 的 5xx 连续计数与
		// 熔断失败计数；穿透成功时存量冷却同时解除（冷却与事实相悖，立即自愈）
		streaks.Reset(cand.k.ID)
		cool.RecordSuccess(cand.k.ID, cand.cooldownModel(routeModel))

		// 成功拿到可透传的响应：改写（可选）后回给下游
		leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
		if resp.StatusCode >= 400 {
			if keep != nil && keep.committedResponse() {
				// 心跳已把 200 SSE 响应头提交给下游：错误状态码/JSON 错误体
				// 都发不出去了，只能把这个候选当失败继续转移（心跳不停）
				lastErr = "upstream " + rejectReason(resp)
				recordTrace(&cand, "rejected_after_commit", lastErr)
				resp.Body.Close()
				i = nextCandidateIndex(cands, i)
				continue
			}
			// 上游 4xx/5xx 业务错误原样透传，但把响应体片段记入请求日志：
			// 不记的话错误日志只有 "Bad Request" 状态文本，回答不了
			//「上游为什么 400/404」（无效 key、模型不存在等具体原因）。
			peekUpstreamError(r, resp)
		}
		// 非对话端点不做 reasoning 改写与 Responses 转换（原样透传）
		rewrite, epType := cand.ch.Rewrite, cand.ch.EndpointType
		if !isChat {
			rewrite, epType = false, ""
		}
		serveUpstreamResponse(sink, resp, stream, rewrite, epType)
		served = &cand
		break
	}

	if served != nil {
		return served
	}

	// 无候选成功：把诊断信息写进请求日志（渠道/key/逐 key 原因），再回给下游
	displayModel := routeModel
	if routeModel != model {
		displayModel = model + "→" + routeModel // 别名解析结果一并展示
	}
	msg := describeRouteFailure(displayModel, len(cands), attempts, cooled, lastErr, trace)
	// 名称处理可观测：上游实际收到的模型名（渠道映射/声明原文与路由名不同时）
	if names := candidateModelNames(cands, routeModel); len(names) > 0 {
		msg += "; upstream model: " + strings.Join(names, ", ")
	}
	if pierced {
		msg += "; cooldown pierced (earliest-expiring key was retried)"
	}
	// 429 余波（全部冷却或本轮撞过 429）：附上最早可重试时间，下游可据此退避
	var retryAfter int64
	if attempts == 0 || rateLimited {
		pairs := make([]cooldownPair, 0, len(cands))
		for _, c := range cands {
			pairs = append(pairs, cooldownPair{c.k.ID, c.cooldownModel(routeModel)})
		}
		if d, ok := cool.EarliestRetry(pairs); ok {
			retryAfter = int64(d.Seconds()) + 1
			msg += fmt.Sprintf("; earliest retry in %ds", retryAfter)
		}
	}
	setReqErrMsg(r, msg)

	status, code := http.StatusBadGateway, "upstream_error"
	if rateLimited {
		status, code = http.StatusTooManyRequests, "rate_limited"
	}
	if keep != nil {
		// 心跳可能已把 200+event-stream 头提交给下游：由 keeper 在锁内二选一
		// ——JSON 错误（含 Retry-After）或流内 error 帧，绝不与心跳撕裂
		keep.finish(msg, code, status, retryAfter)
		return nil
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	}
	writeJSONError(w, status, msg, code)
	return nil
}

// candidateModelNames 收集候选实际发往上游的模型名（去重保序，只保留与
// 路由模型名不同的部分），用于失败信息与日志展示名称处理的结果。
func candidateModelNames(cands []candidate, routeModel string) []string {
	seen := map[string]bool{}
	var out []string
	for i := range cands {
		m := cands[i].upstreamModel
		if m == "" || m == routeModel || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	return out
}

// attemptTrace 一次候选尝试的轨迹（key「名称@渠道」、事件、失败原因）。
type attemptTrace struct {
	Key   string `json:"key"`
	Model string `json:"model,omitempty"`
	Event string `json:"event"`
	Err   string `json:"error,omitempty"`
}

// formatTrace 把逐 key 尝试轨迹压成一行紧凑文本，如
// "key1@ch1 rejected_429(upstream 429: rate limited); key2@ch2 network_error(...)"。
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
	return formatRejectReason(resp.StatusCode, body)
}

// formatRejectReason 由状态码 + 已读响应体前缀拼失败原因（429 分支已预读
// 4KB 响应体用于解析到期时间，复用前缀避免二次读）。
func formatRejectReason(status int, body []byte) string {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return strconv.Itoa(status)
	}
	return fmt.Sprintf("%d: %s", status, truncate(s, 200))
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
// target 为发往上游的完整 URL；contentType 非空（multipart 请求）时覆盖
// 默认的 application/json（boundary 必须保留）；translateResponses 为 true
// 且渠道端点类型为 responses 时，先把 chat/completions 请求体转换为
// Responses API 格式（仅对话端点）。
func doUpstreamRequest(ctx context.Context, client *http.Client, cand *candidate, target string, rawBody []byte, contentType string, translateResponses bool, stream bool, srcHeader http.Header) (*http.Response, error) {
	// 请求体：内部渠道固定注入体优先，再按渠道名称映射改写 model 字段
	body, contentType := cand.upstreamBody(rawBody, contentType)
	if translateResponses && cand.ch.EndpointType == endpointResponses {
		body = chatToResponsesRequest(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
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
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	// 上游失联兜底：读取静默超时后关闭连接，Read 不再无限阻塞
	//（TCP 半开 / NAT 静默回收会让响应体读取永久挂起，goroutine 与连接泄漏）
	resp.Body = &idleTimeoutReader{rc: resp.Body, idle: cfg.UpstreamReadIdle}
	return resp, nil
}

// ---- 路由视图（WebUI「路由」页：按模型展示候选 key 与实时状态）----

// routeStatuses 候选状态取值。
const (
	routeStatusOK       = "ok"       // 渠道与 key 均启用且未冷却：可参与本轮路由
	routeStatusCooling  = "cooling"  // 冷却中（网关转发会跳过）
	routeStatusDisabled = "disabled" // 渠道或 key 被停用：不参与路由
)

// RouteKeyStatus 路由视图中单个 (渠道, key) 候选的状态。一个渠道映射把下游模型
// 映射到多个上游模型时，同一 (渠道, key) 会展开为多行，各自带 upstream_model
// 与独立的冷却状态（cool_model 为该行冷却键的 model 部分，解除冷却时用它）。
type RouteKeyStatus struct {
	ChannelID string `json:"channel_id"`
	Channel   string `json:"channel"`
	ChannelOn bool   `json:"channel_enabled"`
	KeyID     string `json:"key_id"`
	Key       string `json:"key"`
	KeyOn     bool   `json:"key_enabled"`
	Schedule  string `json:"schedule"`                 // 渠道生效的账号调度模式
	Upstream  string `json:"upstream_model,omitempty"` // 该候选实际发往上游的模型名（映射行一律展示，含与分组名同名的写法）
	CoolModel string `json:"cool_model,omitempty"`     // 冷却键的 model 部分（解除该行冷却时用）
	Status    string `json:"status"`                   // ok / cooling / disabled
	Until     int64  `json:"until_unix,omitempty"`
	LeftMS    int64  `json:"left_ms,omitempty"`
}

// RouteModelGroup 按模型聚合的路由视图。
type RouteModelGroup struct {
	Model     string           `json:"model"`
	Total     int              `json:"total"`
	Available int              `json:"available"`
	Cooling   int              `json:"cooling"`
	Keys      []RouteKeyStatus `json:"keys"`
}

// RouteStatusData 路由页数据：全部模型分组 + 生效的全局默认调度。
type RouteStatusData struct {
	DefaultSchedule string            `json:"default_schedule"`
	Models          []RouteModelGroup `json:"models"`
	Total           int               `json:"total"`
	Available       int               `json:"available"`
	Cooling         int               `json:"cooling"`
}

// routeStatusData 构建路由页视图：候选顺序与网关实际转发顺序一致
//（渠道配置序 + key 配置序）。model 非空时只返回该模型的分组。
//
// 模型枚举：取各渠道声明模型列表与渠道模型映射键（下游模型名）的并集（首次出现序）
// 加上全局别名（标注指向的规范模型），渠道未声明模型列表时对全部模型放行，其 key
// 会出现在每个模型分组里。若所有渠道都未声明模型，则合并为单个空模型分组
//（前端标注「全部模型」）。查询参数 model 会先经别名解析再匹配（与网关路由一致）。
//
// 分组名统一走 exposedModelName：历史配置里存着上游原写法（"cline-free/x:free"）
// 时，路由页展示的仍是下游可调用的干净名字；实际发往上游的名字在该分组的
// upstream_model 列（渠道模型映射/原写法）里给出。
//
// 渠道把一个下游模型映射到多个上游模型时（如 cn:x、global:y 都对外叫 x），该分组下
// 每个 (渠道, key) 展开为多行，每行标注 upstream_model 并各自检查冷却——与网关
// 实际候选一致（每个上游模型独立冷却）。
func routeStatusData(model string) *RouteStatusData {
	snap := store.View() // 只读视图，零拷贝
	pol := currentPolicy()
	def := normalizeScheduleDefault(pol.DefaultSchedule)

	var order []string
	groups := map[string]*RouteModelGroup{}
	get := func(m string) *RouteModelGroup {
		if g := groups[m]; g != nil {
			return g
		}
		g := &RouteModelGroup{Model: m, Keys: []RouteKeyStatus{}}
		groups[m] = g
		order = append(order, m)
		return g
	}

	// 已禁用渠道不参与路由：它的模型不出现在路由页（模型分组与候选行都跳过），
	// 与 /v1/models、转发候选的选择保持一致——禁用即整体摘除，而不是显示成「已停用」。
	active := make([]*Channel, 0, len(snap.Channels))
	for _, ch := range snap.Channels {
		if ch.Enabled {
			active = append(active, ch)
		}
	}

	if model != "" {
		get(exposedModelName(model))
	} else {
		for _, ch := range active {
			for _, m := range ch.Models {
				get(exposedModelName(m))
			}
			// 映射键（下游模型名）也是该渠道支持的可调用名，单独列出分组
			for _, m := range ch.modelMapKeys() {
				get(exposedModelName(m))
			}
		}
		// 别名分组（如 my-gpt → gpt-4o）：路由候选与目标模型一致，单独列出便于核对
		for _, alias := range sortedKeys(pol.ModelAliases) {
			if _, ok := groups[alias]; !ok {
				get(alias)
			}
		}
		if len(order) == 0 && len(active) > 0 {
			get("") // 无任何模型声明：单分组代表「对全部模型放行」的候选
		}
	}

	// 每个渠道预构建模型匹配器：allowsModel 是模型列表的线性扫描，而下面的
	// 循环是「模型分组 × 渠道」嵌套，直接调用会退化成 O(模型² × 渠道)。
	// 匹配器化后整体回到 O(模型 × 渠道)。allowAll = 未声明模型列表（放行全部）。
	// 映射键（下游名）一并纳入：配了映射即可用该名调用，不必再写进 Models。
	matchers := make([]*modelMatcher, len(active))
	for i, ch := range active {
		matchers[i] = newModelMatcher(append(append([]string{}, ch.Models...), ch.modelMapKeys()...))
	}

	for _, g := range groups {
		// 冷却键与渠道匹配都用别名解析后的路由模型名
		routeModel := resolveModelAlias(g.Model)
		for i, ch := range active {
			if g.Model != "" && !matchers[i].match(g.Model) && !matchers[i].match(routeModel) {
				continue // 该渠道未声明此模型、也没有对应映射
			}
			rows := channelUpstreamRows(ch, g.Model, routeModel)
			for _, k := range ch.Keys {
				for _, row := range rows {
					st := RouteKeyStatus{
						ChannelID: ch.ID,
						Channel:   ch.Name,
						ChannelOn: ch.Enabled,
						KeyID:     k.ID,
						Key:       k.Name,
						KeyOn:     k.Enabled,
						Schedule:  ch.effectiveSchedule(def),
						CoolModel: row.cool,
					}
					if row.upstream != "" && (row.mapped || row.upstream != g.Model) {
						st.Upstream = row.upstream // 映射行即使与分组名相同也标注（多候选可核对）
					}
					switch {
					case !ch.Enabled || !k.Enabled:
						st.Status = routeStatusDisabled
					default:
						if until, ok := cool.CoolingKey(k.ID, row.cool); ok {
							st.Status = routeStatusCooling
							st.Until = until.Unix()
							st.LeftMS = until.Sub(time.Now()).Milliseconds()
							g.Cooling++
						} else {
							st.Status = routeStatusOK
							g.Available++
						}
					}
					g.Keys = append(g.Keys, st)
					g.Total++
				}
			}
		}
	}

	out := &RouteStatusData{DefaultSchedule: def, Models: []RouteModelGroup{}}
	for _, m := range order {
		g := groups[m]
		out.Models = append(out.Models, *g)
		out.Total += g.Total
		out.Available += g.Available
		out.Cooling += g.Cooling
	}
	return out
}
