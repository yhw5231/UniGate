// 模型名称处理（融合 go-gateway 的 pattern / ModelMapping / SourceModel 语义）：
//
//   - 归一化（canonicalModel）：统一大小写，去掉供应商前缀（"cline-free/deepseek-v4.1-flash"
//     → "deepseek-v4.1-flash"）与变体/装饰后缀（":free" / "-free"），用于判定
//     「同一模型的不同写法」——上游 /models 报的是 "cline-free/x:free"，下游客户端
//     写的可能只是 "x"，两者应命中同一渠道；
//   - 模式匹配：渠道模型列表的每一项可以是精确名、通配（"claude-*" / "gpt-4?"）或
//     正则（"re:^gpt-4.*$"）；
//   - 全局别名（设置页 model_aliases）：下游请求名 → 规范模型名，作用于路由与上游请求体；
//   - 渠道级映射（Channel.ModelMap）：下游模型名 → 该上游实际模型名，可指向多个上游
//     模型（如 cn:x、global:y 都对外叫 x）：每个上游模型各成一个候选，转发时分别改写
//     请求体的 model 字段，冷却也按上游模型分别计算。渠道映射优先于全局别名。
//     未配置映射时按渠道声明列表里的原文写法发送（上游 /models 的写法才是上游认识的名字）。
//
// 落到请求上：路由用「别名解析后的模型名」，发往上游前把请求体里的 model 字段
// 改写为渠道映射/声明原文（仅当与下游请求名不同时才改写，改写失败原样转发）。
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"regexp"
	"sort"
	"strings"
)

// modelVariantTags 模型名的「装饰」后缀（"model:free"、"model:nitro"…）：
// 归一化时去掉，使不同渠道/客户端的写法互相等价（对标 go-gateway 的 variantTags）。
var modelVariantTags = map[string]bool{
	"free": true, "nitro": true, "thinking": true, "online": true,
	"extended": true, "floor": true, "beta": true, "self-moderated": true,
}

// canonicalModel 归一化模型名：小写去空白 → 去掉最后一个 "/" 之前的供应商前缀
// → 去掉 ":free" 类装饰后缀 → 去掉结尾 "-free"。
// 注意 "cn:deepseek-v4" 这类冒号分组名只在冒号后是装饰词时才截断。
func canonicalModel(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.LastIndex(name, ":"); i >= 0 && isModelVariantTag(name[i+1:]) {
		name = name[:i]
	}
	return strings.TrimSuffix(name, "-free")
}

// isModelVariantTag 冒号后的部分是否为装饰词（空串也算，如 "model:"）。
func isModelVariantTag(tag string) bool {
	return tag == "" || modelVariantTags[tag]
}

// modelIdentity 模型对外暴露的名字（对标 go-gateway 的 modelIdentity）：归一化名
// ——小写、去掉供应商前缀（"cline-free/x" → "x"）与变体后缀（"x:free" → "x"）。
// 归一化结果为空（名字里没有模型名，如 "cn:"）时退回原样去空白。
//
// 上游用什么写法、下游看到什么名字由此分开：上游 /models 报
// "cline-free/deepseek-v4.1-flash:free" 时，路由与 /v1/models 对外都是
// "deepseek-v4.1-flash"，而上游仍然收到它自己的写法（见 Channel.ModelMap）。
func modelIdentity(name string) string {
	if c := canonicalModel(name); c != "" {
		return c
	}
	return strings.TrimSpace(name)
}

// exposedModelName 对外展示/可调用的模型名：普通名取 modelIdentity，通配/正则
// 模式原样保留——模式不是模型名，归一化会把 "re:^gpt-4/.*$" 这类写法截断。
// /v1/models 与路由视图统一用它，避免上游报的前缀/后缀/大小写漏到下游。
func exposedModelName(name string) string {
	p := strings.TrimSpace(name)
	if p == "" || !modelPatternIsPlain(p) {
		return p
	}
	return modelIdentity(p)
}

// normalizeDeclaredModels 归一化渠道声明的模型列表，并把上游原写法记进模型映射：
//   - 通配/正则模式原样保留（模式不参与归一化）；
//   - 普通名归一化为对外名（小写、去供应商前缀与 ":free" 类变体后缀），原写法
//     作为该名字的上游模型写进 ModelMap——转发时按原写法发往上游，与 go-gateway
//     「路由用规范名、渠道记 source_model」是同一套语义；
//   - 同一个对外名在上游的多个写法各记一条映射目标，路由时**各成一个候选**：
//     请求按顺序尝试、冷却各自独立计算（"x" 与 "x:free" 同列时两个都是候选，
//     不会只留下其中一个）；对外始终只暴露一个名字（ModelMap 的键）；
//   - 归一化后重复的条目合并（映射目标按出现顺序累加，字面去重保序）。
//
// pruneStale 为真（「以上游为准」全量替换）时丢弃规范名不在新列表里的映射键。
// 返回列表或映射是否发生了变化（加载历史配置时的迁移据此决定要不要写回）。
func (c *Channel) normalizeDeclaredModels(pruneStale bool) bool {
	if c == nil {
		return false
	}
	oldModels, oldMap := c.Models, c.ModelMap
	models := normalizeModelList(c.Models)
	mapping := make(ModelMap, len(c.ModelMap))
	for k, v := range c.ModelMap {
		mapping[k] = append([]string{}, v...)
	}
	// 已有映射键按规范名索引：新的原写法合并进等价键，而不是另起一个等价键
	//（normalizeModelMap 对等价键只保留一个，另起会把已有目标丢掉）
	byCanon := map[string]string{}
	for k := range mapping {
		if canon := canonicalModel(k); canon != "" {
			if prev, ok := byCanon[canon]; !ok || k < prev {
				byCanon[canon] = k
			}
		}
	}
	// 先统计每个对外名在声明列表里出现的写法：同一对外名有多个写法时，与对外名
	// 字面相同的那个也要记成候选（"x" 与 "x:free" 同时声明 = 两个候选）；只有一个
	// 写法时不记（否则保存后再保存会不断给同一个名字加一个自身候选）。
	spellings := map[string][]string{}
	for _, raw := range models {
		p := strings.TrimSpace(raw)
		if p == "" || !modelPatternIsPlain(p) {
			continue
		}
		id := modelIdentity(p)
		if id == "" {
			continue
		}
		if !containsString(spellings[id], p) {
			spellings[id] = append(spellings[id], p)
		}
	}
	out := make([]string, 0, len(models))
	order := make([]string, 0, len(models)) // 对外名出现顺序
	targets := map[string][]string{}        // 对外名 → 该名字的上游写法（保序去重）
	seen := map[string]bool{}
	for _, raw := range models {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		if !modelPatternIsPlain(p) {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
			continue
		}
		id := modelIdentity(p)
		if id == "" {
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
			order = append(order, id)
		}
		if (p != id || len(spellings[id]) > 1) && !containsString(targets[id], p) {
			targets[id] = append(targets[id], p)
		}
	}
	for _, id := range order {
		key := id
		if canon := canonicalModel(id); canon != "" {
			if existing, ok := byCanon[canon]; ok {
				key = existing
			} else {
				byCanon[canon] = id
			}
		}
		for _, up := range targets[id] {
			if !containsString(mapping[key], up) {
				mapping[key] = append(mapping[key], up)
			}
		}
	}
	if pruneStale {
		live := map[string]bool{}
		for _, m := range out {
			if canon := canonicalModel(m); canon != "" {
				live[canon] = true
			}
		}
		for k := range mapping {
			if canon := canonicalModel(k); canon == "" || !live[canon] {
				delete(mapping, k)
			}
		}
	}
	c.Models = out
	c.ModelMap = normalizeModelMap(mapping)
	return !sameModelList(oldModels, c.Models) || !sameModelMap(oldMap, c.ModelMap)
}

// containsString 列表里是否已有该值（字面比较）。
func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// sameModelList 两个模型名列表是否完全相同（含顺序）。
func sameModelList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sameModelMap 两个模型映射是否完全相同（键、目标与顺序）。
func sameModelMap(a, b ModelMap) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || len(va) != len(vb) {
			return false
		}
		for i := range va {
			if va[i] != vb[i] {
				return false
			}
		}
	}
	return true
}

// modelPatternRegexBody "re:" 前缀的正则模式体（大小写不敏感前缀）。
func modelPatternRegexBody(pattern string) (string, bool) {
	if len(pattern) > 3 && strings.EqualFold(pattern[:3], "re:") {
		return pattern[3:], true
	}
	return "", false
}

// modelPatternHasWildcard 模式是否含通配符（* 任意长度、? 单字符）。
func modelPatternHasWildcard(pattern string) bool {
	return strings.ContainsAny(pattern, "*?")
}

// modelPatternIsPlain 普通名（非正则、非通配）：可作为上游模型名原样发送。
func modelPatternIsPlain(pattern string) bool {
	if _, ok := modelPatternRegexBody(pattern); ok {
		return false
	}
	return !modelPatternHasWildcard(pattern)
}

// globMatch 通配匹配（调用方保证两侧已小写去空白）。* 匹配任意长度（含空），
// ? 匹配单个字符；用星号回溯实现，无正则回溯爆炸风险。
func globMatch(s, pattern string) bool {
	si, pi := 0, 0
	star, resume := -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]):
			si++
			pi++
		case pi < len(pattern) && pattern[pi] == '*':
			star, resume = pi, si
			pi++
		case star >= 0:
			pi = star + 1
			resume++
			si = resume
		default:
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// modelMatchesPattern 通配/正则模式匹配：先按原样名，再按归一化名各试一次
//（上游报 "cline-free/x:free"、模式写 "x*" 时后者命中）。普通名返回 false，
// 由 modelMatches 走精确/归一化等价分支。
func modelMatchesPattern(model, pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(model))
	canon := canonicalModel(model)
	if body, ok := modelPatternRegexBody(pattern); ok {
		re, err := regexp.Compile(body)
		if err != nil {
			return false
		}
		if re.MatchString(lower) {
			return true
		}
		return canon != "" && canon != lower && re.MatchString(canon)
	}
	if !modelPatternHasWildcard(pattern) {
		return false
	}
	p := strings.ToLower(pattern)
	if globMatch(lower, p) {
		return true
	}
	return canon != "" && canon != lower && globMatch(canon, p)
}

// modelMatches 判断模型名是否匹配渠道声明的模型条目：普通名按归一化等价
//（大小写/供应商前缀/变体后缀，双向），通配/正则按模式匹配。
func modelMatches(model, pattern string) bool {
	model = strings.TrimSpace(model)
	pattern = strings.TrimSpace(pattern)
	if model == "" || pattern == "" {
		return false
	}
	if modelPatternIsPlain(pattern) {
		c := canonicalModel(model)
		return c != "" && c == canonicalModel(pattern)
	}
	return modelMatchesPattern(model, pattern)
}

// modelMatcher 渠道模型列表的匹配器：普通名走集合（O(1)），通配/正则逐条匹配。
// 未声明模型列表（空）= 对全部模型放行。路由视图的「模型分组 × 渠道」循环里
// 直接调用 allowsModel 会退化成 O(模型² × 渠道)，故预构建匹配器。
type modelMatcher struct {
	allowAll bool
	exact    map[string]bool
	patterns []string
}

func newModelMatcher(models []string) *modelMatcher {
	m := &modelMatcher{}
	if len(models) == 0 {
		m.allowAll = true
		return m
	}
	m.exact = make(map[string]bool, len(models))
	for _, raw := range models {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		if !modelPatternIsPlain(p) {
			m.patterns = append(m.patterns, p)
			continue
		}
		if c := canonicalModel(p); c != "" {
			m.exact[c] = true
		}
	}
	return m
}

// match 模型名是否在该渠道的模型列表内（空模型名 = 不限定的通配分组，放行）。
func (m *modelMatcher) match(model string) bool {
	if m == nil || m.allowAll {
		return true
	}
	if strings.TrimSpace(model) == "" {
		return true
	}
	if c := canonicalModel(model); c != "" && m.exact[c] {
		return true
	}
	for _, p := range m.patterns {
		if modelMatchesPattern(model, p) {
			return true
		}
	}
	return false
}

// modelMapTargets 名称映射查找：精确（原样）键优先，其次归一化等价键；
// 返回该下游名对应的全部上游模型名（去空白、去空项、去重保序），未命中返回 nil。
// 归一化键可能有多条等价写法，取键名排序最靠前的一条，保证结果稳定。
func modelMapTargets(mapping ModelMap, model string) []string {
	if len(mapping) == 0 || strings.TrimSpace(model) == "" {
		return nil
	}
	if v := cleanModelTargets(mapping[model]); len(v) > 0 {
		return v
	}
	c := canonicalModel(model)
	if c == "" {
		return nil
	}
	for _, k := range sortedKeys(mapping) {
		if canonicalModel(k) == c {
			return cleanModelTargets(mapping[k])
		}
	}
	return nil
}

// cleanModelTargets 清理映射目标列表：去空白、去空项、按**字面**去重、保序。
//
// 不按归一化名去重：映射的目标是上游真正认识的写法，同一模型在上游的多个写法
// （如 x 与 cline-free/x:free）各自是一个候选、各自独立冷却，合并会丢掉候选。
func cleanModelTargets(in []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

// declaredModelName 在渠道声明的模型列表里找与 model 等价的普通名条目，返回其
// 原文（上游 /models 的写法，即上游认识的名字）；命中的是通配/正则、未命中、
// 或仅大小写不同时返回 ""（此时调用方按下游请求名原样发送——大小写偏好无从
// 判断，不替上游猜，避免把用户手写的错误大小写当作上游真实模型名发出去）。
func (c *Channel) declaredModelName(model string) string {
	if c == nil {
		return ""
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	plain, fold := "", ""
	for _, raw := range c.Models {
		p := strings.TrimSpace(raw)
		if p == "" {
			continue
		}
		if p == model {
			return p // 写法完全一致（含大小写）：直接采用
		}
		if !modelPatternIsPlain(p) || !modelMatches(model, p) {
			continue
		}
		if plain == "" {
			plain = p
		}
		if fold == "" && strings.EqualFold(p, model) {
			fold = p
		}
	}
	if fold != "" {
		return "" // 仅大小写差异：保持下游请求的写法
	}
	return plain
}

// upstreamModelsFor 计算发往该上游的模型名列表：渠道映射 ModelMap 优先（可多个
// 上游模型，各成一个候选），其次声明列表里的原文写法（单个）；都不适用返回 nil
//（原样发送下游请求名，单候选）。
func (c *Channel) upstreamModelsFor(model string) []string {
	if c == nil {
		return nil
	}
	if v := modelMapTargets(c.ModelMap, model); len(v) > 0 {
		return v
	}
	if d := c.declaredModelName(model); d != "" {
		return []string{d}
	}
	return nil
}

// resolveModelAlias 全局别名解析（设置页 model_aliases）：精确或归一化等价命中
// 即返回目标模型名，未命中返回原值。只解析一层（不递归链式展开）。
// 注意：渠道级映射优先于全局别名——渠道 ModelMap 里写了该下游名时，该渠道按
// 渠道映射发往上游，不受全局别名影响（见 newCandidates）。
func resolveModelAlias(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return model
	}
	if v := modelAliasLookup(currentPolicy().ModelAliases, model); v != "" && v != model {
		return v
	}
	return model
}

// sortedKeys 返回映射的键排序副本（遍历顺序稳定，供路由视图分组等展示用）。
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// modelAliasLookup 全局别名查找（下游名 → 规范模型名）：精确键优先，其次
// 归一化等价键；未命中返回 ""。归一化键可能有多条等价写法，取排序最靠前者。
func modelAliasLookup(mapping map[string]string, model string) string {
	if len(mapping) == 0 || strings.TrimSpace(model) == "" {
		return ""
	}
	if v, ok := mapping[model]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	c := canonicalModel(model)
	if c == "" {
		return ""
	}
	for _, k := range sortedKeys(mapping) {
		if canonicalModel(k) == c && strings.TrimSpace(mapping[k]) != "" {
			return strings.TrimSpace(mapping[k])
		}
	}
	return ""
}

// gwKeyAllowsModel 下游通用 key 的模型白名单校验：未配置（空）放行全部；
// 支持通配/正则模式，且同时校验下游请求名与别名解析后的路由名。
func gwKeyAllowsModel(k *GWKey, models ...string) bool {
	if k == nil || len(k.Models) == 0 {
		return true
	}
	for _, m := range models {
		m = strings.TrimSpace(m)
		if m == "" {
			continue
		}
		for _, p := range k.Models {
			if modelMatches(m, p) {
				return true
			}
		}
	}
	return false
}

// newCandidates 构造该 (渠道, key) 对一次下游请求的全部候选。
//
// rawModel 为下游请求里的模型名（未解析别名），routeModel 为别名解析后的路由名。
// 渠道级映射优先于全局别名：先按下游原名查渠道映射（命中即按渠道映射发往上游，
// 不受全局别名影响），未命中再按路由名查。渠道映射可以指向多个上游模型
// （如 cn:x、global:y），此时每个上游模型各成一个候选，发往上游的 model 不同，
// 冷却也按上游模型分别计算（candidate.coolModel）；未配置映射时退化为单候选
//（按渠道声明列表的原文写法改写，冷却仍按渠道粒度规则）。
func newCandidates(ch *Channel, k *UpKey, rawModel, routeModel string) []candidate {
	if ch == nil {
		return []candidate{{k: k}}
	}
	for _, m := range []string{rawModel, routeModel} {
		targets := modelMapTargets(ch.ModelMap, m)
		if len(targets) == 0 {
			continue
		}
		out := make([]candidate, 0, len(targets))
		for _, up := range targets {
			// 映射出的上游模型各自独立冷却（同一 key 下的 cn:/global: 变体互不牵连）
			out = append(out, candidate{ch: ch, k: k, upstreamModel: up, coolModel: up})
		}
		return out
	}
	return []candidate{{ch: ch, k: k, upstreamModel: ch.declaredModelName(routeModel)}}
}

// newCandidate 构造单候选（映射/声明列表的首个命中）：供探测、渠道测试等
// 「一个 (key, 模型) 只发一次请求」的链路使用。
func newCandidate(ch *Channel, k *UpKey, model string) candidate {
	return newCandidates(ch, k, model, model)[0]
}

// upstreamRow 路由视图里一个 (渠道, key) 展开出的一行候选：上游模型名 + 该行
// 冷却键的 model 部分。
type upstreamRow struct {
	upstream string // 发往上游的模型名（"" = 原样发送下游请求名）
	cool     string // 冷却键 model 部分（"" = 按 key 跨模型共享）
	mapped   bool   // 该行来自渠道模型映射（与分组名相同也要展示，便于核对）
}

// channelUpstreamRows 该渠道对一个模型分组应展开的候选行，规则与 newCandidates
// 完全一致（渠道映射优先于全局别名、多上游映射各成一行、各自独立冷却）：
// 路由视图据此与网关实际转发保持一致，避免「视图显示可用但转发被冷却跳过」。
func channelUpstreamRows(ch *Channel, rawModel, routeModel string) []upstreamRow {
	if ch == nil {
		return []upstreamRow{{}}
	}
	for _, m := range []string{rawModel, routeModel} {
		targets := modelMapTargets(ch.ModelMap, m)
		if len(targets) == 0 {
			continue
		}
		rows := make([]upstreamRow, 0, len(targets))
		for _, up := range targets {
			rows = append(rows, upstreamRow{upstream: up, cool: up, mapped: true})
		}
		return rows
	}
	return []upstreamRow{{upstream: ch.declaredModelName(routeModel), cool: ch.cooldownModelFor(routeModel)}}
}

// upstreamBody 返回真正发往上游的请求体与 Content-Type：候选的内部渠道固定
// 注入体优先，再按需把 model 字段改写为渠道映射后的名称。
func (c *candidate) upstreamBody(raw []byte, contentType string) ([]byte, string) {
	body := c.requestBody(raw)
	if c.upstreamModel == "" {
		return body, contentType
	}
	return rewriteBodyModel(body, contentType, c.upstreamModel)
}

// rewriteBodyModel 把请求体里的 model 字段改写为 upstream（JSON 与 multipart
// 表单两种形态）；改写不了（非法 JSON、multipart 解析失败）时原样返回，
// 转发不受影响。
func rewriteBodyModel(body []byte, contentType, upstream string) ([]byte, string) {
	if len(body) == 0 || upstream == "" {
		return body, contentType
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "multipart/") {
		if out, ct, ok := rewriteMultipartModel(body, contentType, upstream); ok {
			return out, ct
		}
		return body, contentType
	}
	return rewriteJSONModel(body, upstream), contentType
}

// rewriteJSONModel 改写 JSON 请求体的 model 字段（其余字段原样保留）。
func rewriteJSONModel(body []byte, upstream string) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	if cur, ok := obj["model"]; ok {
		var s string
		if json.Unmarshal(cur, &s) == nil && s == upstream {
			return body // 已是目标名：不改写，字节原样透传
		}
	}
	encoded, err := json.Marshal(upstream)
	if err != nil {
		return body
	}
	obj["model"] = encoded
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// rewriteMultipartModel 改写 multipart 表单里的 model 字段（图片 edits/variations
// 的 model 是表单字段）；其余 part（含图片二进制）原样搬运，boundary 重新生成。
// 返回新请求体与新 Content-Type；解析失败返回 ok=false。
func rewriteMultipartModel(raw []byte, contentType, upstream string) ([]byte, string, bool) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", false
	}
	boundary := params["boundary"]
	if boundary == "" {
		return nil, "", false
	}
	rd := multipart.NewReader(bytes.NewReader(raw), boundary)
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	found := false
	for {
		part, err := rd.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", false
		}
		header := textproto.MIMEHeader{}
		for name, values := range part.Header {
			for _, v := range values {
				header.Add(name, v)
			}
		}
		pw, err := w.CreatePart(header)
		if err != nil {
			return nil, "", false
		}
		if part.FormName() == "model" {
			// 丢弃原值，写入映射后的模型名
			_, _ = io.Copy(io.Discard, part)
			if _, err := pw.Write([]byte(upstream)); err != nil {
				return nil, "", false
			}
			found = true
			continue
		}
		if _, err := io.Copy(pw, part); err != nil {
			return nil, "", false
		}
	}
	if !found {
		// 原表单没有 model 字段（如 variations 用 image 字段）：补一个，
		// 与 go-gateway 的 multipart 改写语义一致
		if err := w.WriteField("model", upstream); err != nil {
			return nil, "", false
		}
	}
	if err := w.Close(); err != nil {
		return nil, "", false
	}
	return buf.Bytes(), w.FormDataContentType(), true
}
