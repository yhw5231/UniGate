// 上游模型列表拉取：用渠道 key（含其代理配置）请求上游 /models，
// 解析模型 ID 与价格（上游返回价格且输入/输出均为 0 视为免费），
// 并把结果写回渠道的 Models（作为可用模型，用于路由过滤与 /v1/models）。
// 免费模型清单仅随 Admin API 响应返回，供 WebUI 临时标注展示，不落盘。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// fetchModelsMaxKeys 拉取模型时最多依次尝试的 key 数（key 失败自动换下一个）。
const fetchModelsMaxKeys = 3

// fetchModelsTimeout 单次请求上游模型列表的超时。
const fetchModelsTimeout = 20 * time.Second

// fetchUpstreamModels 用渠道的启用 key 依次拉取上游模型列表（Bearer 鉴权，
// key 配置了代理则走代理）。返回模型条目（含分组与免费标记）与用到的 key 名。
// 全部尝试失败时返回最后一个错误。
func fetchUpstreamModels(ctx context.Context, ch *Channel) (models []fetchedModel, usedKey string, err error) {
	var lastErr error
	attempts := 0
	for _, k := range ch.Keys {
		if !k.Enabled || attempts >= fetchModelsMaxKeys {
			continue
		}
		attempts++
		cand := candidate{ch: ch, k: k}
		list, ferr := fetchModelsWithKey(ctx, &cand)
		if ferr != nil {
			lastErr = fmt.Errorf("key %q: %w", k.Name, ferr)
			continue
		}
		if len(list) == 0 {
			lastErr = fmt.Errorf("key %q: upstream returned an empty model list", k.Name)
			continue
		}
		return list, k.Name, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no enabled key to fetch models")
	}
	return nil, "", lastErr
}

// fetchModelsWithKey 用单个 key 拉取模型列表（应用 key 代理与渠道自定义头）。
func fetchModelsWithKey(ctx context.Context, cand *candidate) ([]fetchedModel, error) {
	route, err := resolveProxy(cand)
	if err != nil {
		return nil, fmt.Errorf("resolve proxy: %w", err)
	}
	client := newUpstreamClient(route)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cand.modelsTarget(), nil)
	if err != nil {
		return nil, err
	}
	if cand.k.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cand.k.APIKey)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "unigate/"+displayVersion())
	applyCustomHeaders(req, effectiveChannelHeaders(cand.ch))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", cand.modelsTarget(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s -> upstream %d: %s", cand.modelsTarget(), resp.StatusCode, truncate(string(body), 200))
	}
	return parseModelsPayload(body)
}

// fetchedModel 上游返回的一个模型条目：ID（上游写法）+ 所属分组 + 是否免费。
// 分组来自响应结构本身（如 Cline 的 recommended/free/clinePass/clineCloud），
// OpenAI 形态（{"data":[...]}）没有分组，Group 为空。
type fetchedModel struct {
	ID    string
	Group string
	Free  bool
}

// modelCandidate 拉取到的一个候选：上游返回的**一个**模型（上游写法 Raw）及其
// 对外名 Name（modelIdentity）。同一对外名在上游有多个写法时是**多条候选**，
// 各自勾选、各自成为转发候选、各自独立冷却；对外只暴露一个 Name。
type modelCandidate struct {
	Name  string `json:"name"`            // 对外名（小写、去供应商前缀与变体后缀）
	Raw   string `json:"raw"`             // 上游写法（发往上游的名字）
	Free  bool   `json:"free"`            // 免费（分组名含 free、:free 后缀或价格为 0）
	Group string `json:"group,omitempty"` // 上游给的分组（recommended / clinePass / …）
}

// fetchedCandidates 把上游返回的模型条目转成候选（上游顺序，字面去重）：
// 每个上游写法各占一条，便于 WebUI 逐个勾选——不按对外名合并，因为上游的
// 不同写法是不同的上游模型（请求与冷却都分别计算），合并会让用户没法分别启用。
func fetchedCandidates(models []fetchedModel) []modelCandidate {
	var out []modelCandidate
	seen := map[string]bool{}
	for _, m := range models {
		p := strings.TrimSpace(m.ID)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, modelCandidate{
			Name:  exposedModelName(p),
			Raw:   p,
			Free:  m.Free,
			Group: strings.TrimSpace(m.Group),
		})
	}
	return out
}

// fetchedRawModels 候选的上游写法列表（WebUI 勾选值 = 上游模型名）。
func fetchedRawModels(cands []modelCandidate) []string {
	out := make([]string, 0, len(cands))
	for _, c := range cands {
		out = append(out, c.Raw)
	}
	return out
}

// fetchedFreeModels 免费候选的上游写法（免费清单不落盘，仅随响应展示）。
func fetchedFreeModels(cands []modelCandidate) []string {
	var out []string
	for _, c := range cands {
		if c.Free {
			out = append(out, c.Raw)
		}
	}
	return out
}

// fetchedModelGroups 候选按上游分组聚合（保持分组首次出现的顺序），
// WebUI 按分组展示候选清单；未分组的候选归到 name 为空的那一组。
// free = 该组**含**免费模型（不代表整组都免费），free_total = 该组免费模型数，
// 便于 WebUI 给分组头与逐个模型加免费标识。
func fetchedModelGroups(cands []modelCandidate) []map[string]any {
	var order []string
	byName := map[string][]string{}
	freeByName := map[string]bool{}
	freeTotal := map[string]int{}
	for _, c := range cands {
		if _, ok := byName[c.Group]; !ok {
			order = append(order, c.Group)
		}
		byName[c.Group] = append(byName[c.Group], c.Raw)
		if c.Free {
			freeByName[c.Group] = true
			freeTotal[c.Group]++
		}
	}
	out := make([]map[string]any, 0, len(order))
	for _, name := range order {
		models := byName[name]
		out = append(out, map[string]any{
			"name":       name,
			"free":       freeByName[name],
			"free_total": freeTotal[name],
			"models":     models,
			"total":      len(models),
		})
	}
	return out
}

// enabledUpstreamModels 本次拉取的候选里「当前已生效」的上游写法（WebUI 预勾选）：
//   - 渠道未声明任何模型（含映射键）= 对全部模型放行：返回全部候选（全选）；
//   - 否则取渠道当前真正会发往上游的名字（映射目标优先，其次声明列表的原写法）；
//   - 声明了通配/正则的渠道：模式命中的候选也算已生效（与「全选 = 以上游为准」
//     的默认一致，避免重建时静默丢掉通配覆盖的模型）。
//
// 返回顺序与 cands 一致，便于前端直接按下标/值取用。
func enabledUpstreamModels(ch *Channel, cands []modelCandidate) []string {
	if ch == nil || (len(ch.Models) == 0 && len(ch.ModelMap) == 0) {
		return fetchedRawModels(cands)
	}
	live := map[string]bool{}
	for _, m := range append(append([]string{}, ch.Models...), ch.modelMapKeys()...) {
		p := strings.TrimSpace(m)
		if p == "" || !modelPatternIsPlain(p) {
			continue // 模式不是上游模型名
		}
		for _, up := range ch.upstreamModelsFor(p) {
			live[up] = true
		}
	}
	var out []string
	for _, c := range cands {
		if live[c.Raw] || ch.matchesDeclaredPattern(c.Raw) {
			out = append(out, c.Raw)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// matchesDeclaredPattern 该模型名是否被渠道声明列表里的通配/正则模式覆盖。
func (c *Channel) matchesDeclaredPattern(model string) bool {
	if c == nil || strings.TrimSpace(model) == "" {
		return false
	}
	for _, p := range c.Models {
		p = strings.TrimSpace(p)
		if p == "" || modelPatternIsPlain(p) {
			continue
		}
		if modelMatchesPattern(model, p) {
			return true
		}
	}
	return false
}

// modelsTarget 返回该候选的上游模型列表端点（key BaseURL 优先，覆盖渠道配置）。
func (c *candidate) modelsTarget() string {
	if u := c.k.BaseURL; u != "" {
		base := normalizeBaseURL(u)
		if strings.HasSuffix(base, "/models") {
			return base
		}
		return base + "/models"
	}
	return c.ch.modelsURL()
}

// parseModelsPayload 解析上游模型列表响应，兼容多种常见形态：
//   - 标准 OpenAI：{"data":[{"id":...}]}（OpenRouter 额外带 pricing），data 里也可以是裸字符串
//   - 裸数组：[{"id":...}] 或 ["model-id", ...]
//   - **分组对象**：每个字段的值是一组模型，字段名就是分组名。Cline 的
//     /api/v1/ai/cline/recommended-models 就是这种形态
//     （recommended / free / clinePass / clineCloud，元素含 id/name/description/tags）；
//     "data"/"models" 里再嵌一层分组对象也支持。
//
// data[].pricing（prompt/completion 或 input/output，数字或字符串）解析价格，两项都存在且
// 均为 0 记为免费；分组名含 free、模型名带 :free/-free 后缀同样记为免费。
func parseModelsPayload(body []byte) ([]fetchedModel, error) {
	groups, err := parseModelGroups(body)
	if err != nil {
		return nil, err
	}
	var out []fetchedModel
	for _, g := range groups {
		groupFree := groupIsFree(g.Name)
		for _, item := range g.Items {
			item.Group = g.Name
			if groupFree || modelNameIsFree(item.ID) {
				item.Free = true
			}
			out = append(out, item)
		}
	}
	return out, nil
}

// modelGroupItems 上游响应里的一组模型（分组名 + 条目；未分组时 Name 为空）。
type modelGroupItems struct {
	Name  string
	Items []fetchedModel
}

// parseModelGroups 把响应体解析成若干分组：顶层数组 = 单个未分组；顶层对象先看常见
// 包装字段（data/models/result/items/list），否则把每个「值是模型数组」的字段当成一组。
func parseModelGroups(body []byte) ([]modelGroupItems, error) {
	trim := bytes.TrimSpace(body)
	if len(trim) == 0 {
		return nil, errors.New("parse models response: empty body")
	}
	switch trim[0] {
	case '[':
		items, err := parseModelItems(trim)
		if err != nil {
			return nil, err
		}
		return []modelGroupItems{{Items: items}}, nil
	case '{':
	default:
		return nil, fmt.Errorf("parse models response: not an object with data nor an array: %.120s", string(trim))
	}

	// 包装字段：值是模型数组就用它（未分组）；值是对象则递归当分组对象
	if _, fields, err := objectFields(trim); err == nil {
		for _, key := range []string{"data", "models", "result", "items", "list"} {
			raw, ok := fields[key]
			if !ok {
				continue
			}
			sub := bytes.TrimSpace(raw)
			if len(sub) == 0 {
				continue
			}
			if sub[0] == '[' {
				// 空数组也是合法结果（上游确实没有模型），交给调用方判定
				if items, err := parseModelItems(sub); err == nil {
					return []modelGroupItems{{Items: items}}, nil
				}
				continue
			}
			if sub[0] == '{' {
				if groups, err := parseGroupedObject(sub); err == nil && len(groups) > 0 {
					return groups, nil
				}
			}
		}
	}
	return parseGroupedObject(trim)
}

// parseGroupedObject 把「字段名 = 模型数组」的对象解析成分组，保持响应里的字段顺序
// （分组顺序对展示有意义，Go 的 map 会丢顺序，所以这里流式解码）。
func parseGroupedObject(obj json.RawMessage) ([]modelGroupItems, error) {
	order, fields, err := objectFields(obj)
	if err != nil {
		return nil, err
	}
	var out []modelGroupItems
	for _, key := range order {
		sub := bytes.TrimSpace(fields[key])
		if len(sub) == 0 || sub[0] != '[' {
			continue
		}
		items, err := parseModelItems(sub)
		if err != nil || len(items) == 0 {
			continue
		}
		out = append(out, modelGroupItems{Name: key, Items: items})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("parse models response: no model list found: %.120s", string(obj))
	}
	return out, nil
}

// objectFields 按 JSON 原文顺序返回顶层对象的字段名与原始值。
func objectFields(body []byte) ([]string, map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, errors.New("not a JSON object")
	}
	var order []string
	fields := map[string]json.RawMessage{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		key, _ := keyTok.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, nil, err
		}
		if _, dup := fields[key]; !dup {
			order = append(order, key)
		}
		fields[key] = raw
	}
	return order, fields, nil
}

// parseModelItems 解析模型数组（JSON 元素为字符串，或含 id/name/model 的对象）。
func parseModelItems(items json.RawMessage) ([]fetchedModel, error) {
	var arr []json.RawMessage
	if err := json.Unmarshal(items, &arr); err != nil {
		return nil, fmt.Errorf("parse models response: not a model array: %.120s", string(items))
	}
	var out []fetchedModel
	for _, it := range arr {
		var s string
		if json.Unmarshal(it, &s) == nil {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, fetchedModel{ID: s})
			}
			continue
		}
		var m struct {
			ID      string          `json:"id"`
			Name    string          `json:"name"`
			Model   string          `json:"model"`
			Pricing json.RawMessage `json:"pricing"`
		}
		if err := json.Unmarshal(it, &m); err != nil {
			continue // 脏元素跳过，不拖垮整个列表
		}
		id := strings.TrimSpace(m.ID)
		if id == "" {
			id = strings.TrimSpace(m.Name)
		}
		if id == "" {
			id = strings.TrimSpace(m.Model)
		}
		if id == "" {
			continue
		}
		entry := fetchedModel{ID: id}
		if p, c, ok := parseModelPricing(m.Pricing); ok && p == 0 && c == 0 {
			entry.Free = true
		}
		out = append(out, entry)
	}
	return out, nil
}

// groupIsFree 分组名是否表示「免费」（Cline 的 free 分组、中文「免费」）。
func groupIsFree(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return n == "free" || strings.Contains(n, "免费")
}

// modelNameIsFree 模型名是否自带免费标记（:free 变体后缀或 -free 结尾）。
func modelNameIsFree(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return strings.HasSuffix(n, ":free") || strings.HasSuffix(n, "-free")
}

// parseModelPricing 从 pricing 对象解析 (输入价格, 输出价格, 是否提供了价格)。
// 兼容数字与字符串形式，键名兼容 prompt/completion 与 input/output。
func parseModelPricing(raw json.RawMessage) (prompt, completion float64, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, 0, false
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0, 0, false
	}
	prompt, pok := pricingField(obj, "prompt", "input")
	completion, cok := pricingField(obj, "completion", "output")
	return prompt, completion, pok || cok
}

func pricingField(obj map[string]any, keys ...string) (float64, bool) {
	for _, k := range keys {
		v, present := obj[k]
		if !present || v == nil {
			continue
		}
		switch n := v.(type) {
		case float64:
			return n, true
		case string:
			if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
				return f, true
			}
		}
	}
	return 0, false
}
