// 冷却表：按 (上游 keyID, model 部分) 记录故障冷却，路由引擎据此跳过不可用 key。
// model 部分由渠道级冷却粒度开关决定：默认按 key 跨模型共享（传空串），
// 渠道显式 "key_model" 时按 (key, model) 独立冷却。
// 目前只有上游 429 记冷却：冷却时长优先取上游明确给出的到期时间
//（Retry-After 头，或响应体里的 "Try again in 14h 23m" 类文本/时间戳），
// 上游没给明确时间才用 RATE_LIMIT_COOLDOWN；
// 其余故障（401/403、5xx、网络错误）只做故障转移，不冷却 key。
package main

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cooldownPair struct {
	keyID string
	model string
}

// Cooldowns 冷却状态表。
type Cooldowns struct {
	mu    sync.Mutex
	until map[cooldownPair]time.Time
}

var cool *Cooldowns

func newCooldowns() *Cooldowns {
	return &Cooldowns{until: map[cooldownPair]time.Time{}}
}

// IsCooling 返回 (keyID, model) 是否处于冷却中。
func (c *Cooldowns) IsCooling(keyID, model string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	until, ok := c.until[cooldownPair{keyID, model}]
	return ok && time.Now().Before(until)
}

// Mark 记录冷却（dur <=0 时忽略）。
func (c *Cooldowns) Mark(keyID, model string, dur time.Duration) {
	if dur <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.until[cooldownPair{keyID, model}] = time.Now().Add(dur)
	// 防膨胀：条目过多时清理已过期项
	if len(c.until) > 10000 {
		c.pruneLocked()
	}
}

// Clear 解除 (keyID, model) 的冷却。渠道测试成功后调用：
// 真实请求已打通该 key，存量冷却与事实相悖（表现为「测试通过但网关 502」）。
func (c *Cooldowns) Clear(keyID, model string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.until, cooldownPair{keyID, model})
}

// ClearKey 解除某 key 的全部冷却（所有模型粒度），返回清除的条数。
// WebUI「解除冷却」手动操作使用。
func (c *Cooldowns) ClearKey(keyID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for k := range c.until {
		if k.keyID == keyID {
			delete(c.until, k)
			n++
		}
	}
	return n
}

// ClearModel 解除 (keyID, model) 一条冷却（按 (key, 模型) 粒度精确清除，
// 渠道页/路由页对 key_model 渠道的单模型解除使用），返回是否清除。
func (c *Cooldowns) ClearModel(keyID, model string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := cooldownPair{keyID, model}
	if _, ok := c.until[p]; !ok {
		return false
	}
	delete(c.until, p)
	return true
}

// CoolingMap 返回某 key 全部生效中的冷却（model 部分 → 到期时间快照）：
// 渠道页按 (key, 模型) 明细展示「冷却模型 / 可用模型」时使用。
func (c *Cooldowns) CoolingMap(keyID string) map[string]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	out := map[string]time.Time{}
	for p, until := range c.until {
		if p.keyID != keyID || !now.Before(until) {
			continue
		}
		out[p.model] = until
	}
	return out
}

// ClearAll 清空全部冷却，返回清除的条数。WebUI「路由」页一键清理使用；
// 测试重置也走这里。
func (c *Cooldowns) ClearAll() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.until)
	c.until = map[cooldownPair]time.Time{}
	return n
}

// CoolingKey 返回 (keyID, model) 的冷却到期时间（未冷却返回零值, false）。
func (c *Cooldowns) CoolingKey(keyID, model string) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	until, ok := c.until[cooldownPair{keyID, model}]
	if !ok || !time.Now().Before(until) {
		return time.Time{}, false
	}
	return until, true
}

// CoolingEntry 一条生效中的冷却（Admin state 用；key 按 ID 引用，WebUI 换算名称）。
type CoolingEntry struct {
	KeyID  string `json:"key_id"`
	Model  string `json:"model,omitempty"` // 空 = 按 key 共享粒度
	Until  int64  `json:"until_unix"`
	LeftMS int64  `json:"left_ms"`
}

// CoolingList 列出全部生效中的冷却（快照）。
func (c *Cooldowns) CoolingList() []CoolingEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	out := []CoolingEntry{}
	for p, until := range c.until {
		left := until.Sub(now)
		if left <= 0 {
			continue
		}
		out = append(out, CoolingEntry{
			KeyID:  p.keyID,
			Model:  p.model,
			Until:  until.Unix(),
			LeftMS: left.Milliseconds(),
		})
	}
	return out
}

// EarliestRetry 返回给定冷却键（keyID+model 部分，由调用方按渠道粒度生成）
// 集合中最早的冷却到期剩余时长；全部未冷却返回 0, false。
func (c *Cooldowns) EarliestRetry(pairs []cooldownPair) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	var earliest time.Time
	for _, p := range pairs {
		until, ok := c.until[p]
		if ok && now.Before(until) {
			if earliest.IsZero() || until.Before(earliest) {
				earliest = until
			}
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	d := time.Until(earliest)
	if d < 0 {
		d = 0
	}
	return d, true
}

func (c *Cooldowns) pruneLocked() {
	now := time.Now()
	for k, until := range c.until {
		if !now.Before(until) {
			delete(c.until, k)
		}
	}
}

// ---- 5xx 连续错误计数 ----

// Streaks 按 key 记录连续上游 5xx 次数：成功请求清零，连续超过阈值
// （ROTATE_AFTER_5XX，默认 3）触发自动换出口 IP。5xx 本身不冷却，只换 key。
type Streaks struct {
	mu sync.Mutex
	n  map[string]int
}

var streaks *Streaks

func newStreaks() *Streaks {
	return &Streaks{n: map[string]int{}}
}

// Inc 记一次 5xx，返回当前连续次数。
func (s *Streaks) Inc(keyID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n[keyID]++
	return s.n[keyID]
}

// Reset 清零该 key 的连续计数（成功请求后调用）。
func (s *Streaks) Reset(keyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.n, keyID)
}

// retryAfterDuration 从上游 Retry-After 响应头解析冷却时长；空/解析失败返回 def。
func retryAfterDuration(header string, def time.Duration) time.Duration {
	if header == "" {
		return def
	}
	seconds, err := strconv.Atoi(strings.TrimSpace(header))
	if err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	t, err := time.Parse(time.RFC1123, header)
	if err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return def
}

// 部分上游 429 不带 Retry-After 头，只在错误体文本里写明到期时间，如
// "Daily free limit reached ... Try again in 14h 23m"、"retry after 120 seconds"。
// 这类明确到期时间应直接作为冷却时长（往往远长于固定 CD，提前重试只会反复撞
// 429）；上游没给明确时间才回落到配置的 RATE_LIMIT_COOLDOWN。

// relDurPhraseRe 匹配 "in/after/within/wait <时长短语>"，短语由 1~4 段
// "数字+单位" 组成（可含 "and"/逗号，允许无空格紧凑写法 1h23m）。RE2 不支持
// 前瞻，单位后不设边界断言；截断型误匹配（如 "5 months" 只吃掉 "5 m"）由
// bodyRetryAfter 的短语尾字符检查 + parseRelDuration 残余校验兜底。
var relDurPhraseRe = regexp.MustCompile(
	`(?i)\b(?:in|after|within|wait)\s+((?:\d+(?:\.\d+)?\s*(?:weeks?|days?|d|hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)[\s,]*(?:and\s+)?){1,4})`)

// durTokenRe 匹配单个 数字+单位 token（同上不设尾边界）。
var durTokenRe = regexp.MustCompile(
	`(?i)(\d+(?:\.\d+)?)\s*(weeks?|days?|d|hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)`)

// absTimeRe 匹配 ISO-8601 / "YYYY-MM-DD HH:MM(:SS)" 绝对时间戳。
var absTimeRe = regexp.MustCompile(
	`(?i)\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`)

// bodyRetryAfter 从 429 响应体文本解析上游明确给出的重试等待时间：
// 优先相对时长短语，其次绝对时间戳（含 JSON 字段值，无时区按 UTC），
// 仅接受未来时刻；解析不到明确时间返回 false。
func bodyRetryAfter(body string) (time.Duration, bool) {
	if body == "" {
		return 0, false
	}
	// 遍历所有 "in/after ..." 锚点：短语尾字符是单词字符说明匹配被单词截断
	//（如 "5 months" 只吃到 "5 m"），跳过并尝试下一个锚点。检查前先去掉捕获
	// 尾部的空白/逗号，保证 "5m extra" 这类短语后接普通单词仍正常命中。
	for _, loc := range relDurPhraseRe.FindAllStringSubmatchIndex(body, -1) {
		phrase := strings.TrimRight(body[loc[2]:loc[3]], " \t\r\n,")
		if end := loc[2] + len(phrase); end < len(body) && isWordByte(body[end]) {
			continue
		}
		if d, ok := parseRelDuration(phrase); ok {
			return d, true
		}
	}
	if m := absTimeRe.FindString(body); m != "" {
		// 匹配串只含数字/-/:/./t/T/z/Z/+-，统一大写 T/Z 后按多布局尝试
		normalized := strings.NewReplacer("t", "T", "z", "Z").Replace(m)
		for _, layout := range []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02T15:04Z07:00",
			"2006-01-02T15:04",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
		} {
			if t, err := time.Parse(layout, normalized); err == nil {
				if d := time.Until(t); d > 0 {
					return d, true
				}
				break // 已是过去时刻：不当作冷却依据
			}
		}
	}
	return 0, false
}

// isWordByte ASCII 单词字符（字母/数字/下划线）判断，用于时长短语尾部截断检查。
func isWordByte(c byte) bool {
	return c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// parseRelDuration 把 "14h 23m" / "5 minutes and 30 seconds" 类短语折算成时长；
// 短语剔除 token 后只允许残留空白/逗号/"and"，否则视为误匹配返回 false。
func parseRelDuration(phrase string) (time.Duration, bool) {
	tokens := durTokenRe.FindAllStringSubmatch(phrase, -1)
	if len(tokens) == 0 {
		return 0, false
	}
	rest := strings.TrimSpace(strings.ToLower(durTokenRe.ReplaceAllString(phrase, "")))
	rest = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(rest, ",", ""), "and", ""))
	if rest != "" {
		return 0, false
	}
	var total time.Duration
	for _, tk := range tokens {
		v, err := strconv.ParseFloat(tk[1], 64)
		if err != nil || v <= 0 {
			return 0, false
		}
		total += time.Duration(v * unitSeconds(tk[2]) * float64(time.Second))
	}
	if total <= 0 {
		return 0, false
	}
	return total, true
}

// unitSeconds 单位 → 秒（token 词表受 durTokenRe 约束，前缀判断无歧义；
// "m" 在限流文案语境按分钟理解）。
func unitSeconds(u string) float64 {
	u = strings.ToLower(u)
	switch {
	case u == "w" || strings.HasPrefix(u, "week"):
		return 7 * 24 * 3600
	case u == "d" || strings.HasPrefix(u, "day"):
		return 24 * 3600
	case strings.HasPrefix(u, "h"): // h / hr / hrs / hour / hours
		return 3600
	case strings.HasPrefix(u, "m"): // m / min / mins / minute / minutes
		return 60
	default:
		return 1
	}
}
