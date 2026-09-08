// 冷却表：按 (上游 keyID, model 部分) 记录故障冷却，路由引擎据此跳过不可用 key。
// model 部分由渠道级冷却粒度开关决定：默认按 key 跨模型共享（传空串），
// 渠道显式 "key_model" 时按 (key, model) 独立冷却。
// 目前只有上游 429 记冷却：按 Retry-After（缺省 RATE_LIMIT_COOLDOWN）；
// 其余故障（401/403、5xx、网络错误）只做故障转移，不冷却 key。
package main

import (
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

// ClearAll 清空（测试用）。
func (c *Cooldowns) ClearAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.until = map[cooldownPair]time.Time{}
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
