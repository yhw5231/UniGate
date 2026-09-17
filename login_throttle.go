// 登录防爆破：对连续失败的登录按用户名与来源 IP 分别计数，达到阈值后锁定
// 一个退避窗口，窗口时长随失败次数指数增长（封顶 1h），防止对默认弱口令 /
// 常见账号的暴力撞库。
//
// 状态为纯内存（重启即清零）：锁定是限速手段而非审计留痕，无需持久化；
// 真正需要长期保留的是失败日志。
package main

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// loginLockout 单个计数键的状态：窗口起点、窗口内失败次数、锁定到期时间。
type loginLockout struct {
	windowAt time.Time // 当前计数窗口起点
	fails    int       // 窗口内失败次数
	until    time.Time // 锁定到期（零值 = 未锁定）
}

// loginThrottleMax 防爆破表条目软阈值：越过时做一次维护（过期清理 + 必要时
// 随机逐出到一半）。公网暴露时撞库者常伪造 X-Real-IP 轮换来源 IP，每个新
// IP 都建条目且无后续查询触发惰性清理——不设上限的话表随时间无限增长。
// 逐出到一半（而非逐出一条）保证维护以摊销 O(1)/写入 的成本触发；逐出
// 等价于重置该来源的失败计数（放宽限流），安全上是可接受的精度损失。
const loginThrottleMax = 10000

// loginThrottle 登录防爆破表。
type loginThrottle struct {
	mu    sync.Mutex
	state map[string]*loginLockout
}

var loginGuard = &loginThrottle{state: map[string]*loginLockout{}}

// resetLoginThrottle 清空全部登录锁定（测试用）。
func resetLoginThrottle() {
	loginGuard.mu.Lock()
	defer loginGuard.mu.Unlock()
	loginGuard.state = map[string]*loginLockout{}
}

// blocked 返回某计数键的锁定剩余时长（未锁定为 0），顺带清理过期条目。
func (t *loginThrottle) blocked(key string) time.Duration {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.state[key]
	if e == nil {
		return 0
	}
	if now.Before(e.until) {
		return e.until.Sub(now)
	}
	if e.windowAt.Add(cfg.LoginFailWindow).Before(now) {
		delete(t.state, key)
	}
	return 0
}

// fail 记一次失败；达到阈值后按超额次数指数退避锁定（基础 = 窗口时长，封顶 1h）。
func (t *loginThrottle) fail(key string) {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.state[key]
	if e == nil {
		e = &loginLockout{windowAt: now}
		t.state[key] = e
	} else if e.windowAt.Add(cfg.LoginFailWindow).Before(now) {
		e.windowAt, e.fails = now, 0 // 窗口过期：重新开始计数
	}
	e.fails++
	if cfg.LoginFailLockout <= 0 || e.fails < cfg.LoginFailLockout {
		return
	}
	excess := e.fails - cfg.LoginFailLockout
	if excess > 8 {
		excess = 8 // 避免移位溢出；8 次翻倍已远超 1h 封顶
	}
	d := cfg.LoginFailWindow << uint(excess)
	if d > time.Hour {
		d = time.Hour
	}
	e.until = now.Add(d)
	// 防膨胀（见 loginThrottleMax）：清理过期条目，仍超则随机逐出到一半
	if len(t.state) <= loginThrottleMax {
		return
	}
	t.sweepLocked(now)
	if n := len(t.state); n > loginThrottleMax {
		target := n / 2
		for k := range t.state {
			if len(t.state) <= target {
				break
			}
			delete(t.state, k)
		}
	}
}

// sweepLocked 清理已过期（既未锁定、计数窗口也结束）的条目（调用方持锁）。
func (t *loginThrottle) sweepLocked(now time.Time) {
	for k, e := range t.state {
		if now.After(e.until) && e.windowAt.Add(cfg.LoginFailWindow).Before(now) {
			delete(t.state, k)
		}
	}
}

// ok 清除计数（登录成功）。
func (t *loginThrottle) ok(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.state, key)
}

// loginKeys 计数键：用户名 + 来源 IP。前者防针对单一账号的撞库，
// 后者防同一来源的多账号枚举。
func loginKeys(user, ip string) []string {
	keys := make([]string, 0, 2)
	if user != "" {
		keys = append(keys, "u:"+user)
	}
	if ip != "" {
		keys = append(keys, "ip:"+ip)
	}
	return keys
}

// loginBlocked 返回 (是否锁定, 剩余时长)。锁定期间调用方应直接回 429。
func loginBlocked(user, ip string) (bool, time.Duration) {
	if cfg.LoginFailLockout <= 0 {
		return false, 0
	}
	var worst time.Duration
	for _, key := range loginKeys(user, ip) {
		if d := loginGuard.blocked(key); d > worst {
			worst = d
		}
	}
	return worst > 0, worst
}

// loginFailed 记录一次失败登录（用户名 + IP 两个键都计数）。
func loginFailed(user, ip string) {
	if cfg.LoginFailLockout <= 0 {
		return
	}
	for _, key := range loginKeys(user, ip) {
		loginGuard.fail(key)
	}
}

// loginSucceeded 清除该用户名与 IP 的失败计数。
func loginSucceeded(user, ip string) {
	for _, key := range loginKeys(user, ip) {
		loginGuard.ok(key)
	}
}

// writeLoginThrottled 回 429 + Retry-After（秒）。
func writeLoginThrottled(w http.ResponseWriter, retryIn time.Duration) {
	if retryIn <= 0 {
		retryIn = cfg.LoginFailWindow
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(retryIn.Seconds())+1))
	writeJSONError(w, http.StatusTooManyRequests,
		"too many failed login attempts, retry later", "rate_limited")
}
