// 「服务器越来越卡」类问题的回归测试：内存表有界增长、上游失联兜底、
// transport 缓存淘汰、secret 读取缓存。
package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---- 上游读取静默超时 ----

// TestUpstreamReadIdleAbortsStalledStream 上游发出首帧后静默挂死（TCP 半开
// 模拟）：读取静默超时必须中断转发并结束请求，而不是永久阻塞（goroutine
// 与连接泄漏）。此前行为：读取无限阻塞，请求对永不释放。
func TestUpstreamReadIdleAbortsStalledStream(t *testing.T) {
	t.Setenv("UPSTREAM_READ_IDLE_TIMEOUT", "80ms")
	setupGateway(t)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// 模拟失联：不发任何字节也不关闭连接（用长睡眠占住连接）
		time.Sleep(500 * time.Millisecond)
		close(done)
	}))
	t.Cleanup(srv.Close)

	mustPutChannel(t, &Channel{Name: "c", BaseURL: srv.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	start := time.Now()
	forwardChat(rr, chatRequestStream("m"), nil, true, "m")
	elapsed := time.Since(start)
	if elapsed >= 400*time.Millisecond {
		t.Fatalf("stalled stream must abort near the idle timeout (80ms), took %s", elapsed)
	}
	if !strings.Contains(rr.Body.String(), `"content":"x"`) {
		t.Fatalf("first frame must still be delivered, got %s", rr.Body.String())
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream handler should have completed")
	}
}

func chatRequestStream(model string) *http.Request {
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":true}`, model)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	return req
}

// TestIdleTimeoutReaderCloseStopsWatchdog 读完成（EOF）后看门狗不应再触发。
func TestIdleTimeoutReaderCloseStopsWatchdog(t *testing.T) {
	rc := io.NopCloser(strings.NewReader("hello"))
	r := &idleTimeoutReader{rc: rc, idle: 30 * time.Millisecond}
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("read = %q, err=%v", buf[:n], err)
	}
	if _, err := r.Read(buf); err != io.EOF {
		t.Fatalf("second read err = %v, want EOF", err)
	}
	time.Sleep(60 * time.Millisecond) // 超过 idle：确认无 panic/副作用（EOF 已返回）
}

// TestReadFrameAfterIdleAbort 帧读取在读取错误时正常结束（配合失联中断）。
func TestReadFrameAfterIdleAbort(t *testing.T) {
	br := bufio.NewReader(strings.NewReader("data: x\n\n"))
	frame, err := readFrame(br)
	if err != nil || string(frame) != "data: x" {
		t.Fatalf("frame=%q err=%v", frame, err)
	}
	if _, err := readFrame(br); err != io.EOF {
		t.Fatalf("EOF expected, got %v", err)
	}
}

// ---- 内存表有界 ----

// TestLoginThrottleBounded 持续撞库（伪造来源 IP 轮换）时防爆破表必须有界。
func TestLoginThrottleBounded(t *testing.T) {
	resetLoginThrottle()
	t.Cleanup(resetLoginThrottle)
	for i := 0; i < loginThrottleMax*4; i++ {
		loginFailed("victim", fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256))
	}
	loginGuard.mu.Lock()
	n := len(loginGuard.state)
	loginGuard.mu.Unlock()
	if n > 3*loginThrottleMax {
		t.Fatalf("throttle table size = %d, must stay bounded near %d", n, loginThrottleMax)
	}
}

// TestActivityBounded 海量不同模型名的请求下 activity 计时线必须有界。
func TestActivityBounded(t *testing.T) {
	a := newKeyActivity()
	for i := 0; i < activityMaxEntries*2; i++ {
		a.note("k1", fmt.Sprintf("model-%d", i))
	}
	a.mu.Lock()
	n := len(a.last)
	a.mu.Unlock()
	if n > activityMaxEntries {
		t.Fatalf("activity table size = %d, must stay under cap %d", n, activityMaxEntries)
	}
}

// TestCooldownsBounded 大量未过期冷却条目（超长 Retry-After 文案）下表必须有界。
func TestCooldownsBounded(t *testing.T) {
	c := newCooldowns()
	for i := 0; i < cooldownSoftCap*4; i++ {
		c.Mark(fmt.Sprintf("k%d", i), fmt.Sprintf("m%d", i), time.Hour)
	}
	c.mu.Lock()
	n := len(c.until)
	c.mu.Unlock()
	if n > 3*cooldownSoftCap {
		t.Fatalf("cooldown table size = %d, must stay bounded near %d", n, cooldownSoftCap)
	}
}

// TestTransportCacheEviction 代理路由缓存按上限淘汰（模拟租约轮换生成无限
// 多路由），淘汰后缓存不超上限。
func TestTransportCacheEviction(t *testing.T) {
	tc := &transportCache{trs: map[string]*http.Transport{}, max: 8}
	for i := 0; i < 100; i++ {
		tc.get(&ProxyRoute{Kind: "socks5", Addr: fmt.Sprintf("127.0.0.1:%d", i), User: fmt.Sprintf("user:%d", i)})
	}
	tc.mu.Lock()
	n := len(tc.trs)
	tc.mu.Unlock()
	if n > 8 {
		t.Fatalf("transport cache size = %d, want <= 8", n)
	}
	// 最近使用的条目应仍在缓存（LRU 命中路径）
	if tc.get(&ProxyRoute{Kind: "socks5", Addr: "127.0.0.1:99", User: "user:99"}) == nil {
		t.Fatal("recent entry must be cached")
	}
}

// TestSecretCached 不设 TOKEN_SECRET 时走文件密钥并缓存（多次调用结果一致，
// 不再每次读盘）。
func TestSecretCached(t *testing.T) {
	t.Setenv("TOKEN_SECRET", "")
	resetCfgForTest()
	s1, s2 := secret(), secret()
	if s1 == "" || s1 != s2 {
		t.Fatalf("secret must be stable, got %q vs %q", s1, s2)
	}
}
