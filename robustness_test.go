// 「服务器越来越卡」类问题的回归测试：内存表有界增长、上游失联兜底、
// transport 缓存淘汰、secret 读取缓存。
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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

// ---- 配置视图：不可变发布 + 零拷贝读取 ----

// TestStoreViewSharedSnapshotCopied 已发布配置不可变且零拷贝共享：
//   - View() 多次调用返回同一对象（热路径不再每请求 JSON 深拷贝全量配置）；
//   - Put 之后调用方继续修改自己持有的对象不影响运行中的配置（写入克隆）；
//   - Snapshot() 是深拷贝副本，管理端「取副本 → 改 → 写回」的流程不受影响。
func TestStoreViewSharedSnapshotCopied(t *testing.T) {
	setupGateway(t)
	ch := &Channel{Name: "c1", BaseURL: "https://up.example", Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}}
	if err := store.PutChannel(ch); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}

	if v1, v2 := store.View(), store.View(); v1 != v2 {
		t.Fatal("View must return the same published config object (zero copy)")
	}

	// 调用方继续改自己持有的对象：已发布配置不得被污染
	ch.Keys[0].Name = "mutated-after-put"
	if got := store.View().Channels[0].Keys[0].Name; got != "k1" {
		t.Fatalf("published config polluted by caller mutation: %q", got)
	}
	ch.Name = "mutated"
	if got := store.View().Channels[0].Name; got != "c1" {
		t.Fatalf("published channel polluted by caller mutation: %q", got)
	}

	// Snapshot 是副本：改副本不影响 View；副本写回（管理端流程）生效
	snap := store.Snapshot()
	snap.Channels[0].Name = "edited"
	if got := store.View().Channels[0].Name; got != "c1" {
		t.Fatalf("Snapshot must be a deep copy, published name = %q", got)
	}
	snap.Channels[0].Models = []string{"m1"}
	if err := store.PutChannel(snap.Channels[0]); err != nil {
		t.Fatalf("PutChannel from snapshot: %v", err)
	}
	if got := store.View().Channels[0].Models; len(got) != 1 || got[0] != "m1" {
		t.Fatalf("put-back from snapshot lost, models = %+v", got)
	}
	// 写回后副本对象仍与存储脱钩
	snap.Channels[0].Models = []string{"m2"}
	if got := store.View().Channels[0].Models; len(got) != 1 || got[0] != "m1" {
		t.Fatalf("put-back must clone, models = %+v", got)
	}
}

// ---- 冷却落盘：锁外写盘且并发不丢条目 ----

// TestCooldownPersistConcurrentMarks 并发记冷却（429 风暴）时不得丢条目：
// 落盘在 mu 之外进行并按变更代号去重，旧快照绝不覆盖更新的状态；Mark 返回
// 时该笔冷却必须已落盘（重启恢复语义不变）。
func TestCooldownPersistConcurrentMarks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cooldowns.json")
	c := newCooldowns()
	if err := c.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Mark(fmt.Sprintf("k%d", i), "", time.Hour)
		}(i)
	}
	wg.Wait()

	// 模拟重启：全部冷却必须都在
	c2 := newCooldowns()
	if err := c2.SetPersistPath(path); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := len(c2.CoolingList()); got != n {
		t.Fatalf("persisted cooldowns = %d, want %d (entries lost under concurrency)", got, n)
	}

	// 单笔 Mark 返回后立即可被新实例看到（同步落盘语义）
	c3 := newCooldowns()
	_ = c3.SetPersistPath(path)
	c3.Mark("solo", "m1", 30*time.Minute)
	c4 := newCooldowns()
	if err := c4.SetPersistPath(path); err != nil {
		t.Fatalf("reload after solo mark: %v", err)
	}
	if !c4.IsCooling("solo", "m1") {
		t.Fatal("Mark must be durable before returning")
	}
}

// TestCooldownIsCoolingNotBlockedByPersist 冷却检查不得等待落盘：persist 在
// mu 之外执行，IsCooling 只受内存状态保护。
func TestCooldownIsCoolingNotBlockedByPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cooldowns.json")
	c := newCooldowns()
	if err := c.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	c.Mark("k1", "", time.Hour)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			c.Mark(fmt.Sprintf("burst-%d", i), "", time.Hour)
		}
	}()
	// 落盘风暴期间冷却查询必须保持可用（旧实现持 mu 做磁盘 IO，会被堵住）
	for i := 0; i < 5000; i++ {
		if !c.IsCooling("k1", "") {
			t.Fatal("existing cooldown must stay visible")
		}
	}
	<-done
}

// ---- SSE 帧上限 ----

// TestReadFrameSizeLimit 未结束的超长帧必须在上限处中止（此前无限累积，
// 单连接就能吃光内存）。
func TestReadFrameSizeLimit(t *testing.T) {
	old := maxSSEFrameBytes
	maxSSEFrameBytes = 4096
	t.Cleanup(func() { maxSSEFrameBytes = old })

	br := bufio.NewReader(strings.NewReader("data: " + strings.Repeat("x", 32<<10) + "\n\n"))
	if _, err := readFrame(br); err != errSSEFrameTooLarge {
		t.Fatalf("readFrame err = %v, want %v", err, errSSEFrameTooLarge)
	}

	// 正常帧不受影响（含跨 bufio 缓冲区的多段行）
	small := bufio.NewReader(strings.NewReader("data: hi\n\n"))
	frame, err := readFrame(small)
	if err != nil || string(frame) != "data: hi" {
		t.Fatalf("frame=%q err=%v", frame, err)
	}
	big := bufio.NewReader(strings.NewReader("data: " + strings.Repeat("y", 3000) + "\n\n"))
	frame, err = readFrame(big)
	if err != nil || len(frame) != len("data: ")+3000 {
		t.Fatalf("multi-chunk frame len=%d err=%v", len(frame), err)
	}
}

// TestStreamAbortsOnOversizedFrame 上游发超长帧时转发必须中止并返回，
// 而不是无限读下去（帧缓冲无界增长）。
func TestStreamAbortsOnOversizedFrame(t *testing.T) {
	old := maxSSEFrameBytes
	maxSSEFrameBytes = 4096
	t.Cleanup(func() { maxSSEFrameBytes = old })
	setupGateway(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		// 一条永不换行的超长「行」
		_, _ = io.WriteString(w, "data: "+strings.Repeat("x", 64<<10))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(200 * time.Millisecond)
	}))
	t.Cleanup(up.Close)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		rr := httptest.NewRecorder()
		forwardChat(rr, chatRequest("m1"), nil, true, "m1")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("oversized frame must abort the stream promptly")
	}
}

// ---- 下游停读兜底（写超时） ----

// TestDownstreamWriteStallAborts 客户端保持连接但完全不读响应时必须中止请求：
// 每次下游写出武装写超时，写阻塞超时即结束转发，释放 goroutine 与上下游连接
// （旧实现无下游写侧保护，此类客户端会让请求永久挂住）。
func TestDownstreamWriteStallAborts(t *testing.T) {
	t.Setenv("DOWNSTREAM_WRITE_TIMEOUT", "200ms")
	setupGateway(t)
	if err := store.PutGWKey(&GWKey{Name: "t", Key: "sk-gw-test", Enabled: true}); err != nil {
		t.Fatalf("PutGWKey: %v", err)
	}

	// 上游持续产出（远超内核缓冲），网关只能一直往不读的客户端写
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f, _ := w.(http.Flusher)
		chunk := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"" +
			strings.Repeat("x", 4000) + "\"}}]}\n\n")
		for i := 0; i < 4000; i++ { // ~16MB
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if f != nil {
				f.Flush()
			}
		}
	}))
	t.Cleanup(up.Close)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	handlerDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		statsMiddleware(rootHandler)(w, r)
	}))
	t.Cleanup(srv.Close)

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	body := `{"model":"m1","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := fmt.Sprintf("POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\n"+
		"Authorization: Bearer sk-gw-test\r\nContent-Type: application/json\r\n"+
		"Content-Length: %d\r\n\r\n%s", srv.Listener.Addr().String(), len(body), body)
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	// 此后完全不读：请求必须在写超时附近结束

	select {
	case <-handlerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("stalled downstream must abort near the write timeout, handler still blocked")
	}
}
