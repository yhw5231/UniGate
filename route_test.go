// 路由引擎测试：故障转移（429→下一 key）、冷却跳过、鉴权失败冷却、
// ipv6pool 换 IP 联动（经真实 SOCKS5 stub 隧道）、reasoning 改写开关、模型过滤。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- 测试基建 ----

func setupGateway(t *testing.T) {
	t.Helper()
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("USAGE_DB_PATH", "") // :memory:
	t.Setenv("GW_KEY_AUTH", "true")
	t.Setenv("RATE_LIMIT_COOLDOWN", "60s")
	t.Setenv("REQ_LOG_SIZE", "100")
	resetCfgForTest()
	store = newGatewayStore(filepath.Join(t.TempDir(), "gateway.json"))
	if err := store.load(); err != nil {
		t.Fatalf("store load: %v", err)
	}
	leaseMgr = newLeaseManager()
	cool.ClearAll()
}

func mustPutChannel(t *testing.T, ch *Channel) {
	t.Helper()
	if err := store.PutChannel(ch); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}
}

func chatRequest(model string) *http.Request {
	body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}],"stream":false}`, model)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	return req
}

// upstreamRecorder 记录收到的 Authorization 与请求次数。
type upstreamRecorder struct {
	mu     sync.Mutex
	calls  int
	auths  []string
	status int
	body   string
	srv    *httptest.Server
	URL    string
}

func newUpstream(t *testing.T, status int, body string) *upstreamRecorder {
	t.Helper()
	u := &upstreamRecorder{status: status, body: body}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.calls++
		u.auths = append(u.auths, r.Header.Get("Authorization"))
		st, b := u.status, u.body
		u.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(st)
		_, _ = io.WriteString(w, b)
	}))
	u.URL = u.srv.URL
	t.Cleanup(u.srv.Close)
	return u
}

func (u *upstreamRecorder) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func (u *upstreamRecorder) lastAuth() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.auths) == 0 {
		return ""
	}
	return u.auths[len(u.auths)-1]
}

// setStatus 动态切换上游返回状态码（模拟故障恢复）。
func (u *upstreamRecorder) setStatus(status int) {
	u.mu.Lock()
	u.status = status
	u.mu.Unlock()
}

// fakeSocks5 最小 SOCKS5 服务端：no-auth 握手 + CONNECT 后转发到真实目标。
func startFakeSocks5(t *testing.T) string {
	addr, _ := startFakeSocks5Opts(t, 0)
	return addr
}

// fakeSocksState fake SOCKS5 的可观测/可配置状态。
type fakeSocksState struct {
	mu       sync.Mutex
	connects int // 收到的 CONNECT 请求数
	refuseN  int // 前 N 次 CONNECT 直接回 rep 0x05（模拟坏出口被拒）
}

func (st *fakeSocksState) count() int {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.connects
}

// startFakeSocks5Opts 带状态的 fake SOCKS5：refuseN>0 时前 N 次 CONNECT 返回
// rep 0x05 connection refused（模拟代理出口连目标被拒，如被目标封禁）。
func startFakeSocks5Opts(t *testing.T, refuseN int) (string, *fakeSocksState) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("socks5 listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	st := &fakeSocksState{refuseN: refuseN}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleFakeSocks5State(conn, st)
		}
	}()
	return ln.Addr().String(), st
}

func handleFakeSocks5State(conn net.Conn, st *fakeSocksState) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	// 握手：VER + NMETHODS + METHODS
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(br, methods); err != nil {
		return
	}
	if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
		return
	}
	// CONNECT 请求（复用被测代码的地址解析）
	ver := make([]byte, 4)
	if _, err := io.ReadFull(br, ver); err != nil {
		return
	}
	host, err := readSocksAddr(br, ver[3])
	if err != nil {
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(br, portBytes); err != nil {
		return
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	target := net.JoinHostPort(host, strconv.Itoa(port))
	if st != nil {
		st.mu.Lock()
		st.connects++
		refuse := st.connects <= st.refuseN
		st.mu.Unlock()
		if refuse {
			// rep 0x05 connection refused：模拟代理出口连目标被拒
			_, _ = conn.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			return
		}
	}
	up, err := net.Dial("tcp", target)
	if err != nil {
		conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer up.Close()
	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	go io.Copy(up, br)
	io.Copy(conn, up)
}

// ---- 故障转移 ----

func TestFailoverOn429(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusTooManyRequests, `{"error":"rate limited"}`)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.srv.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if up1.count() != 1 || up2.count() != 1 {
		t.Fatalf("calls: up1=%d up2=%d", up1.count(), up2.count())
	}
	if up2.lastAuth() != "Bearer sk-2" {
		t.Fatalf("upstream auth = %q", up2.lastAuth())
	}
	// k1 进入冷却，下一次请求直接打 up2
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	if up1.count() != 1 {
		t.Fatalf("cooling key should be skipped, up1 calls = %d", up1.count())
	}
}

func TestAllRateLimitedReturns429(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusTooManyRequests, `err`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.srv.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
}

// TestAuthFailNoCooldownJustFailover：401/403 不冷却 key，只故障转移到下一个 key；
// 后续请求仍会先尝试该 key（不因鉴权失败被跳过）。
func TestAuthFailNoCooldownJustFailover(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusUnauthorized, `{"error":"bad key"}`)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.srv.URL, Enabled: true, CooldownScope: "key_model",
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})
	k1 := store.Snapshot().Channels[0].Keys[0].ID

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if up2.count() != 1 {
		t.Fatalf("expected failover to up2, calls=%d", up2.count())
	}
	if cool.IsCooling(k1, "m1") {
		t.Fatal("401 must not mark cooldown")
	}

	// 不冷却：下一次请求仍先试 k1（仍 401）再切换 k2
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	if up1.count() != 2 || up2.count() != 2 {
		t.Fatalf("auth-failed key must be retried next request: up1=%d up2=%d", up1.count(), up2.count())
	}
}

func TestNetworkErrorFailover(t *testing.T) {
	setupGateway(t)
	// 关闭的服务器 → 网络错误
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: deadURL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if up2.count() != 1 {
		t.Fatalf("expected failover to up2, calls=%d", up2.count())
	}
}

func TestModelFilterSkipsChannel(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true, Models: []string{"gpt-4o-mini"},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("claude-3"), nil, false, "claude-3")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (channel filtered)", rr.Code)
	}
	if up.count() != 0 {
		t.Fatal("filtered channel must not be called")
	}
}

func TestDisabledChannelAndKeySkipped(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: false,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	mustPutChannel(t, &Channel{Name: "c2", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: false}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m"), nil, false, "m")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if up.count() != 0 {
		t.Fatal("disabled channel/key must not be called")
	}
}

// ---- reasoning 改写 ----

func TestRewriteToggle(t *testing.T) {
	setupGateway(t)
	streamBody := "data: {\"choices\":[{\"delta\":{\"reasoning\":\"think\"}}]}\n\n" +
		"data: [DONE]\n\n"

	up1 := newUpstreamStream(t, streamBody)
	mustPutChannel(t, &Channel{Name: "rw", BaseURL: up1.URL, Enabled: true, Rewrite: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m"), nil, true, "m")
	if !strings.Contains(rr.Body.String(), `"reasoning_content":"think"`) {
		t.Fatalf("expected rewrite, got %s", rr.Body.String())
	}

	// 禁用第一个渠道，避免第二个请求仍被 rw 渠道服务
	snap := store.Snapshot()
	rw := snap.Channels[0]
	rw.Enabled = false
	_ = store.PutChannel(rw)

	up2 := newUpstreamStream(t, streamBody)
	mustPutChannel(t, &Channel{Name: "norw", BaseURL: up2.URL, Enabled: true, Rewrite: false,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m"), nil, true, "m")
	if strings.Contains(rr2.Body.String(), "reasoning_content") {
		t.Fatalf("rewrite must be off for this channel, got %s", rr2.Body.String())
	}
}

func newUpstreamStream(t *testing.T, sse string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, sse)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// ---- ipv6pool 联动：真实 SOCKS5 stub + fake pool ----

func TestIPv6PoolProxyRouting(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"via-socks"}}]}`)
	socksAddr := startFakeSocks5(t)
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL, SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "via-socks") {
		t.Fatalf("expected response via socks tunnel: %s", rr.Body.String())
	}
	if up.count() != 1 {
		t.Fatalf("upstream calls = %d", up.count())
	}
}

// TestNetErrRotatesEgress：网络/代理错误（如出口连不上上游）只换 key 不冷却，
// 且立即自动换出口 IP。
func TestNetErrRotatesEgress(t *testing.T) {
	setupGateway(t)
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	socksAddr, socks := startFakeSocks5Opts(t, 0)
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	mustPutChannel(t, &Channel{Name: "c", BaseURL: deadURL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
				SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502, body=%s", rr.Code, rr.Body.String())
	}
	pool.mu.Lock()
	n := pool.rotateN
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("network error must rotate egress, got %d", n)
	}
	if cool.IsCooling(kid, "") {
		t.Fatal("network error must not mark cooldown")
	}
	// 换出口后同 key 恰好重试一次（首次 CONNECT 被拒 + 重试），不多不少
	if c := socks.count(); c != 2 {
		t.Fatalf("socks connects = %d, want 2 (first attempt + one rotate retry)", c)
	}
}

// TestNetErrEgressRetrySameKey：ipv6pool 候选首次请求因出口被拒（rep 0x05）
// 失败时，换出口 IP 后同 key 原地重试并成功——单 key 渠道一次请求内自愈，
// 不必等下一次请求才用上新出口。
func TestNetErrEgressRetrySameKey(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	socksAddr, socks := startFakeSocks5Opts(t, 1) // 第 1 次 CONNECT 回 rep 0x05
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
				SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (success on rotate retry), body=%s", rr.Code, rr.Body.String())
	}
	if up.count() != 1 {
		t.Fatalf("upstream calls = %d, want 1 (only the successful retry reaches it)", up.count())
	}
	pool.mu.Lock()
	n := pool.rotateN
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("rotate count = %d, want 1", n)
	}
	if c := socks.count(); c != 2 {
		t.Fatalf("socks connects = %d, want 2 (refused first + successful retry)", c)
	}
	if cool.IsCooling(store.Snapshot().Channels[0].Keys[0].ID, "") {
		t.Fatal("network error must not mark cooldown")
	}
}

// TestStaticProxyNetErrNoSameKeyRetry：固定代理（static）网络错误不重试同 key
// （固定出口无法更换，重试只会重复失败），直接转移到下一个候选。
func TestStaticProxyNetErrNoSameKeyRetry(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	socksAddr, socks := startFakeSocks5Opts(t, 100) // 全部拒绝

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true,
				Proxy: &ProxySpec{Kind: "static", URL: "socks5://" + socksAddr}},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
		}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (failover to k2), body=%s", rr.Code, rr.Body.String())
	}
	// k1 的固定代理只被尝试一次（不重试），k2 直连成功
	if c := socks.count(); c != 1 {
		t.Fatalf("socks connects = %d, want 1 (static proxy must not retry same key)", c)
	}
	if up.count() != 1 {
		t.Fatalf("upstream calls = %d, want 1", up.count())
	}
}

// Test5xxStreakTriggersRotate：5xx 不冷却只换 key，但按 key 记连续次数
// （成功清零）；连续超过阈值（ROTATE_AFTER_5XX，默认 3，即第 4 次）触发换出口 IP。
func Test5xxStreakTriggersRotate(t *testing.T) {
	setupGateway(t)
	t.Setenv("ROTATE_AFTER_5XX", "3")
	resetCfgForTest()
	up := newUpstream(t, http.StatusInternalServerError, `{"error":"boom"}`)
	socksAddr := startFakeSocks5(t)
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
				SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})

	// 前 3 次 5xx：只换 key 不换出口
	for i := 0; i < 3; i++ {
		rr := httptest.NewRecorder()
		forwardChat(rr, chatRequest("m1"), nil, false, "m1")
		if rr.Code != http.StatusBadGateway {
			t.Fatalf("req %d: status=%d want 502", i, rr.Code)
		}
	}
	pool.mu.Lock()
	n := pool.rotateN
	pool.mu.Unlock()
	if n != 0 {
		t.Fatalf("no rotate before threshold exceeded, got %d", n)
	}

	// 第 4 次连续 5xx（>3）：触发换出口
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	pool.mu.Lock()
	n = pool.rotateN
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected rotate after %d consecutive 5xx, got %d", cfg.RotateAfter5xx, n)
	}

	// 恢复成功：计数清零，后续再连 3 次 5xx 不触发换出口
	up.setStatus(http.StatusOK)
	rrOK := httptest.NewRecorder()
	forwardChat(rrOK, chatRequest("m1"), nil, false, "m1")
	if rrOK.Code != http.StatusOK {
		t.Fatalf("recovery status=%d body=%s", rrOK.Code, rrOK.Body.String())
	}
	up.setStatus(http.StatusInternalServerError)
	for i := 0; i < 3; i++ {
		rr2 := httptest.NewRecorder()
		forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	}
	pool.mu.Lock()
	n = pool.rotateN
	pool.mu.Unlock()
	if n != 1 {
		t.Fatalf("success must reset streak; expected still 1 rotate, got %d", n)
	}
}

// TestChannelLevelPoolProxyInheritance：渠道级代理池设置（key 未单独配置代理）。
// 渠道内每个 key 继承同一池设置但各持独立租约/出口 IP（同设置不同 IP）；
// key 显式配置直连（proxy: {kind:"none"}）时覆盖渠道级代理。
func TestChannelLevelPoolProxyInheritance(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	socksAddr := startFakeSocks5(t)
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort
	// 同一 fake SOCKS 端口服务所有租约；用分配表区分各 key 的租约

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
			SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]},
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
			{Name: "k3", APIKey: "sk-3", Enabled: true, Proxy: &ProxySpec{Kind: "none"}},
		}})

	snap := store.Snapshot().Channels[0]
	// k1、k2 继承渠道级池代理 → 各自独立租约；k3 显式直连不占用租约
	for _, idx := range []int{0, 1, 2} {
		k := snap.Keys[idx]
		route, err := resolveProxy(&candidate{ch: snap, k: k})
		if idx < 2 {
			if err != nil || route == nil || route.Kind != "socks5" {
				t.Fatalf("k%d must resolve pool route, got %v err=%v", idx+1, route, err)
			}
		} else if route != nil {
			t.Fatalf("k3 explicit direct must not use proxy, got %s", route.describe())
		}
	}
	// 池上应有 k1、k2 两个独立租约（k3 显式直连不占用）
	if n := len(pool.leases); n != 2 {
		t.Fatalf("expected 2 pool leases (k1,k2; k3 direct), got %d: %v", n, pool.leases)
	}
	// 继承语义单元检查
	if p := snap.Keys[0].effectiveProxy(snap); p == nil || p.Kind != "ipv6pool" {
		t.Fatalf("k1 must inherit channel pool proxy, got %+v", p)
	}
	// k3 显式直连：proxy 非 nil 且 Kind 归一化为空（normalize 后 "" = 直连）
	if p := snap.Keys[2].effectiveProxy(snap); p == nil || p.Kind != "" {
		t.Fatalf("k3 must override with direct (empty kind), got %+v", p)
	}
	// 端到端：正常转发经池隧道成功
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("forward status=%d body=%s", rr.Code, rr.Body.String())
	}
}

// ---- 下游鉴权 ----

func TestGatewayAuth(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	gw := &GWKey{Name: "downstream", Key: "sk-gw-test123", Enabled: true}
	_ = store.PutGWKey(gw)

	// 无 key → 401
	rr := httptest.NewRecorder()
	req := chatRequest("m")
	rootHandler(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr.Code)
	}
	// 正确 key → 200 且下游 key 记入日志
	rr2 := httptest.NewRecorder()
	req2 := chatRequest("m")
	req2.Header.Set("Authorization", "Bearer sk-gw-test123")
	rootHandler(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr2.Code, rr2.Body.String())
	}
	// 错误 key → 401
	rr3 := httptest.NewRecorder()
	req3 := chatRequest("m")
	req3.Header.Set("Authorization", "Bearer sk-gw-wrong")
	rootHandler(rr3, req3)
	if rr3.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr3.Code)
	}
}

func TestModelsAggregation(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusOK, `{}`)
	mustPutChannel(t, &Channel{Name: "static", BaseURL: up1.srv.URL, Enabled: true, Models: []string{"m-static", "shared"},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	dyn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/models") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"m-dyn"},{"id":"shared"},{"id":"m-dyn2"}]}`))
	}))
	t.Cleanup(dyn.Close)
	mustPutChannel(t, &Channel{Name: "dyn", BaseURL: dyn.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})

	_ = store.PutGWKey(&GWKey{Name: "t", Key: "sk-gw-mt", Enabled: true})
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer sk-gw-mt")
	rr := httptest.NewRecorder()
	rootHandler(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := map[string]bool{}
	for _, m := range out.Data {
		got[m.ID] = true
	}
	for _, want := range []string{"m-static", "m-dyn", "m-dyn2"} {
		if !got[want] {
			t.Fatalf("missing model %q; got %v", want, got)
		}
	}
	if len(out.Data) != 4 { // m-static, shared, m-dyn, m-dyn2（shared 去重）
		t.Fatalf("expected 4 models, got %d: %v", len(out.Data), got)
	}
}

// ---- 冷却表 ----

func TestCooldowns(t *testing.T) {
	c := newCooldowns()
	c.Mark("k", "m", 50*time.Millisecond)
	if !c.IsCooling("k", "m") {
		t.Fatal("expected cooling")
	}
	if c.IsCooling("k", "other") || c.IsCooling("other", "m") {
		t.Fatal("cooldown is per (key, model)")
	}
	time.Sleep(60 * time.Millisecond)
	if c.IsCooling("k", "m") {
		t.Fatal("expected expiry")
	}
}

// TestCooldownScopeDefaultIsKey 默认冷却粒度为按 key：一个模型的故障冷却对该
// key 的所有模型生效（含旧配置空值）；显式 "key_model" 时不同模型互不影响。
func TestCooldownScopeDefaultIsKey(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusTooManyRequests, `{"error":"rate limited"}`)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "bykey", BaseURL: up1.srv.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})

	// m1 触发 429 → k1 按 key 冷却
	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	// 换模型 m2：k1 仍应被跳过（跨模型共享冷却）
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m2"), nil, false, "m2")
	if rr2.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr2.Code, rr2.Body.String())
	}
	if up1.count() != 1 {
		t.Fatalf("default scope=key: m2 must skip cooling k1, up1 calls = %d", up1.count())
	}
}

func TestCooldownScopeKeyModel(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusTooManyRequests, `{"error":"rate limited"}`)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "permodel", BaseURL: up1.srv.URL, Enabled: true, CooldownScope: "key_model",
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	// key_model 粒度：m1 的冷却不影响 k1 对 m2 的可用性 → k1 再次被尝试
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m2"), nil, false, "m2")
	if rr2.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr2.Code, rr2.Body.String())
	}
	if up1.count() != 2 {
		t.Fatalf("scope=key_model: m2 must retry k1, up1 calls = %d", up1.count())
	}
}

// TestClientCancelNoCooldownPoisoning：下游客户端断开/超时导致上游请求被中止时，
// 不得把该 key 记入网络错误冷却（否则后续请求全部跳过健康 key，表现为
// 「渠道测试通过但网关 502」），且不应继续对剩余候选做无谓的故障转移。
func TestClientCancelNoCooldownPoisoning(t *testing.T) {
	setupGateway(t)
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-releaseCh // 挂起直到测试放行，模拟上游慢响应
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	t.Cleanup(release)
	t.Cleanup(up.Close)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID

	// 客户端在 50ms 后取消，上游 1s 内不回包
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	defer cancel()
	req := chatRequest("m1").WithContext(ctx)
	rr := httptest.NewRecorder()
	forwardChat(rr, req, nil, false, "m1")

	if cool.IsCooling(kid, "") {
		t.Fatal("client cancel must not mark upstream key cooldown")
	}

	// 冷却未中毒：同一 key 立即重试应正常命中并成功
	release()
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	if rr2.Code != http.StatusOK {
		t.Fatalf("retry after client cancel: status=%d body=%s", rr2.Code, rr2.Body.String())
	}
}

// TestFailureMessageCarriesUpstreamDetail：上游拒绝时，502 错误体应包含
// 上游状态码与响应体摘要，让「为什么失败」在请求日志/下游侧可直接定位。
func TestFailureMessageCarriesUpstreamDetail(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusInternalServerError, `{"error":{"message":"quota exceeded for org"}}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{"500", "quota exceeded for org", "1 attempted"} {
		if !strings.Contains(body, want) {
			t.Fatalf("502 body missing %q: %s", want, body)
		}
	}
}

// TestAllCoolingPiercesEarliest：全部 key 因上一轮 429 冷却时，不再硬 502，
// 而是对最早到期的 key 穿透试探一次——上游往往已恢复，试探成功即解除冷却
// 自愈（修复「渠道测试可用、网关却持续 502」的错位）。
func TestAllCoolingPiercesEarliest(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	kid := store.Snapshot().Channels[0].Keys[0].ID
	cool.Mark(kid, "", time.Minute)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 (cooldown pierced), body=%s", rr.Code, rr.Body.String())
	}
	if up.count() != 1 {
		t.Fatalf("pierced key must be attempted, calls=%d", up.count())
	}
	if cool.IsCooling(kid, "") {
		t.Fatal("successful pierce must clear stale cooldown")
	}

	// 冷却已解除：后续请求恢复正常路由，不再需要穿透
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	if rr2.Code != http.StatusOK || up.count() != 2 {
		t.Fatalf("after self-heal: status=%d calls=%d", rr2.Code, up.count())
	}
}

// TestAllCoolingPierceFailsKeepsCooldown：穿透试探仍失败时冷却保持，
// 错误体附完整逐 key 轨迹与 Retry-After 提示。
func TestAllCoolingPierceFailsKeepsCooldown(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusTooManyRequests, `{"error":"still limited"}`)
	up2 := newUpstream(t, http.StatusTooManyRequests, `{"error":"still limited 2"}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.srv.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})
	k1 := store.Snapshot().Channels[0].Keys[0].ID
	k2 := store.Snapshot().Channels[0].Keys[1].ID
	// k1 已有存量冷却且更早到期 → k1 被穿透，k2 冷却跳过
	cool.Mark(k1, "", 2*time.Minute)
	cool.Mark(k2, "", 5*time.Minute)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status=%d want 429, body=%s", rr.Code, rr.Body.String())
	}
	if up1.count() != 1 || up2.count() != 0 {
		t.Fatalf("pierce should hit earliest k1 only: up1=%d up2=%d", up1.count(), up2.count())
	}
	if !cool.IsCooling(k1, "") {
		t.Fatal("failed pierce must keep cooldown")
	}
	body := rr.Body.String()
	for _, want := range []string{"2 upstream key(s)", "cooldown_pierced", "cooldown_skipped", "still limited"} {
		if !strings.Contains(body, want) {
			t.Fatalf("429 body missing %q: %s", want, body)
		}
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After hint when pierce fails")
	}
}

// TestSettingsOverrideRoutePolicy：WebUI 设置（gateway.json settings）应覆盖
// 环境变量默认值并即时生效——max_route_tries=1 时首个 key 失败即 502，
// 恢复默认（0=全部）后故障转移到第二个 key 成功。
func TestSettingsOverrideRoutePolicy(t *testing.T) {
	setupGateway(t)
	up1 := newUpstream(t, http.StatusInternalServerError, `{"error":"boom"}`)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up1.srv.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})

	tries := 1
	if err := store.PutSettings(&GatewaySettings{MaxRouteTries: &tries}); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())
	if p := currentPolicy(); p.MaxRouteTries != 1 {
		t.Fatalf("policy max_route_tries = %d, want 1", p.MaxRouteTries)
	}

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("max_route_tries=1: status=%d want 502", rr.Code)
	}
	if up2.count() != 0 {
		t.Fatalf("max_route_tries=1 must not reach k2, calls=%d", up2.count())
	}

	// 设置持久化：重新加载 gateway.json 仍读得到（在恢复默认之前检查）
	path := store.path
	store2 := newGatewayStore(path)
	if err := store2.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := store2.Settings().MaxRouteTries; got == nil || *got != 1 {
		t.Fatalf("settings not persisted, got %v", got)
	}

	// 恢复默认（全量替换、字段缺省）：故障转移恢复
	if err := store.PutSettings(&GatewaySettings{}); err != nil {
		t.Fatalf("PutSettings(reset): %v", err)
	}
	applySettings(store.Settings())
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m1"), nil, false, "m1")
	if rr2.Code != http.StatusOK {
		t.Fatalf("after reset: status=%d body=%s", rr2.Code, rr2.Body.String())
	}
}

// 防止 import 未用告警的引用点
var (
	_ = context.Background
	_ = os.Getenv
	_ = json.Marshal
)
