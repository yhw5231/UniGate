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

// ---- 流式保活（heartbeat）----

func setKeepalive(t *testing.T, d time.Duration) {
	t.Helper()
	p := currentPolicy()
	p.KeepaliveInterval = d
	policy.Store(p)
}

// slowSSEUpstream 延迟 delay 后才回首包并写一帧 SSE 的上游（模拟模型排队）。
func slowSSEUpstream(t *testing.T, delay time.Duration, content string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\""+content+"\"}}]}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestStreamKeepaliveDuringSlowUpstream：上游首包慢于心跳间隔时，下游先持续
// 收到 ": keepalive" 注释帧（空闲超时不触发），上游出包后真实内容照常透传。
// 这正是「测试可用但下游 60s 超时」场景的修复。
func TestStreamKeepaliveDuringSlowUpstream(t *testing.T) {
	setupGateway(t)
	slow := slowSSEUpstream(t, 500*time.Millisecond, "slow ok")
	mustPutChannel(t, &Channel{Name: "c", BaseURL: slow.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	setKeepalive(t, 100*time.Millisecond)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, true, "m1")
	body := rr.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("expected heartbeat frames, body=%q", body)
	}
	if ka, data := strings.Index(body, ": keepalive"), strings.Index(body, "slow ok"); data < 0 || ka > data {
		t.Fatalf("keepalive must precede content: ka=%d data=%d body=%q", ka, data, body)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, body)
	}
}

// TestStreamKeepaliveFailoverAfterCommit：心跳提交 200 流之后上游才报错
// （500），网关继续把剩余 key 试完；健康 key 的响应经同一条已提交的流透传，
// 下游只看到 keepalive 注释 + 正常内容。
func TestStreamKeepaliveFailoverAfterCommit(t *testing.T) {
	setupGateway(t)
	slow500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"boom"}`)
	}))
	t.Cleanup(slow500.Close)
	up2 := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok2"}}]}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: slow500.URL, Enabled: true,
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true, BaseURL: up2.srv.URL},
		}})
	setKeepalive(t, 100*time.Millisecond)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, true, "m1")
	body := rr.Body.String()
	if !strings.Contains(body, ": keepalive") || !strings.Contains(body, "ok2") {
		t.Fatalf("expected keepalive + failover content, body=%q", body)
	}
	if up2.count() != 1 {
		t.Fatalf("healthy key should serve after commit, calls=%d", up2.count())
	}
}

// TestStreamKeepaliveAllFailErrorFrame：心跳提交后所有候选都失败——失败不能
// 再用 JSON 错误体表达（200 头已发出），须以流内 error 帧 + [DONE] 收尾。
func TestStreamKeepaliveAllFailErrorFrame(t *testing.T) {
	setupGateway(t)
	slow500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"boom"}`)
	}))
	t.Cleanup(slow500.Close)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: slow500.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	setKeepalive(t, 80*time.Millisecond)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, true, "m1")
	body := rr.Body.String()
	if !strings.Contains(body, ": keepalive") {
		t.Fatalf("expected heartbeat frames, body=%q", body)
	}
	if !strings.Contains(body, `data: {"error"`) || !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("expected in-stream error frame + [DONE], body=%q", body)
	}
}

// TestKeepaliveSkippedForNonStream：非流式请求不启用心跳（JSON 前垫注释帧会
// 破坏响应体），失败路径保持原 JSON 502 语义。
func TestKeepaliveSkippedForNonStream(t *testing.T) {
	setupGateway(t)
	slow500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(slow500.Close)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: slow500.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	setKeepalive(t, 40*time.Millisecond)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, false, "m1")
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status=%d want 502", rr.Code)
	}
	if strings.Contains(rr.Body.String(), ": keepalive") {
		t.Fatalf("non-stream must not receive heartbeats: %q", rr.Body.String())
	}
}

// TestKeepaliveZeroDisables：间隔 0（WebUI/环境关闭）时流式请求不写任何心跳。
func TestKeepaliveZeroDisables(t *testing.T) {
	setupGateway(t)
	slow := slowSSEUpstream(t, 150*time.Millisecond, "plain")
	mustPutChannel(t, &Channel{Name: "c", BaseURL: slow.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	setKeepalive(t, 0)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m1"), nil, true, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), ": keepalive") {
		t.Fatalf("keepalive must be off: %q", rr.Body.String())
	}
}

// TestKeepaliveSettingsWiring：WebUI 设置 keepalive_sec → RoutePolicy 生效。
func TestKeepaliveSettingsWiring(t *testing.T) {
	setupGateway(t)
	n := 8
	if err := store.PutSettings(&GatewaySettings{KeepaliveSec: &n}); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())
	if got := currentPolicy().KeepaliveInterval; got != 8*time.Second {
		t.Fatalf("policy KeepaliveInterval=%s, want 8s", got)
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

// TestChannelHeadersOverwriteAndAdd：渠道自定义头同名覆盖、无同名新增。
// 头名不区分大小写（小写/混排也要真正覆盖网关注入的同名头，且不产生重复头），
// 空值跳过；Host 同名头转入 req.Host（头表里的 Host 本身不会上线）。
func TestChannelHeadersOverwriteAndAdd(t *testing.T) {
	setupGateway(t)
	var mu sync.Mutex
	var got http.Header
	var gotHost string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got, gotHost = r.Header.Clone(), r.Host
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	t.Cleanup(up.Close)

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Headers: map[string]string{
			"authorization": "Bearer from-headers", // 同名覆盖网关注入的 Bearer key；头名小写也必须生效且不重复
			"x-api-key":     "v123",                // 无同名新增（服务端会规范化为 X-Api-Key）
			"user-agent":    "custom-ua",           // 覆盖透传的下游 UA
			"X-Empty":       "",                    // 空值跳过
		},
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-real", Enabled: true}}})

	req := chatRequest("m1")
	req.Header.Set("User-Agent", "downstream-ua")
	rr := httptest.NewRecorder()
	forwardChat(rr, req, nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	mu.Lock()
	auth := got.Values("Authorization")
	apiKey := got.Get("X-Api-Key")
	ua := got.Values("User-Agent")
	empty := got.Get("X-Empty")
	host1 := gotHost
	mu.Unlock()
	if len(auth) != 1 || auth[0] != "Bearer from-headers" {
		t.Fatalf("Authorization = %v, want exactly [Bearer from-headers] (no duplicates)", auth)
	}
	if apiKey != "v123" {
		t.Fatalf("X-Api-Key = %q, want v123", apiKey)
	}
	if len(ua) != 1 || ua[0] != "custom-ua" {
		t.Fatalf("User-Agent = %v, want exactly [custom-ua]", ua)
	}
	if empty != "" {
		t.Fatalf("X-Empty = %q, want absent", empty)
	}
	if want := strings.TrimPrefix(up.URL, "http://"); host1 != want {
		t.Fatalf("Host = %q, want unchanged %q", host1, want)
	}

	// Host 覆盖：同名头转入 req.Host（先停用渠道 c，让请求路由到 c2）
	snap := store.Snapshot()
	c1 := snap.Channels[0]
	c1.Enabled = false
	_ = store.PutChannel(c1)
	mustPutChannel(t, &Channel{Name: "c2", BaseURL: up.URL, Enabled: true,
		Headers: map[string]string{"Host": "upstream.example"},
		Keys:    []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}})
	req2 := chatRequest("m1")
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, req2, nil, false, "m1")
	if rr2.Code != http.StatusOK {
		t.Fatalf("c2 status=%d body=%s", rr2.Code, rr2.Body.String())
	}
	mu.Lock()
	host2 := gotHost
	mu.Unlock()
	if host2 != "upstream.example" {
		t.Fatalf("Host = %q, want upstream.example", host2)
	}
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

// TestUpstream400PassthroughLogsSnippet：上游 4xx 业务错误原样透传（状态码与
// 响应体不动），同时把响应体片段记入错误日志——否则错误日志只有 "Bad Request"
// 状态文本，回答不了「上游为什么 400」（坏 key 被上游拒绝的典型现场）。
func TestUpstream400PassthroughLogsSnippet(t *testing.T) {
	setupGateway(t)
	initStats()
	up := newUpstream(t, http.StatusBadRequest, `{"error":{"message":"Invalid API key provided","type":"invalid_request_error"}}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-bad", Enabled: true}}})

	rr := httptest.NewRecorder()
	// 走完整 statsMiddleware 链路：errMsg 只在 statsServe 收尾时进错误日志
	statsMiddleware(gatewayChat)(rr, chatRequest("m1"))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400 (passthrough), body=%s", rr.Code, rr.Body.String())
	}
	// 预读片段重新拼回 Body，透传内容必须完整（不能被预读截掉开头）
	if !strings.Contains(rr.Body.String(), "Invalid API key provided") {
		t.Fatalf("downstream body truncated: %q", rr.Body.String())
	}
	recs := errLog.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("errLog has %d records, want 1", len(recs))
	}
	for _, want := range []string{"upstream 400", "Invalid API key provided"} {
		if !strings.Contains(recs[0].ErrMsg, want) {
			t.Fatalf("errLog error missing %q: %q", want, recs[0].ErrMsg)
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

// TestRoundRobinSchedule 顺序轮询调度：每次请求从下一个 key 开始轮流分配，
// 3 个 key 各服务一次；显式 failover 渠道仍固定从第一个 key 开始。
func TestRoundRobinSchedule(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "rr", BaseURL: up.srv.URL, Enabled: true, Schedule: scheduleRoundRobin,
		Models: []string{"m1"},
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
			{Name: "k3", APIKey: "sk-3", Enabled: true},
		}})

	served := func() string {
		rr := httptest.NewRecorder()
		cand := forwardChat(rr, chatRequest("m1"), nil, false, "m1")
		if cand == nil {
			t.Fatalf("no candidate served: status=%d body=%s", rr.Code, rr.Body.String())
		}
		return cand.k.Name
	}
	var got []string
	for i := 0; i < 3; i++ {
		got = append(got, served())
	}
	if len(got) != 3 || got[0] == got[1] || got[1] == got[2] || got[0] == got[2] {
		t.Fatalf("round robin must rotate across all keys, got %v", got)
	}

	// 显式故障转移：始终用第一个 key
	mustPutChannel(t, &Channel{Name: "fo", BaseURL: up.srv.URL, Enabled: true, Schedule: scheduleFailover,
		Models: []string{"m2"},
		Keys: []*UpKey{
			{Name: "f1", APIKey: "sk-f1", Enabled: true},
			{Name: "f2", APIKey: "sk-f2", Enabled: true},
		}})
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		cand := forwardChat(rr, chatRequest("m2"), nil, false, "m2")
		if cand == nil || cand.k.Name != "f1" {
			t.Fatalf("failover must always pick first key, got %+v", cand)
		}
	}
}

// TestRoundRobinFollowsGlobalDefault 渠道未显式配置调度时跟随全局默认：
// 默认设置改为 round_robin 后开始轮询；渠道显式 failover 可覆盖回固定顺序。
func TestRoundRobinFollowsGlobalDefault(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "def", BaseURL: up.srv.URL, Enabled: true, // 未显式配置调度
		Models: []string{"m1"},
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
		}})
	mustPutChannel(t, &Channel{Name: "fix", BaseURL: up.srv.URL, Enabled: true, Schedule: scheduleFailover,
		Models: []string{"m2"},
		Keys: []*UpKey{
			{Name: "x1", APIKey: "sk-x1", Enabled: true},
			{Name: "x2", APIKey: "sk-x2", Enabled: true},
		}})

	// 全局默认 round_robin
	rrMode := scheduleRoundRobin
	if err := store.PutSettings(&GatewaySettings{DefaultSchedule: &rrMode}); err != nil {
		t.Fatalf("PutSettings: %v", err)
	}
	applySettings(store.Settings())

	// 未配置调度的渠道开始轮询（两次请求命中两个不同 key）
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		cand := forwardChat(rr, chatRequest("m1"), nil, false, "m1")
		if cand == nil {
			t.Fatalf("no candidate: status=%d", rr.Code)
		}
		seen[cand.k.Name] = true
	}
	if len(seen) != 2 {
		t.Fatalf("default schedule round_robin should rotate channel keys, got %v", seen)
	}

	// 渠道级显式 failover 覆盖全局轮询：固定第一个 key
	for i := 0; i < 2; i++ {
		rr := httptest.NewRecorder()
		cand := forwardChat(rr, chatRequest("m2"), nil, false, "m2")
		if cand == nil || cand.k.Name != "x1" {
			t.Fatalf("channel override failover must always pick first key, got %+v", cand)
		}
	}
}

// TestRouteStatusData 路由视图：按模型聚合候选 key，标注 可用/冷却中/停用，
// 未声明模型列表的渠道对每个模型放行；候选顺序与配置顺序一致。
func TestRouteStatusData(t *testing.T) {
	setupGateway(t)
	mustPutChannel(t, &Channel{Name: "a", BaseURL: "http://up-a", Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models: []string{"m1", "m2"},
		Keys: []*UpKey{
			{Name: "a1", APIKey: "sk-1", Enabled: true},
			{Name: "a2", APIKey: "sk-2", Enabled: false},
		}})
	mustPutChannel(t, &Channel{Name: "b", BaseURL: "http://up-b", Enabled: false,
		Models: []string{"m1"},
		Keys:   []*UpKey{{Name: "b1", APIKey: "sk-3", Enabled: true}}})
	mustPutChannel(t, &Channel{Name: "c", BaseURL: "http://up-c", Enabled: true, // 未声明模型：对全部模型放行
		Keys: []*UpKey{{Name: "c1", APIKey: "sk-4", Enabled: true}}})

	snap := store.Snapshot()
	a1 := snap.Channels[0].Keys[0].ID
	a2 := snap.Channels[0].Keys[1].ID
	cool.Mark(a1, "m2", time.Hour) // key_model 渠道：只冷却 (a1, m2)

	view := routeStatusData("")
	if view.DefaultSchedule != scheduleFailover {
		t.Fatalf("default schedule = %q", view.DefaultSchedule)
	}
	var names []string
	for _, g := range view.Models {
		names = append(names, g.Model)
	}
	if len(names) != 2 || names[0] != "m1" || names[1] != "m2" {
		t.Fatalf("model groups = %v, want [m1 m2]", names)
	}

	byModel := map[string]RouteModelGroup{}
	for _, g := range view.Models {
		byModel[g.Model] = g
	}
	m1 := byModel["m1"]
	if m1.Available != 2 || m1.Cooling != 0 || m1.Total != 4 {
		t.Fatalf("m1 group: available=%d cooling=%d total=%d, want 2/0/4 (%+v)", m1.Available, m1.Cooling, m1.Total, m1.Keys)
	}
	m2 := byModel["m2"]
	if m2.Available != 1 || m2.Cooling != 1 || m2.Total != 3 {
		t.Fatalf("m2 group: available=%d cooling=%d total=%d, want 1/1/3 (%+v)", m2.Available, m2.Cooling, m2.Total, m2.Keys)
	}
	// a1 在 m2 上冷却，在 m1 上可用
	statusOf := func(g RouteModelGroup, keyID string) string {
		for _, k := range g.Keys {
			if k.KeyID == keyID {
				return k.Status
			}
		}
		return ""
	}
	if statusOf(m1, a1) != routeStatusOK || statusOf(m2, a1) != routeStatusCooling {
		t.Fatalf("a1 status: m1=%q m2=%q", statusOf(m1, a1), statusOf(m2, a1))
	}
	if statusOf(m1, a2) != routeStatusDisabled || statusOf(m1, snap.Channels[1].Keys[0].ID) != routeStatusDisabled {
		t.Fatal("disabled key/channel must be marked disabled")
	}
	if m2.Keys[0].Channel != "a" || m2.Keys[0].Key != "a1" {
		t.Fatalf("candidate order must follow config order, got %+v", m2.Keys[0])
	}

	// 指定模型过滤：只返回该模型分组
	only := routeStatusData("m2")
	if len(only.Models) != 1 || only.Models[0].Model != "m2" || only.Models[0].Cooling != 1 {
		t.Fatalf("model filter: %+v", only.Models)
	}
}

// TestCooldownsClearModelAndMap 冷却表的按模型清除/快照/全清。
func TestCooldownsClearModelAndMap(t *testing.T) {
	c := newCooldowns()
	c.Mark("k", "m1", time.Hour)
	c.Mark("k", "m2", time.Hour)
	c.Mark("k", "", time.Hour)
	if got := len(c.CoolingMap("k")); got != 3 {
		t.Fatalf("CoolingMap = %d entries, want 3", got)
	}
	if got := len(c.CoolingMap("other")); got != 0 {
		t.Fatalf("CoolingMap(other) = %d entries, want 0", got)
	}
	if !c.ClearModel("k", "m1") {
		t.Fatal("ClearModel(m1) should clear an active entry")
	}
	if c.ClearModel("k", "m1") {
		t.Fatal("ClearModel(m1) twice should report false")
	}
	if c.IsCooling("k", "m1") || !c.IsCooling("k", "m2") || !c.IsCooling("k", "") {
		t.Fatal("ClearModel must only remove the (key, model) pair")
	}
	if n := c.ClearAll(); n != 2 {
		t.Fatalf("ClearAll = %d, want 2", n)
	}
	if c.IsCooling("k", "m2") || len(c.CoolingMap("k")) != 0 {
		t.Fatal("ClearAll must empty the table")
	}
}
