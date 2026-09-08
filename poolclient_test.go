// ipv6-proxy-pool 集成测试：httptest 模拟池管理端，覆盖申请/复用/换IP/释放、
// per_ipv6 与 multiplex 两种 SOCKS 模式、以及自动换 IP 策略。
package main

import (
	"context"
	"encoding/json"
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

// readJSONBody 解码 JSON 请求体。
func readJSONBody(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

// fakePool 模拟 ipv6-proxy-pool 管理端（足够覆盖网关用到的接口）。
type fakePool struct {
	mu           sync.Mutex
	leases       map[string]*PoolLease
	rotateN      int
	releaseN     int
	status       PoolStatus
	portOverride int // >0 时租约返回该端口（模拟池子的真实 SOCKS 监听）
}

func newFakePool() *fakePool {
	return &fakePool{
		leases: map[string]*PoolLease{},
		status: PoolStatus{
			Status: "ok", SocksMode: "per_ipv6",
			SocksListenAddress: "[::]:1080",
		},
	}
}

func (f *fakePool) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		path := r.URL.Path
		switch {
		case path == "/v1/status" && r.Method == http.MethodGet:
			writeJSON(w, http.StatusOK, f.status)
		case path == "/v1/leases" && r.Method == http.MethodPost:
			var body struct {
				ID         string `json:"id"`
				Persistent bool   `json:"persistent"`
			}
			if err := readJSONBody(r, &body); err != nil {
				writeJSONError(w, http.StatusBadRequest, err.Error(), "bad_request")
				return
			}
			if lease, ok := f.leases[body.ID]; ok {
				writeJSON(w, http.StatusCreated, lease) // 幂等
				return
			}
			port := 20000 + len(f.leases)
			if f.portOverride > 0 {
				port = f.portOverride
			}
			if f.status.SocksMode == "multiplex" {
				port = 0
			}
			lease := &PoolLease{
				ID: body.ID, IPv6: "2001:db8::" + body.ID, Port: port,
				Persistent: body.Persistent, Role: "client",
			}
			f.leases[body.ID] = lease
			writeJSON(w, http.StatusCreated, lease)
		case len(path) > len("/v1/leases/") && path[:len("/v1/leases/")] == "/v1/leases/":
			rest := path[len("/v1/leases/"):]
			if rest == "" {
				writeJSONError(w, http.StatusNotFound, "lease not found", "not_found")
				return
			}
			lease, ok := f.leases[rest]
			switch {
			case r.Method == http.MethodGet:
				if !ok {
					writeJSONError(w, http.StatusNotFound, "lease not found", "not_found")
					return
				}
				writeJSON(w, http.StatusOK, lease)
			case r.Method == http.MethodPost && path[len(path)-len("/rotate"):] == "/rotate":
				id := rest[:len(rest)-len("/rotate")]
				l, ok := f.leases[id]
				if !ok {
					writeJSONError(w, http.StatusNotFound, "lease not found", "not_found")
					return
				}
				f.rotateN++
				l.IPv6 = "2001:db8::r" + itoa(f.rotateN)
				writeJSON(w, http.StatusOK, l)
			case r.Method == http.MethodDelete:
				id := rest
				if _, ok := f.leases[id]; !ok {
					writeJSONError(w, http.StatusNotFound, "lease not found", "not_found")
					return
				}
				delete(f.leases, id)
				f.releaseN++
				w.WriteHeader(http.StatusNoContent)
			default:
				writeJSONError(w, http.StatusNotFound, "not found", "not_found")
			}
		default:
			writeJSONError(w, http.StatusNotFound, "not found", "not_found")
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func startFakePool(t *testing.T) (*fakePool, *httptest.Server) {
	t.Helper()
	f := newFakePool()
	srv := httptest.NewServer(f.handler(t))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { leaseMgr = newLeaseManager() })
	return f, srv
}

func TestPoolClientAcquireIdempotent(t *testing.T) {
	_, srv := startFakePool(t)
	c := newPoolClient(srv.URL, "tok")
	l1, err := c.Acquire(context.Background(), "gw-k1", false)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	l2, err := c.Acquire(context.Background(), "gw-k1", false)
	if err != nil {
		t.Fatalf("Acquire again: %v", err)
	}
	if l1.ID != l2.ID || l1.Port != l2.Port {
		t.Fatalf("expected idempotent acquire: %+v vs %+v", l1, l2)
	}
}

func TestLeaseManagerPerIPv6(t *testing.T) {
	f, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, PoolToken: "tok"}
	route, err := leaseMgr.Ensure(context.Background(), spec, "k1", "")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if route.Kind != "socks5" || route.Addr == "" {
		t.Fatalf("unexpected route: %+v", route)
	}
	// 端口来自租约
	if got := route.Addr[len(route.Addr)-5:]; got != "20000" {
		t.Fatalf("expected lease port 20000 suffix, got %q (addr %s)", got, route.Addr)
	}
	// 第二次 Ensure 复用（无新租约）
	if _, err := leaseMgr.Ensure(context.Background(), spec, "k1", ""); err != nil {
		t.Fatalf("Ensure again: %v", err)
	}
	if n := len(f.leases); n != 1 {
		t.Fatalf("expected 1 lease on pool, got %d", n)
	}
}

func TestLeaseManagerMultiplex(t *testing.T) {
	f, srv := startFakePool(t)
	f.status.SocksMode = "multiplex"
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL}
	route, err := leaseMgr.Ensure(context.Background(), spec, "k1", "")
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// multiplex：SOCKS 端口 = 池基础端口 1080，用户名 user:<leaseID>
	if route.User != "user:gw-k1" {
		t.Fatalf("expected multiplex user, got %q", route.User)
	}
	if route.Addr[len(route.Addr)-4:] != "1080" {
		t.Fatalf("expected base socks port 1080, got %s", route.Addr)
	}
}

func TestLeaseManagerAutoRotateByInterval(t *testing.T) {
	f, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, RotateIntervalS: 1}
	if _, err := leaseMgr.Ensure(context.Background(), spec, "k1", ""); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	// 缓存 lastRotate=now，立即 Ensure 不应触发 rotate
	time.Sleep(1100 * time.Millisecond)
	if _, err := leaseMgr.Ensure(context.Background(), spec, "k1", ""); err != nil {
		t.Fatalf("Ensure after interval: %v", err)
	}
	f.mu.Lock()
	n := f.rotateN
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 auto rotate, got %d", n)
	}
}

func TestLeaseManagerRotateAndRelease(t *testing.T) {
	f, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL}
	if _, err := leaseMgr.Ensure(context.Background(), spec, "k1", ""); err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	lease, err := leaseMgr.Rotate(context.Background(), spec, "k1")
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if lease.IPv6 == "2001:db8::gw-k1" {
		t.Fatal("expected ipv6 change after rotate")
	}
	// 释放：分配与缓存一起清除
	if err := leaseMgr.ReleaseLease(context.Background(), srv.URL, "gw-k1"); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}
	f.mu.Lock()
	n := f.releaseN
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 release, got %d", n)
	}
	if infos := leaseMgr.ListLeases(); len(infos) != 0 {
		t.Fatalf("expected empty lease cache, got %d", len(infos))
	}
	if leaseMgr.AssignCount() != 0 {
		t.Fatalf("expected empty assignments, got %d", leaseMgr.AssignCount())
	}
}

func TestPoolClientReleaseMissingOK(t *testing.T) {
	_, srv := startFakePool(t)
	c := newPoolClient(srv.URL, "")
	if err := c.Release(context.Background(), "nope"); err != nil {
		t.Fatalf("release of missing lease should 404-tolerate, got %v", err)
	}
}

// ---- 跨渠道复用（Share）----

// ---- 跨渠道复用：同组互斥、跨组共享、最优装填 ----

// TestLeaseShareAssignment：A1,A2 同渠道（同组）必须不同 IP；
// B1,B2 不同渠道（异组）可与其共用 —— B1↔A1、B2↔A2，共 2 个租约。
func TestLeaseShareAssignment(t *testing.T) {
	f, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, Share: true}
	X := "https://a.example.com/v1"
	Y := "https://b.example.com/v1"
	ctx := context.Background()

	rA1, err := leaseMgr.Ensure(ctx, spec, "A1", X)
	if err != nil {
		t.Fatalf("Ensure A1: %v", err)
	}
	rA2, err := leaseMgr.Ensure(ctx, spec, "A2", X)
	if err != nil {
		t.Fatalf("Ensure A2: %v", err)
	}
	if rA1.Addr == rA2.Addr {
		t.Fatalf("same channel keys must use different IPs: %s", rA1.Addr)
	}
	rB1, err := leaseMgr.Ensure(ctx, spec, "B1", Y)
	if err != nil {
		t.Fatalf("Ensure B1: %v", err)
	}
	if rB1.Addr != rA1.Addr {
		t.Fatalf("B1 should share A1's IP, got %s vs %s", rB1.Addr, rA1.Addr)
	}
	rB2, err := leaseMgr.Ensure(ctx, spec, "B2", Y)
	if err != nil {
		t.Fatalf("Ensure B2: %v", err)
	}
	if rB2.Addr != rA2.Addr {
		t.Fatalf("B2 should share A2's IP, got %s vs %s", rB2.Addr, rA2.Addr)
	}
	f.mu.Lock()
	n := len(f.leases)
	f.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 leases total, got %d", n)
	}
	// 本地代理池列表：共享标记 + 分组
	infos := leaseMgr.ListLeases()
	sharedCount := 0
	for _, i := range infos {
		if i.Shared {
			sharedCount++
		}
		if len(i.Groups) != 2 {
			t.Fatalf("each shared lease should have 2 groups, got %v", i.Groups)
		}
	}
	if sharedCount != 2 {
		t.Fatalf("expected 2 shared entries, got %d", sharedCount)
	}
}

// TestLeaseShareStickyAndPersistent：分配结果粘性且跨重启不变（IP 稳定）。
func TestLeaseShareStickyAndPersistent(t *testing.T) {
	_, srv := startFakePool(t)
	path := filepath.Join(t.TempDir(), "lease-assignments.json")
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, Share: true}
	X := "https://a.example.com/v1"
	Y := "https://b.example.com/v1"
	ctx := context.Background()

	m1 := newLeaseManager()
	if err := m1.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	rA1, _ := m1.Ensure(ctx, spec, "A1", X)
	rB1, _ := m1.Ensure(ctx, spec, "B1", Y)
	if rB1.Addr != rA1.Addr {
		t.Fatalf("precondition: B1 shares A1, got %s vs %s", rB1.Addr, rA1.Addr)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("assignment file not written: %v", err)
	}

	// 模拟重启：新 manager 加载分配表
	m2 := newLeaseManager()
	if err := m2.SetPersistPath(path); err != nil {
		t.Fatalf("reload: %v", err)
	}
	rA1b, _ := m2.Ensure(ctx, spec, "A1", X)
	rB1b, _ := m2.Ensure(ctx, spec, "B1", Y)
	if rA1b.Addr != rA1.Addr || rB1b.Addr != rB1.Addr {
		t.Fatalf("assignments must survive restart: %s/%s vs %s/%s", rA1b.Addr, rB1b.Addr, rA1.Addr, rB1.Addr)
	}
}

// TestLeaseShareRotateAffectsAllUsers：共享租约换 IP 后所有使用方仍在同一租约上。
func TestLeaseShareRotateAffectsAllUsers(t *testing.T) {
	_, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, Share: true}
	X := "https://a.example.com/v1"
	Y := "https://b.example.com/v1"
	ctx := context.Background()
	if _, err := leaseMgr.Ensure(ctx, spec, "A1", X); err != nil {
		t.Fatalf("Ensure A1: %v", err)
	}
	if _, err := leaseMgr.Ensure(ctx, spec, "B1", Y); err != nil {
		t.Fatalf("Ensure B1: %v", err)
	}
	lease, err := leaseMgr.Rotate(ctx, spec, "B1")
	if err != nil {
		t.Fatalf("Rotate via B1: %v", err)
	}
	info := leaseMgr.ListLeases()[0]
	if info.Requests != 0 || !info.Shared {
		t.Fatalf("shared entry not reset after rotate: %+v", info)
	}
	// A1 再取应仍绑定同一租约（不新增租约）
	if _, err := leaseMgr.Ensure(ctx, spec, "A1", X); err != nil {
		t.Fatalf("Ensure A1 after rotate: %v", err)
	}
	_ = lease
	if len(leaseMgr.ListLeases()) != 1 {
		t.Fatal("rotate must not create a new lease")
	}
}

// TestReconcileReleasesOrphans：key 删除后其独占租约被回收，共享租约保留到最后一个使用方。
func TestReconcileReleasesOrphans(t *testing.T) {
	f, srv := startFakePool(t)
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, Share: true}
	X := "https://a.example.com/v1"
	Y := "https://b.example.com/v1"
	ctx := context.Background()
	for _, c := range []struct{ id, group string }{{"A1", X}, {"A2", X}, {"B1", Y}} {
		if _, err := leaseMgr.Ensure(ctx, spec, c.id, c.group); err != nil {
			t.Fatalf("Ensure %s: %v", c.id, err)
		}
	}
	f.mu.Lock()
	if n := len(f.leases); n != 2 {
		f.mu.Unlock()
		t.Fatalf("precondition: 2 leases, got %d", n)
	}
	f.mu.Unlock()

	// A2 删除：gw-A2 成为孤儿 → 释放；gw-A1 仍被 A1+B1 引用 → 保留
	leaseMgr.Reconcile(map[string]map[string]livePoolKey{
		srv.URL: {"A1": {Group: X, Shared: true}, "B1": {Group: Y, Shared: true}},
	})
	f.mu.Lock()
	n := len(f.leases)
	f.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 lease after A2 removed, got %d", n)
	}

	// 全部删除 → 全部释放
	leaseMgr.Reconcile(map[string]map[string]livePoolKey{})
	f.mu.Lock()
	n = len(f.leases)
	f.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected 0 leases after all removed, got %d", n)
	}
	if leaseMgr.AssignCount() != 0 {
		t.Fatalf("expected 0 assignments, got %d", leaseMgr.AssignCount())
	}
}

// TestNonShareKeysStayDedicated：未开 Share 的 key 独占租约，不参与共享装填。
func TestNonShareKeysStayDedicated(t *testing.T) {
	f, srv := startFakePool(t)
	dedicated := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL}
	shared := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL, Share: true}
	ctx := context.Background()
	rD, err := leaseMgr.Ensure(ctx, dedicated, "D1", "https://x/v1")
	if err != nil {
		t.Fatalf("Ensure dedicated: %v", err)
	}
	rS, err := leaseMgr.Ensure(ctx, shared, "S1", "https://y/v1")
	if err != nil {
		t.Fatalf("Ensure shared: %v", err)
	}
	if rS.Addr == rD.Addr {
		t.Fatal("shared key must not borrow a dedicated lease")
	}
	f.mu.Lock()
	n := len(f.leases)
	f.mu.Unlock()
	if n != 2 {
		t.Fatalf("expected 2 leases, got %d", n)
	}
}

// TestSocksAddr 归一化：完整 host:port、裸 host、IPv6 字面量（带/不带方括号）。
func TestSocksAddr(t *testing.T) {
	cases := []struct {
		host string
		port int
		want string
	}{
		{"127.0.0.1:19109", 1080, "127.0.0.1:19109"}, // socks_host 带端口时覆盖
		{"127.0.0.1", 1080, "127.0.0.1:1080"},
		{"2001:db8::1", 1080, "[2001:db8::1]:1080"},
		{"[2001:db8::1]", 1080, "[2001:db8::1]:1080"},
	}
	for _, c := range cases {
		if got := socksAddr(c.host, c.port); got != c.want {
			t.Errorf("socksAddr(%q,%d) = %q, want %q", c.host, c.port, got, c.want)
		}
	}
}

// TestProbeSocksReachable：活的 SOCKS5（真实握手回方法）通过；accept 后不响应
// 的挂死端口 5s 内报错；连接拒绝立刻报错。
func TestProbeSocksReachable(t *testing.T) {
	// 活的 SOCKS5：模拟 greeting → 返回方法选择
	okLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer okLn.Close()
	go func() {
		for {
			c, err := okLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 3)
				if _, err := io.ReadFull(c, buf); err != nil {
					return
				}
				c.Write([]byte{0x05, 0x00}) // method 选择回复
			}(c)
		}
	}()
	host, portStr, _ := net.SplitHostPort(okLn.Addr().String())
	port, _ := strconv.Atoi(portStr)
	if err := probeSocksReachable(host, port); err != nil {
		t.Fatalf("alive socks should pass probe: %v", err)
	}

	// 挂死端口：accept 后不读不回
	deadLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer deadLn.Close()
	go func() {
		for {
			c, err := deadLn.Accept()
			if err != nil {
				return
			}
			_ = c // 保持连接但永不响应
		}
	}()
	host2, portStr2, _ := net.SplitHostPort(deadLn.Addr().String())
	port2, _ := strconv.Atoi(portStr2)
	start := time.Now()
	err = probeSocksReachable(host2, port2)
	if err == nil {
		t.Fatal("hung socks must fail probe")
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("probe should fail fast (~5s), took %v", d)
	}

	// 连接拒绝
	if err := probeSocksReachable("127.0.0.1", 1); err == nil {
		t.Fatal("refused port must fail probe")
	}

	// 信息不全时跳过
	if err := probeSocksReachable("", 0); err != nil {
		t.Fatalf("empty host/port must skip probe: %v", err)
	}
}

// TestEnsureProbed：探测通过的路由与 Ensure 一致（复用 fake socks + fake pool）。
func TestEnsureProbed(t *testing.T) {
	f, srv := startFakePool(t)
	socksAddr := startFakeSocks5(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	f.portOverride = socksPort
	spec := &ProxySpec{Kind: "ipv6pool", PoolURL: srv.URL,
		SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}
	route, err := leaseMgr.EnsureProbed(context.Background(), spec, "P1", "https://x/v1")
	if err != nil {
		t.Fatalf("EnsureProbed: %v", err)
	}
	if route.Kind != "socks5" {
		t.Fatalf("expected socks5 route, got %q", route.Kind)
	}
}
