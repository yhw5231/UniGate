// 通用代理拨号层：把解析后的代理（http / socks5 / 直连）变成上游 HTTP client。
// 从原 cline2api 的 proxy_http.go / proxy_socks.go 提炼，ipv6pool 租约的动态解析见 poolclient.go。
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ProxyRoute 一个已解析的静态代理路由。
type ProxyRoute struct {
	Kind string // "none" | "http" | "socks5"
	Addr string // host:port
	User string
	Pass string
}

// parseProxyURL 解析代理 URL：http(s)://user:pass@host:port 或 socks5://user:pass@host:port。
func parseProxyURL(raw string) (*ProxyRoute, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	kind := strings.ToLower(u.Scheme)
	if kind == "" {
		kind = "http"
	}
	if kind == "socks5h" {
		kind = "socks5"
	}
	if kind != "http" && kind != "https" && kind != "socks5" {
		return nil, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("proxy url missing host")
	}
	user, pass := "", ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	return &ProxyRoute{Kind: kind, Addr: u.Host, User: user, Pass: pass}, nil
}

// key 返回路由的唯一标识（transport 缓存键）。密码不出现在键里。
func (r *ProxyRoute) key() string {
	if r == nil {
		return "none"
	}
	return r.Kind + "://" + r.User + "@" + r.Addr
}

// AddrHost 返回 Addr 的 host 部分（IPv6 字面量去掉方括号）。
func (r *ProxyRoute) AddrHost() string {
	if r == nil {
		return ""
	}
	h, _, err := net.SplitHostPort(r.Addr)
	if err != nil {
		return strings.Trim(r.Addr, "[]")
	}
	return h
}

// AddrPort 返回 Addr 的端口部分（解析失败为 0）。
func (r *ProxyRoute) AddrPort() int {
	if r == nil {
		return 0
	}
	_, p, err := net.SplitHostPort(r.Addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}

// describe 返回可读描述（脱敏密码）。
func (r *ProxyRoute) describe() string {
	if r == nil || r.Kind == "none" {
		return "direct"
	}
	s := r.Kind + "://" + r.Addr
	if r.User != "" {
		s = r.Kind + "://" + r.User + ":***@" + r.Addr
	}
	return s
}

// transportFor 返回该路由的 *http.Transport（带进程级缓存，路由相同即复用连接池）。
func transportFor(r *ProxyRoute) *http.Transport {
	return globalTransportCache.get(r)
}

type transportCache struct {
	mu  sync.Mutex
	trs map[string]*http.Transport
}

var globalTransportCache = &transportCache{trs: map[string]*http.Transport{}}

func (tc *transportCache) get(r *ProxyRoute) *http.Transport {
	k := r.key()
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tr, ok := tc.trs[k]; ok {
		return tr
	}
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
		ResponseHeaderTimeout: cfg.UpstreamHeaderTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
	}
	if r != nil && r.Kind != "none" {
		switch r.Kind {
		case "http":
			proxyURL := &url.URL{Scheme: "http", Host: r.Addr}
			if r.User != "" {
				proxyURL.User = url.UserPassword(r.User, r.Pass)
			}
			tr.Proxy = http.ProxyURL(proxyURL)
		case "socks5":
			addr, user, pass := r.Addr, r.User, r.Pass
			tr.DialContext = func(ctx context.Context, network, target string) (net.Conn, error) {
				return dialSocks5(ctx, addr, user, pass, target)
			}
		}
	}
	tc.trs[k] = tr
	return tr
}

// newUpstreamClient 构造访问上游的 HTTP client。LLM 非流式响应可能要数分钟才回包，
// 因此不设整体超时，仅限制响应头等待（transport 内）。
func newUpstreamClient(r *ProxyRoute) *http.Client {
	return &http.Client{Transport: transportFor(r)}
}

// ---- SOCKS5 客户端 ----

// bufConn 包装 net.Conn + bufio.Reader，便于读取整块。
type bufConn struct {
	net.Conn
	reader *bufio.Reader
}

func newBufConn(c net.Conn) *bufConn {
	return &bufConn{Conn: c, reader: bufio.NewReader(c)}
}

func (c *bufConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *bufConn) ReadByte() (byte, error) {
	return c.reader.ReadByte()
}

// socksReplyText 把 SOCKS5 CONNECT 应答 rep 码翻成可读原因（RFC 1928），
// 供日志/错误信息直接定位代理池问题。
func socksReplyText(rep byte) string {
	switch rep {
	case 0x01:
		return "general SOCKS server failure"
	case 0x02:
		return "connection not allowed by ruleset"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused by upstream (出口被拒)"
	case 0x06:
		return "TTL expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	default:
		return fmt.Sprintf("unknown reply 0x%02x", rep)
	}
}

// dialSocks5 作为 SOCKS5 客户端连接到 target（host:port），支持用户名/密码认证。
func dialSocks5(ctx context.Context, serverAddr, user, pass, target string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("dial socks5 server %s: %w", serverAddr, err)
	}
	br := newBufConn(conn)

	// 握手：优先 username/password，其次 no-auth
	methods := []byte{0x00}
	if user != "" {
		methods = []byte{0x02, 0x00}
	}
	if _, err := conn.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		conn.Close()
		return nil, err
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(br, resp); err != nil {
		conn.Close()
		return nil, err
	}
	if resp[0] != 0x05 {
		conn.Close()
		return nil, errors.New("bad socks5 greeting response")
	}
	switch resp[1] {
	case 0x00:
		// no auth ok
	case 0x02:
		if user == "" {
			conn.Close()
			return nil, errors.New("server requires auth")
		}
		if _, err := conn.Write(buildUserPass(user, pass)); err != nil {
			conn.Close()
			return nil, err
		}
		authResp := make([]byte, 2)
		if _, err := io.ReadFull(br, authResp); err != nil {
			conn.Close()
			return nil, err
		}
		if authResp[1] != 0x00 {
			conn.Close()
			return nil, errors.New("socks5 auth failed")
		}
	default:
		conn.Close()
		return nil, errors.New("no acceptable socks5 auth method")
	}

	// CONNECT 请求
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		conn.Close()
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		conn.Close()
		return nil, err
	}
	req := buildConnectRequest(host, uint16(port))
	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, err
	}
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(br, hdr); err != nil {
		conn.Close()
		return nil, err
	}
	if hdr[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("socks5 connect to %s via %s failed: %s (rep 0x%02x)",
			target, serverAddr, socksReplyText(hdr[1]), hdr[1])
	}
	if _, err := readSocksAddr(br, hdr[3]); err != nil {
		conn.Close()
		return nil, err
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(br, portBytes); err != nil {
		conn.Close()
		return nil, err
	}
	return &bufferedConn{Conn: conn, reader: br.reader}, nil
}

func buildUserPass(user, pass string) []byte {
	b := []byte{0x01, byte(len(user))}
	b = append(b, []byte(user)...)
	b = append(b, byte(len(pass)))
	b = append(b, []byte(pass)...)
	return b
}

// buildConnectRequest 构造 SOCKS5 CONNECT 请求。
func buildConnectRequest(host string, port uint16) []byte {
	req := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			req = append(req, 0x01)
			req = append(req, v4...)
		} else {
			req = append(req, 0x04)
			req = append(req, ip.To16()...)
		}
	} else {
		req = append(req, 0x03, byte(len(host)))
		req = append(req, []byte(host)...)
	}
	req = append(req, byte(port>>8), byte(port))
	return req
}

// readSocksAddr 读取 SOCKS5 响应中的绑定地址（按 ATYP）。
func readSocksAddr(c io.Reader, atyp byte) (string, error) {
	switch atyp {
	case 0x01: // IPv4
		b := make([]byte, 4)
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	case 0x03: // 域名
		l := make([]byte, 1)
		if _, err := io.ReadFull(c, l); err != nil {
			return "", err
		}
		b := make([]byte, l[0])
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return string(b), nil
	case 0x04: // IPv6
		b := make([]byte, 16)
		if _, err := io.ReadFull(c, b); err != nil {
			return "", err
		}
		return net.IP(b).String(), nil
	}
	return "", fmt.Errorf("unsupported ATYP %d", atyp)
}

// ---- HTTP 代理 CONNECT 隧道 ----

// dialViaHTTPProxy 通过 HTTP 正向代理建立到目标的 TCP 连接（CONNECT 隧道）。
func dialViaHTTPProxy(ctx context.Context, route *ProxyRoute, target string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", route.Addr)
	if err != nil {
		return nil, fmt.Errorf("connect to proxy %s: %w", route.Addr, err)
	}
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
	if route.User != "" {
		cred := base64.StdEncoding.EncodeToString([]byte(route.User + ":" + route.Pass))
		req += "Proxy-Authorization: Basic " + cred + "\r\n"
	}
	req += "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("read CONNECT headers: %w", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if !strings.Contains(statusLine, " 200 ") {
		conn.Close()
		return nil, fmt.Errorf("CONNECT to %s returned: %s", target, strings.TrimSpace(statusLine))
	}
	return &bufferedConn{Conn: conn, reader: br}, nil
}

// bufferedConn 包装 net.Conn，提供一个 bufio.Reader 用于读取已缓冲的数据。
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
