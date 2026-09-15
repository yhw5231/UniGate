// 代理拨号层测试：代理 URL 解析、脱敏描述、transport 缓存键，
// 以及 5xx/网络错误路径依赖的 SOCKS5 应答码翻译。
package main

import (
	"strings"
	"testing"
)

func TestParseProxyURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    ProxyRoute
		wantErr bool
	}{
		{"http 无认证", "http://1.2.3.4:8080", ProxyRoute{Kind: "http", Addr: "1.2.3.4:8080"}, false},
		{"http 带认证", "http://u:p@1.2.3.4:8080", ProxyRoute{Kind: "http", Addr: "1.2.3.4:8080", User: "u", Pass: "p"}, false},
		{"https", "https://proxy.example:443", ProxyRoute{Kind: "https", Addr: "proxy.example:443"}, false},
		{"socks5", "socks5://u:p@1.2.3.4:1080", ProxyRoute{Kind: "socks5", Addr: "1.2.3.4:1080", User: "u", Pass: "p"}, false},
		{"socks5h 归一为 socks5", "socks5h://1.2.3.4:1080", ProxyRoute{Kind: "socks5", Addr: "1.2.3.4:1080"}, false},
		{"IPv6 字面量", "socks5://[::1]:1080", ProxyRoute{Kind: "socks5", Addr: "[::1]:1080"}, false},
		{"缺 host", "http://", ProxyRoute{}, true},
		{"不支持的协议", "ftp://1.2.3.4:21", ProxyRoute{}, true},
		{"空串", "", ProxyRoute{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProxyURL(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseProxyURL(%q) = %+v, want error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseProxyURL(%q): %v", tc.raw, err)
			}
			if *got != tc.want {
				t.Fatalf("parseProxyURL(%q) = %+v, want %+v", tc.raw, *got, tc.want)
			}
		})
	}
}

// TestProxyRouteDescribe：describe 必须脱敏密码、且 nil 接收者安全
//（渠道测试链路在直连时 route 为 nil，直接调用 describe 展示代理描述）。
func TestProxyRouteDescribe(t *testing.T) {
	var nilRoute *ProxyRoute
	if got := nilRoute.describe(); got != "direct" {
		t.Fatalf("nil route describe = %q, want direct", got)
	}
	if got := (&ProxyRoute{Kind: "none"}).describe(); got != "direct" {
		t.Fatalf("none route describe = %q, want direct", got)
	}
	withAuth := &ProxyRoute{Kind: "http", Addr: "1.2.3.4:8080", User: "u", Pass: "s3cret"}
	got := withAuth.describe()
	if strings.Contains(got, "s3cret") {
		t.Fatalf("describe leaked password: %q", got)
	}
	if got != "http://u:***@1.2.3.4:8080" {
		t.Fatalf("describe = %q", got)
	}
	if got := (&ProxyRoute{Kind: "socks5", Addr: "1.2.3.4:1080"}).describe(); got != "socks5://1.2.3.4:1080" {
		t.Fatalf("describe = %q", got)
	}
}

// TestProxyRouteKey：缓存键不含密码（同一路由不同密码共用 transport 池），
// nil 路由归一为 "none"。
func TestProxyRouteKey(t *testing.T) {
	var nilRoute *ProxyRoute
	if got := nilRoute.key(); got != "none" {
		t.Fatalf("nil key = %q, want none", got)
	}
	a := &ProxyRoute{Kind: "http", Addr: "1.2.3.4:8080", User: "u", Pass: "p1"}
	b := &ProxyRoute{Kind: "http", Addr: "1.2.3.4:8080", User: "u", Pass: "p2"}
	if a.key() != b.key() {
		t.Fatalf("key should ignore password: %q vs %q", a.key(), b.key())
	}
}

// TestProxyRouteAddrParts：AddrHost/AddrPort 解析（含 IPv6 去括号、异常输入）。
func TestProxyRouteAddrParts(t *testing.T) {
	r := &ProxyRoute{Kind: "socks5", Addr: "[2001:db8::1]:1080"}
	if got := r.AddrHost(); got != "2001:db8::1" {
		t.Fatalf("AddrHost = %q", got)
	}
	if got := r.AddrPort(); got != 1080 {
		t.Fatalf("AddrPort = %d", got)
	}
	// 无端口的裸地址：SplitHostPort 失败，host 去方括号、port 为 0
	bad := &ProxyRoute{Kind: "http", Addr: "hostonly"}
	if got := bad.AddrHost(); got != "hostonly" {
		t.Fatalf("AddrHost = %q", got)
	}
	if got := bad.AddrPort(); got != 0 {
		t.Fatalf("AddrPort = %d, want 0", got)
	}
	var nilRoute *ProxyRoute
	if got := nilRoute.AddrHost(); got != "" {
		t.Fatalf("nil AddrHost = %q", got)
	}
	if got := nilRoute.AddrPort(); got != 0 {
		t.Fatalf("nil AddrPort = %d", got)
	}
}

// TestSocksReplyText：应答码翻译（含未知码），错误信息可读性依赖它。
func TestSocksReplyText(t *testing.T) {
	if got := socksReplyText(0x05); !strings.Contains(got, "refused") {
		t.Fatalf("rep 0x05 = %q", got)
	}
	if got := socksReplyText(0x03); !strings.Contains(got, "network unreachable") {
		t.Fatalf("rep 0x03 = %q", got)
	}
	if got := socksReplyText(0x99); !strings.Contains(got, "unknown reply") {
		t.Fatalf("rep 0x99 = %q", got)
	}
}

// TestBuildConnectRequest：CONNECT 请求按目标类型选择 ATYP（IPv4/IPv6/域名）。
func TestBuildConnectRequest(t *testing.T) {
	// IPv4
	req := buildConnectRequest("1.2.3.4", 443)
	if req[3] != 0x01 || len(req) != 4+4+2 {
		t.Fatalf("ipv4 req = % x", req)
	}
	// IPv6
	req = buildConnectRequest("2001:db8::1", 443)
	if req[3] != 0x04 || len(req) != 4+16+2 {
		t.Fatalf("ipv6 req = % x", req)
	}
	// 域名
	req = buildConnectRequest("example.com", 8080)
	if req[3] != 0x03 || req[4] != byte(len("example.com")) {
		t.Fatalf("domain req = % x", req)
	}
	if got := int(req[len(req)-2])<<8 | int(req[len(req)-1]); got != 8080 {
		t.Fatalf("port = %d, want 8080", got)
	}
}
