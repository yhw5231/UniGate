// 渠道测试链路的端到端回归：ipv6pool key 首次网络失败（出口被拒）换 IP 重试后，
// test-model / testkey 响应必须可正常 JSON 序列化（regression：Prior 指向被覆盖
// 的局部变量导致自引用成环，json.Marshal 报错，HTTP 500 "marshal failed"），
// 且两条尝试都必须写入请求/错误日志。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestTestModelRotatedResponseMarshals(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	socksAddr, socks := startFakeSocks5Opts(t, 1) // 首次 CONNECT 回 rep 0x05 → 换 IP 重试
	pool, poolSrv := startFakePool(t)
	socksPort, _ := strconv.Atoi(socksAddr[strings.LastIndex(socksAddr, ":")+1:])
	pool.portOverride = socksPort

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true,
			Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: poolSrv.URL,
				SocksHost: socksAddr[:strings.LastIndex(socksAddr, ":")]}}}})
	chid := store.Snapshot().Channels[0].ID

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/test-model", `{"models":"m1"}`, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("test-model status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Results []testResult `json:"results"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response: %v body=%s", err, rr.Body.String())
	}
	if len(out.Results) != 1 {
		t.Fatalf("results = %d, want 1", len(out.Results))
	}
	res := out.Results[0]
	if !res.OK {
		t.Fatalf("expected success after rotate retry, got %+v", res)
	}
	if !res.Rotated {
		t.Fatal("expected rotated=true (retry after egress change)")
	}
	if res.Prior == nil || res.Prior.OK || res.Prior.Error == "" {
		t.Fatalf("expected prior attempt with network error, got %+v", res.Prior)
	}
	if res.Prior == nil || res.Prior.Prior != nil {
		t.Fatal("prior must not recurse (cycle breaks json.Marshal)")
	}
	// 恰好一次原始尝试 + 一次重试
	if c := socks.count(); c != 2 {
		t.Fatalf("socks connects = %d, want 2", c)
	}
	// 两条尝试（失败 + 重试成功）都要写入请求记录与错误日志
	rrq := httptest.NewRecorder()
	rootHandler(rrq, adminReq(http.MethodGet, "/admin/api/requests?page_size=100", "", tok))
	var reqs struct {
		Total   int            `json:"total"`
		Records []RequestRecord `json:"records"`
	}
	if err := json.Unmarshal(rrq.Body.Bytes(), &reqs); err != nil {
		t.Fatalf("decode requests: %v body=%s", err, rrq.Body.String())
	}
	if reqs.Total != 2 {
		t.Fatalf("request log total = %d, want 2 (first attempt + retry)", reqs.Total)
	}
	rre := httptest.NewRecorder()
	rootHandler(rre, adminReq(http.MethodGet, "/admin/api/errors?page_size=100", "", tok))
	var errs struct {
		Total   int            `json:"total"`
		Records []RequestRecord `json:"records"`
	}
	if err := json.Unmarshal(rre.Body.Bytes(), &errs); err != nil {
		t.Fatalf("decode errors: %v body=%s", err, rre.Body.String())
	}
	if errs.Total != 1 {
		t.Fatalf("error log total = %d, want 1 (failed first attempt)", errs.Total)
	}
}
