// 故障转移规则增强测试：渠道优先级（priority）、平滑加权轮询（weight）、
// 强制切换渠道（failover_mode=force_channel）、错误记录请求/返回内容、
// 错误按天清理（logPruneByDays）。
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustPutChannels(t *testing.T, chans ...*Channel) {
	t.Helper()
	for _, ch := range chans {
		mustPutChannel(t, ch)
	}
}

// TestChannelPriorityOrdersCandidates：priority 越小越先路由；同优先级保持配置序。
func TestChannelPriorityOrdersCandidates(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannels(t,
		&Channel{Name: "low", BaseURL: up.srv.URL, Enabled: true, Priority: 10,
			Keys: []*UpKey{{Name: "klow", APIKey: "sk-low", Enabled: true}}},
		&Channel{Name: "high", BaseURL: up.srv.URL, Enabled: true, Priority: 1,
			Keys: []*UpKey{{Name: "khigh", APIKey: "sk-high", Enabled: true}}},
	)
	cands := buildCandidates("m1", "m1", nil)
	if len(cands) != 2 {
		t.Fatalf("candidates = %d, want 2", len(cands))
	}
	if cands[0].k.Name != "khigh" || cands[1].k.Name != "klow" {
		t.Fatalf("order = [%s %s], want [khigh klow]", cands[0].k.Name, cands[1].k.Name)
	}
}

// TestChannelWeightDistribution：同优先级渠道带权重时按比例轮流优先
// （平滑加权轮询，3:1 权重下高频渠道占明显多数）。
func TestChannelWeightDistribution(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannels(t,
		&Channel{Name: "w3", BaseURL: up.srv.URL, Enabled: true, Weight: 3,
			Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}},
		&Channel{Name: "w1", BaseURL: up.srv.URL, Enabled: true, Weight: 1,
			Keys: []*UpKey{{Name: "k2", APIKey: "sk-2", Enabled: true}}},
	)
	first := map[string]int{}
	for i := 0; i < 200; i++ {
		cands := buildCandidates("m1", "m1", nil)
		first[cands[0].k.Name]++
	}
	if first["k1"] < 100 || first["k2"] < 30 {
		t.Fatalf("first-choice distribution = %v, want ~3:1", first)
	}
	// 权重全为 0 的渠道组不参与加权：保持配置顺序
	cands := buildCandidates("m2", "m2", nil)
	if cands[0].k.Name != "k1" || cands[1].k.Name != "k2" {
		t.Fatalf("zero-weight order = [%s %s], want config order", cands[0].k.Name, cands[1].k.Name)
	}
}

// keyedUpstream 按 Authorization 区分返回状态的上游（用于观察「哪些 key 被尝试」）。
func keyedUpstream(t *testing.T, okKey string) (*httptest.Server, *sync.Map) {
	t.Helper()
	calls := &sync.Map{} // key 名称 → 次数
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		n, _ := calls.LoadOrStore(auth, 0)
		calls.Store(auth, n.(int)+1)
		if auth != "Bearer "+okKey {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":"rate limited"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

func countKey(calls *sync.Map, apiKey string) int {
	n, _ := calls.Load("Bearer " + apiKey)
	v, _ := n.(int)
	return v
}

// TestForceChannelSkipsSameChannelKeys：force_channel 渠道第一个 key 失败后
// 直接进入下一渠道（本渠道其余 key 不再尝试）；默认 same_channel 逐个尝试。
func TestForceChannelSkipsSameChannelKeys(t *testing.T) {
	setupGateway(t)
	srv, calls := keyedUpstream(t, "sk-b")
	// 渠道 A：两个 key 都 429（用不同模型区分两轮请求，避免 429 冷却干扰）
	mustPutChannels(t,
		&Channel{Name: "A", BaseURL: srv.URL, Enabled: true, FailoverMode: "force_channel",
			Keys: []*UpKey{
				{Name: "a1", APIKey: "sk-a1", Enabled: true},
				{Name: "a2", APIKey: "sk-a2", Enabled: true},
			}},
		&Channel{Name: "B", BaseURL: srv.URL, Enabled: true,
			Keys: []*UpKey{{Name: "b", APIKey: "sk-b", Enabled: true}}},
	)

	rr := httptest.NewRecorder()
	forwardChat(rr, chatRequest("m-force"), nil, false, "m-force")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if countKey(calls, "sk-a1") != 1 || countKey(calls, "sk-a2") != 0 || countKey(calls, "sk-b") != 1 {
		t.Fatalf("force_channel: a1=%d a2=%d b=%d, want 1/0/1",
			countKey(calls, "sk-a1"), countKey(calls, "sk-a2"), countKey(calls, "sk-b"))
	}

	// 默认 same_channel：a 渠道第一个 key 失败后仍会尝试 a2
	setupGateway(t) // 重置冷却
	srv2, calls2 := keyedUpstream(t, "sk-b2")
	mustPutChannels(t,
		&Channel{Name: "A", BaseURL: srv2.URL, Enabled: true,
			Keys: []*UpKey{
				{Name: "a1", APIKey: "sk-a1", Enabled: true},
				{Name: "a2", APIKey: "sk-a2", Enabled: true},
			}},
		&Channel{Name: "B", BaseURL: srv2.URL, Enabled: true,
			Keys: []*UpKey{{Name: "b", APIKey: "sk-b2", Enabled: true}}},
	)
	rr2 := httptest.NewRecorder()
	forwardChat(rr2, chatRequest("m-same"), nil, false, "m-same")
	if countKey(calls2, "sk-a1") != 1 || countKey(calls2, "sk-a2") != 1 || countKey(calls2, "sk-b2") != 1 {
		t.Fatalf("same_channel: a1=%d a2=%d b=%d, want 1/1/1",
			countKey(calls2, "sk-a1"), countKey(calls2, "sk-a2"), countKey(calls2, "sk-b2"))
	}
}

// TestErrorLogCapturesRequestAndResponse：网关错误时请求与错误日志携带
// 下游请求内容与返回内容（截断），并写入 SQLite（:memory:）。
func TestErrorLogCapturesRequestAndResponse(t *testing.T) {
	setupGateway(t)
	up := newUpstream(t, http.StatusInternalServerError, `{"error":"upstream exploded"}`)
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.srv.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	_ = store.PutGWKey(&GWKey{Name: "d", Key: "sk-gw-test123", Enabled: true})

	req := chatRequest("m-err")
	req.Header.Set("Authorization", "Bearer sk-gw-test123")
	rr := httptest.NewRecorder()
	// 生产环境 statsMiddleware 包在 Server.Handler 层（请求日志收尾记录），测试同样包一层
	statsMiddleware(rootHandler)(rr, req)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502; body=%s", rr.Code, rr.Body.String())
	}

	// 请求/错误日志都记了这条错误（日志已挂 :memory: SQLite）
	for _, log := range []*RequestLog{reqLog, errLog} {
		recs, total := log.Query(1, 10)
		if total == 0 {
			t.Fatal("expected at least one error record")
		}
		last := recs[0]
		if last.Status != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", last.Status)
		}
		if !strings.Contains(last.RequestBody, "m-err") {
			t.Errorf("request_body = %q, want to contain model m-err", last.RequestBody)
		}
		if !strings.Contains(last.ResponseBody, "upstream key(s) unavailable") &&
			!strings.Contains(last.ResponseBody, "Bad Gateway") {
			t.Errorf("response_body = %q, want gateway error body", last.ResponseBody)
		}
		// 请求体截断上限生效
		if len(last.RequestBody) > errReqBodyMax || len(last.ResponseBody) > errRespBodyMax {
			t.Errorf("captured bodies exceed caps: %d/%d", len(last.RequestBody), len(last.ResponseBody))
		}
	}
}

// TestLogPruneByDays：超过保留天数的错误记录被清理（条数上限之外的第二天数上限）。
func TestLogPruneByDays(t *testing.T) {
	db := newUsageDB("", 30, 1000)
	defer db.Close()
	old := time.Now().Add(-10 * 24 * time.Hour)
	now := time.Now()
	db.LogAppend(logTableError, RequestRecord{ID: "r1", Time: old, ErrMsg: "old"}, 0)
	db.LogAppend(logTableError, RequestRecord{ID: "r2", Time: now, ErrMsg: "new"}, 0)

	db.logPruneByDays(logTableError, 7)
	recs, total := db.LogQuery(logTableError, 1, 10)
	if total != 1 || len(recs) != 1 || recs[0].ID != "r2" {
		t.Fatalf("after 7d prune: total=%d recs=%v, want only r2", total, recs)
	}
	// days<=0 关闭天数清理
	db.logPruneByDays(logTableError, 0)
}

// TestErrLogRetentionSettingWiring：设置保存后错误日志按天上限即时生效。
func TestErrLogRetentionSettingWiring(t *testing.T) {
	setupGateway(t)
	if got := errLog.retentionDays; got != 7 {
		t.Fatalf("default retention days = %d, want 7", got)
	}
	days := 30
	if err := store.PutSettings(&GatewaySettings{ErrLogRetentionDays: &days}); err != nil {
		t.Fatal(err)
	}
	applySettings(store.Settings())
	if got := errLog.retentionDays; got != 30 {
		t.Fatalf("retention days after setting = %d, want 30", got)
	}
	if got := currentPolicy().ErrLogRetentionDays; got != 30 {
		t.Fatalf("policy retention days = %d, want 30", got)
	}
}
