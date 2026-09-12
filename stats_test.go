// 可观察性测试：请求记录环形缓冲与用量聚合。
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRequestLogAddSnapshot(t *testing.T) {
	l := newRequestLog(3)
	l.Add(RequestRecord{ID: "a"})
	l.Add(RequestRecord{ID: "b"})
	l.Add(RequestRecord{ID: "c"})
	l.Add(RequestRecord{ID: "d"}) // 覆盖 a

	recs := l.Snapshot()
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if recs[0].ID != "d" || recs[1].ID != "c" || recs[2].ID != "b" {
		t.Fatalf("order wrong (newest first): %+v", recs)
	}
}

func TestRequestLogSmallCap(t *testing.T) {
	l := newRequestLog(1)
	l.Add(RequestRecord{ID: "x"})
	l.Add(RequestRecord{ID: "y"})
	recs := l.Snapshot()
	if len(recs) != 1 || recs[0].ID != "y" {
		t.Fatalf("got %+v", recs)
	}
}

func TestRequestLogQueryPagination(t *testing.T) {
	l := newRequestLog(10)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		l.Add(RequestRecord{ID: id})
	}
	// 第 1 页（最新在前）
	recs, total := l.Query(1, 2)
	if total != 5 || len(recs) != 2 || recs[0].ID != "e" || recs[1].ID != "d" {
		t.Fatalf("page1: total=%d recs=%+v", total, recs)
	}
	// 第 2、3 页
	recs, _ = l.Query(2, 2)
	if len(recs) != 2 || recs[0].ID != "c" || recs[1].ID != "b" {
		t.Fatalf("page2: %+v", recs)
	}
	recs, _ = l.Query(3, 2)
	if len(recs) != 1 || recs[0].ID != "a" {
		t.Fatalf("page3: %+v", recs)
	}
	// 越界页返回空，但 total 保持
	recs, total = l.Query(9, 2)
	if total != 5 || len(recs) != 0 {
		t.Fatalf("out-of-range page: total=%d recs=%+v", total, recs)
	}
}

// TestErrorLogSeparateRecording：错误请求（状态 >=400 或带错误信息）必须
// 单独写入错误日志，成功请求不进；主日志仍保留全部请求。
func TestErrorLogSeparateRecording(t *testing.T) {
	setupGateway(t)
	initStats()

	recordRequest(RequestRecord{ID: "ok1", Status: 200})
	recordRequest(RequestRecord{ID: "ok2", Status: 200})
	recordRequest(RequestRecord{ID: "bad1", Status: 502, ErrMsg: "model \"m\": upstream unavailable"})
	recordRequest(RequestRecord{ID: "bad2", Status: 0, ErrMsg: "proxy resolve failed"})

	if got := reqLog.Snapshot(); len(got) != 4 {
		t.Fatalf("reqLog should keep all requests, got %d", len(got))
	}
	errs := errLog.Snapshot()
	if len(errs) != 2 {
		t.Fatalf("errLog should keep only errors, got %d: %+v", len(errs), errs)
	}
	if errs[0].ID != "bad2" || errs[1].ID != "bad1" {
		t.Fatalf("errLog order wrong (newest first): %+v", errs)
	}
}

// TestRequestLogClear：Clear 清空主/错误日志，清空后环形缓冲继续正常写入。
func TestRequestLogClear(t *testing.T) {
	setupGateway(t)
	initStats()

	recordRequest(RequestRecord{ID: "ok1", Status: 200})
	recordRequest(RequestRecord{ID: "bad1", Status: 502, ErrMsg: "boom"})
	reqLog.Clear()
	errLog.Clear()
	if got := reqLog.Snapshot(); len(got) != 0 {
		t.Fatalf("reqLog after clear = %d records, want 0", len(got))
	}
	if got := errLog.Snapshot(); len(got) != 0 {
		t.Fatalf("errLog after clear = %d records, want 0", len(got))
	}
	recordRequest(RequestRecord{ID: "ok2", Status: 200})
	recs := reqLog.Snapshot()
	if len(recs) != 1 || recs[0].ID != "ok2" {
		t.Fatalf("after clear add: %+v", recs)
	}
}

func TestUsageStatsRecord(t *testing.T) {
	s := newUsageStats()
	now := time.Now()
	s.Record(RequestRecord{Status: 200, BytesOut: 100, DurationMs: 5, Time: now, User: "alice", Model: "m1", Key: "sk-a"})
	s.Record(RequestRecord{Status: 500, BytesOut: 20, DurationMs: 3, Time: now, User: "alice", Model: "m1", Key: "sk-a"})
	s.Record(RequestRecord{Status: 200, BytesOut: 50, DurationMs: 2, Time: now, User: "bob", Model: "m2", Key: "sk-b"})

	if s.Totals.Requests != 3 || s.Totals.Errors != 1 || s.Totals.BytesOut != 170 {
		t.Fatalf("totals = %+v", s.Totals)
	}
	if u := s.ByUser["alice"]; u == nil || u.Requests != 2 || u.Errors != 1 {
		t.Fatalf("alice = %+v", u)
	}
	if m := s.ByModel["m1"]; m == nil || m.Requests != 2 || m.Errors != 1 {
		t.Fatalf("m1 = %+v", m)
	}
	if k := s.ByKey["sk-b"]; k == nil || k.Requests != 1 {
		t.Fatalf("sk-b = %+v", k)
	}
}

func TestMaskKey(t *testing.T) {
	if maskKey("sk-12345678abcd") != "sk-1****abcd" {
		t.Fatalf("got %q", maskKey("sk-12345678abcd"))
	}
	if maskKey("short") != "****" {
		t.Fatalf("got %q", maskKey("short"))
	}
	// 中文名称按 rune 边界截取：按字节切会把 UTF-8 字符斩成乱码（导�****b.ai）
	if got, want := maskKey("导入key1@b.ai"), "导****b.ai"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := maskKey("测试渠道名称@channel"), "测****nnel"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestResponseRecorder(t *testing.T) {
	inner := httptest.NewRecorder()
	rr := &responseRecorder{ResponseWriter: inner}
	rr.WriteHeader(http.StatusCreated)
	_, _ = rr.Write([]byte("hello"))
	if rr.status != http.StatusCreated || rr.bytes != 5 {
		t.Fatalf("status=%d bytes=%d", rr.status, rr.bytes)
	}
}
