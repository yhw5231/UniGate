// 可观察性测试：请求记录环形缓冲与用量聚合。
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
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

// TestRequestLogSQLBacked：日志挂上 SQLite 表后，Add/Query/Clear 走数据库。
// USAGE_DB_PATH 为空（内存库）时同样可验证：结果与内存环形缓冲一致。
func TestRequestLogSQLBacked(t *testing.T) {
	setupGateway(t)
	initUsageDB()
	initStats()

	reqLog.attachTable(logTableRequest)
	errLog.attachTable(logTableError)

	recordRequest(RequestRecord{ID: "a1", Status: 200})
	recordRequest(RequestRecord{ID: "e1", Status: 502, ErrMsg: "boom"})
	recordRequest(RequestRecord{ID: "a2", Status: 200})

	// 全部 + 错误分离
	all, total := reqLog.Query(1, 10)
	if total != 3 || len(all) != 3 {
		t.Fatalf("reqLog query: total=%d len=%d", total, len(all))
	}
	errs, etotal := errLog.Query(1, 10)
	if etotal != 1 || len(errs) != 1 || errs[0].ErrMsg != "boom" {
		t.Fatalf("errLog query: total=%d recs=%+v", etotal, errs)
	}

	// 清空后归一
	reqLog.Clear()
	errLog.Clear()
	if _, total := reqLog.Query(1, 10); total != 0 {
		t.Fatalf("after clear reqLog total=%d", total)
	}
	if _, total := errLog.Query(1, 10); total != 0 {
		t.Fatalf("after clear errLog total=%d", total)
	}
}

// TestRequestLogSQLClip：超出容量后裁剪到环形上限附近。裁剪按每 logPruneEvery
// 条摊销（避免每条 DELETE），瞬时允许超出约 logPruneEvery 条；attach 时立刻
// 裁一次，保证重启/日志挂载后不累积过量。
func TestRequestLogSQLClip(t *testing.T) {
	setupGateway(t)
	initUsageDB()
	initStats()

	for i := 0; i < 500; i++ {
		reqLog.Add(RequestRecord{ID: fmt.Sprintf("r%d", i), Status: 200})
	}
	_, total := reqLog.Query(1, 10000)
	if total > 100+logPruneEvery || total < 100 {
		t.Fatalf("wanted clipped within [100, %d), got %d", 100+logPruneEvery, total)
	}

	// attach 时立即裁剪：重挂载后不应超过容量
	reqLog.attachTable(logTableRequest)
	_, total = reqLog.Query(1, 10000)
	if total != 100 {
		t.Fatalf("after attach wanted exactly 100, got %d", total)
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
