// 用量库降级/自愈测试：库暂时不可用（路径不存在、被占用）时不崩溃、不静默，
// 条件恢复后下一次读写自动重开并继续记账。
package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestUsageDBRecoversFromUnavailablePath 目标目录先不存在（打开必然失败）：
// 写入与查询降级为空操作且不 panic；目录出现后（等价于磁盘/挂载点恢复）下一次
// 写入自动重开，数据落库、查询可见。
func TestUsageDBRecoversFromUnavailablePath(t *testing.T) {
	old := usageReopenEvery
	usageReopenEvery = 0 // 测试里不受退避间隔限制
	defer func() { usageReopenEvery = old }()

	base := t.TempDir()
	path := filepath.Join(base, "missing", "usage.db") // missing/ 尚不存在
	db := newUsageDB(path, 30, 1000)
	defer db.Close()
	if db.Available() {
		t.Fatal("db should be unavailable while the parent dir does not exist")
	}
	db.Append(UsageEvent{Time: time.Now(), User: "u", Status: 200}) // 降级：不 panic
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 0 {
		t.Fatalf("unavailable db must return empty result, got %+v", res)
	}
	if n := db.Count(); n != 0 {
		t.Fatalf("Count on unavailable db = %d, want 0", n)
	}
	db.LogAppend(logTableRequest, RequestRecord{ID: "x", Time: time.Now(), Status: 200}, 10)
	if recs, total := db.LogQuery(logTableRequest, 1, 10); len(recs) != 0 || total != 0 {
		t.Fatalf("LogQuery on unavailable db = %d/%d, want 0/0", len(recs), total)
	}

	// 目录出现 = 条件恢复：下一次使用应自动重开并真正记账
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db.Append(UsageEvent{Time: time.Now(), User: "u", Model: "m1", Key: "n@c", PromptTokens: 7, Status: 200})
	if !db.Available() {
		t.Fatal("db should be available again after the dir appears")
	}
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 1 || res.PromptTokens != 7 {
		t.Fatalf("after recovery = %+v, want 1 request / 7 prompt tokens", res)
	}
	// 重开后旧语句缓存不得生效（句柄换代）：再次写入仍正常
	db.Append(UsageEvent{Time: time.Now(), User: "u", Model: "m1", Key: "n@c", PromptTokens: 3, Status: 200})
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 2 || res.PromptTokens != 10 {
		t.Fatalf("second append after recovery = %+v, want 2 requests / 10 prompt tokens", res)
	}
	if n := db.Count(); n != 2 {
		t.Fatalf("Count = %d, want 2", n)
	}
}

// TestUsageDBReopenKeepsWorking 已在用的库被外部替换（模拟运维换库/删除重建）：
// 下一次写入不 panic，且句柄换代后语句缓存自动重准备。
func TestUsageDBReopenKeepsWorking(t *testing.T) {
	old := usageReopenEvery
	usageReopenEvery = 0
	defer func() { usageReopenEvery = old }()

	path := filepath.Join(t.TempDir(), "usage.db")
	db := newUsageDB(path, 30, 1000)
	defer db.Close()
	db.Append(UsageEvent{Time: time.Now(), User: "u", Status: 200})
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 1 {
		t.Fatalf("initial append = %+v, want 1", res)
	}
	// 模拟外部把库换掉：关句柄、删文件（写入会报错一次，随后自愈）
	if h := db.handle.Load(); h != nil {
		_ = h.Close()
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	db.Append(UsageEvent{Time: time.Now(), User: "u", Status: 200}) // 可能失败一次
	db.Append(UsageEvent{Time: time.Now(), User: "u", Status: 200}) // 自愈后应成功
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests == 0 {
		t.Fatalf("db did not recover after the file was replaced: %+v", res)
	}
}

// TestRequestLogPersistsAfterDBRecovery 库不可用期间请求日志退回内存环形缓冲
// （WebUI 仍看得到），库自愈后新记录自动落库——不需要重启进程，清空也不会把
// 内存里的旧记录再读出来。
func TestRequestLogPersistsAfterDBRecovery(t *testing.T) {
	oldEvery := usageReopenEvery
	usageReopenEvery = 0
	defer func() { usageReopenEvery = oldEvery }()

	oldDB, oldReq, oldErr := usageDB, reqLog, errLog
	defer func() { usageDB, reqLog, errLog = oldDB, oldReq, oldErr }()

	base := t.TempDir()
	path := filepath.Join(base, "missing", "usage.db") // 上级目录不存在 → 建库必失败
	usageDB = newUsageDB(path, 30, 1000)
	defer usageDB.Close()
	initStats() // 建库失败也挂表：库恢复后日志要能自动落库

	recordRequest(RequestRecord{ID: "r1", Time: time.Now(), Method: "POST",
		Path: "/v1/chat/completions", Status: 502, Model: "m1"})
	recs := reqLog.Snapshot()
	if len(recs) != 1 || recs[0].ID != "r1" {
		t.Fatalf("log must stay visible from the memory ring while the db is down, got %+v", recs)
	}
	if page, total := reqLog.Query(1, 10); total != 1 || len(page) != 1 {
		t.Fatalf("paged query while db down = %d/%d, want 1/1", len(page), total)
	}
	if usageDB.Available() {
		t.Fatal("db should still be unavailable here")
	}

	// 条件恢复：目录出现，下一次写入应落库
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	recordRequest(RequestRecord{ID: "r2", Time: time.Now(), Method: "POST",
		Path: "/v1/chat/completions", Status: 200, Model: "m1"})
	if !usageDB.Available() {
		t.Fatal("db should have recovered and be available again")
	}
	page, total := reqLog.Query(1, 10)
	if total != 1 || len(page) != 1 || page[0].ID != "r2" {
		t.Fatalf("after recovery the record must come from the db, got %d/%d %+v", len(page), total, page)
	}

	// 清空：库与内存环形缓冲一起清，旧记录不再被读出来
	reqLog.Clear()
	if page, total := reqLog.Query(1, 10); total != 0 || len(page) != 0 {
		t.Fatalf("cleared log still returns records: %d/%d", len(page), total)
	}
}
