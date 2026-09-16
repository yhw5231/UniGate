// 用量数据库测试：SQLite 持久化、窗口聚合、条件过滤、token 记录。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMain 默认把 USAGE_DB_PATH 置空（内存模式），避免测试在仓库目录落盘
// 或残留未关闭的 SQLite 连接导致文件锁；需要真实文件的用例用 t.Setenv 覆盖。
// 同时把 DATA_DIR 指向临时目录并重新加载配置：包 init() 在 TestMain 之前
// 已经读取过仓库 data/ 下的 accounts.json、token-secret 等本地文件，
// 不重载会让账号与密钥相关的测试结果随环境漂移。
func TestMain(m *testing.M) {
	if os.Getenv("USAGE_DB_PATH") == "" {
		_ = os.Setenv("USAGE_DB_PATH", "")
	}
	code := func() int {
		if os.Getenv("DATA_DIR") == "" {
			dir, err := os.MkdirTemp("", "unigate-test-data")
			if err != nil {
				fmt.Fprintln(os.Stderr, "create test data dir:", err)
				os.Exit(1)
			}
			_ = os.Setenv("DATA_DIR", dir)
			defer os.RemoveAll(dir)
		}
		resetCfgForTest()
		return m.Run()
	}()
	os.Exit(code)
}

func TestUsageDBAppendAndPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")

	db := newUsageDB(path, 30, 1000)
	defer db.Close()
	db.Append(UsageEvent{Time: time.Now(), User: "alice", Model: "m1", Key: "sk-a",
		PromptTokens: 100, CompletionTokens: 50, Status: 200})
	db.Append(UsageEvent{Time: time.Now(), User: "bob", Model: "m2", Key: "sk-b",
		PromptTokens: 10, CompletionTokens: 5, Status: 500})

	// 重新加载，验证持久化
	db2 := newUsageDB(path, 30, 1000)
	defer db2.Close()
	res := db2.Query(UsageFilter{Window: "all"})
	if res.Requests != 2 {
		t.Fatalf("requests = %d, want 2", res.Requests)
	}
	if res.PromptTokens != 110 || res.CompletionTokens != 55 || res.TotalTokens != 165 {
		t.Fatalf("tokens = %+v", res)
	}
}

func TestUsageDBWindowFilter(t *testing.T) {
	dir := t.TempDir()
	db := newUsageDB(filepath.Join(dir, "usage.db"), 30, 1000)
	defer db.Close()

	now := time.Now()
	// 今天（within today）
	db.Append(UsageEvent{Time: now, User: "alice", Model: "m1", Key: "sk-a", PromptTokens: 100, Status: 200})
	// 昨天（25h 前）
	db.Append(UsageEvent{Time: now.Add(-25 * time.Hour), User: "alice", Model: "m1", Key: "sk-a", PromptTokens: 50, Status: 200})
	// 8 天前
	db.Append(UsageEvent{Time: now.Add(-8 * 24 * time.Hour), User: "bob", Model: "m2", Key: "sk-b", PromptTokens: 30, Status: 200})

	if res := db.Query(UsageFilter{Window: "today"}); res.Requests != 1 || res.PromptTokens != 100 {
		t.Fatalf("today = %+v", res)
	}
	if res := db.Query(UsageFilter{Window: "24h"}); res.Requests != 1 {
		t.Fatalf("24h = %+v", res)
	}
	if res := db.Query(UsageFilter{Window: "7d"}); res.Requests != 2 {
		t.Fatalf("7d = %+v", res)
	}
	if res := db.Query(UsageFilter{Window: "30d"}); res.Requests != 3 {
		t.Fatalf("30d = %+v", res)
	}
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 3 {
		t.Fatalf("all = %+v", res)
	}
}

func TestUsageDBDimensionFilter(t *testing.T) {
	dir := t.TempDir()
	db := newUsageDB(filepath.Join(dir, "usage.db"), 30, 1000)
	defer db.Close()
	now := time.Now()
	db.Append(UsageEvent{Time: now, User: "alice", Model: "m1", Key: "sk-a", PromptTokens: 100, Status: 200})
	db.Append(UsageEvent{Time: now, User: "bob", Model: "m2", Key: "sk-b", PromptTokens: 50, Status: 200})

	res := db.Query(UsageFilter{Window: "all", User: "alice"})
	if res.Requests != 1 || res.PromptTokens != 100 {
		t.Fatalf("by user = %+v", res)
	}
	res = db.Query(UsageFilter{Window: "all", Model: "m2"})
	if res.Requests != 1 {
		t.Fatalf("by model = %+v", res)
	}
	res = db.Query(UsageFilter{Window: "all", Key: "sk-a"})
	if res.Requests != 1 {
		t.Fatalf("by key = %+v", res)
	}
	res = db.Query(UsageFilter{Window: "all", User: "nobody"})
	if res.Requests != 0 {
		t.Fatalf("none = %+v", res)
	}
}

func TestUsageDBBreakdowns(t *testing.T) {
	dir := t.TempDir()
	db := newUsageDB(filepath.Join(dir, "usage.db"), 30, 1000)
	defer db.Close()
	now := time.Now()
	db.Append(UsageEvent{Time: now, User: "alice", Model: "m1", Key: "sk-a", PromptTokens: 100, CompletionTokens: 20, Status: 200})
	db.Append(UsageEvent{Time: now, User: "alice", Model: "m2", Key: "sk-a", PromptTokens: 50, CompletionTokens: 10, Status: 500})
	db.Append(UsageEvent{Time: now, User: "bob", Model: "m1", Key: "sk-b", PromptTokens: 30, Status: 200})

	res := db.Query(UsageFilter{Window: "all"})
	if len(res.ByUser) != 2 || res.ByUser[0].Name != "alice" || res.ByUser[0].Requests != 2 || res.ByUser[0].Errors != 1 {
		t.Fatalf("by_user = %+v", res.ByUser)
	}
	if len(res.ByModel) != 2 || res.ByModel[0].Name != "m1" || res.ByModel[0].Requests != 2 {
		t.Fatalf("by_model = %+v", res.ByModel)
	}
	if len(res.ByKey) != 2 || res.ByKey[0].Name != "sk-a" || res.ByKey[0].TotalTokens != 180 {
		t.Fatalf("by_key = %+v", res.ByKey)
	}
	if res.Errors != 1 {
		t.Fatalf("errors = %d, want 1", res.Errors)
	}
}

func TestUsageDBRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	db := newUsageDB(path, 2, 10) // 2 天保留
	defer db.Close()
	now := time.Now()
	db.Append(UsageEvent{Time: now, User: "a", Model: "m", Key: "k", Status: 200})
	db.Append(UsageEvent{Time: now.Add(-3 * 24 * time.Hour), User: "b", Model: "m", Key: "k", Status: 200}) // 过期

	// 重新加载：过期事件应被丢弃
	db2 := newUsageDB(path, 2, 10)
	defer db2.Close()
	if res := db2.Query(UsageFilter{Window: "all"}); res.Requests != 1 {
		t.Fatalf("requests = %d, want 1", res.Requests)
	}
}

func TestUsageDBCompactOnOverflow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	db := newUsageDB(path, 30, 5) // 上限 5
	defer db.Close()
	now := time.Now()
	for i := 0; i < 10; i++ {
		db.Append(UsageEvent{Time: now, User: "u", Model: "m", Key: "k", Status: 200})
	}
	db.Cleanup() // 触发压缩
	if n := db.Count(); n > 5 {
		t.Fatalf("count = %d, want <= 5 after compact", n)
	}
}

// TestParseUsageJSON 验证 usage 解析。
func TestParseUsageJSON(t *testing.T) {
	p, c := parseUsageJSON([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":34}}`))
	if p != 12 || c != 34 {
		t.Fatalf("got (%d,%d)", p, c)
	}
	p, c = parseUsageJSON([]byte(`{"choices":[]}`))
	if p != 0 || c != 0 {
		t.Fatalf("no usage should be 0: (%d,%d)", p, c)
	}
}

// TestParseUsageFromFrame 验证流式帧 usage 解析。
func TestParseUsageFromFrame(t *testing.T) {
	frame := []byte("data: {\"id\":\"x\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":8}}\n\n")
	p, c, ok := parseUsageFromFrame(frame)
	if !ok || p != 7 || c != 8 {
		t.Fatalf("got (%d,%d,%v)", p, c, ok)
	}
	p, c, ok = parseUsageFromFrame([]byte("data: [DONE]\n\n"))
	if ok {
		t.Fatal("[DONE] should not parse usage")
	}
}

// TestUsageDBEmptyPath 验证空路径为纯内存模式。
func TestUsageDBEmptyPath(t *testing.T) {
	db := newUsageDB("", 30, 100)
	db.Append(UsageEvent{Time: time.Now(), User: "a", Model: "m", Key: "k", Status: 200})
	if res := db.Query(UsageFilter{Window: "all"}); res.Requests != 1 {
		t.Fatalf("res = %+v", res)
	}
}

// TestUsageDBRequestLogTables：请求/错误日志持久化（Add/Query/Clear + 重启不丢）。
func TestUsageDBRequestLogTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.db")
	db := newUsageDB(path, 30, 1000)
	if !db.Available() {
		t.Fatal("db should be available")
	}

	db.LogAppend(logTableRequest, RequestRecord{ID: "r1", Time: time.Now(), Status: 200, Path: "/v1/chat/completions"}, 0)
	db.LogAppend(logTableRequest, RequestRecord{ID: "r2", Time: time.Now(), Status: 502,
		Path: "/v1/chat/completions", ErrMsg: "all keys cooling"}, 0)
	db.LogAppend(logTableError, RequestRecord{ID: "e1", Time: time.Now(), Status: 502, ErrMsg: "boom"}, 0)
	db.Close()

	// 重新打开：日志仍在（这是相对内存环形缓冲的关键改进）
	db2 := newUsageDB(path, 30, 1000)
	defer db2.Close()
	recs, total := db2.LogQuery(logTableRequest, 1, 10)
	if total != 2 || len(recs) != 2 {
		t.Fatalf("request_log total=%d len=%d", total, len(recs))
	}
	// 最新在前
	if recs[0].ID != "r2" {
		t.Fatalf("order wrong, newest first expected: %+v", recs)
	}
	if recs[0].ErrMsg != "all keys cooling" {
		t.Fatalf("error text lost: %+v", recs[0])
	}
	errs, etotal := db2.LogQuery(logTableError, 1, 10)
	if etotal != 1 || len(errs) != 1 || errs[0].ID != "e1" {
		t.Fatalf("error_log = %+v", errs)
	}

	// 分页
	if page2, _ := db2.LogQuery(logTableRequest, 2, 1); len(page2) != 1 || page2[0].ID != "r1" {
		t.Fatalf("page2 = %+v", page2)
	}

	// 清空
	if n := db2.LogClear(logTableRequest); n != 2 {
		t.Fatalf("cleared = %d, want 2", n)
	}
	if _, total := db2.LogQuery(logTableRequest, 1, 10); total != 0 {
		t.Fatalf("request_log should be empty, total=%d", total)
	}
}

// TestUsageDBLogPrune：logPrune 只保留最新 keep 条。
func TestUsageDBLogPrune(t *testing.T) {
	db := newUsageDB("", 30, 1000)
	defer db.Close()
	for i := 0; i < 20; i++ {
		db.LogAppend(logTableRequest, RequestRecord{ID: fmt.Sprintf("r%02d", i), Time: time.Now(), Status: 200}, 0)
	}
	db.logPrune(logTableRequest, 5)
	recs, total := db.LogQuery(logTableRequest, 1, 100)
	if total != 5 || len(recs) != 5 {
		t.Fatalf("after prune total=%d len=%d", total, len(recs))
	}
	// 保留的是最新的 5 条
	if recs[0].ID != "r19" || recs[4].ID != "r15" {
		t.Fatalf("kept wrong rows: %v .. %v", recs[0].ID, recs[4].ID)
	}
}

// TestUsageDBInvalidLogTable：表名白名单之外的调用一律拒绝（防 SQL 注入）。
func TestUsageDBInvalidLogTable(t *testing.T) {
	db := newUsageDB("", 30, 1000)
	defer db.Close()
	db.LogAppend("usage_events; DROP TABLE usage_events", RequestRecord{ID: "x"}, 0)
	if _, total := db.LogQuery("evil", 1, 10); total != 0 {
		t.Fatalf("invalid table should return empty, total=%d", total)
	}
	if n := db.LogClear("evil"); n != 0 {
		t.Fatalf("invalid table clear = %d", n)
	}
}

// TestUsageDBMaskStoredKeys：历史明文 key 就地脱敏（仅精确匹配现存真实
// api_key），幂等（再跑一次不改动）；「名称@渠道」等非凭证文本不误伤。
func TestUsageDBMaskStoredKeys(t *testing.T) {
	db := newUsageDB("", 30, 1000)
	defer db.Close()
	now := time.Now()
	db.Append(UsageEvent{Time: now, User: "a", Model: "m", Key: "sk-plaintextkey12345", Status: 200})
	db.Append(UsageEvent{Time: now, User: "a", Model: "m", Key: "sk-plaintextkey12345", Status: 200})
	db.Append(UsageEvent{Time: now, User: "b", Model: "m", Key: "alread****masked", Status: 200})
	db.Append(UsageEvent{Time: now, User: "c", Model: "m", Key: "daphne@cline", Status: 200})

	secrets := map[string]bool{"sk-plaintextkey12345": true}
	if n := db.MaskStoredKeys(secrets); n != 2 {
		t.Fatalf("masked rows = %d, want 2", n)
	}
	if n := db.MaskStoredKeys(secrets); n != 0 {
		t.Fatalf("second run should be a no-op, got %d", n)
	}

	// 明文已消失，且 by_key 维度仍可用（掩码值分组）
	var plain int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM usage_events WHERE key = ?`, "sk-plaintextkey12345").Scan(&plain); err != nil {
		t.Fatal(err)
	}
	if plain != 0 {
		t.Fatalf("plaintext key still present: %d rows", plain)
	}
	// 「名称@渠道」不是凭证：迁移不得动它
	var names int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM usage_events WHERE key = ?`, "daphne@cline").Scan(&names); err != nil {
		t.Fatal(err)
	}
	if names != 1 {
		t.Fatalf("key name rows corrupted: %d", names)
	}
	res := db.Query(UsageFilter{Window: "all"})
	found, nameFound := false, false
	for _, b := range res.ByKey {
		if b.Name == maskKey("sk-plaintextkey12345") && b.Requests == 2 {
			found = true
		}
		if b.Name == "daphne@cline" && b.Requests == 1 {
			nameFound = true
		}
	}
	if !found {
		t.Fatalf("masked key group missing from by_key: %+v", res.ByKey)
	}
	if !nameFound {
		t.Fatalf("key name group missing from by_key: %+v", res.ByKey)
	}
}

// TestUsageDBAvailable：空路径（内存库）也应可用。
func TestUsageDBAvailable(t *testing.T) {
	db := newUsageDB("", 30, 100)
	defer db.Close()
	if !db.Available() {
		t.Fatal("in-memory db should be available")
	}
	var nilDB *UsageDB
	if nilDB.Available() {
		t.Fatal("nil db should not be available")
	}
	if _, total := nilDB.LogQuery(logTableRequest, 1, 10); total != 0 {
		t.Fatalf("nil db query total=%d", total)
	}
}
