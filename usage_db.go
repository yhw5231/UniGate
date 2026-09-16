// 用量数据库：SQLite（modernc.org/sqlite 纯 Go 驱动，无 CGO）记录每次请求的
// 输入/输出 token（来自上游 usage），按用户/模型/上游 key 记账，
// 支持 今日/24h/7天/30天/全部 窗口与条件过滤。
package main

import (
	"database/sql"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // 注册 "sqlite" 驱动
)

// UsageEvent 一条用量事件（对应 usage_events 表一行）。
type UsageEvent struct {
	ID               string    `json:"id"`
	Time             time.Time `json:"time"`
	User             string    `json:"user,omitempty"`
	Channel          string    `json:"channel,omitempty"`
	Model            string    `json:"model,omitempty"`
	Key              string    `json:"key,omitempty"` // 上游 key（原始，内部记账用）
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	Status           int       `json:"status"`
	BytesOut         int64     `json:"bytes_out"`
}

// UsageDB SQLite 用量库。
type UsageDB struct {
	mu            sync.Mutex
	path          string // 空 = :memory:
	retentionDays int
	maxRecords    int
	db            *sql.DB
	appendCount   int
}

// usageSchema 用量库 + 请求/错误日志表。日志表与用量事件分离：用量库是
// 聚合账本（可按维度分组），日志表保存逐条请求明细（含失败诊断），重启不丢。
const usageSchema = `
CREATE TABLE IF NOT EXISTS usage_events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	time INTEGER NOT NULL,
	user TEXT NOT NULL DEFAULT '',
	channel TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	key TEXT NOT NULL DEFAULT '',
	prompt_tokens INTEGER NOT NULL DEFAULT 0,
	completion_tokens INTEGER NOT NULL DEFAULT 0,
	status INTEGER NOT NULL DEFAULT 0,
	bytes_out INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_usage_time ON usage_events(time);
CREATE INDEX IF NOT EXISTS idx_usage_user ON usage_events(user);
CREATE INDEX IF NOT EXISTS idx_usage_model ON usage_events(model);
CREATE INDEX IF NOT EXISTS idx_usage_key ON usage_events(key);
CREATE INDEX IF NOT EXISTS idx_usage_channel ON usage_events(channel);

CREATE TABLE IF NOT EXISTS request_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	rid TEXT NOT NULL DEFAULT '',
	time INTEGER NOT NULL,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	method TEXT NOT NULL DEFAULT '',
	path TEXT NOT NULL DEFAULT '',
	status INTEGER NOT NULL DEFAULT 0,
	bytes_out INTEGER NOT NULL DEFAULT 0,
	client_ip TEXT NOT NULL DEFAULT '',
	user TEXT NOT NULL DEFAULT '',
	channel TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	key TEXT NOT NULL DEFAULT '',
	prompt_tokens INTEGER NOT NULL DEFAULT 0,
	completion_tokens INTEGER NOT NULL DEFAULT 0,
	error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_request_log_time ON request_log(time);

CREATE TABLE IF NOT EXISTS error_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	rid TEXT NOT NULL DEFAULT '',
	time INTEGER NOT NULL,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	method TEXT NOT NULL DEFAULT '',
	path TEXT NOT NULL DEFAULT '',
	status INTEGER NOT NULL DEFAULT 0,
	bytes_out INTEGER NOT NULL DEFAULT 0,
	client_ip TEXT NOT NULL DEFAULT '',
	user TEXT NOT NULL DEFAULT '',
	channel TEXT NOT NULL DEFAULT '',
	model TEXT NOT NULL DEFAULT '',
	key TEXT NOT NULL DEFAULT '',
	prompt_tokens INTEGER NOT NULL DEFAULT 0,
	completion_tokens INTEGER NOT NULL DEFAULT 0,
	error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_error_log_time ON error_log(time);
`

// logTableRequest / logTableError 允许的日志表名（SQL 表名白名单，杜绝拼接注入）。
const (
	logTableRequest = "request_log"
	logTableError   = "error_log"
)

func validLogTable(table string) bool {
	return table == logTableRequest || table == logTableError
}

// usageMigrations 老库补列（ALTER TABLE 幂等性由检查保证）。
var usageMigrations = []string{
	`ALTER TABLE usage_events ADD COLUMN channel TEXT NOT NULL DEFAULT ''`,
}

func newUsageDB(path string, retentionDays, maxRecords int) *UsageDB {
	db := &UsageDB{
		path:          path,
		retentionDays: retentionDays,
		maxRecords:    maxRecords,
	}
	if retentionDays <= 0 {
		db.retentionDays = 30
	}
	if maxRecords <= 0 {
		db.maxRecords = 100000
	}
	dsn := path
	if dsn == "" {
		dsn = ":memory:"
	}
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return db // 打开失败则保持 nil db，Append/Query 均为空操作
	}
	// 单连接：保证 :memory: 语义一致，也避免 SQLite 写锁冲突
	sqldb.SetMaxOpenConns(1)
	db.db = sqldb
	if _, err := sqldb.Exec(usageSchema); err == nil {
		db.migrate(sqldb)
		db.cleanupLocked()
	}
	return db
}

// migrate 执行增量迁移（列已存在时忽略错误）。
func (db *UsageDB) migrate(sqldb *sql.DB) {
	for _, stmt := range usageMigrations {
		_, _ = sqldb.Exec(stmt)
	}
}

// Close 关闭底层连接。
func (db *UsageDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

// Append 记录一条事件（INSERT）。
func (db *UsageDB) Append(ev UsageEvent) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return
	}
	_, err := db.db.Exec(`INSERT INTO usage_events
		(time, user, channel, model, key, prompt_tokens, completion_tokens, status, bytes_out)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		ev.Time.UnixNano(), ev.User, ev.Channel, ev.Model, ev.Key,
		ev.PromptTokens, ev.CompletionTokens, ev.Status, ev.BytesOut)
	if err != nil {
		return
	}
	db.appendCount++
	if db.appendCount%512 == 0 {
		db.cleanupLocked() // 定期清理，避免大表膨胀
	}
}

// Cleanup 主动清理过期事件并裁剪到 maxRecords（可被定时任务调用）。
func (db *UsageDB) Cleanup() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.cleanupLocked()
}

// cleanupLocked 丢弃保留期外事件，并裁剪到 maxRecords（调用方需持锁）。
func (db *UsageDB) cleanupLocked() {
	if db.db == nil {
		return
	}
	cutoff := time.Now().AddDate(0, 0, -db.retentionDays).UnixNano()
	_, _ = db.db.Exec(`DELETE FROM usage_events WHERE time < ?`, cutoff)
	// 只保留最近 maxRecords 条（按 time 降序取前 N，删除其余）
	if db.maxRecords > 0 {
		_, _ = db.db.Exec(`DELETE FROM usage_events WHERE id NOT IN (
			SELECT id FROM usage_events ORDER BY time DESC, id DESC LIMIT ?)`, db.maxRecords)
	}
}

// Available 报告底层数据库是否可用（打开失败时为 false，全部读写降级为空操作）。
func (db *UsageDB) Available() bool {
	if db == nil {
		return false
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.db != nil
}

// Count 返回当前事件总数。
func (db *UsageDB) Count() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return 0
	}
	var n int64
	_ = db.db.QueryRow(`SELECT COUNT(*) FROM usage_events`).Scan(&n)
	return n
}

// UsageFilter 查询条件。
type UsageFilter struct {
	Window  string // "today" | "24h" | "7d" | "30d" | "all"（默认 all）
	Start   time.Time
	End     time.Time
	User    string
	Channel string
	Model   string
	Key     string
}

// UsageBreakdown 单维度聚合。
type UsageBreakdown struct {
	Name             string `json:"name"`
	Requests         int64  `json:"requests"`
	Errors           int64  `json:"errors"`
	BytesOut         int64  `json:"bytes_out"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	TotalTokens      int64  `json:"total_tokens"`
}

// UsageResult 聚合结果。
type UsageResult struct {
	Window           string           `json:"window"`
	Start            time.Time        `json:"start"`
	End              time.Time        `json:"end"`
	Requests         int64            `json:"requests"`
	Errors           int64            `json:"errors"`
	BytesOut         int64            `json:"bytes_out"`
	PromptTokens     int64            `json:"prompt_tokens"`
	CompletionTokens int64            `json:"completion_tokens"`
	TotalTokens      int64            `json:"total_tokens"`
	ByUser           []UsageBreakdown `json:"by_user,omitempty"`
	ByChannel        []UsageBreakdown `json:"by_channel,omitempty"`
	ByModel          []UsageBreakdown `json:"by_model,omitempty"`
	ByKey            []UsageBreakdown `json:"by_key,omitempty"`
}

// resolveWindow 把窗口字符串解析成 [start,end)。
func resolveWindow(win string, now time.Time) (time.Time, time.Time) {
	switch win {
	case "today":
		y, m, d := now.Date()
		start := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		return start, now
	case "24h":
		return now.Add(-24 * time.Hour), now
	case "7d":
		return now.Add(-7 * 24 * time.Hour), now
	case "30d":
		return now.Add(-30 * 24 * time.Hour), now
	default: // all / 空
		return time.Time{}, time.Time{}
	}
}

// buildWhere 构建 WHERE 子句与参数（时间用 UnixNano，保证亚秒精度）。
func buildWhere(f UsageFilter) (string, []any) {
	now := time.Now()
	start, end := resolveWindow(f.Window, now)
	if !f.Start.IsZero() {
		start = f.Start
	}
	if !f.End.IsZero() {
		end = f.End
	}
	conds := []string{"1=1"}
	args := []any{}
	if !start.IsZero() {
		conds = append(conds, "time >= ?")
		args = append(args, start.UnixNano())
	}
	if !end.IsZero() {
		conds = append(conds, "time < ?")
		args = append(args, end.UnixNano())
	}
	if f.User != "" {
		conds = append(conds, "user = ?")
		args = append(args, f.User)
	}
	if f.Channel != "" {
		conds = append(conds, "channel = ?")
		args = append(args, f.Channel)
	}
	if f.Model != "" {
		conds = append(conds, "model = ?")
		args = append(args, f.Model)
	}
	if f.Key != "" {
		conds = append(conds, "key = ?")
		args = append(args, f.Key)
	}
	return strings.Join(conds, " AND "), args
}

// Query 按条件聚合用量（SQL 聚合，含 by_user/by_model/by_key 分解）。
func (db *UsageDB) Query(f UsageFilter) UsageResult {
	now := time.Now()
	start, end := resolveWindow(f.Window, now)
	if !f.Start.IsZero() {
		start = f.Start
	}
	if !f.End.IsZero() {
		end = f.End
	}
	res := UsageResult{Window: f.Window, Start: start, End: end}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return res
	}

	where, args := buildWhere(f)

	// 总量
	var promptTok, compTok, bytesOut, errors int64
	err := db.db.QueryRow(`SELECT COUNT(*),
		COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0),
		COALESCE(SUM(bytes_out),0),
		COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END),0)
		FROM usage_events WHERE `+where, args...).
		Scan(&res.Requests, &promptTok, &compTok, &bytesOut, &errors)
	if err != nil {
		return res
	}
	res.PromptTokens = promptTok
	res.CompletionTokens = compTok
	res.BytesOut = bytesOut
	res.Errors = errors
	res.TotalTokens = promptTok + compTok

	res.ByUser = db.queryBreakdown(where, args, "user")
	res.ByChannel = db.queryBreakdown(where, args, "channel")
	res.ByModel = db.queryBreakdown(where, args, "model")
	res.ByKey = db.queryBreakdown(where, args, "key")
	return res
}

// queryBreakdown 按某列分组聚合（col 为 user/model/key）。
func (db *UsageDB) queryBreakdown(where string, args []any, col string) []UsageBreakdown {
	rows, err := db.db.Query(`SELECT COALESCE(NULLIF(`+col+`,''),'unknown') AS name,
		COUNT(*),
		COALESCE(SUM(CASE WHEN status >= 400 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(bytes_out),0),
		COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0)
		FROM usage_events WHERE `+where+` GROUP BY name`, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []UsageBreakdown
	for rows.Next() {
		var b UsageBreakdown
		if err := rows.Scan(&b.Name, &b.Requests, &b.Errors, &b.BytesOut, &b.PromptTokens, &b.CompletionTokens); err != nil {
			continue
		}
		b.TotalTokens = b.PromptTokens + b.CompletionTokens
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TotalTokens > out[j].TotalTokens })
	return out
}

// ---- 请求/错误日志（持久化环形缓冲） ----

// LogAppend 追加一条日志记录，并把表裁剪到 keep 条以内（保留最新）。
// keep <= 0 时仅追加不裁剪。
func (db *UsageDB) LogAppend(table string, rec RequestRecord, keep int) {
	if db == nil || !validLogTable(table) {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return
	}
	_, err := db.db.Exec(`INSERT INTO `+table+`
		(rid, time, duration_ms, method, path, status, bytes_out, client_ip, user, channel, model, key, prompt_tokens, completion_tokens, error)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ID, rec.Time.UnixNano(), rec.DurationMs, rec.Method, rec.Path, rec.Status,
		rec.BytesOut, rec.ClientIP, rec.User, rec.Channel, rec.Model, rec.Key,
		rec.PromptTokens, rec.CompletionTokens, rec.ErrMsg)
	if err != nil {
		log.Printf("usage db: append %s: %v", table, err)
		return
	}
	if keep > 0 {
		// 环形缓冲语义：只保留最新 keep 条
		_, _ = db.db.Exec(`DELETE FROM `+table+` WHERE id NOT IN (
			SELECT id FROM `+table+` ORDER BY id DESC LIMIT ?)`, keep)
	}
}

// LogQuery 分页返回日志（最新在前）。返回 (记录, 总数)。
func (db *UsageDB) LogQuery(table string, page, pageSize int) ([]RequestRecord, int) {
	if db == nil || !validLogTable(table) {
		return []RequestRecord{}, 0
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return []RequestRecord{}, 0
	}
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1
	}
	var total int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&total); err != nil {
		return []RequestRecord{}, 0
	}
	rows, err := db.db.Query(`SELECT rid, time, duration_ms, method, path, status, bytes_out,
		client_ip, user, channel, model, key, prompt_tokens, completion_tokens, error
		FROM `+table+` ORDER BY id DESC LIMIT ? OFFSET ?`, pageSize, (page-1)*pageSize)
	if err != nil {
		return []RequestRecord{}, total
	}
	defer rows.Close()
	out := make([]RequestRecord, 0, pageSize)
	for rows.Next() {
		var (
			rec  RequestRecord
			unix int64
		)
		if err := rows.Scan(&rec.ID, &unix, &rec.DurationMs, &rec.Method, &rec.Path, &rec.Status,
			&rec.BytesOut, &rec.ClientIP, &rec.User, &rec.Channel, &rec.Model, &rec.Key,
			&rec.PromptTokens, &rec.CompletionTokens, &rec.ErrMsg); err != nil {
			continue
		}
		rec.Time = time.Unix(0, unix)
		rec.Duration = time.Duration(rec.DurationMs) * time.Millisecond
		out = append(out, rec)
	}
	return out, total
}

// logPrune 把日志表裁剪到最新 keep 条（环形上限）。
func (db *UsageDB) logPrune(table string, keep int) {
	if db == nil || !validLogTable(table) || keep <= 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return
	}
	_, _ = db.db.Exec(`DELETE FROM `+table+` WHERE id NOT IN (
		SELECT id FROM `+table+` ORDER BY id DESC LIMIT ?)`, keep)
}

// LogClear 清空日志表，返回删除条数。
func (db *UsageDB) LogClear(table string) int64 {
	if db == nil || !validLogTable(table) {
		return 0
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return 0
	}
	res, err := db.db.Exec(`DELETE FROM ` + table)
	if err != nil {
		log.Printf("usage db: clear %s: %v", table, err)
		return 0
	}
	n, _ := res.RowsAffected()
	return n
}

// MaskStoredKeys 把历史遗留的明文上游 key 就地脱敏（升级迁移，幂等）。
// 仅精确匹配 secrets（store 里现存的真实 api_key）的行会被脱敏：请求记录/
// 用量库现在写入的是「key 名称@渠道」的用户标签而非凭证，不匹配凭证的值
// 一律不动，避免误伤正常名称。返回更新的行数。
func (db *UsageDB) MaskStoredKeys(secrets map[string]bool) int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return 0
	}
	rows, err := db.db.Query(`SELECT DISTINCT key FROM usage_events
		WHERE key <> '' AND key NOT LIKE '%****%'`)
	if err != nil {
		return 0
	}
	var plain []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err == nil && k != "" && secrets[k] {
			plain = append(plain, k)
		}
	}
	rows.Close()
	var n int64
	for _, k := range plain {
		res, err := db.db.Exec(`UPDATE usage_events SET key = ? WHERE key = ?`, maskKey(k), k)
		if err != nil {
			continue
		}
		if c, err := res.RowsAffected(); err == nil {
			n += c
		}
	}
	return n
}

// ---- 全局实例 ----

var usageDB *UsageDB

// initUsageDB 用配置初始化用量库（幂等，reloadConfig 时调用）。
func initUsageDB() {
	if usageDB != nil {
		_ = usageDB.Close() // 释放旧连接，避免 reload 泄漏
	}
	usageDB = newUsageDB(cfg.UsageDBPath, cfg.UsageRetentionDays, cfg.UsageMaxRecords)
}

// handleAdminUsage 返回窗口化用量统计（?window=&user=&model=&key=）。
func handleAdminUsage(w http.ResponseWriter, r *http.Request) {
	f := UsageFilter{
		Window:  r.URL.Query().Get("window"),
		User:    r.URL.Query().Get("user"),
		Channel: r.URL.Query().Get("channel"),
		Model:   r.URL.Query().Get("model"),
		Key:     r.URL.Query().Get("key"),
	}
	if f.Window == "" {
		f.Window = "today"
	}
	if f.Window != "today" && f.Window != "24h" && f.Window != "7d" &&
		f.Window != "30d" && f.Window != "all" {
		writeJSONError(w, http.StatusBadRequest, "invalid window; use today|24h|7d|30d|all", "bad_request")
		return
	}
	res := usageDB.Query(f)
	writeJSON(w, http.StatusOK, res)
}
