// 可观察性：请求/错误记录日志（SQLite 持久化，无数据库时退化为内存环形缓冲）。
// 通过 GET /admin/api/requests 与 GET /admin/api/errors 查询（需管理员登录）；
// 窗口化聚合统计见 usage_db.go 的 GET /admin/api/usage。
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ---- 请求记录 ----

// RequestRecord 一条大模型请求记录。
type RequestRecord struct {
	ID               string        `json:"id"`
	Time             time.Time     `json:"time"`
	Duration         time.Duration `json:"-"`
	DurationMs       int64         `json:"duration_ms"`
	Method           string        `json:"method"`
	Path             string        `json:"path"`
	Status           int           `json:"status"`
	BytesOut         int64         `json:"bytes_out"`
	ClientIP         string        `json:"client_ip,omitempty"`
	User             string        `json:"user,omitempty"`
	Channel          string        `json:"channel,omitempty"`
	Model            string        `json:"model,omitempty"`
	Key              string        `json:"key,omitempty"` // 「key 名称@渠道」——名称是用户自定义标签而非凭证，完整显示
	PromptTokens     int64         `json:"prompt_tokens,omitempty"`
	CompletionTokens int64         `json:"completion_tokens,omitempty"`
	ErrMsg           string        `json:"error,omitempty"`
}

// RequestLog 请求日志：固定容量，保留最新 capacity 条。挂上 SQLite 表名后
// 以数据库为存储（重启不丢），内存环形缓冲退化为无数据库时的兜底
// （单元测试 / USAGE_DB_PATH 打开失败时的降级路径）。
type RequestLog struct {
	mu   sync.Mutex
	recs []RequestRecord
	next int // 下一个写入位置
	cap  int
	n    int // 已写条数（含被覆盖的）

	// table 由 initStats 在开始服务前设置一次，之后只读（不参与 mu 保护，
	// 避免与 Add/Query 的自有锁嵌套）。
	table     string
	dbAppends int // 距上次裁剪的写入计数（由 mu 保护）
}

func newRequestLog(capacity int) *RequestLog {
	if capacity <= 0 {
		capacity = 1
	}
	return &RequestLog{recs: make([]RequestRecord, capacity), cap: capacity}
}

// attachTable 把日志挂到 SQLite 表（logTableRequest / logTableError），并立即
// 裁一次（重启/挂载时表内可能积存上次运行的过量记录）。只应在启动初始化
// 阶段调用。
func (l *RequestLog) attachTable(table string) {
	l.mu.Lock()
	l.table = table
	keep := l.cap
	hasDB := usageDB != nil
	l.mu.Unlock()
	if hasDB && keep > 0 {
		usageDB.logPrune(table, keep)
	}
}

// dbLog 返回挂载的日志表（未挂载或无用量库实例时为空串）。库暂时不可用也算
// 挂载：写入会失败并退回内存环形缓冲，库恢复后自动继续落库。
func (l *RequestLog) dbLog() string {
	if l.table == "" || usageDB == nil {
		return ""
	}
	if !validLogTable(l.table) {
		return ""
	}
	return l.table
}

// logPruneEvery 每写入这么多条裁剪一次（而非每条都裁剪）：环形上限仍然生效，
// 允许瞬时超出若干条以摊薄 DELETE 的成本。
const logPruneEvery = 256

// Add 写入一条记录（有数据库时持久化，否则写入内存环形缓冲）。
// 库暂时不可用（LogAppend 失败）时同样退回内存环形缓冲：记录仍能在 WebUI 里
// 看到，库恢复后新记录继续落库（这段窗口内的旧记录不会被回填）。
func (l *RequestLog) Add(rec RequestRecord) {
	table := l.dbLog()
	if table == "" {
		l.addMem(rec)
		return
	}
	l.mu.Lock()
	l.dbAppends++
	prune := l.dbAppends%logPruneEvery == 0
	keep := l.cap
	l.mu.Unlock()
	if err := usageDB.LogAppend(table, rec, 0); err != nil {
		l.addMem(rec)
		return
	}
	if prune {
		usageDB.logPrune(table, keep)
	}
}

// addMem 写入内存环形缓冲（最新覆盖最旧），供无数据库或库不可用时使用。
func (l *RequestLog) addMem(rec RequestRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs[l.next] = rec
	l.next = (l.next + 1) % l.cap
	l.n++
}

// Snapshot 返回现有记录，最新在前。库可用时以库为准（即使为空 = 已被清空）；
// 库暂时不可用时退回内存环形缓冲——库不可用期间写入的记录只存在于内存里。
func (l *RequestLog) Snapshot() []RequestRecord {
	if table := l.dbLog(); table != "" {
		recs, _ := usageDB.LogQuery(table, 1, l.cap)
		if len(recs) > 0 || usageDB.Available() {
			return recs
		}
	}
	return l.memSnapshot()
}

// memSnapshot 内存环形缓冲里的记录（最新在前）。
func (l *RequestLog) memSnapshot() []RequestRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]RequestRecord, 0, l.count())
	for i := 0; i < l.count(); i++ {
		idx := (l.next - 1 - i + l.cap) % l.cap
		out = append(out, l.recs[idx])
	}
	return out
}

func (l *RequestLog) count() int {
	if l.n < l.cap {
		return l.n
	}
	return l.cap
}

// Clear 清空全部记录（管理员在 WebUI 手动清空日志用）。重新分配底层数组，
// 顺带释放已存错误信息（单条可达 8KB）的内存引用。库不可用期间写入内存环形
// 缓冲的记录一并清掉，避免清空后又被读出来。
func (l *RequestLog) Clear() {
	l.mu.Lock()
	l.recs = make([]RequestRecord, l.cap)
	l.next, l.n = 0, 0
	l.dbAppends = 0
	l.mu.Unlock()
	if table := l.dbLog(); table != "" {
		usageDB.LogClear(table)
	}
}

// Query 分页返回记录（最新在前；page 从 1 起，越界返回空页）。
func (l *RequestLog) Query(page, pageSize int) ([]RequestRecord, int) {
	if table := l.dbLog(); table != "" {
		recs, total := usageDB.LogQuery(table, page, pageSize)
		if len(recs) > 0 || total > 0 || usageDB.Available() {
			return recs, total
		}
	}
	all := l.memSnapshot()
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 1
	}
	start := (page - 1) * pageSize
	if start >= len(all) {
		return []RequestRecord{}, len(all)
	}
	end := start + pageSize
	if end > len(all) {
		end = len(all)
	}
	return all[start:end], len(all)
}

// ---- 全局实例 ----

var (
	reqLog *RequestLog // 全部请求（环形缓冲，新记录覆盖最旧）
	errLog *RequestLog // 错误记录（独立环形缓冲，不被成功请求挤出）
)

// errMsgMax 请求记录错误信息的最大长度（逐 key 失败轨迹完整保留，仅设上限防膨胀）。
const errMsgMax = 8192

// initStats 用配置初始化（幂等，reloadConfig 时调用）。用量库就绪时把请求/错误
// 日志挂到 SQLite 表（重启不丢），否则退化为内存环形缓冲。
// 必须先 initUsageDB 再调用本函数。
// 注意：建库失败时也照样挂表（LogAppend 会失败并退回内存环形缓冲），这样库一旦
// 运行期自愈，日志不需要重启进程就会重新落库。
func initStats() {
	reqLog = newRequestLog(cfg.ReqLogSize)
	errLog = newRequestLog(cfg.ErrLogSize)
	if usageDB != nil {
		reqLog.attachTable(logTableRequest)
		errLog.attachTable(logTableError)
	}
}

// isErrorRecord 判定错误记录：状态码 >=400，或带失败诊断信息
// （覆盖测试链路 status=0 的代理解析失败等）。
func isErrorRecord(rec RequestRecord) bool {
	return rec.Status >= 400 || rec.ErrMsg != ""
}

// recordRequest 请求记录统一入口：全部请求进主日志；错误另存独立的错误
// 日志，保证少量错误不会被海量成功请求在环形缓冲中挤出。
func recordRequest(rec RequestRecord) {
	if reqLog != nil {
		reqLog.Add(rec)
	}
	if errLog != nil && isErrorRecord(rec) {
		errLog.Add(rec)
	}
}

// ---- 请求上下文元数据 ----

type reqStatsKey struct{}

// reqStats 请求内元数据：handler/proxyChat 写入，statsHandler 收尾读取。
type reqStats struct {
	user             string
	channel          string
	model            string
	key              string
	promptTokens     int64
	completionTokens int64
	errMsg           string // 网关转发失败的诊断信息（覆盖通用的 HTTP 状态文本）
}

func reqStatsFrom(ctx context.Context) *reqStats {
	rs, _ := ctx.Value(reqStatsKey{}).(*reqStats)
	return rs
}

// setReqErrMsg 记录本次请求的失败诊断信息（路由引擎在放弃时调用），
// 请求日志会优先展示它而非笼统的 "Bad Gateway"。
func setReqErrMsg(r *http.Request, msg string) {
	if rs := reqStatsFrom(r.Context()); rs != nil && rs.errMsg == "" {
		rs.errMsg = msg
	}
}

// ---- responseRecorder：捕获状态码与输出字节数 ----

type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
	rs          *reqStats // 供上游响应写入 token 用量
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	// 每次写出武装下游写超时：客户端保持连接但停止读取时，写（含底层
	// bufio 刷出）会在超时后报错而不再永久阻塞（客户端停读兜底）
	restore := armWriteDeadline(r)
	n, err := r.ResponseWriter.Write(b)
	restore()
	r.bytes += int64(n)
	return n, err
}

// Flush 透传 SSE 刷新；同样武装写超时——HTTP/1 的缓冲数据正是在 flush 时
// 真正写向连接，客户端停读时阻塞点在这里。
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		restore := armWriteDeadline(r)
		f.Flush()
		restore()
	}
}

// Unwrap 暴露被包装的 writer：http.ResponseController 据此把下游写超时
// 穿透本包装层设在底层连接上（本类型自身不实现 SetWriteDeadline）。
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// clientIP 取请求来源 IP。TRUST_PROXY_HEADERS（默认开）时按反代部署处理：
// RemoteAddr 只是反代地址，改取转发头——优先 X-Real-IP（反代一般用 $remote_addr
// 覆写，客户端伪造不了），其次 X-Forwarded-For 最左合法段（单层反代即真实客户端）。
// 转发头只接受可解析的 IP，脏值跳过，最终回退 RemoteAddr，避免伪造/脏值污染
// 请求记录与登录防爆破的 IP 计数键。
func clientIP(r *http.Request) string {
	if cfg.TrustProxyHeaders {
		if v := strings.TrimSpace(r.Header.Get("X-Real-IP")); v != "" {
			if ip := net.ParseIP(v); ip != nil {
				return ip.String()
			}
		}
		for _, part := range strings.Split(r.Header.Get("X-Forwarded-For"), ",") {
			if ip := net.ParseIP(strings.TrimSpace(part)); ip != nil {
				return ip.String()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// maskKey 脱敏 key：sk-12345678abcd -> sk-12****abcd；过短整体打码。
// 掩码常作用于 "名称@渠道"（中文名称），必须按 rune 边界截取——按字节切
// 会把 UTF-8 字符拦腰斩断，日志里出现「导�****b.ai」式的乱码。
func maskKey(key string) string {
	const mask = "****"
	if len(key) <= 8 {
		return mask
	}
	return maskPrefix(key, 4) + mask + maskSuffix(key, 4)
}

// maskPrefix 取前 n 字节，越界回退到 rune 起点。
func maskPrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// maskSuffix 取后 n 字节，越界推进到 rune 起点。
func maskSuffix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := len(s) - n
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return s[cut:]
}

// statsMiddleware 包裹任意 handler：为请求注入 reqStats 上下文与 responseRecorder；
// 仅大模型接口请求写入请求日志与用量统计，系统后台请求（WebUI/Admin）不记。
func statsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statsServe(w, r, next)
	}
}

// isLLMPath 判断请求路径是否为大模型网关接口（请求日志只记录这些）：
// /v1/*（chat/completions、models、responses、embeddings 等）以及不带 /v1
// 前缀的兼容别名（/models、/chat/completions），与 rootHandler 的网关分支
// 保持一致；Admin / WebUI 等系统后台请求不记。
func isLLMPath(path string) bool {
	return strings.HasPrefix(path, "/v1/") || matchesPath(path, "/models", "/models/", "/chat/completions")
}

func statsServe(w http.ResponseWriter, r *http.Request, next http.HandlerFunc) {
	start := time.Now()
	rs := &reqStats{}
	rw := &responseRecorder{ResponseWriter: w, rs: rs}
	ctx := context.WithValue(r.Context(), reqStatsKey{}, rs)
	next(rw, r.WithContext(ctx))

	// 只记录大模型接口请求；WebUI / Admin 等系统后台请求不进日志
	if !isLLMPath(r.URL.Path) {
		return
	}

	status := rw.status
	if status == 0 {
		status = http.StatusOK
	}
	rec := RequestRecord{
		ID:               strconv.FormatInt(time.Now().UnixNano(), 36),
		Time:             start,
		Duration:         time.Since(start),
		DurationMs:       time.Since(start).Milliseconds(),
		Method:           r.Method,
		Path:             r.URL.Path,
		Status:           status,
		BytesOut:         rw.bytes,
		ClientIP:         clientIP(r),
		User:             rs.user,
		Channel:          rs.channel,
		Model:            rs.model,
		Key:              rs.key,
		PromptTokens:     rs.promptTokens,
		CompletionTokens: rs.completionTokens,
	}
	if status >= 400 {
		// 网关主动返回的错误（含逐 key 失败轨迹）优先于状态文本，完整入日志
		if rs.errMsg != "" {
			rec.ErrMsg = truncate(rs.errMsg, errMsgMax)
		} else {
			rec.ErrMsg = http.StatusText(status)
		}
	}
	// key 列是「名称@渠道」的用户标签而非凭证，完整显示不脱敏
	recordRequest(rec)

	// 用量库记账（含输入/输出 token）。key 列同上：存「名称@渠道」标识，
	// 不是上游凭证，明文落盘无泄漏风险；by_key 维度仍是「一个 key 一行」，
	// 聚合语义不变。历史上真实 key 曾直接入库，启动时由 MaskStoredKeys 收敛。
	if usageDB != nil {
		usageDB.Append(UsageEvent{
			Time:             start,
			User:             rs.user,
			Channel:          rs.channel,
			Model:            rs.model,
			Key:              rs.key,
			PromptTokens:     rs.promptTokens,
			CompletionTokens: rs.completionTokens,
			Status:           status,
			BytesOut:         rw.bytes,
		})
	}
}

// ---- admin 端点 ----

// isAdmin 判断登录用户是否为管理员。
func isAdmin(user string) bool {
	return cfg.LoginRequired && user != "" && user == cfg.AdminUser
}

// parsePageArgs 解析分页参数：page（从 1 起）、page_size（默认 50，上限 500）。
// 兼容旧参数 limit（等价 page_size，page=1）。
func parsePageArgs(r *http.Request) (page, pageSize int) {
	page, pageSize = 1, 50
	q := r.URL.Query()
	if v := q.Get("page_size"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			pageSize = n
		}
	} else if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > 500 {
		pageSize = 500
	}
	if v := q.Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	return page, pageSize
}

// handleAdminRequests 返回请求记录（最新在前，分页：?page=&page_size=，
// 兼容旧 ?limit=；响应带 total / page / page_size / total_pages）。
func handleAdminRequests(w http.ResponseWriter, r *http.Request) {
	page, pageSize := parsePageArgs(r)
	recs, total := reqLog.Query(page, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": (total + pageSize - 1) / pageSize,
		"records":     recs,
	})
}

// handleAdminErrors 返回错误请求记录（独立环形缓冲，分页同请求记录）。
// 错误单独存储：不被成功请求挤出，便于事后排查「为什么 502」。
func handleAdminErrors(w http.ResponseWriter, r *http.Request) {
	page, pageSize := parsePageArgs(r)
	recs, total := errLog.Query(page, pageSize)
	writeJSON(w, http.StatusOK, map[string]any{
		"total":       total,
		"page":        page,
		"page_size":   pageSize,
		"total_pages": (total + pageSize - 1) / pageSize,
		"records":     recs,
	})
}

// handleAdminClearLogs 清空请求日志/错误日志：body {"scope":"requests"|"errors"|"all"}，
// scope 省略默认 all。只清内存环形缓冲，不影响用量统计与用库记录。
func handleAdminClearLogs(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Scope string `json:"scope"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
			writeJSONError(w, http.StatusBadRequest, "invalid json: "+err.Error(), "bad_request")
			return
		}
	}
	cleared := body.Scope
	switch body.Scope {
	case "", "all":
		reqLog.Clear()
		errLog.Clear()
		cleared = "all"
	case "requests":
		reqLog.Clear()
	case "errors":
		errLog.Clear()
	default:
		writeJSONError(w, http.StatusBadRequest, "scope must be requests / errors / all", "bad_request")
		return
	}
	log.Printf("admin cleared logs: scope=%s", cleared)
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}
