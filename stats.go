// 可观察性：请求记录（环形缓冲）与用量统计（按用户 / 模型 / key 聚合）。
// 通过 GET /admin/stats 与 GET /admin/requests 查询（需管理员登录）。
package main

import (
	"context"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	Key              string        `json:"key,omitempty"` // 脱敏后
	PromptTokens     int64         `json:"prompt_tokens,omitempty"`
	CompletionTokens int64         `json:"completion_tokens,omitempty"`
	ErrMsg           string        `json:"error,omitempty"`
}

// RequestLog 固定容量环形缓冲，新记录覆盖最旧。
type RequestLog struct {
	mu   sync.Mutex
	recs []RequestRecord
	next int // 下一个写入位置
	cap  int
	n    int // 已写条数（含被覆盖的）
}

func newRequestLog(capacity int) *RequestLog {
	if capacity <= 0 {
		capacity = 1
	}
	return &RequestLog{recs: make([]RequestRecord, capacity), cap: capacity}
}

// Add 写入一条记录（环形覆盖）。
func (l *RequestLog) Add(rec RequestRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.recs[l.next] = rec
	l.next = (l.next + 1) % l.cap
	l.n++
}

// Snapshot 返回现有记录，最新在前。
func (l *RequestLog) Snapshot() []RequestRecord {
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

// ---- 用量统计 ----

type UsageTotals struct {
	Requests   int64 `json:"requests"`
	Errors     int64 `json:"errors"`
	BytesOut   int64 `json:"bytes_out"`
	DurationMs int64 `json:"duration_ms"`
}

type UserUsage struct {
	Requests   int64     `json:"requests"`
	Errors     int64     `json:"errors"`
	BytesOut   int64     `json:"bytes_out"`
	LastActive time.Time `json:"last_active"`
}

type ModelUsage struct {
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
}

type KeyUsage struct {
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
}

// UsageStats 用量聚合。
type UsageStats struct {
	mu        sync.Mutex
	StartedAt time.Time
	Totals    UsageTotals
	ByUser    map[string]*UserUsage
	ByModel   map[string]*ModelUsage
	ByKey     map[string]*KeyUsage
}

func newUsageStats() *UsageStats {
	return &UsageStats{
		StartedAt: time.Now(),
		ByUser:    make(map[string]*UserUsage),
		ByModel:   make(map[string]*ModelUsage),
		ByKey:     make(map[string]*KeyUsage),
	}
}

// Record 把一条请求记录计入聚合。
func (s *UsageStats) Record(rec RequestRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Totals.Requests++
	s.Totals.BytesOut += rec.BytesOut
	s.Totals.DurationMs += rec.DurationMs
	isErr := rec.Status >= 400
	if isErr {
		s.Totals.Errors++
	}
	if rec.User != "" {
		u := s.ByUser[rec.User]
		if u == nil {
			u = &UserUsage{}
			s.ByUser[rec.User] = u
		}
		u.Requests++
		u.BytesOut += rec.BytesOut
		if isErr {
			u.Errors++
		}
		u.LastActive = rec.Time
	}
	if rec.Model != "" {
		m := s.ByModel[rec.Model]
		if m == nil {
			m = &ModelUsage{}
			s.ByModel[rec.Model] = m
		}
		m.Requests++
		if isErr {
			m.Errors++
		}
	}
	if rec.Key != "" {
		k := s.ByKey[rec.Key]
		if k == nil {
			k = &KeyUsage{}
			s.ByKey[rec.Key] = k
		}
		k.Requests++
		if isErr {
			k.Errors++
		}
	}
}

// ---- 全局实例 ----

var (
	reqLog    *RequestLog
	usageStat *UsageStats
)

// initStats 用配置初始化（幂等，reloadConfig 时调用）。
func initStats() {
	reqLog = newRequestLog(cfg.ReqLogSize)
	usageStat = newUsageStats()
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
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Flush 透传 SSE 刷新。
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// clientIP 取请求来源 IP（RemoteAddr 去端口）。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// maskKey 脱敏 key：sk-12345678abcd -> sk-12****abcd；过短整体打码。
func maskKey(key string) string {
	const mask = "****"
	if len(key) <= 8 {
		return mask
	}
	return key[:4] + mask + key[len(key)-4:]
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
		// 网关主动返回的错误（如 502 前逐 key 的失败原因）优先于状态文本
		if rs.errMsg != "" {
			rec.ErrMsg = truncate(rs.errMsg, 500)
		} else {
			rec.ErrMsg = http.StatusText(status)
		}
	}
	if rec.Key != "" {
		rec.Key = maskKey(rec.Key)
	}
	reqLog.Add(rec)
	usageStat.Record(rec)

	// 用量库记账（含输入/输出 token）
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

// handleAdminStats 返回用量统计 JSON。
func handleAdminStats(w http.ResponseWriter) {
	usageStat.mu.Lock()
	defer usageStat.mu.Unlock()

	type userRow struct {
		User       string    `json:"user"`
		Requests   int64     `json:"requests"`
		Errors     int64     `json:"errors"`
		BytesOut   int64     `json:"bytes_out"`
		LastActive time.Time `json:"last_active"`
	}
	type modelRow struct {
		Model    string `json:"model"`
		Requests int64  `json:"requests"`
		Errors   int64  `json:"errors"`
	}
	type keyRow struct {
		Key      string `json:"key"`
		Requests int64  `json:"requests"`
		Errors   int64  `json:"errors"`
	}

	users := make([]userRow, 0, len(usageStat.ByUser))
	for u, v := range usageStat.ByUser {
		users = append(users, userRow{u, v.Requests, v.Errors, v.BytesOut, v.LastActive})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Requests > users[j].Requests })

	models := make([]modelRow, 0, len(usageStat.ByModel))
	for m, v := range usageStat.ByModel {
		models = append(models, modelRow{m, v.Requests, v.Errors})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].Requests > models[j].Requests })

	keys := make([]keyRow, 0, len(usageStat.ByKey))
	for k, v := range usageStat.ByKey {
		keys = append(keys, keyRow{k, v.Requests, v.Errors})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Requests > keys[j].Requests })

	writeJSON(w, http.StatusOK, map[string]any{
		"started_at":     usageStat.StartedAt,
		"uptime_seconds": int64(time.Since(usageStat.StartedAt).Seconds()),
		"totals":         usageStat.Totals,
		"by_user":        users,
		"by_model":       models,
		"by_key":         keys,
	})
}

// handleAdminRequests 返回最近请求记录（最新在前，?limit= 控制条数）。
func handleAdminRequests(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	recs := reqLog.Snapshot()
	if len(recs) > limit {
		recs = recs[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total":   reqLog.count(),
		"records": recs,
	})
}
