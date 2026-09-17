// unigate —— 通用 AI 网关：多渠道账号、每 key 独立代理（含 ipv6-proxy-pool
// 动态租约）、OpenAI 兼容转发、故障转移、请求日志与 WebUI。
//
// 路由总览（默认单端口 :10010 同时服务网关与 WebUI/Admin；设 WEBUI_PORT 可拆分独立管理端口）：
//
//	网关端口（PORT）：
//	POST /v1/chat/completions      下游 OpenAI 兼容转发（需通用 key，GW_KEY_AUTH=false 时免鉴权）
//	GET  /v1/models                聚合各渠道模型列表
//	管理端口（WEBUI_PORT）：
//	POST /login                    管理员登录（WebUI / Admin API 用）
//	/admin/api/*                   Admin REST API（需管理员 token）
//	GET  /api/version              版本号（登录页展示）
//	/                              WebUI 静态资源（嵌入二进制）
package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	// 用量数据库所在目录（容器需挂载该目录以持久化）
	if dir := filepath.Dir(cfg.UsageDBPath); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Printf("warning: cannot create usage db dir %s: %v", dir, err)
		}
	}

	// 网关配置存储
	store = newGatewayStore(cfg.GWPath)
	if err := store.load(); err != nil {
		log.Fatalf("load gateway config %s: %v", cfg.GWPath, err)
	}
	snap := store.Snapshot()
	log.Printf("gateway config %s: %d channels, %d gateway keys", cfg.GWPath, len(snap.Channels), len(snap.GWKeys))

	// 租约分配表持久化（跨渠道复用：重启后分配与 IP 保持稳定）
	assignPath := getenv("LEASE_ASSIGN_PATH", filepath.Join(cfg.DataDir, "lease-assignments.json"))
	if err := leaseMgr.SetPersistPath(assignPath); err != nil {
		log.Printf("warning: load lease assignments: %v", err)
	}

	cool = newCooldowns()
	streaks = newStreaks()
	policy.Store(defaultPolicy())   // 环境变量默认
	applySettings(store.Settings()) // WebUI 设置覆盖（gateway.json）
	initUsageDB()                   // 先建库：请求/错误日志持久化需要它
	initStats()
	probes.Start() // 账号自动探测调度器（渠道 auto_probe 开关控制是否实际探测）
	if usageDB != nil {
		// 请求记录/用量库现在只写「key 名称@渠道」（非凭证），但更早版本的
		// 库里可能残留明文真实 key：按现存 api_key 精确匹配就地脱敏一次
		secrets := map[string]bool{}
		for _, ch := range snap.Channels {
			for _, k := range ch.Keys {
				if k.APIKey != "" {
					secrets[k.APIKey] = true
				}
			}
		}
		if n := usageDB.MaskStoredKeys(secrets); n > 0 {
			log.Printf("usage db: masked %d row(s) carrying plaintext upstream key", n)
		}
	}

	// 服务端空闲 keep-alive 连接必须设超时：ReadTimeout/IdleTimeout 均为零时
	// Go 对空闲连接永不回收，死掉/异常的客户端连接（每个占一个 goroutine 与
	// 读写缓冲）会随时间累积。只影响「请求之间」的空闲复用，不影响在途的
	// 长流式响应（SSE 转发期间 IdleTimeout 不计时）。
	const serverIdleTimeout = 2 * time.Minute

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           statsMiddleware(gatewayHandler),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       serverIdleTimeout,
	}
	if webUIMerged() {
		srv.Handler = statsMiddleware(rootHandler)
		log.Printf("unigate v%s listening on :%s (gateway+webui, admin default %s/password hidden)",
			displayVersion(), cfg.Port, cfg.AdminUser)
		log.Fatal(srv.ListenAndServe())
	}

	uiSrv := &http.Server{
		Addr:              ":" + cfg.WebUIPort,
		Handler:           statsMiddleware(managementHandler),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       serverIdleTimeout,
	}
	go func() {
		if err := uiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("webui server :%s: %v", cfg.WebUIPort, err)
		}
	}()
	log.Printf("unigate v%s listening: gateway on :%s, webui on :%s (admin default %s/password hidden)",
		displayVersion(), cfg.Port, cfg.WebUIPort, cfg.AdminUser)
	log.Fatal(srv.ListenAndServe())
}

// webUIMerged WEBUI_PORT 未设置（默认）或与网关端口相同时，单端口同时服务网关与管理面。
func webUIMerged() bool {
	return cfg.WebUIPort == "" || cfg.WebUIPort == cfg.Port
}

// gatewayHandler 网关端口路由：仅大模型转发接口。
func gatewayHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/") || path == "/models" || path == "/models/" || path == "/chat/completions" {
		user, ok := authorizeGW(w, r)
		if !ok {
			return
		}
		if rs := reqStatsFrom(r.Context()); rs != nil {
			rs.user = user
		}
		handleGateway(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte("Not Found (management UI on webui port)"))
}

// managementHandler 管理端口路由：WebUI、Admin API、登录、版本号。
func managementHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/api/version":
		handleVersion(w, r)
	case path == "/login":
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = w.Write([]byte("Method Not Allowed"))
			return
		}
		handleLogin(w, r)
	case strings.HasPrefix(path, "/admin/api/"):
		adminAPIHandler().ServeHTTP(w, r)
	default:
		webHandler(w, r)
	}
}

// rootHandler 合并监听（WEBUI_PORT 未设或与网关同端口）时的顶层分发。
func rootHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/") || path == "/models" || path == "/models/" || path == "/chat/completions" {
		gatewayHandler(w, r)
		return
	}
	managementHandler(w, r)
}
