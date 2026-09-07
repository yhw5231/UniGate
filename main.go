// unigate —— 通用 AI 网关：多渠道账号、每 key 独立代理（含 ipv6-proxy-pool
// 动态租约）、OpenAI 兼容转发、故障转移、请求日志与 WebUI。
//
// 路由总览（默认双端口：网关 :10080，WebUI/Admin :10070；WEBUI_PORT 置空时合并单端口）：
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
	initStats()
	initUsageDB()

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           statsMiddleware(gatewayHandler),
		ReadHeaderTimeout: 30 * time.Second,
	}
	if webUIMerged() {
		srv.Handler = statsMiddleware(rootHandler)
		log.Printf("unigate v%s listening on :%s (gateway+webui, admin default %s/%s)",
			displayVersion(), cfg.Port, cfg.AdminUser, cfg.AdminPass)
		log.Fatal(srv.ListenAndServe())
	}

	uiSrv := &http.Server{
		Addr:              ":" + cfg.WebUIPort,
		Handler:           statsMiddleware(managementHandler),
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		if err := uiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("webui server :%s: %v", cfg.WebUIPort, err)
		}
	}()
	log.Printf("unigate v%s listening: gateway on :%s, webui on :%s (admin default %s/%s)",
		displayVersion(), cfg.Port, cfg.WebUIPort, cfg.AdminUser, cfg.AdminPass)
	log.Fatal(srv.ListenAndServe())
}

// webUIMerged WEBUI_PORT 显式置空（或与网关端口相同）时，单端口同时服务网关与管理面。
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

// rootHandler 合并监听（WEBUI_PORT 置空或与网关同端口）时的顶层分发。
func rootHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/") || path == "/models" || path == "/models/" || path == "/chat/completions" {
		gatewayHandler(w, r)
		return
	}
	managementHandler(w, r)
}
