// WebUI 静态资源嵌入（web/ 目录）。go build 时通过 embed 打包进二进制。
package main

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed web
var webFS embed.FS

// webHandler 服务 WebUI 静态资源；未命中路径回落到 index.html（SPA）。
//
// 缓存策略：WebUI 资源随二进制更新而更新，但 embed.FS 无修改时间/ETag 可供
// 协商缓存——浏览器曾启发式缓存旧 app.js 时，升级后界面仍是旧版（「改了没生效」）。
// 因此所有静态资源显式 no-cache（每次协商，资源很小），且 index.html 里的
// app.js/style.css 引用追加构建版本参数，版本一变浏览器必然拉新。
func webHandler(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" || path == "index.html" {
		serveIndex(w, sub)
		return
	}
	if f, err := sub.Open(path); err == nil {
		f.Close()
		http.FileServer(http.FS(sub)).ServeHTTP(w, r)
		return
	}
	serveIndex(w, sub) // SPA 回落
}

// serveIndex 输出 index.html，并给静态资源引用追加版本参数（缓存击穿）。
func serveIndex(w http.ResponseWriter, sub fs.FS) {
	index, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	v := displayVersion()
	html := strings.ReplaceAll(string(index), `src="/app.js"`, `src="/app.js?v=`+v+`"`)
	html = strings.ReplaceAll(html, `href="/style.css"`, `href="/style.css?v=`+v+`"`)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}
