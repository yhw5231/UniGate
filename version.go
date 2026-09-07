// 构建版本号：发布时通过 -ldflags "-X main.version=vX.Y.Z" 注入
// （Dockerfile ARG VERSION → CI 的 docker/metadata-action 输出），本地构建为 dev。
package main

import "net/http"

// version 程序版本；dev 表示本地开发构建。
var version = "dev"

// displayVersion 返回展示用的版本文本（dev 不加前缀）。
func displayVersion() string {
	if version == "" || version == "dev" {
		return "dev"
	}
	return version
}

// handleVersion 公开返回版本号（WebUI 登录页/顶栏展示，无需鉴权）。
func handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"version": displayVersion()})
}
