// 构建版本号：
//   - 优先取 -ldflags "-X main.version=..." 注入值（Dockerfile 构建时自动用
//     git describe 从 .git 生成：有 tag 显示 tag，无 tag 显示短提交哈希）；
//   - 本地 go build 未注入时，回退读取 Go 编译器内嵌的 VCS 信息（git 仓库内
//     go build 默认打 vcs.revision 戳），显示为短提交哈希，有未提交改动加 -dirty；
//   - 都没有（源码包构建、非 git 目录）为 dev。
package main

import (
	"net/http"
	"runtime/debug"
)

// version 程序版本；dev 表示构建时未注入版本号。
var version = "dev"

// resolvedVersion 进程内解析一次：ldflags 注入值 > 内嵌 VCS 短哈希 > dev。
var resolvedVersion = resolveVersion()

func resolveVersion() string {
	if version != "" && version != "dev" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		rev, modified := "", false
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				modified = s.Value == "true"
			}
		}
		if len(rev) >= 7 {
			if modified {
				return rev[:7] + "-dirty"
			}
			return rev[:7]
		}
	}
	return "dev"
}

// displayVersion 返回展示用的版本文本（顶栏/登录页/启动日志/上游 User-Agent）。
func displayVersion() string {
	return resolvedVersion
}

// handleVersion 公开返回版本号（WebUI 登录页/顶栏展示，无需鉴权）。
func handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"version": displayVersion()})
}
