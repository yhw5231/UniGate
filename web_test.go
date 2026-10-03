// WebUI 源码级回归守卫：冷却相关的显示只放「冷却条目本身 + 计数」，不允许把渠道的
// 全部模型铺进来。
//
// 背景（真实反馈）：渠道卡片的冷却明细曾经直接列出「可用模型：m2、m3、…」——渠道声明
// 几十个模型、只有 1 个在冷却时，冷却那一行就等于把**全部模型**都显示出来了
// （「渠道冷却还是显示了所有模型」）。同类问题还有「冷却清单条目多把编辑弹窗撑高」。
//
// 这两条都是「显示长度随模型数/冷却数增长」的问题，靠人眼 review 容易漏，所以钉在测试里：
//   - 冷却渲染函数里不得内联铺开模型清单（可用模型只给个数，完整名单放 title）；
//   - 冷却清单容器必须是限高可滚动的（.cool-list）。
package main

import (
	"os"
	"strings"
	"testing"
)

// jsFuncBody 取 app.js 里某个顶层函数的函数体（从函数签名到行首的 `}`）。
// web/app.js 里顶层函数一律以行首 `}` 收尾，按此截取足够稳。
func jsFuncBody(t *testing.T, src, signature string) string {
	t.Helper()
	i := strings.Index(src, signature)
	if i < 0 {
		t.Fatalf("web/app.js 里找不到 %s", signature)
	}
	rest := src[i:]
	if j := strings.Index(rest, "\n}"); j >= 0 {
		return rest[:j]
	}
	return rest
}

func TestCoolingUIHasNoModelDump(t *testing.T) {
	raw, err := os.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("读取 web/app.js: %v", err)
	}
	src := string(raw)

	// 渠道卡片：可用模型只给个数，名单只能放进 title（悬浮提示）
	card := jsFuncBody(t, src, "function keyCoolHTML(")
	if !strings.Contains(card, "可用模型") {
		t.Error("keyCoolHTML 应说明该 key 的可用模型情况（个数即可）")
	}
	for _, dump := range []string{".available.join", ".available.map"} {
		idx := strings.Index(card, dump)
		if idx < 0 {
			continue
		}
		// 名单必须落在 title="…" 属性值里：title 之后、清单之前不得出现属性收尾的 `">`
		before := card[:idx]
		ti := strings.LastIndex(before, `title="`)
		if ti < 0 || strings.Contains(before[ti:], `">`) {
			t.Errorf("keyCoolHTML 的可用模型清单只能放进 title（悬浮提示），不得拼进可见文本（%s）：渠道模型一多，冷却那一行就等于显示全部模型", dump)
		}
	}

	// 其它冷却渲染：不得铺开渠道模型清单
	for _, sig := range []string{
		"function coolingBadge(",
		"function renderCoolingKeys(",
		"function renderCoolClearRows(",
		"function keyCoolDetail(",
	} {
		body := jsFuncBody(t, src, sig)
		for _, bad := range []string{"ch.models.join", "ch.models.map", "models.join(\"、\")"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s 不得铺开渠道模型列表（%s）：冷却显示的长度只应随冷却条数增长", sig, bad)
			}
		}
	}
}

// TestCoolingListBoundedHeight 冷却清单必须限高 + 内部滚动：冷却条目多时不能把
// 编辑弹窗撑高（真实反馈）。
func TestCoolingListBoundedHeight(t *testing.T) {
	raw, err := os.ReadFile("web/style.css")
	if err != nil {
		t.Fatalf("读取 web/style.css: %v", err)
	}
	src := string(raw)
	i := strings.Index(src, ".cool-list")
	if i < 0 {
		t.Fatal("web/style.css 缺少 .cool-list（渠道编辑弹窗的冷却清单容器）")
	}
	body := src[i:]
	if j := strings.Index(body, "}"); j >= 0 {
		body = body[:j]
	}
	if !strings.Contains(body, "max-height") || !strings.Contains(body, "overflow") {
		t.Errorf(".cool-list 必须限高并可内部滚动（当前：%q）", strings.TrimSpace(body))
	}

	html, err := os.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("读取 web/index.html: %v", err)
	}
	if !strings.Contains(string(html), `id="chCoolingKeys" class="cool-list"`) {
		t.Error(`index.html 的冷却清单容器应使用 class="cool-list"（限高滚动）`)
	}
}
