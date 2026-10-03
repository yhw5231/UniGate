// WebUI 源码级回归守卫：
//  1. 冷却相关的显示只放「冷却条目本身 + 计数」，不允许把渠道的全部模型铺进来；
//  2. 日间/夜间两套配色变量必须一一对应（漏一个就会在日间模式下露出暗色）；
//  3. 渠道测试页的待测模型清单必须能一键清空；
//  4. 完全空白的 key 行不得写进配置、也不得在批量导入后残留在第一位（它没有凭证，
//     会以「缺 Authorization 头」的 401 掩盖真正可用的 key）。
//
// 背景（真实反馈）：渠道卡片的冷却明细曾经直接列出「可用模型：m2、m3、…」——渠道声明
// 几十个模型、只有 1 个在冷却时，冷却那一行就等于把**全部模型**都显示出来了
// （「渠道冷却还是显示了所有模型」）。同类问题还有「冷却清单条目多把编辑弹窗撑高」、
// 「渠道测试全部 401 缺 authorization，看起来像网关没带认证」（实际是自动补的空白
// key 行排在第一位）。
//
// 这些靠人眼 review 容易漏，所以钉在测试里：
//   - 冷却渲染函数里不得内联铺开模型清单（可用模型只给个数，完整名单放 title）；
//   - 冷却清单容器必须是限高可滚动的（.cool-list）；
//   - :root 里的配色变量必须全部在 :root[data-theme="light"] 里重新定义；
//   - 测试页的清空按钮与空白 key 行清理必须保持接线。
package main

import (
	"math"
	"os"
	"regexp"
	"strconv"
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

var cssVarRe = regexp.MustCompile(`--([a-z0-9-]+)\s*:`)
var cssHexVarRe = regexp.MustCompile(`--([a-z0-9-]+):\s*(#[0-9a-fA-F]{6})`)

// cssBlockVars 取某个 CSS 规则块（从 marker 到其后第一个 `}`）里定义的变量名集合。
func cssBlockVars(t *testing.T, src, marker string) map[string]bool {
	t.Helper()
	i := strings.Index(src, marker)
	if i < 0 {
		t.Fatalf("web/style.css 里找不到规则块 %q", marker)
	}
	body := src[i+len(marker):]
	if j := strings.Index(body, "}"); j >= 0 {
		body = body[:j]
	}
	got := map[string]bool{}
	for _, m := range cssVarRe.FindAllStringSubmatch(body, -1) {
		got[m[1]] = true
	}
	return got
}

// TestThemeVariablesComplete 日间/夜间两套主题必须覆盖同一批配色变量：样式里的颜色
// 全走变量，漏定义一个就会在日间模式下露出暗色（或反之）。新增变量忘了同步时这条会失败。
func TestThemeVariablesComplete(t *testing.T) {
	raw, err := os.ReadFile("web/style.css")
	if err != nil {
		t.Fatalf("读取 web/style.css: %v", err)
	}
	src := string(raw)
	dark := cssBlockVars(t, src, ":root {")
	light := cssBlockVars(t, src, `:root[data-theme="light"] {`)
	if len(dark) < 8 {
		t.Fatalf(":root 解析出的变量只有 %d 个，规则块可能被改动：%v", len(dark), dark)
	}
	for name := range dark {
		if !light[name] {
			t.Errorf("日间模式缺少变量 --%s：请在 web/style.css 的 :root[data-theme=\"light\"] 里同步定义", name)
		}
	}
	// 弹窗遮罩：日间模式需要更浅的遮罩，所以不能写死 rgba(0,0,0,.55)
	if !strings.Contains(src, "background: var(--backdrop)") {
		t.Error("弹窗遮罩应使用 var(--backdrop)（两种模式遮罩深浅不同）")
	}
	if strings.Contains(src, "rgba(0,0,0,.55)") {
		t.Error("web/style.css 里不应再出现写死的遮罩色 rgba(0,0,0,.55)")
	}
}

// cssBlock 取某个 CSS 规则块（从 marker 到其后第一个 `}`）的原文。
func cssBlock(t *testing.T, src, marker string) string {
	t.Helper()
	i := strings.Index(src, marker)
	if i < 0 {
		t.Fatalf("web/style.css 里找不到规则块 %q", marker)
	}
	body := src[i+len(marker):]
	if j := strings.Index(body, "}"); j >= 0 {
		body = body[:j]
	}
	return body
}

// cssHexVars 规则块里的「十六进制颜色」变量（--backdrop 这类 rgba 不在其中）。
func cssHexVars(t *testing.T, src, marker string) map[string]string {
	t.Helper()
	got := map[string]string{}
	for _, m := range cssHexVarRe.FindAllStringSubmatch(cssBlock(t, src, marker), -1) {
		got[m[1]] = m[2]
	}
	return got
}

// relLum / contrastRatio：WCAG 2.x 相对亮度与对比度。
func relLum(t *testing.T, hex string) float64 {
	t.Helper()
	if len(hex) != 7 || hex[0] != '#' {
		t.Fatalf("颜色格式不支持：%q（需要 #rrggbb）", hex)
	}
	lin := func(v float64) float64 {
		if v <= 0.03928 {
			return v / 12.92
		}
		return math.Pow((v+0.055)/1.055, 2.4)
	}
	var c [3]float64
	for i := 0; i < 3; i++ {
		n, err := strconv.ParseInt(hex[1+i*2:3+i*2], 16, 32)
		if err != nil {
			t.Fatalf("颜色解析失败 %q: %v", hex, err)
		}
		c[i] = lin(float64(n) / 255)
	}
	return 0.2126*c[0] + 0.7152*c[1] + 0.0722*c[2]
}

func contrastRatio(t *testing.T, fg, bg string) float64 {
	t.Helper()
	a, b := relLum(t, fg), relLum(t, bg)
	if a < b {
		a, b = b, a
	}
	return (a + 0.05) / (b + 0.05)
}

// TestThemeContrast 两套主题的常用前景/背景组合对比度都不低于 4.5:1（WCAG AA 正文标准）：
// 调色时容易只顾好看，导致日间模式下浅色文字看不清——这里用数值兜住。
func TestThemeContrast(t *testing.T) {
	raw, err := os.ReadFile("web/style.css")
	if err != nil {
		t.Fatalf("读取 web/style.css: %v", err)
	}
	src := string(raw)
	pairs := [][2]string{
		{"text", "panel"}, {"text", "bg"}, {"text", "panel2"},
		{"muted", "panel"}, {"accent", "panel"},
		{"ok", "panel"}, {"warn", "panel"}, {"err", "panel"},
	}
	for _, marker := range []string{":root {", `:root[data-theme="light"] {`} {
		vars := cssHexVars(t, src, marker)
		for _, p := range pairs {
			fg, bg := vars[p[0]], vars[p[1]]
			if fg == "" || bg == "" {
				t.Fatalf("%s 缺少变量 --%s / --%s", marker, p[0], p[1])
			}
			if r := contrastRatio(t, fg, bg); r < 4.5 {
				t.Errorf("%s：%s(%s) 在 %s(%s) 上的对比度 %.2f < 4.5，请调深/调亮配色", marker, p[0], fg, p[1], bg, r)
			}
		}
	}
}

// TestThemeToggleWiring 主题切换的接线：<head> 里先于首屏渲染应用主题（否则日间模式
// 下会先闪一下暗色），顶栏与登录页各有一个切换按钮，选择持久化在 localStorage。
func TestThemeToggleWiring(t *testing.T) {
	raw, err := os.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("读取 web/index.html: %v", err)
	}
	html := string(raw)
	for _, want := range []string{`id="themeBtn"`, `id="themeBtnLogin"`, "unigate_theme"} {
		if !strings.Contains(html, want) {
			t.Errorf("web/index.html 缺少 %s", want)
		}
	}
	head := strings.Index(html, "</head>")
	boot := strings.Index(html, "unigate_theme")
	if head < 0 || boot < 0 || boot > head {
		t.Error("主题内联脚本必须在 <head> 里、样式之前执行，避免首屏闪一下暗色")
	}

	js, err := os.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("读取 web/app.js: %v", err)
	}
	app := string(js)
	for _, want := range []string{
		`const THEME_KEY = "unigate_theme"`,
		"function applyTheme(",
		"function toggleTheme(",
		`$("#themeBtn").addEventListener("click", toggleTheme)`,
		`$("#themeBtnLogin").addEventListener("click", toggleTheme)`,
		`document.documentElement.dataset.theme`,
		"prefers-color-scheme: light",
	} {
		if !strings.Contains(app, want) {
			t.Errorf("web/app.js 缺少主题切换接线：%s", want)
		}
	}
}

// jsArrowBody 取 app.js 里某个事件处理器（`xxx.addEventListener("click", () => {` 形态）
// 的函数体：从签名到行首的 `});`。
func jsArrowBody(t *testing.T, src, signature string) string {
	t.Helper()
	i := strings.Index(src, signature)
	if i < 0 {
		t.Fatalf("web/app.js 里找不到 %s", signature)
	}
	rest := src[i:]
	if j := strings.Index(rest, "\n});"); j >= 0 {
		return rest[:j]
	}
	return rest
}

// TestTestModelClearButtonWired 渠道测试页的待测模型清单必须能一键清空（真实反馈：
// 清单只能一个个点掉，换一批模型很别扭），且清空要同步右侧 chips 的 ✓ 选中态，
// 并实时显示待测条数（清单为空 = 运行测试时用渠道已启用的全部模型）。
func TestTestModelClearButtonWired(t *testing.T) {
	html, err := os.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("读取 web/index.html: %v", err)
	}
	for _, want := range []string{`id="testModels"`, `id="testModelsClearBtn"`, `id="testModelsCount"`} {
		if !strings.Contains(string(html), want) {
			t.Errorf("web/index.html 缺少 %s", want)
		}
	}

	raw, err := os.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("读取 web/app.js: %v", err)
	}
	src := string(raw)
	body := jsArrowBody(t, src, `$("#testModelsClearBtn").addEventListener("click"`)
	if !strings.Contains(body, `$("#testModels")`) || !strings.Contains(body, `ta.value = ""`) {
		t.Error("清空按钮必须清空 #testModels 文本框")
	}
	if !strings.Contains(body, "testModelChips()") {
		t.Error("清空后必须刷新 chips（否则 ✓ 高亮与内容不一致）")
	}
	if !strings.Contains(src, "function testModelsCount(") {
		t.Error("web/app.js 缺少 testModelsCount（待测模型条数显示）")
	}
}

// TestBlankPlaceholderKeyRowsNeverSaved 完全空白的 key 行（无名称、无 API Key、无
// key 级代理）不得写进配置，也不能在批量导入后残留在第一位。
//
// 背景（真实反馈）：新建渠道时编辑器自动补一个空 key 行，批量导入 key 是往后追加，
// 于是这个空行成了「第一个启用的 key」——它没有凭证，测试（默认只测第一个 key）与
// 路由都会先撞上它，上游回 401 `Header of type `authorization` was missing`，看起来
// 像「渠道没正确携带认证」。修复分三处，这里钉住前端两处（后端跳过逻辑见
// adminapi_test.go 的 TestAdminTestModelSkipsPlaceholderKey）。
func TestBlankPlaceholderKeyRowsNeverSaved(t *testing.T) {
	raw, err := os.ReadFile("web/app.js")
	if err != nil {
		t.Fatalf("读取 web/app.js: %v", err)
	}
	src := string(raw)

	if !strings.Contains(jsFuncBody(t, src, "function collectChannelForm("), "dropBlankKeyRows(") {
		t.Error("collectChannelForm 必须丢弃空白占位 key 行：保存时不能把它写进配置")
	}
	if !strings.Contains(jsArrowBody(t, src, `$("#bulkImportBtn").addEventListener("click"`), "removeBlankKeyBlocks()") {
		t.Error("批量导入 key 后必须清掉编辑器里残留的空白占位行（否则它排在被测/被路由的第一位）")
	}
	// 无鉴权渠道可能只用一个空白 key 行表达「该渠道可路由」：全是空白时必须原样保留
	drop := jsFuncBody(t, src, "function dropBlankKeyRows(")
	if !strings.Contains(drop, "kept.length ? kept : keys") {
		t.Error("dropBlankKeyRows 在全是空白行时必须原样返回（无鉴权渠道只有一个空白 key 行）")
	}
	blank := jsFuncBody(t, src, "function isBlankKeyForm(")
	for _, want := range []string{"k.name", "k.api_key", "k.proxy"} {
		if !strings.Contains(blank, want) {
			t.Errorf("占位行判定必须同时看名称 / API Key / key 级代理，缺 %s", want)
		}
	}
}
