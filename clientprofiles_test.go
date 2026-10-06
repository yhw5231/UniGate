// 内置客户端协议头预设测试：清单完整性、头数据自洽、合并/覆盖语义、
// 渠道校验、以及转发链路上真的把预设头发出去了。
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// wantClientProfileIDs 用户要求内置的客户端清单（顺序即 WebUI 下拉顺序）。
var wantClientProfileIDs = []string{
	"claude-code", "codex", "openclaw", "pi", "opencode", "ohmyopencode",
	"kilo-code", "roocode", "cline", "crush", "droid", "qwencode", "hermes",
	"zcode", "qoder", "kimi-code", "craft-agent", "trae", "github-copilot",
	"workbuddy", "cursor", "deepseek-harness", "mimo-code",
}

// forbiddenProfileHeaders 预设里不允许出现的头：这些头由网关按请求/按 key 决定，
// 预设里写死会破坏鉴权或消息体（Authorization 会顶掉 key 的 Bearer）。
var forbiddenProfileHeaders = map[string]string{
	"Authorization":     "预设不得写 Authorization（会顶掉 key 的 Bearer 鉴权）",
	"Content-Length":    "长度由 Go 的 http 客户端按实际消息体计算",
	"Host":              "Host 由目标 URL 决定",
	"Connection":        "连接复用由传输层管理",
	"Transfer-Encoding": "由传输层决定",
}

func TestClientProfileCatalogMatchesSupportedClients(t *testing.T) {
	if len(clientProfiles) != len(wantClientProfileIDs) {
		t.Fatalf("内置预设 %d 个，清单要求 %d 个", len(clientProfiles), len(wantClientProfileIDs))
	}
	seen := map[string]bool{}
	for i, want := range wantClientProfileIDs {
		got := clientProfiles[i]
		if got.ID != want {
			t.Fatalf("第 %d 个预设 id = %q, want %q（顺序应与支持清单一致）", i, got.ID, want)
		}
		if seen[got.ID] {
			t.Fatalf("预设 id 重复：%q", got.ID)
		}
		seen[got.ID] = true
		if strings.TrimSpace(got.Name) == "" {
			t.Fatalf("预设 %q 缺少展示名", got.ID)
		}
		if normalizeClientProfileID(got.ID) != got.ID {
			t.Fatalf("预设 id %q 不是归一化形态（WebUI 保存后会对不上）", got.ID)
		}
		if clientProfileByID(got.ID) == nil {
			t.Fatalf("预设 %q 无法按 id 查到", got.ID)
		}
	}
}

// TestClientProfileStarredClients：支持清单里标 ⭐ 的重点客户端要标出来（WebUI 下拉）。
func TestClientProfileStarredClients(t *testing.T) {
	for _, id := range []string{"claude-code", "codex"} {
		p := clientProfileByID(id)
		if p == nil {
			t.Fatalf("重点客户端 %q 缺少预设", id)
		}
		if !p.Starred {
			t.Fatalf("重点客户端 %q 应标记 starred", id)
		}
	}
}

// profileHasHeader 预设里（静态或动态）是否存在该头名（大小写不敏感）。
func profileHasHeader(p clientProfile, name string) bool {
	want := http.CanonicalHeaderKey(name)
	for n := range p.Headers {
		if http.CanonicalHeaderKey(n) == want {
			return true
		}
	}
	for n := range p.Dynamic {
		if http.CanonicalHeaderKey(n) == want {
			return true
		}
	}
	return false
}

func TestClientProfileHeaderSanity(t *testing.T) {
	validConfidence := map[string]bool{
		confidenceVerified: true, confidencePartial: true, confidenceUnverified: true,
	}
	for _, p := range clientProfiles {
		if !validConfidence[p.Confidence] {
			t.Fatalf("预设 %q 的可信度 %q 非法", p.ID, p.Confidence)
		}
		// 已核实/部分核实的预设必须有实际头，否则「内置指纹」是空壳
		if p.Confidence != confidenceUnverified && len(p.Headers) == 0 {
			t.Fatalf("预设 %q 标为 %s 但没有任何静态头", p.ID, p.Confidence)
		}
		if p.Confidence != confidenceVerified && strings.TrimSpace(p.Note) == "" {
			t.Fatalf("预设 %q 未标 verified，必须用 Note 说明数据来源与推测成分", p.ID)
		}
		if p.Confidence == confidenceVerified && len(p.Evidence) == 0 {
			t.Fatalf("预设 %q 标为 verified，必须给出证据出处", p.ID)
		}
		for name, value := range p.Headers {
			if strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
				t.Fatalf("预设 %q 有空头名或空值头：%q=%q", p.ID, name, value)
			}
			if reason, bad := forbiddenProfileHeaders[http.CanonicalHeaderKey(name)]; bad {
				t.Fatalf("预设 %q 含禁用头 %q：%s", p.ID, name, reason)
			}
			if strings.ContainsAny(value, "\r\n") || strings.ContainsAny(name, "\r\n") {
				t.Fatalf("预设 %q 的头含换行：%q=%q", p.ID, name, value)
			}
		}
		for name, kind := range p.Dynamic {
			if strings.TrimSpace(name) == "" {
				t.Fatalf("预设 %q 有空的动态头名", p.ID)
			}
			if ref, ok := sameHeaderRef(kind); ok {
				if !profileHasHeader(p, ref) {
					t.Fatalf("预设 %q 的动态头 %q 引用了不存在的头 %q", p.ID, name, ref)
				}
				continue
			}
			if dynamicHeaderValue(kind) == "" {
				t.Fatalf("预设 %q 的动态头 %q 用了未知生成器 %q", p.ID, name, kind)
			}
		}
	}
}

// TestClientProfileSameHeaderRef：动态头支持 "same:<头名>"，同一请求内取同值
// （WorkBuddy 的 X-Conversation-Request-ID 与 X-Conversation-ID 必须成对相同）。
func TestClientProfileSameHeaderRef(t *testing.T) {
	p := &clientProfile{
		Headers: map[string]string{"User-Agent": "x/1"},
		Dynamic: map[string]string{
			"X-Conversation-ID":         "rand32",
			"X-Conversation-Request-ID": "same:X-Conversation-ID",
			"X-Request-ID":              "same:Missing-Header",
		},
	}
	got := p.requestHeaders()
	if got["X-Conversation-Id"] == "" {
		t.Fatalf("rand32 未生成：%v", got)
	}
	if got["X-Conversation-Request-Id"] != got["X-Conversation-Id"] {
		t.Fatalf("same: 引用未取同值：%v", got)
	}
	if _, exists := got["X-Request-Id"]; exists {
		t.Fatalf("引用不存在的头时该头不应发送：%v", got)
	}
	if got["User-Agent"] != "x/1" {
		t.Fatalf("静态头丢失：%v", got)
	}
}

func TestClientProfileAliases(t *testing.T) {
	for alias, target := range clientProfileAliases {
		if normalizeClientProfileID(alias) != target {
			t.Fatalf("别名 %q 未归一化到 %q", alias, target)
		}
		if clientProfileByID(target) == nil {
			t.Fatalf("别名 %q 指向不存在的预设 %q", alias, target)
		}
	}
	// 大小写、空格、下划线、展示名写法都要能落到同一个预设
	for _, in := range []string{"Claude-Code", "claude_code", "  CLAUDE-CODE  ", "claude", "Claude Code"} {
		if got := normalizeClientProfileID(in); got != "claude-code" {
			t.Fatalf("normalizeClientProfileID(%q) = %q, want claude-code", in, got)
		}
	}
	if got := normalizeClientProfileID("  "); got != "" {
		t.Fatalf("空白应归一化为空串，得到 %q", got)
	}
	if clientProfileByID("no-such-client") != nil {
		t.Fatal("未知 id 不应查到预设")
	}
}

func TestDynamicHeaderValue(t *testing.T) {
	uuidRe := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if v := dynamicHeaderValue("uuid"); !uuidRe.MatchString(v) {
		t.Fatalf("uuid = %q, 不是 v4 UUID", v)
	}
	if v := dynamicHeaderValue("UUID"); !uuidRe.MatchString(v) {
		t.Fatalf("生成器名应大小写不敏感，得到 %q", v)
	}
	if v := dynamicHeaderValue("uuid32"); len(v) != 32 || strings.Contains(v, "-") {
		t.Fatalf("uuid32 = %q, want 32 位无横线", v)
	}
	if v := dynamicHeaderValue("rand16"); len(v) != 16 {
		t.Fatalf("rand16 = %q, want 16 位", v)
	}
	if v := dynamicHeaderValue("ts_ms"); len(v) < 12 {
		t.Fatalf("ts_ms = %q, 不像毫秒时间戳", v)
	}
	if v := dynamicHeaderValue("nope"); v != "" {
		t.Fatalf("未知生成器应返回空串，得到 %q", v)
	}
	if a, b := dynamicHeaderValue("uuid"), dynamicHeaderValue("uuid"); a == b {
		t.Fatal("两次生成的 uuid 不应相同")
	}
}

// TestEffectiveChannelHeaders：预设作基线、渠道自定义头覆盖（大小写不敏感）、
// 空值删除预设头、无预设时只发自定义头。
func TestEffectiveChannelHeaders(t *testing.T) {
	profile := clientProfileByID("cline")
	if profile == nil || len(profile.Headers) == 0 {
		t.Skip("cline 预设没有静态头，跳过合并语义测试")
	}
	var presetName, presetValue string
	for name, value := range profile.Headers {
		presetName, presetValue = http.CanonicalHeaderKey(name), value
		break
	}

	// 基线：只用预设
	base := effectiveChannelHeaders(&Channel{ClientProfile: "cline"})
	if base[presetName] != presetValue {
		t.Fatalf("预设头未生效：%v", base)
	}

	// 覆盖：小写同名头 + 新增头 + 空值删除另一个预设头
	victim := ""
	for name := range profile.Headers {
		if http.CanonicalHeaderKey(name) != presetName {
			victim = http.CanonicalHeaderKey(name)
			break
		}
	}
	headers := map[string]string{strings.ToLower(presetName): "overridden", "x-extra": "1"}
	if victim != "" {
		headers[victim] = "   "
	}
	got := effectiveChannelHeaders(&Channel{ClientProfile: "cline", Headers: headers})
	if got[http.CanonicalHeaderKey(presetName)] != "overridden" {
		t.Fatalf("小写同名头未覆盖预设：%v", got)
	}
	if got["X-Extra"] != "1" {
		t.Fatalf("新增头丢失：%v", got)
	}
	if victim != "" && got[http.CanonicalHeaderKey(victim)] != "" {
		t.Fatalf("空值应删掉预设头 %q，实际 %v", victim, got)
	}
	for name := range got {
		if name != http.CanonicalHeaderKey(name) {
			t.Fatalf("生效头名未规范化：%q", name)
		}
	}

	// 无预设：只发自定义头；全空 → nil
	only := effectiveChannelHeaders(&Channel{Headers: map[string]string{"X-Only": "v"}})
	if len(only) != 1 || only["X-Only"] != "v" {
		t.Fatalf("无预设时应只发自定义头：%v", only)
	}
	if effectiveChannelHeaders(&Channel{Headers: map[string]string{"X-Empty": ""}}) != nil {
		t.Fatal("自定义头全为空值时应返回 nil")
	}
	if effectiveChannelHeaders(nil) != nil {
		t.Fatal("nil 渠道应返回 nil")
	}
}

func TestChannelClientProfileNormalizedAndValidated(t *testing.T) {
	setupGateway(t)
	// 别名/大小写写进来也要落盘成规范 id
	mustPutChannel(t, &Channel{Name: "c", BaseURL: "https://example.com/v1", Enabled: true, ClientProfile: "Roo Code"})
	if got := store.Snapshot().Channels[0].ClientProfile; got != "roocode" {
		t.Fatalf("client_profile 归一化 = %q, want roocode", got)
	}
	// 未知预设直接拒绝，避免静默不生效
	if err := store.PutChannel(&Channel{Name: "bad", BaseURL: "https://example.com/v1", Enabled: true, ClientProfile: "no-such-client"}); err == nil {
		t.Fatal("未知 client_profile 应报错")
	}
	// 空 = 不伪装
	mustPutChannel(t, &Channel{Name: "plain", BaseURL: "https://example.com/v1", Enabled: true})
	if got := store.Snapshot().Channels[1].ClientProfile; got != "" {
		t.Fatalf("空 client_profile = %q, want \"\"", got)
	}
}

// firstProfileWithUA 取第一个带静态 User-Agent 的预设（数据调整后测试仍然有效）。
func firstProfileWithUA(t *testing.T) *clientProfile {
	t.Helper()
	for i := range clientProfiles {
		for name, value := range clientProfiles[i].Headers {
			if http.CanonicalHeaderKey(name) == "User-Agent" && value != "" {
				return &clientProfiles[i]
			}
		}
	}
	t.Skip("没有任何预设带静态 User-Agent，跳过链路测试")
	return nil
}

// TestClientProfileHeadersReachUpstream：选了预设的渠道，转发时上游真的收到预设头，
// 且预设头覆盖下游透传的 UA。
func TestClientProfileHeadersReachUpstream(t *testing.T) {
	setupGateway(t)
	profile := firstProfileWithUA(t)
	var mu sync.Mutex
	var got http.Header
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Clone()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	t.Cleanup(up.Close)

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true, ClientProfile: profile.ID,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-real", Enabled: true}}})

	req := chatRequest("m1")
	req.Header.Set("User-Agent", "downstream-ua")
	rr := httptest.NewRecorder()
	forwardChat(rr, req, nil, false, "m1")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	for name, value := range profile.Headers {
		if got.Get(name) != value {
			t.Fatalf("上游未收到预设头 %s=%q，实际 %q（全部：%v）", name, value, got.Get(name), got)
		}
	}
	if got.Get("User-Agent") == "downstream-ua" {
		t.Fatal("预设 UA 应覆盖下游透传的 UA")
	}
	// 动态头每次请求生成：只要求非空且符合生成器语义
	for name, kind := range profile.Dynamic {
		if got.Get(name) == "" {
			t.Fatalf("动态头 %s（%s）未发出", name, kind)
		}
	}
}

// TestModelsFetchUsesClientProfileHeaders：模型列表拉取（另一条发往上游的链路）
// 同样带预设头。
func TestModelsFetchUsesClientProfileHeaders(t *testing.T) {
	setupGateway(t)
	profile := firstProfileWithUA(t)
	var mu sync.Mutex
	var got http.Header
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = r.Header.Clone()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"m1"}]}`)
	}))
	t.Cleanup(up.Close)

	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.URL, Enabled: true, ClientProfile: profile.ID,
		Keys: []*UpKey{{Name: "k1", APIKey: "sk-test", Enabled: true}}})
	chid := store.Snapshot().Channels[0].ID
	tok := adminToken(t)

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+chid+"/fetch-models", "", tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("fetch-models status=%d body=%s", rr.Code, rr.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	for name, value := range profile.Headers {
		if got.Get(name) != value {
			t.Fatalf("模型拉取未带预设头 %s=%q，实际 %q", name, value, got.Get(name))
		}
	}
}

// TestAdminClientProfilesEndpoint：清单接口（WebUI 下拉的数据来源）返回全部预设，
// 且渠道保存接口拒绝未知预设。
func TestAdminClientProfilesEndpoint(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)

	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodGet, "/admin/api/client-profiles", "", tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("client-profiles status=%d body=%s", rr.Code, rr.Body.String())
	}
	var out struct {
		Profiles []clientProfile `json:"profiles"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Profiles) != len(wantClientProfileIDs) {
		t.Fatalf("接口返回 %d 个预设，want %d", len(out.Profiles), len(wantClientProfileIDs))
	}
	for i, want := range wantClientProfileIDs {
		if out.Profiles[i].ID != want {
			t.Fatalf("第 %d 个预设 = %q, want %q", i, out.Profiles[i].ID, want)
		}
		if out.Profiles[i].Name == "" || out.Profiles[i].Confidence == "" {
			t.Fatalf("预设 %q 缺少展示信息：%+v", want, out.Profiles[i])
		}
	}

	// state 里也带一份（WebUI 打开编辑弹窗即用，无需额外请求）
	rs := httptest.NewRecorder()
	rootHandler(rs, adminReq(http.MethodGet, "/admin/api/state", "", tok))
	if rs.Code != http.StatusOK {
		t.Fatalf("state status=%d", rs.Code)
	}
	var state struct {
		ClientProfiles []clientProfile `json:"client_profiles"`
	}
	if err := json.Unmarshal(rs.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.ClientProfiles) != len(wantClientProfileIDs) {
		t.Fatalf("state.client_profiles = %d 个，want %d", len(state.ClientProfiles), len(wantClientProfileIDs))
	}

	// 未知预设 → 400
	body := `{"name":"c","base_url":"https://example.com/v1","enabled":true,"client_profile":"no-such-client"}`
	rbad := httptest.NewRecorder()
	rootHandler(rbad, adminReq(http.MethodPut, "/admin/api/channels", body, tok))
	if rbad.Code != http.StatusBadRequest {
		t.Fatalf("未知 client_profile 应 400，得到 %d body=%s", rbad.Code, rbad.Body.String())
	}

	// 合法预设（别名写法）→ 200 且落盘为规范 id
	body = `{"name":"c","base_url":"https://example.com/v1","enabled":true,"client_profile":"Claude Code"}`
	rok := httptest.NewRecorder()
	rootHandler(rok, adminReq(http.MethodPut, "/admin/api/channels", body, tok))
	if rok.Code != http.StatusOK {
		t.Fatalf("合法 client_profile 应 200，得到 %d body=%s", rok.Code, rok.Body.String())
	}
	if got := store.Snapshot().Channels[0].ClientProfile; got != "claude-code" {
		t.Fatalf("落盘 client_profile = %q, want claude-code", got)
	}
}

// TestDefaultClientProfileSetting：设置页「默认使用客户端」——校验、落盘、即时生效，
// 且渠道自身的 client_profile 优先于全局默认。
func TestDefaultClientProfileSetting(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	// policy 是进程级全局，收尾必须清空设置，避免污染其它用例
	t.Cleanup(func() {
		rr := httptest.NewRecorder()
		rootHandler(rr, adminReq(http.MethodPut, "/admin/api/settings", `{}`, tok))
		applySettings(store.Settings())
	})

	// 未知预设 → 400，且不落盘
	rbad := httptest.NewRecorder()
	rootHandler(rbad, adminReq(http.MethodPut, "/admin/api/settings", `{"default_client_profile":"no-such-client"}`, tok))
	if rbad.Code != http.StatusBadRequest {
		t.Fatalf("未知 default_client_profile 应 400，得到 %d body=%s", rbad.Code, rbad.Body.String())
	}
	if got := store.Settings().DefaultClientProfile; got != nil {
		t.Fatalf("非法设置不应落盘，得到 %q", *got)
	}

	// 合法（展示名写法）→ 200，归一化落盘并即时生效
	rok := httptest.NewRecorder()
	rootHandler(rok, adminReq(http.MethodPut, "/admin/api/settings", `{"default_client_profile":"Claude Code"}`, tok))
	if rok.Code != http.StatusOK {
		t.Fatalf("合法 default_client_profile 应 200，得到 %d body=%s", rok.Code, rok.Body.String())
	}
	if got := store.Settings().DefaultClientProfile; got == nil || *got != "claude-code" {
		t.Fatalf("落盘 default_client_profile = %v, want claude-code", got)
	}
	if got := currentPolicy().DefaultClientProfile; got != "claude-code" {
		t.Fatalf("生效默认客户端 = %q, want claude-code", got)
	}

	// 渠道没配 → 用全局默认
	inherited := effectiveChannelHeaders(&Channel{})
	if inherited["X-App"] != "cli" || inherited["Anthropic-Version"] != "2023-06-01" {
		t.Fatalf("渠道未配置时应使用默认预设，实际 %v", inherited)
	}
	// 渠道配了 → 以渠道为准（且不带默认预设的头）
	own := effectiveChannelHeaders(&Channel{ClientProfile: "roocode"})
	if own["User-Agent"] != "RooCode/3.53.0" || own["X-App"] != "" {
		t.Fatalf("渠道预设应覆盖全局默认，实际 %v", own)
	}
	// 自定义头仍然覆盖默认预设里的同名头
	override := effectiveChannelHeaders(&Channel{Headers: map[string]string{"x-app": "cli-bg"}})
	if override["X-App"] != "cli-bg" {
		t.Fatalf("自定义头未覆盖默认预设：%v", override)
	}

	// 显式空串 = 不使用（覆盖环境变量默认）
	rnone := httptest.NewRecorder()
	rootHandler(rnone, adminReq(http.MethodPut, "/admin/api/settings", `{"default_client_profile":""}`, tok))
	if rnone.Code != http.StatusOK {
		t.Fatalf("清空 default_client_profile 应 200，得到 %d", rnone.Code)
	}
	if got := effectiveChannelHeaders(&Channel{}); len(got) != 0 {
		t.Fatalf("显式不使用默认预设时不应注入头：%v", got)
	}
}
