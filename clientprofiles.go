// 内置客户端协议头预设：把「真实客户端向模型 API 发请求时携带的请求头」内置成
// 可选项，渠道只要选一个预设（client_profile）就能以该客户端的指纹访问上游，
// 不必手写一堆头；渠道自定义头仍可逐条覆盖（空值 = 删掉预设里的同名头）。
//
// 数据可信度用 Confidence 标注，并在 WebUI 与 README 里如实展示：
//   verified   —— 有公开源码/文档/抓包证据（Note/Evidence 给出出处）
//   partial    —— 部分头有证据，其余按同族客户端惯例补齐
//   unverified —— 无公开证据，仅有 id/名称与家族惯例推测，勿当作已核实
package main

import (
	"crypto/rand"
	"encoding/hex"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// clientProfile 一个客户端指纹预设。
type clientProfile struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Starred    bool              `json:"starred,omitempty"`  // 支持清单里标 ⭐ 的重点客户端（WebUI 下拉置顶标注）
	Confidence string            `json:"confidence"`         // verified / partial / unverified
	Headers    map[string]string `json:"headers"`            // 静态头（每次请求原样发送）
	Dynamic    map[string]string `json:"dynamic,omitempty"`  // 头名 → 生成器（每请求生成，见 dynamicHeaderValue）
	Evidence   []string          `json:"evidence,omitempty"` // 证据出处（URL 或本仓库路径）
	Note       string            `json:"note,omitempty"`     // 中文说明：版本号来源、哪些头是推测
}

// clientProfileConfidence* 预设可信度取值。
const (
	confidenceVerified   = "verified"
	confidencePartial    = "partial"
	confidenceUnverified = "unverified"
)

// clientProfileAliases 常见别名 → 预设 id（大小写不敏感，便于手写配置/接口调用）。
var clientProfileAliases = map[string]string{
	"claude":          "claude-code",
	"claudecli":       "claude-code",
	"anthropic-cli":   "claude-code",
	"openai-codex":    "codex",
	"roo":             "roocode",
	"roo-code":        "roocode",
	"roocline":        "roocode",
	"kilo":            "kilo-code",
	"kilocode":        "kilo-code",
	"qwen":            "qwencode",
	"qwen-code":       "qwencode",
	"qwencli":         "qwencode",
	"kimi":            "kimi-code",
	"kimicode":        "kimi-code",
	"copilot":         "github-copilot",
	"githubcopilot":   "github-copilot",
	"dsh":             "deepseek-harness",
	"deepseekharness": "deepseek-harness",
	"oh-my-opencode":  "ohmyopencode",
	"mimocode":        "mimo-code",
	"z-code":          "zcode",
	"zai":             "zcode",
}

// normalizeClientProfileID 归一化预设 id：trim + 小写 + 别名展开。
// 空串 = 不使用预设（返回 ""）；未知名原样返回，由调用方校验。
func normalizeClientProfileID(id string) string {
	s := strings.ToLower(strings.TrimSpace(id))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "_", "-")
	if isClientProfileID(s) {
		return s
	}
	if alias, ok := clientProfileAliases[s]; ok {
		return alias
	}
	// 允许写成展示名（"Kimi Code" / "Claude Code"）或带多余空白
	alt := strings.Join(strings.Fields(s), "-")
	if alt != s {
		if isClientProfileID(alt) {
			return alt
		}
		if alias, ok := clientProfileAliases[alt]; ok {
			return alias
		}
	}
	return s
}

// isClientProfileID 是否恰好是某个内置预设的 id。
func isClientProfileID(id string) bool {
	for i := range clientProfiles {
		if clientProfiles[i].ID == id {
			return true
		}
	}
	return false
}

// clientProfileByID 按 id（或别名）查预设；未知名返回 nil。
func clientProfileByID(id string) *clientProfile {
	norm := normalizeClientProfileID(id)
	if norm == "" {
		return nil
	}
	for i := range clientProfiles {
		if clientProfiles[i].ID == norm {
			return &clientProfiles[i]
		}
	}
	return nil
}

// requestHeaders 展开该预设本次请求要发送的头：静态头 + 动态头（每次生成新值）。
// 头名统一走 CanonicalMIMEHeaderKey，避免同名不同大小写在合并时互不覆盖。
// 动态头里 "same:<头名>" 表示与同一请求中的另一个头取同值（如 WorkBuddy 的
// X-Conversation-Request-ID 与 X-Conversation-ID 成对相同）。
func (p *clientProfile) requestHeaders() map[string]string {
	if p == nil {
		return nil
	}
	out := make(map[string]string, len(p.Headers)+len(p.Dynamic))
	for name, value := range p.Headers {
		if key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name)); key != "" && value != "" {
			out[key] = value
		}
	}
	refs := map[string]string{} // 头名 → 引用的另一个头名（同请求取同值）
	for name, kind := range p.Dynamic {
		key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if ref, ok := sameHeaderRef(kind); ok {
			refs[key] = ref
			continue
		}
		if value := dynamicHeaderValue(kind); value != "" {
			out[key] = value
		}
	}
	for key, ref := range refs {
		if value := out[ref]; value != "" {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// sameHeaderRef 解析 "same:<头名>" 形式的动态头引用。
func sameHeaderRef(kind string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(kind))
	if !strings.HasPrefix(s, "same:") {
		return "", false
	}
	ref := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(s[len("same:"):]))
	if ref == "" {
		return "", false
	}
	return ref, true
}

// effectiveChannelHeaders 计算渠道发往上游的最终自定义头：预设头作为基线，
// 渠道自定义头逐条覆盖（同名大小写不敏感）；渠道里值为空的同名头表示
// **删除**该预设头（便于只关掉预设里的某一项）。无预设且无自定义头时返回 nil。
//
// 预设来源：渠道自身的 client_profile 优先；渠道没配时用「设置」页的默认客户端
// （default_client_profile / 环境变量 DEFAULT_CLIENT_PROFILE）。
func effectiveChannelHeaders(ch *Channel) map[string]string {
	if ch == nil {
		return nil
	}
	out := map[string]string{}
	if p := clientProfileByID(effectiveClientProfileID(ch)); p != nil {
		for name, value := range p.requestHeaders() {
			out[name] = value
		}
	}
	for name, value := range ch.Headers {
		key := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		if strings.TrimSpace(value) == "" {
			delete(out, key)
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// effectiveClientProfileID 该渠道实际生效的预设 id：渠道自身配置优先，
// 未配置时回退到全局默认（设置页 / 环境变量）。
func effectiveClientProfileID(ch *Channel) string {
	if ch != nil {
		if id := normalizeClientProfileID(ch.ClientProfile); id != "" {
			return id
		}
	}
	return normalizeClientProfileID(currentPolicy().DefaultClientProfile)
}

// dynamicHeaderValue 按生成器名产生一个头值（每请求调用一次）。
// 未知生成器返回空串（该头本次不发送）。
func dynamicHeaderValue(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "uuid":
		return randomUUID()
	case "uuid32":
		return strings.ReplaceAll(randomUUID(), "-", "")
	case "ts_ms":
		return strconv.FormatInt(time.Now().UnixMilli(), 10)
	case "ts_s":
		return strconv.FormatInt(time.Now().Unix(), 10)
	case "rand16":
		return randomHex(8)
	case "rand32":
		return randomHex(16)
	default:
		return ""
	}
}

// randomUUID 生成 RFC 4122 v4 UUID（动态头里 session id 之类的常用形态）。
func randomUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 熵源不可用时退化为时间戳，宁可重复也不要中断请求
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// clientProfileCatalog 返回供 WebUI/接口展示的预设清单（深拷贝，调用方可随意改）。
func clientProfileCatalog() []clientProfile {
	out := make([]clientProfile, 0, len(clientProfiles))
	for i := range clientProfiles {
		p := clientProfiles[i]
		p.Headers = cloneStringMap(p.Headers)
		p.Dynamic = cloneStringMap(p.Dynamic)
		out = append(out, p)
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
