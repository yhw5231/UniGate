// 网关配置存储：渠道（上游供应商）、渠道内多账号 key（每个 key 可绑定独立代理）、
// 下游通用 key。持久化为 gateway.json（原子写入），供 WebUI 与 Admin API 读写。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ProxyPool 代理池：连接信息（管理端、Token、SOCKS5 地址）作为独立实体配置，
// 渠道 key 通过 ProxySpec.PoolID 引用，不在渠道内重复填写连接信息。
// ProxyPool 代理池：连接信息（管理端、Token、SOCKS5 地址）作为独立实体配置，
// 渠道 key 通过 ProxySpec.PoolID 引用，不在渠道内重复填写连接信息。
type ProxyPool struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	PoolURL   string `json:"pool_url"`             // 管理端基地址，如 http://1.2.3.4:8080
	PoolToken string `json:"pool_token,omitempty"` // Bearer token（可空）
	SocksHost string `json:"socks_host,omitempty"` // SOCKS5 服务地址（默认取池管理端 host）
}

// normalizeProxyPool 清理代理池配置并校验。
func normalizeProxyPool(p *ProxyPool) error {
	p.Name = strings.TrimSpace(p.Name)
	p.PoolURL = strings.TrimSpace(p.PoolURL)
	p.SocksHost = strings.TrimSpace(p.SocksHost)
	if p.Name == "" {
		return errors.New("proxy pool name required")
	}
	if p.PoolURL == "" {
		return errors.New("proxy pool pool_url required")
	}
	if !strings.Contains(p.PoolURL, "://") {
		p.PoolURL = "http://" + p.PoolURL
	}
	p.PoolURL = strings.TrimRight(p.PoolURL, "/")
	return nil
}

// poolNameFromURL 从管理端 URL 提取默认池名称（host:port）。
func poolNameFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return raw
	}
	if u.Port() != "" {
		return u.Hostname() + ":" + u.Port()
	}
	return u.Hostname()
}

// ProxySpec 渠道 key 的代理配置。
type ProxySpec struct {
	Kind string `json:"kind"` // "" / "none" | "static" | "ipv6pool"

	// static：固定代理 URL
	URL string `json:"url,omitempty"` // http(s)://user:pass@host:port 或 socks5://...

	// ipv6pool：引用已配置的代理池（连接信息在 ProxyPool 实体上）
	PoolID string `json:"pool_id,omitempty"` // 代理池 ID

	// 旧格式兼容字段（已迁移到 ProxyPool 实体，仅读取/迁移使用，保存时不再写入）
	PoolURL   string `json:"pool_url,omitempty"`
	PoolToken string `json:"pool_token,omitempty"`

	LeaseID   string `json:"lease_id,omitempty"`   // 租约 ID；空则自动 "gw-<keyID>"
	SocksHost string `json:"socks_host,omitempty"` // SOCKS5 服务地址（默认取池管理端 host；新配置在代理池上）
	Share     bool   `json:"share,omitempty"`      // 跨渠道复用：同「池+BaseURL」分组的 key 共用同一租约/IP

	// 租约行为
	Persistent      bool  `json:"persistent,omitempty"`          // 常驻租约（免空闲回收）
	RotateOnNetErr  bool  `json:"rotate_on_net_err,omitempty"`   // 网络失败自动换 IP
	RotateStatuses  []int `json:"rotate_statuses,omitempty"`     // 上游返回这些状态码时换 IP（如 403,429）
	RotateIntervalS int   `json:"rotate_interval_sec,omitempty"` // 每 N 秒自动换 IP（0 关闭）
	RotateRequests  int   `json:"rotate_requests,omitempty"`     // 每 N 次请求自动换 IP（0 关闭）
}

// normalize 清理代理配置并校验。
func (p *ProxySpec) normalize() error {
	if p == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(p.Kind)) {
	case "", "none", "direct", "-":
		p.Kind = ""
		p.Share = false
		return nil
	case "static", "url":
		p.Kind = "static"
		p.Share = false
		if strings.TrimSpace(p.URL) == "" {
			return errors.New("static proxy requires url")
		}
		if _, err := parseProxyURL(p.URL); err != nil {
			return err
		}
		return nil
	case "ipv6pool", "pool":
		p.Kind = "ipv6pool"
		p.PoolID = strings.TrimSpace(p.PoolID)
		p.PoolURL = strings.TrimSpace(p.PoolURL)
		if p.PoolID == "" && p.PoolURL == "" {
			return errors.New("ipv6pool proxy requires pool_id")
		}
		if p.PoolID != "" {
			p.PoolToken = "" // 连接信息在代理池实体上，清理旧内联值
			return nil
		}
		// 旧格式：内联连接信息（load 时自动迁移为代理池实体）
		if !strings.Contains(p.PoolURL, "://") {
			p.PoolURL = "http://" + p.PoolURL
		}
		p.PoolURL = strings.TrimRight(p.PoolURL, "/")
		return nil
	default:
		return fmt.Errorf("unsupported proxy kind %q", p.Kind)
	}
}

// UpKey 渠道内的一个上游账号。
type UpKey struct {
	ID      string     `json:"id"`
	Name    string     `json:"name"`
	APIKey  string     `json:"api_key"`
	BaseURL string     `json:"base_url,omitempty"` // 覆盖渠道 BaseURL
	Enabled bool       `json:"enabled"`
	Proxy   *ProxySpec `json:"proxy,omitempty"`
}

// Channel 一个上游渠道（OpenAI 兼容供应商）。
type Channel struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Group         string            `json:"group,omitempty"`          // 自定义分组标签（仅用于 WebUI 归类/过滤，空为未分组）
	BaseURL       string            `json:"base_url"`                 // 如 https://api.cline.bot/api/v1
	EndpointType  string            `json:"endpoint_type,omitempty"`  // 上游对话端点类型："chat"（默认，/chat/completions）；"responses"（OpenAI Responses API /responses）
	ModelsURL     string            `json:"models_url,omitempty"`     // 模型列表端点；默认 BaseURL + /models
	Models        []string          `json:"models,omitempty"`         // 静态模型列表（用于 /v1/models 聚合与路由过滤）
	Headers       map[string]string `json:"headers,omitempty"`        // 渠道级自定义请求头
	Rewrite       bool              `json:"rewrite_reasoning"`        // reasoning -> reasoning_content 改写（Cline 等需要）
	CooldownScope string            `json:"cooldown_scope,omitempty"` // 冷却粒度："" / "key" 按 key 跨模型共享（默认）；"key_model" 按 (key,model)
	Enabled       bool              `json:"enabled"`
	Keys          []*UpKey          `json:"keys"`
}

// endpointChat / endpointResponses 渠道对话端点类型。
const (
	endpointChat      = "chat"
	endpointResponses = "responses"
)

// normalizeEndpointType 归一化端点类型（"" 视为默认 chat）。
func normalizeEndpointType(t string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "", "chat", "chat_completions", "chat-completions", "chat/completions":
		return endpointChat, nil
	case "responses", "response", "openai_responses", "openai/responses":
		return endpointResponses, nil
	default:
		return "", fmt.Errorf("unsupported endpoint_type %q (want \"chat\" / \"responses\")", t)
	}
}

// cooldownScopeKeyModel 冷却粒度：按 (key, model) 独立冷却（显式选择时使用）。
const cooldownScopeKeyModel = "key_model"

// normalizeCooldownScope 归一化冷却粒度（"" 视为默认 key）。
func normalizeCooldownScope(scope string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case cooldownScopeKeyModel, "key-model":
		return cooldownScopeKeyModel, nil
	case "", "key":
		return "key", nil
	default:
		return "", fmt.Errorf("unsupported cooldown_scope %q (want \"key\" / \"key_model\")", scope)
	}
}

// chatURL 返回该渠道的 chat/completions 端点。
func (c *Channel) chatURL() string {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return ""
	}
	if strings.HasSuffix(base, "/chat/completions") {
		return base
	}
	return base + "/chat/completions"
}

// responsesURL 返回该渠道的 OpenAI Responses API 端点（/v1/responses）。
func (c *Channel) responsesURL() string {
	return responsesURLOf(strings.TrimRight(strings.TrimSpace(c.BaseURL), "/"))
}

// responsesURLOf 在给定 base 上推导 Responses API 端点。
func responsesURLOf(base string) string {
	if base == "" {
		return ""
	}
	// 已指到 /responses 端点则直接使用
	if strings.HasSuffix(base, "/responses") {
		return base
	}
	// base 末段已是版本号（如 /v1、/api/v2）时直接在其下挂 /responses，
	// 否则补一段 /v1（OpenAI 官方为 /v1/responses）
	if last := lastSeg(base); len(last) > 1 && last[0] == 'v' && isAllDigits(last[1:]) {
		return base + "/responses"
	}
	return base + "/v1/responses"
}

// lastSeg 返回 URL path 的最后一段（不含前导 /）。
func lastSeg(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// endpointURL 按渠道端点类型返回对话端点。
func (c *Channel) endpointURL() string {
	if c.EndpointType == endpointResponses {
		return c.responsesURL()
	}
	return c.chatURL()
}

// modelsURL 返回该渠道的模型列表端点。
func (c *Channel) modelsURL() string {
	if u := strings.TrimSpace(c.ModelsURL); u != "" {
		return u
	}
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return ""
	}
	return base + "/models"
}

// allowsModel 判断渠道是否声明支持该模型：无模型信息（静态列表与已拉取列表皆空）时放行所有。
func (c *Channel) allowsModel(model string) bool {
	if model == "" {
		return true
	}
	if len(c.Models) == 0 {
		return true // 无声明，不设限
	}
	for _, m := range c.Models {
		if m == model {
			return true
		}
	}
	return false
}

// keyByID 查找渠道内的上游 key。
func (c *Channel) keyByID(id string) *UpKey {
	for _, k := range c.Keys {
		if k.ID == id {
			return k
		}
	}
	return nil
}

// GWKey 下游通用 key（供客户端调用本网关）。
type GWKey struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Key       string    `json:"key"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// gatewayConfig gateway.json 的持久化格式。
type gatewayConfig struct {
	Channels   []*Channel   `json:"channels"`
	GWKeys     []*GWKey     `json:"gateway_keys"`
	ProxyPools []*ProxyPool `json:"proxy_pools"`
}

// GatewayStore 配置存储（进程内单例，mutex 保护）。
type GatewayStore struct {
	mu   sync.RWMutex
	path string
	data gatewayConfig
}

var store *GatewayStore

func newGatewayStore(path string) *GatewayStore {
	return &GatewayStore{path: path}
}

// load 从磁盘读取；文件不存在时初始化空配置并落盘。
func (s *GatewayStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		s.data = gatewayConfig{Channels: []*Channel{}, GWKeys: []*GWKey{}}
		return s.saveLocked()
	}
	if err != nil {
		return fmt.Errorf("read gateway config: %w", err)
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		s.data = gatewayConfig{Channels: []*Channel{}, GWKeys: []*GWKey{}}
		return nil
	}
	var data gatewayConfig
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("decode gateway config: %w", err)
	}
	if data.Channels == nil {
		data.Channels = []*Channel{}
	}
	if data.GWKeys == nil {
		data.GWKeys = []*GWKey{}
	}
	if data.ProxyPools == nil {
		data.ProxyPools = []*ProxyPool{}
	}
	for _, ch := range data.Channels {
		if ch.Keys == nil {
			ch.Keys = []*UpKey{}
		}
	}
	s.data = data
	if s.migrateInlinePoolsLocked() {
		return s.saveLocked() // 迁移写回，保证之后保存的都是新的引用格式
	}
	return nil
}

// migrateInlinePoolsLocked 把旧格式的内联代理池连接信息（key 上的 pool_url/
// pool_token/socks_host）迁移为独立的 ProxyPool 实体：按连接信息合并去重，
// key 改为引用 PoolID 并清空内联字段（调用方持有写锁）。无变化时返回 false。
func (s *GatewayStore) migrateInlinePoolsLocked() bool {
	byKey := map[string]*ProxyPool{}
	for _, p := range s.data.ProxyPools {
		byKey[poolConnKey(p.PoolURL, p.PoolToken, p.SocksHost)] = p
	}
	changed := false
	for _, ch := range s.data.Channels {
		for _, k := range ch.Keys {
			spec := k.Proxy
			if spec == nil || spec.Kind != "ipv6pool" || strings.TrimSpace(spec.PoolID) != "" {
				continue
			}
			if strings.TrimSpace(spec.PoolURL) == "" {
				continue // 无连接信息也无法迁移，交给 normalize 报错
			}
			key := poolConnKey(spec.PoolURL, spec.PoolToken, spec.SocksHost)
			pool := byKey[key]
			if pool == nil {
				pool = &ProxyPool{
					ID:        randomHex(8),
					Name:      poolNameFromURL(spec.PoolURL),
					PoolURL:   spec.PoolURL,
					PoolToken: spec.PoolToken,
					SocksHost: spec.SocksHost,
				}
				s.data.ProxyPools = append(s.data.ProxyPools, pool)
				byKey[key] = pool
			}
			spec.PoolID = pool.ID
			spec.PoolURL = ""
			spec.PoolToken = ""
			spec.SocksHost = ""
			changed = true
		}
	}
	return changed
}

// poolConnKey 代理池连接信息的去重键。
func poolConnKey(poolURL, token, socksHost string) string {
	return strings.TrimRight(strings.TrimSpace(poolURL), "/") + "|" + token + "|" + socksHost
}

// saveLocked 原子落盘（调用方持有写锁）。
func (s *GatewayStore) saveLocked() error {
	dir := filepath.Dir(s.path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create config dir: %w", err)
		}
	}
	body, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return fmt.Errorf("write config tmp: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

// Snapshot 返回配置深拷贝（含全部敏感字段，仅 Admin API 使用）。
func (s *GatewayStore) Snapshot() gatewayConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, _ := json.Marshal(s.data)
	var out gatewayConfig
	_ = json.Unmarshal(body, &out)
	return out
}

// PutChannel 新增或整体替换渠道（含内嵌 keys）。id 为空时生成。
func (s *GatewayStore) PutChannel(ch *Channel) error {
	if err := normalizeChannel(ch); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// 校验 ipv6pool key 引用的代理池存在（连接信息统一在池实体上配置）
	for _, k := range ch.Keys {
		if k.Proxy != nil && k.Proxy.Kind == "ipv6pool" && k.Proxy.PoolID != "" && s.proxyPoolByIDLocked(k.Proxy.PoolID) == nil {
			return fmt.Errorf("ipv6pool key %q references unknown proxy pool %q (create it on the pool page first)", k.Name, k.Proxy.PoolID)
		}
	}
	if ch.ID == "" {
		ch.ID = randomHex(8)
	}
	replaced := false
	for i, cur := range s.data.Channels {
		if cur.ID == ch.ID {
			s.data.Channels[i] = ch
			replaced = true
			break
		}
	}
	if !replaced {
		s.data.Channels = append(s.data.Channels, ch)
	}
	return s.saveLocked()
}

func normalizeChannel(ch *Channel) error {
	ch.Name = strings.TrimSpace(ch.Name)
	ch.Group = strings.TrimSpace(ch.Group)
	ch.BaseURL = strings.TrimSpace(ch.BaseURL)
	if ch.Name == "" {
		return errors.New("channel name required")
	}
	if ch.BaseURL == "" {
		return errors.New("channel base_url required")
	}
	et, err := normalizeEndpointType(ch.EndpointType)
	if err != nil {
		return err
	}
	ch.EndpointType = et
	scope, err := normalizeCooldownScope(ch.CooldownScope)
	if err != nil {
		return err
	}
	ch.CooldownScope = scope
	if ch.Keys == nil {
		ch.Keys = []*UpKey{}
	}
	for _, k := range ch.Keys {
		if err := normalizeUpKey(k); err != nil {
			return fmt.Errorf("key %q: %w", k.Name, err)
		}
		if k.ID == "" {
			k.ID = randomHex(8)
		}
	}
	return nil
}

// normalizeModelList 清理模型 ID 列表：去空白、去空项、去重、保序。
func normalizeModelList(list []string) []string {
	out := make([]string, 0, len(list))
	seen := map[string]bool{}
	for _, m := range list {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeUpKey(k *UpKey) error {
	k.Name = strings.TrimSpace(k.Name)
	k.APIKey = strings.TrimSpace(k.APIKey)
	k.BaseURL = strings.TrimSpace(k.BaseURL)
	return k.Proxy.normalize()
}

// DeleteChannel 删除渠道。
func (s *GatewayStore) DeleteChannel(id string) (*Channel, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, ch := range s.data.Channels {
		if ch.ID == id {
			removed := ch
			s.data.Channels = append(s.data.Channels[:i], s.data.Channels[i+1:]...)
			_ = s.saveLocked()
			return removed, true
		}
	}
	return nil, false
}

// PutProxyPool 新增或整体替换代理池。id 为空时生成。
func (s *GatewayStore) PutProxyPool(p *ProxyPool) error {
	if err := normalizeProxyPool(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.ID == "" {
		p.ID = randomHex(8)
	}
	replaced := false
	for i, cur := range s.data.ProxyPools {
		if cur.ID == p.ID {
			s.data.ProxyPools[i] = p
			replaced = true
			break
		}
	}
	if !replaced {
		s.data.ProxyPools = append(s.data.ProxyPools, p)
	}
	return s.saveLocked()
}

// DeleteProxyPool 删除代理池。仍被渠道 key 引用时拒绝并返回引用渠道名。
// 返回 (删除的池, 是否删除, 引用方描述)；引用方非空时删除失败。
func (s *GatewayStore) DeleteProxyPool(id string) (*ProxyPool, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pool := s.proxyPoolByIDLocked(id)
	if pool == nil {
		return nil, false, ""
	}
	// 引用检查：key 显式绑定的 PoolID，或尚未迁移的旧内联同 URL 配置
	var usedBy []string
	for _, ch := range s.data.Channels {
		for _, k := range ch.Keys {
			if k.Proxy == nil || k.Proxy.Kind != "ipv6pool" {
				continue
			}
			if k.Proxy.PoolID == id ||
				(k.Proxy.PoolID == "" && strings.TrimRight(strings.TrimSpace(k.Proxy.PoolURL), "/") == pool.PoolURL) {
				usedBy = append(usedBy, ch.Name)
				break // 同一渠道只记一次
			}
		}
	}
	if len(usedBy) > 0 {
		return nil, false, strings.Join(usedBy, "、")
	}
	for i, cur := range s.data.ProxyPools {
		if cur.ID == id {
			removed := cur
			s.data.ProxyPools = append(s.data.ProxyPools[:i], s.data.ProxyPools[i+1:]...)
			_ = s.saveLocked()
			return removed, true, ""
		}
	}
	return nil, false, ""
}

// proxyPoolByIDLocked 按 ID 查找代理池（调用方持有读/写锁）。
func (s *GatewayStore) proxyPoolByIDLocked(id string) *ProxyPool {
	if id == "" {
		return nil
	}
	for _, p := range s.data.ProxyPools {
		if p.ID == id {
			return p
		}
	}
	return nil
}

// proxyPoolByID 按 ID 查找代理池（快照，供锁外调用）。
func (s *GatewayStore) proxyPoolByID(id string) (*ProxyPool, bool) {
	if s == nil || id == "" {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p := s.proxyPoolByIDLocked(id); p != nil {
		body, _ := json.Marshal(p)
		var out ProxyPool
		_ = json.Unmarshal(body, &out)
		return &out, true
	}
	return nil, false
}

// poolSpecReady 把 ipv6pool key 上绑定的池引用解析为带连接信息的完整 spec（快照）。
// 新格式（PoolID）从代理池实体取连接信息；旧格式（内联 PoolURL）原样使用，
// 用于兼容存量配置与单元测试。非 ipv6pool 原样返回。
func poolSpecReady(spec *ProxySpec) (*ProxySpec, error) {
	if spec == nil || spec.Kind != "ipv6pool" {
		return spec, nil
	}
	if strings.TrimSpace(spec.PoolID) != "" {
		pool, ok := store.proxyPoolByID(spec.PoolID)
		if !ok {
			return nil, fmt.Errorf("proxy pool %q not found", spec.PoolID)
		}
		merged := *spec
		merged.PoolURL = pool.PoolURL
		merged.PoolToken = pool.PoolToken
		merged.SocksHost = pool.SocksHost
		return &merged, nil
	}
	if strings.TrimSpace(spec.PoolURL) == "" {
		return nil, errors.New("ipv6pool proxy requires pool_id")
	}
	return spec, nil // 旧格式：连接信息内联
}

// PutGWKey 新增或更新下游 key。
func (s *GatewayStore) PutGWKey(k *GWKey) error {
	k.Name = strings.TrimSpace(k.Name)
	k.Key = strings.TrimSpace(k.Key)
	if k.Name == "" {
		return errors.New("gateway key name required")
	}
	if k.Key == "" {
		return errors.New("gateway key required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if k.ID == "" {
		k.ID = randomHex(8)
	}
	if k.CreatedAt.IsZero() {
		k.CreatedAt = time.Now()
	}
	replaced := false
	for i, cur := range s.data.GWKeys {
		if cur.ID == k.ID {
			k.CreatedAt = cur.CreatedAt
			s.data.GWKeys[i] = k
			replaced = true
			break
		}
	}
	if !replaced {
		s.data.GWKeys = append(s.data.GWKeys, k)
	}
	return s.saveLocked()
}

// DeleteGWKey 删除下游 key。
func (s *GatewayStore) DeleteGWKey(id string) (*GWKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, k := range s.data.GWKeys {
		if k.ID == id {
			removed := k
			s.data.GWKeys = append(s.data.GWKeys[:i], s.data.GWKeys[i+1:]...)
			_ = s.saveLocked()
			return removed, true
		}
	}
	return nil, false
}

// FindUpKey 按 keyID 全局查找（返回渠道 + key 快照）。
func (s *GatewayStore) FindUpKey(keyID string) (*Channel, *UpKey, bool) {
	snap := s.Snapshot()
	for _, ch := range snap.Channels {
		if k := ch.keyByID(keyID); k != nil {
			return ch, k, true
		}
	}
	return nil, nil, false
}

// FindUpKey2 按 (channelID, keyID) 精确查找（返回快照）。
func (s *GatewayStore) FindUpKey2(channelID, keyID string) (*Channel, *UpKey, bool) {
	snap := s.Snapshot()
	for _, ch := range snap.Channels {
		if ch.ID != channelID {
			continue
		}
		if k := ch.keyByID(keyID); k != nil {
			return ch, k, true
		}
	}
	return nil, nil, false
}

// newGWKeyValue 生成下游 key：sk-gw-<32hex>。
func newGWKeyValue() string {
	return "sk-gw-" + randomHex(16)
}
