// ipv6-proxy-pool 集成：管理端 REST 客户端 + 租约生命周期管理。
//
// 支持池子的两种 SOCKS5 模式（自动探测）：
//   - per_ipv6：每个租约独立 SOCKS5 端口 → socks5://socksHost:<lease.Port>
//   - multiplex：所有租约共用基础端口，凭用户名 user:<leaseID> 区分
//     → socks5://user:<leaseID>@socksHost:<basePort>
//
// 生命周期：Ensure（解析分配 → 申请/复用，幂等）→ RecordUse（计数）→ Rotate（换 IP）
// → Reconcile（按当前配置回收孤儿租约）。申请接口幂等（同 ID 直接返回现有租约），
// 因此网关重启后无需额外恢复逻辑。自动换 IP 策略：按时间间隔、按请求次数（懒触发）、
// 按上游失败（网络错误 / 指定状态码，由路由引擎回调 Rotate）。
//
// 跨渠道复用（ProxySpec.Share）：本地维护「key → 租约」分配表（持久化到
// lease-assignments.json，重启不变、IP 稳定）。分配约束：
//   - 同一分组（生效 BaseURL 相同，即同一上游站点）的 key 保证不同租约（不同 IP），
//     避免同站多账号共用 IP 触发风控；
//   - 不同分组的 key 允许共用同一租约（对每个上游仍是一个账号一个 IP）；
//   - 新分配做最优装填（优先复用已承载分组数最多的可用租约），池容量不足才新申请；
//   - 显式配置 LeaseID 时跳过自动分配（手动绑定，约束由用户自担）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PoolLease 对应池子的 Lease 对象（只取网关关心的字段）。
type PoolLease struct {
	ID         string `json:"id"`
	IPv6       string `json:"ipv6"`
	Port       int    `json:"port"`
	Persistent bool   `json:"persistent"`
	Role       string `json:"role"`
}

// PoolStatus 对应池子 GET /v1/status 的响应。
type PoolStatus struct {
	Status             string `json:"status"`
	LeaseCount         int    `json:"lease_count"`
	StandbyCount       int    `json:"standby_count"`
	MinLeases          int    `json:"min_leases"`
	MaxLeases          int    `json:"max_leases"`
	IPv6Prefix         string `json:"ipv6_prefix"`
	SocksMode          string `json:"socks_mode"`
	SocksListenAddress string `json:"socks_listen_address"`
}

// PoolClient 池子管理端 REST 客户端。
type PoolClient struct {
	base  string
	token string
	httpc *http.Client
}

func newPoolClient(baseURL, token string) *PoolClient {
	base := strings.TrimSpace(baseURL)
	if base != "" && !strings.Contains(base, "://") {
		base = "http://" + base
	}
	return &PoolClient{
		base:  strings.TrimRight(base, "/"),
		token: token,
		httpc: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *PoolClient) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := fmt.Sprintf("pool api %s %s returned %d", method, path, resp.StatusCode)
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			msg = e.Error
		}
		return nil, &poolAPIError{status: resp.StatusCode, msg: msg}
	}
	return data, nil
}

// poolAPIError 池子返回的非 2xx 错误（保留状态码供上层判断，如 404）。
type poolAPIError struct {
	status int
	msg    string
}

func (e *poolAPIError) Error() string { return e.msg }

// Status 读取池子状态。
func (c *PoolClient) Status(ctx context.Context) (*PoolStatus, error) {
	data, err := c.do(ctx, http.MethodGet, "/v1/status", nil)
	if err != nil {
		return nil, err
	}
	var st PoolStatus
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// Acquire 申请租约（幂等：ID 已存在直接返回现有租约）。
func (c *PoolClient) Acquire(ctx context.Context, id string, persistent bool) (*PoolLease, error) {
	data, err := c.do(ctx, http.MethodPost, "/v1/leases", map[string]any{
		"id":         id,
		"persistent": persistent,
	})
	if err != nil {
		return nil, err
	}
	var lease PoolLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

// GetLease 查询租约。
func (c *PoolClient) GetLease(ctx context.Context, id string) (*PoolLease, error) {
	data, err := c.do(ctx, http.MethodGet, "/v1/leases/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	var lease PoolLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

// Rotate 更换租约出口 IP（per_ipv6 模式下端口不变）。
func (c *PoolClient) Rotate(ctx context.Context, id string) (*PoolLease, error) {
	data, err := c.do(ctx, http.MethodPost, "/v1/leases/"+url.PathEscape(id)+"/rotate", nil)
	if err != nil {
		return nil, err
	}
	var lease PoolLease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, err
	}
	return &lease, nil
}

// Release 销毁租约（404 视为成功）。
func (c *PoolClient) Release(ctx context.Context, id string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/leases/"+url.PathEscape(id), nil)
	var perr *poolAPIError
	if err != nil && (!errors.As(err, &perr) || perr.status != http.StatusNotFound) {
		return err
	}
	return nil
}

// ---- 租约管理器 ----

type leaseEntry struct {
	lease      *PoolLease
	multiplex  bool
	socksPort  int    // multiplex 模式的基础 SOCKS5 端口
	poolToken  string // 最近一次使用的池 token（供缓存直连操作复用）
	lastRotate time.Time
	requests   int
}

// leaseAssign 一条「key → 租约」分配记录。
type leaseAssign struct {
	LeaseID string `json:"lease_id"`
	Group   string `json:"group"`
	Shared  bool   `json:"shared"`
}

// LeaseManager 管理全部 ipv6pool 租约：进程内缓存 + 池端幂等申请 +
// 跨渠道复用的分配表（可持久化）。
type LeaseManager struct {
	mu        sync.Mutex
	path      string                  // 分配表持久化文件；空 = 不持久化（测试）
	entries   map[string]*leaseEntry  // key: poolURL|leaseID
	socksBase map[string]int          // poolURL → multiplex 基础端口
	assigns   map[string]*leaseAssign // key: poolURL|keyID
}

var leaseMgr = newLeaseManager()

func newLeaseManager() *LeaseManager {
	return &LeaseManager{
		entries:   map[string]*leaseEntry{},
		socksBase: map[string]int{},
		assigns:   map[string]*leaseAssign{},
	}
}

// persistedAssigns lease-assignments.json 的文件格式。
type persistedAssigns struct {
	Assignments map[string]*leaseAssign `json:"assignments"`
}

// SetPersistPath 设置分配表持久化文件并加载已有分配（进程启动时调用一次）。
func (m *LeaseManager) SetPersistPath(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.path = path
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read lease assignments: %w", err)
	}
	var data persistedAssigns
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("decode lease assignments: %w", err)
	}
	if data.Assignments != nil {
		m.assigns = data.Assignments
	}
	return nil
}

// AssignCount 返回当前分配记录数（诊断用）。
func (m *LeaseManager) AssignCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.assigns)
}

// saveAssignsLocked 原子落盘分配表（调用方持有写锁；未配置路径时跳过）。
func (m *LeaseManager) saveAssignsLocked() {
	if m.path == "" {
		return
	}
	dir := filepath.Dir(m.path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			log.Printf("save lease assignments: %v", err)
			return
		}
	}
	body, err := json.MarshalIndent(persistedAssigns{Assignments: m.assigns}, "", "  ")
	if err != nil {
		return
	}
	body = append(body, '\n')
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		log.Printf("save lease assignments: %v", err)
		return
	}
	if err := os.Rename(tmp, m.path); err != nil {
		_ = os.Remove(tmp)
		log.Printf("save lease assignments: %v", err)
	}
}

func assignKey(poolURL, keyID string) string { return poolURL + "|" + keyID }

func splitAssignKey(k string) (poolURL, keyID string) {
	parts := strings.SplitN(k, "|", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return k, ""
}

func leaseCacheKey(poolURL, leaseID string) string { return poolURL + "|" + leaseID }

// poolHost 提取池管理端 URL 的 host（SOCKS5 默认与之同机）。
func poolHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// socksBasePort 解析池状态中的 SOCKS5 基础监听端口（multiplex 模式用）。
func socksBasePort(listenAddr string) int {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(port)
	return n
}

// leaseIDFor 非共享模式的确定性租约 ID：显式配置优先，否则 gw-<keyID>。
func leaseIDFor(spec *ProxySpec, keyID string) string {
	if id := strings.TrimSpace(spec.LeaseID); id != "" {
		return id
	}
	return "gw-" + keyID
}

// proxyGroup 计算代理共享分组（生效 BaseURL）：key 级 BaseURL 覆盖优先，否则渠道 BaseURL。
// 尾部斜杠归一化，避免同一渠道因书写差异被拆成两组。
func proxyGroup(ch *Channel, k *UpKey) string {
	u := ""
	if k != nil {
		u = strings.TrimSpace(k.BaseURL)
	}
	if u == "" && ch != nil {
		u = strings.TrimSpace(ch.BaseURL)
	}
	return strings.TrimRight(u, "/")
}

// leaseUsageLocked 统计某池下每个租约的占用情况（调用方持有写锁）：
//   - groups: 租约 → 占用它的分组集合
//   - shareable: 租约是否可参与共享装填（其全部分配都来自 Share 模式；
//     只要有一个专用（非 Share）key 占用，就不可复用）
func (m *LeaseManager) leaseUsageLocked(poolURL string) (groups map[string]map[string]bool, shareable map[string]bool) {
	groups = map[string]map[string]bool{}
	dedicated := map[string]bool{}
	for ak, a := range m.assigns {
		pool, _ := splitAssignKey(ak)
		if pool != poolURL {
			continue
		}
		if groups[a.LeaseID] == nil {
			groups[a.LeaseID] = map[string]bool{}
		}
		groups[a.LeaseID][a.Group] = true
		if !a.Shared {
			dedicated[a.LeaseID] = true
		}
	}
	shareable = map[string]bool{}
	for lid := range groups {
		shareable[lid] = !dedicated[lid]
	}
	return groups, shareable
}

// groupOnLeaseLocked 判断（除 excludeKey 外）是否已有 group 的 key 占用租约
// （调用方持有写锁）。
func (m *LeaseManager) groupOnLeaseLocked(poolURL, leaseID, group, excludeKey string) bool {
	for ak, a := range m.assigns {
		pool, keyID := splitAssignKey(ak)
		if pool == poolURL && a.LeaseID == leaseID && keyID != excludeKey && a.Group == group {
			return true
		}
	}
	return false
}

// recordAssignLocked 记录/覆盖分配（调用方持有写锁）。
func (m *LeaseManager) recordAssignLocked(poolURL, keyID, leaseID, group string, shared bool) {
	m.assigns[assignKey(poolURL, keyID)] = &leaseAssign{LeaseID: leaseID, Group: group, Shared: shared}
	m.saveAssignsLocked()
}

// leaseReferencedLocked 租约是否仍被任一分配引用（同池，调用方持有写锁）。
func (m *LeaseManager) leaseReferencedLocked(poolURL, leaseID string) bool {
	for ak, a := range m.assigns {
		pool, _ := splitAssignKey(ak)
		if pool == poolURL && a.LeaseID == leaseID {
			return true
		}
	}
	return false
}

// resolveLease 计算某 key 应使用的租约 ID（调用方持有写锁）。
// 返回 needNew=true 表示本地代理池无可复用租约，需按 gw-<keyID> 新申请。
func (m *LeaseManager) resolveLease(spec *ProxySpec, keyID, group string) (leaseID string, needNew bool) {
	pool := spec.PoolURL
	// 显式租约 ID：手动绑定，跳过自动分配
	if id := strings.TrimSpace(spec.LeaseID); id != "" {
		m.recordAssignLocked(pool, keyID, id, group, false)
		return id, false
	}
	// 非共享：每 key 独占
	if !spec.Share {
		id := "gw-" + keyID
		m.recordAssignLocked(pool, keyID, id, group, false)
		return id, false
	}
	// 共享：已有分配且仍满足「同组互斥」约束 → 直接复用（粘性）。
	// 约束检查对象是「其它 key」对该租约的占用：仅本 key 自己的记录不算冲突
	//（分配表里的分组集合包含本 key 自己的分组，属正常状态）。
	if a, ok := m.assigns[assignKey(pool, keyID)]; ok && a.Shared {
		_, shareable := m.leaseUsageLocked(pool)
		if shareable[a.LeaseID] && !m.groupOnLeaseLocked(pool, a.LeaseID, group, keyID) {
			return a.LeaseID, false
		}
	}
	// 最优装填：优先复用已承载分组数最多的、可共享的、且不包含本分组的租约
	groups, shareable := m.leaseUsageLocked(pool)
	best := ""
	for lid, gs := range groups {
		if gs[group] || !shareable[lid] {
			continue
		}
		if best == "" || len(gs) > len(groups[best]) || (len(gs) == len(groups[best]) && lid < best) {
			best = lid
		}
	}
	if best != "" {
		m.recordAssignLocked(pool, keyID, best, group, true)
		return best, false
	}
	return "", true
}

// refreshEntryLocked 创建/刷新租约缓存条目（调用方持有写锁）。
func (m *LeaseManager) refreshEntryLocked(ck string, lease *PoolLease, spec *ProxySpec) *leaseEntry {
	entry, ok := m.entries[ck]
	if !ok {
		entry = &leaseEntry{lastRotate: time.Now()}
		m.entries[ck] = entry
	}
	entry.lease = lease
	entry.multiplex = lease.Port == 0
	entry.poolToken = spec.PoolToken
	if entry.multiplex {
		entry.socksPort = m.socksBase[spec.PoolURL] // 可能尚未查询，由调用方补齐
	}
	return entry
}

// Ensure 返回该 key 当前可用的 SOCKS5 路由（必要时分配并申请租约；命中轮换策略时先换 IP）。
// group 为共享分组（proxyGroup），仅 Share 模式的分配约束使用。
func (m *LeaseManager) Ensure(ctx context.Context, spec *ProxySpec, keyID, group string) (*ProxyRoute, error) {
	if spec == nil || spec.Kind != "ipv6pool" {
		return nil, errors.New("not an ipv6pool proxy spec")
	}
	resolved, err := poolSpecReady(spec)
	if err != nil {
		return nil, err
	}
	spec = resolved
	client := newPoolClient(spec.PoolURL, spec.PoolToken)

	m.mu.Lock()
	leaseID, needNew := m.resolveLease(spec, keyID, group)
	ck := leaseCacheKey(spec.PoolURL, leaseID)
	entry, cached := m.entries[ck]
	needRotate := false
	if cached {
		now := time.Now()
		if spec.RotateIntervalS > 0 && now.Sub(entry.lastRotate) > time.Duration(spec.RotateIntervalS)*time.Second {
			needRotate = true
		}
		if spec.RotateRequests > 0 && entry.requests >= spec.RotateRequests {
			needRotate = true
		}
	}
	m.mu.Unlock()

	if needNew || !cached {
		// 新分配 / 缓存未命中：向池子申请（幂等，同 ID 返回现有租约）
		id := leaseID
		if needNew {
			id = "gw-" + keyID
		}
		lease, err := client.Acquire(ctx, id, spec.Persistent)
		if err != nil {
			return nil, fmt.Errorf("acquire lease %s from %s: %w", id, spec.PoolURL, err)
		}
		if needNew {
			m.mu.Lock()
			m.recordAssignLocked(spec.PoolURL, keyID, lease.ID, group, true)
			m.mu.Unlock()
			leaseID = lease.ID
			ck = leaseCacheKey(spec.PoolURL, leaseID)
		}
		m.mu.Lock()
		entry = m.refreshEntryLocked(ck, lease, spec)
		needBasePort := entry.multiplex && entry.socksPort == 0
		m.mu.Unlock()

		if needBasePort {
			// multiplexPort 内部会加锁，必须在锁外调用
			port, err := m.multiplexPort(ctx, client, spec.PoolURL)
			if err != nil {
				return nil, err
			}
			m.mu.Lock()
			entry.socksPort = port
			m.mu.Unlock()
		}
	} else if needRotate {
		// 换 IP 失败不阻塞请求：保留旧租约继续用，仅记录日志
		if lease, err := client.Rotate(ctx, leaseID); err == nil {
			m.mu.Lock()
			entry.lease = lease
			entry.lastRotate = time.Now()
			entry.requests = 0
			m.mu.Unlock()
		} else {
			log.Printf("lease %s rotate failed: %v", leaseID, err)
		}
	}

	host := strings.TrimSpace(spec.SocksHost)
	if host == "" {
		host = poolHost(spec.PoolURL)
	}
	if host == "" {
		return nil, fmt.Errorf("cannot resolve socks host for pool %s", spec.PoolURL)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.multiplex {
		if entry.socksPort == 0 {
			return nil, fmt.Errorf("pool %s multiplex socks port unknown", spec.PoolURL)
		}
		return &ProxyRoute{Kind: "socks5", Addr: net.JoinHostPort(host, strconv.Itoa(entry.socksPort)),
			User: "user:" + leaseID, Pass: "x"}, nil
	}
	if entry.lease.Port == 0 {
		return nil, fmt.Errorf("pool %s lease %s has no socks port", spec.PoolURL, leaseID)
	}
	return &ProxyRoute{Kind: "socks5", Addr: net.JoinHostPort(host, strconv.Itoa(entry.lease.Port))}, nil
}

// multiplexPort 查询（并缓存）multiplex 模式的基础 SOCKS5 端口。
func (m *LeaseManager) multiplexPort(ctx context.Context, client *PoolClient, poolBase string) (int, error) {
	m.mu.Lock()
	if p, ok := m.socksBase[poolBase]; ok && p > 0 {
		m.mu.Unlock()
		return p, nil
	}
	m.mu.Unlock()

	st, err := client.Status(ctx)
	if err != nil {
		return 0, fmt.Errorf("query pool status: %w", err)
	}
	p := socksBasePort(st.SocksListenAddress)
	if p == 0 {
		return 0, fmt.Errorf("pool %s reported no socks listen address", poolBase)
	}
	m.mu.Lock()
	m.socksBase[poolBase] = p
	m.mu.Unlock()
	return p, nil
}

// leaseIDForKey 解析 key 当前绑定的租约 ID（分配表优先，确定性 ID 兜底）。
func (m *LeaseManager) leaseIDForKey(spec *ProxySpec, keyID string) string {
	if a, ok := m.assigns[assignKey(spec.PoolURL, keyID)]; ok {
		return a.LeaseID
	}
	return leaseIDFor(spec, keyID)
}

// RecordUse 累计租约的请求次数（共享租约按租约累计，供按次数轮换策略使用）。
func (m *LeaseManager) RecordUse(spec *ProxySpec, keyID string) {
	if spec == nil || spec.Kind != "ipv6pool" {
		return
	}
	resolved, err := poolSpecReady(spec)
	if err != nil {
		return // 池不可解析时无法计数，忽略
	}
	spec = resolved
	leaseID := m.leaseIDForKey(spec, keyID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[leaseCacheKey(spec.PoolURL, leaseID)]; ok {
		e.requests++
	}
}

// Rotate 强制更换某 key 所用租约的出口 IP（共享租约的所有使用方一起切换）。
func (m *LeaseManager) Rotate(ctx context.Context, spec *ProxySpec, keyID string) (*PoolLease, error) {
	if spec == nil || spec.Kind != "ipv6pool" {
		return nil, errors.New("not an ipv6pool proxy spec")
	}
	resolved, err := poolSpecReady(spec)
	if err != nil {
		return nil, err
	}
	spec = resolved
	leaseID := m.leaseIDForKey(spec, keyID)
	lease, err := newPoolClient(spec.PoolURL, spec.PoolToken).Rotate(ctx, leaseID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if e, ok := m.entries[leaseCacheKey(spec.PoolURL, leaseID)]; ok {
		e.lease = lease
		e.lastRotate = time.Now()
		e.requests = 0
	}
	m.mu.Unlock()
	return lease, nil
}

// livePoolKey Reconcile 的在用 key 信息。
type livePoolKey struct {
	Group  string
	Shared bool
}

// Reconcile 按当前配置回收租约：删除/迁移/改组的 key 释放其分配；
// 不再被任何 key 引用的租约从池子销毁。每次渠道配置变更后由 Admin API 调用。
// live: poolURL → keyID → 在用信息（含停用的 key，保留其租约与 IP）。
func (m *LeaseManager) Reconcile(live map[string]map[string]livePoolKey) {
	m.mu.Lock()
	changed := false
	type orphan struct{ pool, leaseID string }
	var orphans []orphan
	for ak, a := range m.assigns {
		pool, keyID := splitAssignKey(ak)
		lk, liveOK := live[pool][keyID]
		stale := !liveOK ||
			a.Shared != lk.Shared ||
			(a.Shared && a.Group != lk.Group)
		if !stale {
			continue
		}
		delete(m.assigns, ak)
		changed = true
		if !m.leaseReferencedLocked(pool, a.LeaseID) {
			orphans = append(orphans, orphan{pool, a.LeaseID})
		}
	}
	if changed {
		m.saveAssignsLocked()
	}
	type rel struct{ pool, leaseID, token string }
	var rels []rel
	for _, o := range orphans {
		token := ""
		if e, ok := m.entries[leaseCacheKey(o.pool, o.leaseID)]; ok {
			token = e.poolToken
		}
		delete(m.entries, leaseCacheKey(o.pool, o.leaseID))
		rels = append(rels, rel{o.pool, o.leaseID, token})
	}
	m.mu.Unlock()

	for _, r := range rels {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := newPoolClient(r.pool, r.token).Release(ctx, r.leaseID); err != nil {
			log.Printf("reconcile: release lease %s @ %s: %v", r.leaseID, r.pool, err)
		} else {
			log.Printf("reconcile: released orphan lease %s @ %s", r.leaseID, r.pool)
		}
		cancel()
	}
}

// ReleaseLease 释放指定租约并清除本地缓存与引用它的全部分配
// （共享租约的使用方将在下次请求时重新分配）。
func (m *LeaseManager) ReleaseLease(ctx context.Context, poolURL, leaseID string) error {
	m.mu.Lock()
	token := ""
	if e, ok := m.entries[leaseCacheKey(poolURL, leaseID)]; ok {
		token = e.poolToken
	}
	for ak, a := range m.assigns {
		pool, _ := splitAssignKey(ak)
		if pool == poolURL && a.LeaseID == leaseID {
			delete(m.assigns, ak)
		}
	}
	delete(m.entries, leaseCacheKey(poolURL, leaseID))
	m.saveAssignsLocked()
	m.mu.Unlock()
	return newPoolClient(poolURL, token).Release(ctx, leaseID)
}

// RotateCached 按 (poolURL, leaseID) 直接换 IP（租约须在本地缓存中，
// token 取最近一次使用值）。供 Admin UI 的本地代理池列表操作。
func (m *LeaseManager) RotateCached(ctx context.Context, poolURL, leaseID string) (*PoolLease, error) {
	m.mu.Lock()
	e, ok := m.entries[leaseCacheKey(poolURL, leaseID)]
	token := ""
	if ok {
		token = e.poolToken
	}
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("lease %s not cached locally (request once first)", leaseID)
	}
	lease, err := newPoolClient(poolURL, token).Rotate(ctx, leaseID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if e2, ok := m.entries[leaseCacheKey(poolURL, leaseID)]; ok {
		e2.lease = lease
		e2.lastRotate = time.Now()
		e2.requests = 0
	}
	m.mu.Unlock()
	return lease, nil
}

// LeaseInfo Admin UI 展示用的租约缓存条目（本地代理池列表）。
type LeaseInfo struct {
	PoolURL    string    `json:"pool_url"`
	LeaseID    string    `json:"lease_id"`
	Shared     bool      `json:"shared"`
	Groups     []string  `json:"groups"` // 占用该租约的分组（生效 BaseURL）
	IPv6       string    `json:"ipv6"`
	Port       int       `json:"port"`
	Multiplex  bool      `json:"multiplex"`
	Persistent bool      `json:"persistent"`
	Requests   int       `json:"requests"`
	LastRotate time.Time `json:"last_rotate"`
}

// ListLeases 返回当前缓存的所有租约（本地代理池列表）。
func (m *LeaseManager) ListLeases() []LeaseInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	gset := map[string]map[string]bool{}
	for ak, a := range m.assigns {
		pool, _ := splitAssignKey(ak)
		ck := leaseCacheKey(pool, a.LeaseID)
		if gset[ck] == nil {
			gset[ck] = map[string]bool{}
		}
		gset[ck][a.Group] = true
	}
	out := make([]LeaseInfo, 0, len(m.entries))
	for ck, e := range m.entries {
		pool, _ := splitAssignKey(ck)
		info := LeaseInfo{
			PoolURL:    pool,
			LeaseID:    e.lease.ID,
			Shared:     len(gset[ck]) > 1,
			Groups:     sortedGroups(gset[ck]),
			IPv6:       e.lease.IPv6,
			Port:       e.lease.Port,
			Multiplex:  e.multiplex,
			Persistent: e.lease.Persistent,
			Requests:   e.requests,
			LastRotate: e.lastRotate,
		}
		out = append(out, info)
	}
	return out
}

func sortedGroups(gs map[string]bool) []string {
	out := make([]string, 0, len(gs))
	for g := range gs {
		out = append(out, g)
	}
	// 插入排序：分组数量少，保持确定性输出即可
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
