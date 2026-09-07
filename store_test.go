// GatewayStore 测试：读写往返、归一化校验、下游 key 生成。
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *GatewayStore {
	t.Helper()
	s := newGatewayStore(filepath.Join(t.TempDir(), "gateway.json"))
	if err := s.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	return s
}

func TestStoreChannelRoundtrip(t *testing.T) {
	s := newTestStore(t)
	ch := &Channel{
		Name: "Cline", BaseURL: "https://api.cline.bot/api/v1",
		Rewrite: true, Enabled: true,
		Keys: []*UpKey{
			{Name: "acc1", APIKey: "sk-a", Enabled: true},
			{Name: "acc2", APIKey: "sk-b", Enabled: false, Proxy: &ProxySpec{Kind: "static", URL: "socks5://1.2.3.4:1080"}},
		},
	}
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}
	if ch.ID == "" || ch.Keys[0].ID == "" {
		t.Fatal("expected ids to be generated")
	}

	snap := s.Snapshot()
	if len(snap.Channels) != 1 || snap.Channels[0].Name != "Cline" {
		t.Fatalf("unexpected snapshot: %+v", snap.Channels)
	}
	if snap.Channels[0].Keys[1].Proxy.Kind != "static" {
		t.Fatalf("proxy spec lost: %+v", snap.Channels[0].Keys[1].Proxy)
	}
}

func TestStoreChannelValidation(t *testing.T) {
	s := newTestStore(t)
	if err := s.PutChannel(&Channel{Name: "", BaseURL: "http://x"}); err == nil {
		t.Fatal("expected name required")
	}
	if err := s.PutChannel(&Channel{Name: "x", BaseURL: ""}); err == nil {
		t.Fatal("expected base_url required")
	}
	if err := s.PutChannel(&Channel{Name: "x", BaseURL: "http://x", Keys: []*UpKey{{Name: "k", Enabled: true, Proxy: &ProxySpec{Kind: "static"}}}}); err == nil {
		t.Fatal("expected static proxy url required")
	}
	if err := s.PutChannel(&Channel{Name: "x", BaseURL: "http://x", Keys: []*UpKey{{Name: "k", Enabled: true, Proxy: &ProxySpec{Kind: "bogus"}}}}); err == nil {
		t.Fatal("expected bogus proxy kind rejected")
	}
	if err := s.PutChannel(&Channel{Name: "x", BaseURL: "http://x", CooldownScope: "bogus"}); err == nil {
		t.Fatal("expected bogus cooldown_scope rejected")
	}
	// 冷却粒度归一化："" 与 "key" 变体 → key（默认）；"key_model" 变体保持
	for raw, want := range map[string]string{"": "key", "key": "key", "KEY": "key", " Key ": "key", "key_model": "key_model", "KEY-MODEL": "key_model"} {
		ch := &Channel{Name: "x", BaseURL: "http://x", CooldownScope: raw}
		if err := s.PutChannel(ch); err != nil {
			t.Fatalf("cooldown scope %q: %v", raw, err)
		}
		if got := s.Snapshot().Channels; got[len(got)-1].CooldownScope != want {
			t.Fatalf("cooldown scope %q normalized to %q, want %q", raw, got[len(got)-1].CooldownScope, want)
		}
	}
	// ipv6pool 无 scheme 自动补 http://
	ch := &Channel{Name: "x", BaseURL: "http://x", Enabled: true, Keys: []*UpKey{{Name: "k", Enabled: true, Proxy: &ProxySpec{Kind: "ipv6pool", PoolURL: "1.2.3.4:8080"}}}}
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("ipv6pool normalize: %v", err)
	}
	if got := s.Snapshot().Channels; got[len(got)-1].Keys[0].Proxy.PoolURL != "http://1.2.3.4:8080" {
		t.Fatalf("pool url normalized to %q", got[len(got)-1].Keys[0].Proxy.PoolURL)
	}
}

func TestStoreUpdateAndDelete(t *testing.T) {
	s := newTestStore(t)
	ch := &Channel{Name: "a", BaseURL: "http://a", Enabled: true, Keys: []*UpKey{{Name: "k1", Enabled: true}}}
	_ = s.PutChannel(ch)
	ch.Name = "a2"
	ch.Keys = append(ch.Keys, &UpKey{Name: "k2", Enabled: true})
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("update: %v", err)
	}
	snap := s.Snapshot()
	if len(snap.Channels) != 1 || snap.Channels[0].Name != "a2" || len(snap.Channels[0].Keys) != 2 {
		t.Fatalf("update failed: %+v", snap.Channels)
	}
	if _, ok := s.DeleteChannel(ch.ID); !ok {
		t.Fatal("delete failed")
	}
	if len(s.Snapshot().Channels) != 0 {
		t.Fatal("expected empty channels")
	}
}

func TestStoreGWKeys(t *testing.T) {
	s := newTestStore(t)
	k := &GWKey{Name: "sub2api", Key: newGWKeyValue(), Enabled: true}
	if err := s.PutGWKey(k); err != nil {
		t.Fatalf("PutGWKey: %v", err)
	}
	if len(k.Key) < 16 || k.Key[:6] != "sk-gw-" {
		t.Fatalf("unexpected key format %q", k.Key)
	}
	if err := s.PutGWKey(&GWKey{Name: "", Key: "x"}); err == nil {
		t.Fatal("expected name required")
	}
	if _, ok := s.DeleteGWKey(k.ID); !ok {
		t.Fatal("delete failed")
	}
}

func TestStoreFindUpKey2(t *testing.T) {
	s := newTestStore(t)
	ch := &Channel{Name: "c", BaseURL: "http://c", Enabled: true, Keys: []*UpKey{{Name: "k", Enabled: true}}}
	_ = s.PutChannel(ch)
	kid := s.Snapshot().Channels[0].Keys[0].ID
	gotCh, gotK, ok := s.FindUpKey2(ch.ID, kid)
	if !ok || gotCh.Name != "c" || gotK.Name != "k" {
		t.Fatalf("FindUpKey2 failed: %v %+v", ok, gotK)
	}
	if _, _, ok := s.FindUpKey2("nope", kid); ok {
		t.Fatal("expected miss")
	}
}

func TestStoreAtomicSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "gateway.json")
	s := newGatewayStore(path)
	if err := s.load(); err != nil {
		t.Fatalf("load (creates file): %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file not created: %v", err)
	}
	// 无残留 .tmp
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("tmp file should be renamed away")
	}
}

func TestStoreProxyPoolCRUD(t *testing.T) {
	s := newTestStore(t)
	p := &ProxyPool{Name: "主池", PoolURL: "1.2.3.4:8080", PoolToken: "tok", SocksHost: "1.2.3.4"}
	if err := s.PutProxyPool(p); err != nil {
		t.Fatalf("PutProxyPool: %v", err)
	}
	if p.ID == "" {
		t.Fatal("expected pool id generated")
	}
	snap := s.Snapshot()
	if len(snap.ProxyPools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(snap.ProxyPools))
	}
	if snap.ProxyPools[0].PoolURL != "http://1.2.3.4:8080" {
		t.Fatalf("pool url normalized to %q", snap.ProxyPools[0].PoolURL)
	}
	// 校验：名称 / URL 必填
	if err := s.PutProxyPool(&ProxyPool{Name: "", PoolURL: "http://x"}); err == nil {
		t.Fatal("expected name required")
	}
	if err := s.PutProxyPool(&ProxyPool{Name: "x", PoolURL: ""}); err == nil {
		t.Fatal("expected pool_url required")
	}
	// 删除不存在的池 → 未删除
	if _, ok, _ := s.DeleteProxyPool("nope"); ok {
		t.Fatal("expected delete miss")
	}
	// 无引用时删除成功
	if pool, ok, usedBy := s.DeleteProxyPool(p.ID); !ok || pool == nil || usedBy != "" {
		t.Fatalf("expected clean delete, ok=%v usedBy=%q", ok, usedBy)
	}
	if len(s.Snapshot().ProxyPools) != 0 {
		t.Fatal("expected empty pools")
	}
}

func TestStoreDeleteProxyPoolRefusedWhenUsed(t *testing.T) {
	s := newTestStore(t)
	p := &ProxyPool{Name: "主池", PoolURL: "http://1.2.3.4:8080"}
	if err := s.PutProxyPool(p); err != nil {
		t.Fatalf("PutProxyPool: %v", err)
	}
	ch := &Channel{Name: "c", BaseURL: "http://c", Enabled: true, Keys: []*UpKey{{
		Name: "k", Enabled: true, Proxy: &ProxySpec{Kind: "ipv6pool", PoolID: p.ID},
	}}}
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("PutChannel: %v", err)
	}
	// 被引用 → 拒绝并列出渠道
	if _, ok, usedBy := s.DeleteProxyPool(p.ID); ok || usedBy == "" {
		t.Fatalf("expected refuse delete, ok=%v usedBy=%q", ok, usedBy)
	}
	if len(s.Snapshot().ProxyPools) != 1 {
		t.Fatal("pool should still exist")
	}
	// 解绑后删除成功
	ch.Keys[0].Proxy = nil
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if _, ok, usedBy := s.DeleteProxyPool(p.ID); !ok || usedBy != "" {
		t.Fatalf("expected delete after unbind, ok=%v usedBy=%q", ok, usedBy)
	}
}

func TestStoreMigrateInlinePools(t *testing.T) {
	// 旧格式：连接信息内联在 key 上 → 加载后自动迁移为代理池实体
	raw := `{
	  "channels": [{
	    "id": "ch1", "name": "c", "base_url": "http://c", "enabled": true,
	    "keys": [
	      {"id": "k1", "name": "a", "api_key": "x", "enabled": true,
	       "proxy": {"kind": "ipv6pool", "pool_url": "http://1.2.3.4:8080", "pool_token": "tok", "socks_host": "1.2.3.4"}},
	      {"id": "k2", "name": "b", "api_key": "y", "enabled": true,
	       "proxy": {"kind": "ipv6pool", "pool_url": "http://1.2.3.4:8080", "pool_token": "tok", "socks_host": "1.2.3.4"}},
	      {"id": "k3", "name": "c", "api_key": "z", "enabled": true,
	       "proxy": {"kind": "ipv6pool", "pool_url": "http://9.9.9.9:8080"}}
	    ]
	  }]
	}`
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	s := newGatewayStore(path)
	if err := s.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	snap := s.Snapshot()
	if len(snap.ProxyPools) != 2 {
		t.Fatalf("expected 2 pools (dedup by conn info), got %d", len(snap.ProxyPools))
	}
	got := snap.Channels[0].Keys
	// 相同连接信息的 k1/k2 合并到同一池，k3 独立一池
	if got[0].Proxy.PoolID == "" || got[0].Proxy.PoolID != got[1].Proxy.PoolID {
		t.Fatalf("k1/k2 should share migrated pool: %q vs %q", got[0].Proxy.PoolID, got[1].Proxy.PoolID)
	}
	if got[2].Proxy.PoolID == got[0].Proxy.PoolID {
		t.Fatal("k3 should be a different pool")
	}
	// 内联字段已清空
	if got[0].Proxy.PoolURL != "" || got[0].Proxy.PoolToken != "" || got[0].Proxy.SocksHost != "" {
		t.Fatalf("inline conn fields should be cleared: %+v", got[0].Proxy)
	}
	// 池实体带上了连接信息
	p1 := snap.ProxyPools[0]
	if p1.PoolToken != "tok" || p1.SocksHost != "1.2.3.4" {
		t.Fatalf("migrated pool conn info lost: %+v", p1)
	}
	// 迁移已落盘：再加载不会重复建池
	s2 := newGatewayStore(path)
	if err := s2.load(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := len(s2.Snapshot().ProxyPools); got != 2 {
		t.Fatalf("reload should keep 2 pools, got %d", got)
	}
	// 引用校验放行（池已存在）
	ch := snap.Channels[0]
	ch.Name = "c2"
	if err := s.PutChannel(ch); err != nil {
		t.Fatalf("PutChannel with migrated pool ref: %v", err)
	}
}

func TestPoolSpecReady(t *testing.T) {
	old := store
	defer func() { store = old }()
	store = newTestStore(t)
	p := &ProxyPool{Name: "主池", PoolURL: "http://1.2.3.4:8080", PoolToken: "tok", SocksHost: "1.2.3.4"}
	if err := store.PutProxyPool(p); err != nil {
		t.Fatalf("PutProxyPool: %v", err)
	}

	// 新格式：PoolID → 池实体连接信息
	spec := &ProxySpec{Kind: "ipv6pool", PoolID: p.ID, Share: true}
	got, err := poolSpecReady(spec)
	if err != nil {
		t.Fatalf("poolSpecReady: %v", err)
	}
	if got.PoolURL != p.PoolURL || got.PoolToken != "tok" || got.SocksHost != "1.2.3.4" {
		t.Fatalf("conn info not merged: %+v", got)
	}
	if !got.Share {
		t.Fatal("behavior fields should be preserved")
	}
	if spec.PoolURL != "" {
		t.Fatal("caller spec must not be mutated")
	}

	// 旧格式：内联连接信息原样使用
	legacy := &ProxySpec{Kind: "ipv6pool", PoolURL: "http://9.9.9.9:8080", PoolToken: "lt"}
	got2, err := poolSpecReady(legacy)
	if err != nil {
		t.Fatalf("legacy poolSpecReady: %v", err)
	}
	if got2.PoolURL != legacy.PoolURL || got2.PoolToken != "lt" {
		t.Fatalf("legacy passthrough broken: %+v", got2)
	}

	// 引用了不存在的池 → 报错
	if _, err := poolSpecReady(&ProxySpec{Kind: "ipv6pool", PoolID: "nope"}); err == nil {
		t.Fatal("expected unknown pool error")
	}
	// 既无 PoolID 也无 PoolURL → 报错
	if _, err := poolSpecReady(&ProxySpec{Kind: "ipv6pool"}); err == nil {
		t.Fatal("expected missing pool_id error")
	}
	// 非 ipv6pool 原样返回
	if got, _ := poolSpecReady(&ProxySpec{Kind: "static", URL: "http://x:1"}); got.URL != "http://x:1" {
		t.Fatal("non-pool spec should pass through")
	}
}
