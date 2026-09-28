// 渠道级 / 路由模型级清除冷却测试：WebUI 渠道卡片「清除冷却」（整渠道全清、
// 按模型清本渠道所有 key）与路由页模型分组「清除冷却」（该模型的所有渠道所有 key）。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestCooldownsClearKeysAndPairs：批量清除的范围与计数（无关条目保留，重复项只计一次）。
func TestCooldownsClearKeysAndPairs(t *testing.T) {
	setupGateway(t)
	cool.Mark("k1", "", time.Hour)
	cool.Mark("k1", "m1", time.Hour)
	cool.Mark("k2", "m1", time.Hour)
	cool.Mark("k3", "", time.Hour)

	if n := cool.ClearPairs([]cooldownPair{
		{keyID: "k1", model: "m1"},
		{keyID: "k1", model: "m1"}, // 重复项只计一次
		{keyID: "k2", model: "m1"},
		{keyID: "k2", model: "absent"}, // 不存在：不计
	}); n != 2 {
		t.Fatalf("ClearPairs cleared=%d want 2", n)
	}
	if cool.IsCooling("k1", "m1") || cool.IsCooling("k2", "m1") {
		t.Fatal("ClearPairs must remove the given pairs")
	}
	if !cool.IsCooling("k1", "") {
		t.Fatal("ClearPairs must keep other models of the same key")
	}

	if n := cool.ClearKeys([]string{"k1", "k3"}); n != 2 {
		t.Fatalf("ClearKeys cleared=%d want 2", n)
	}
	if cool.IsCooling("k1", "") || cool.IsCooling("k3", "") {
		t.Fatal("ClearKeys must clear every model part for the given keys")
	}
	if n := cool.ClearKeys(nil); n != 0 {
		t.Fatalf("ClearKeys(nil)=%d want 0", n)
	}
}

// clearCoolingReq 发一次清除冷却请求并返回 cleared 计数。
func clearCoolingReq(t *testing.T, tok, path, body string) int {
	t.Helper()
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, path, body, tok))
	if rr.Code != http.StatusOK {
		t.Fatalf("%s status=%d body=%s", path, rr.Code, rr.Body.String())
	}
	var out struct {
		Cleared int `json:"cleared"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Cleared
}

// TestAdminClearChannelCoolingAll：整渠道清除只影响该渠道的 key，其它渠道保留。
func TestAdminClearChannelCoolingAll(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "a", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models: []string{"m1"},
		Keys: []*UpKey{
			{Name: "a1", APIKey: "sk-a1", Enabled: true},
			{Name: "a2", APIKey: "sk-a2", Enabled: true},
		}})
	mustPutChannel(t, &Channel{Name: "b", BaseURL: up.srv.URL, Enabled: true,
		Keys: []*UpKey{{Name: "b1", APIKey: "sk-b1", Enabled: true}}})
	snap := store.Snapshot()
	a, b := snap.Channels[0], snap.Channels[1]
	cool.Mark(a.Keys[0].ID, "m1", time.Hour)
	cool.Mark(a.Keys[1].ID, "", time.Hour)
	cool.Mark(b.Keys[0].ID, "m1", time.Hour)

	if n := clearCoolingReq(t, tok, "/admin/api/channels/"+a.ID+"/cooling/clear-all", ""); n != 2 {
		t.Fatalf("clear-all cleared=%d want 2", n)
	}
	if cool.IsCooling(a.Keys[0].ID, "m1") || cool.IsCooling(a.Keys[1].ID, "") {
		t.Fatal("channel clear-all must clear every cooldown of the channel keys")
	}
	if !cool.IsCooling(b.Keys[0].ID, "m1") {
		t.Fatal("channel clear-all must not touch other channels")
	}

	// 未知渠道 → 404
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/nope/cooling/clear-all", "", tok))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown channel: status=%d want 404", rr.Code)
	}
}

// TestAdminClearChannelCoolingModel：按模型清除本渠道全部 key 的该模型冷却
//（含渠道映射扇出的多个上游模型）；其它模型、其它渠道不受影响。
func TestAdminClearChannelCoolingModel(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "permodel", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models:   []string{"m1", "m2"},
		ModelMap: ModelMap{"x": {"cn:x", "global:y"}},
		Keys: []*UpKey{
			{Name: "k1", APIKey: "sk-1", Enabled: true},
			{Name: "k2", APIKey: "sk-2", Enabled: true},
		}})
	mustPutChannel(t, &Channel{Name: "bykey", BaseURL: up.srv.URL, Enabled: true,
		Keys: []*UpKey{{Name: "k3", APIKey: "sk-3", Enabled: true}}})
	snap := store.Snapshot()
	ch, other := snap.Channels[0], snap.Channels[1]
	k1, k2, k3 := ch.Keys[0].ID, ch.Keys[1].ID, other.Keys[0].ID
	cool.Mark(k1, "m1", time.Hour)
	cool.Mark(k1, "m2", time.Hour)
	cool.Mark(k2, "m1", time.Hour)
	cool.Mark(k1, "cn:x", time.Hour)
	cool.Mark(k1, "global:y", time.Hour)
	cool.Mark(k3, "m1", time.Hour)

	// 按模型清除：本渠道两个 key 的 m1 全部解除，m2 与其它渠道保留
	if n := clearCoolingReq(t, tok, "/admin/api/channels/"+ch.ID+"/cooling/clear-model", `{"model":"m1"}`); n != 2 {
		t.Fatalf("clear-model m1 cleared=%d want 2", n)
	}
	if cool.IsCooling(k1, "m1") || cool.IsCooling(k2, "m1") {
		t.Fatal("channel clear-model must clear that model for every key of the channel")
	}
	if !cool.IsCooling(k1, "m2") || !cool.IsCooling(k3, "m1") {
		t.Fatal("channel clear-model must keep other models and other channels")
	}

	// 渠道映射扇出：清下游名 x → cn:x 与 global:y 两条一起清
	if n := clearCoolingReq(t, tok, "/admin/api/channels/"+ch.ID+"/cooling/clear-model", `{"model":"x"}`); n != 2 {
		t.Fatalf("clear-model x cleared=%d want 2", n)
	}
	if cool.IsCooling(k1, "cn:x") || cool.IsCooling(k1, "global:y") {
		t.Fatal("mapped model clear must clear every upstream variant")
	}

	// 按 key 粒度渠道：model 部分为空串 = 跨模型共享条目，显式传空串即可清
	cool.Mark(k3, "", time.Hour)
	if n := clearCoolingReq(t, tok, "/admin/api/channels/"+other.ID+"/cooling/clear-model", `{"model":""}`); n != 1 {
		t.Fatalf("clear-model \"\" cleared=%d want 1", n)
	}
	if cool.IsCooling(k3, "") {
		t.Fatal("empty model clear must drop the shared entry")
	}

	// 缺少 model 字段 → 400（字段存在但为空串才是合法请求）
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPost, "/admin/api/channels/"+ch.ID+"/cooling/clear-model", `{}`, tok))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing model: status=%d want 400", rr.Code)
	}
	// 未知渠道 → 404
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/channels/nope/cooling/clear-model", `{"model":"m1"}`, tok))
	if rr2.Code != http.StatusNotFound {
		t.Fatalf("unknown channel: status=%d want 404", rr2.Code)
	}
}

// TestAdminClearRouteModelCooling：清除某模型在全部渠道全部 key 上的冷却，
// 未声明该模型的渠道、以及其它模型的冷却都保留；路由视图随之归零。
func TestAdminClearRouteModelCooling(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "a", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models: []string{"m1"},
		Keys: []*UpKey{
			{Name: "a1", APIKey: "sk-a1", Enabled: true},
			{Name: "a2", APIKey: "sk-a2", Enabled: true},
		}})
	mustPutChannel(t, &Channel{Name: "b", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models: []string{"m1", "m2"},
		Keys:   []*UpKey{{Name: "b1", APIKey: "sk-b1", Enabled: true}}})
	mustPutChannel(t, &Channel{Name: "c", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		Models: []string{"m2"},
		Keys:   []*UpKey{{Name: "c1", APIKey: "sk-c1", Enabled: true}}})
	snap := store.Snapshot()
	k1, k2 := snap.Channels[0].Keys[0].ID, snap.Channels[0].Keys[1].ID
	k3, k4 := snap.Channels[1].Keys[0].ID, snap.Channels[2].Keys[0].ID
	cool.Mark(k1, "m1", time.Hour)
	cool.Mark(k2, "m1", time.Hour)
	cool.Mark(k3, "m1", time.Hour)
	cool.Mark(k3, "m2", time.Hour)
	cool.Mark(k4, "m1", time.Hour) // 渠道 c 未声明 m1：路由视图不显示，也不该被清

	if n := clearCoolingReq(t, tok, "/admin/api/route/cooling/clear-model", `{"model":"m1"}`); n != 3 {
		t.Fatalf("route clear-model cleared=%d want 3", n)
	}
	for _, kid := range []string{k1, k2, k3} {
		if cool.IsCooling(kid, "m1") {
			t.Fatalf("route clear-model must clear m1 on key %s", kid)
		}
	}
	if !cool.IsCooling(k3, "m2") || !cool.IsCooling(k4, "m1") {
		t.Fatal("route clear-model must keep other models and non-matching channels")
	}

	// 路由视图：m1 分组冷却归零
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodGet, "/admin/api/route?model=m1", "", tok))
	var view RouteStatusData
	if err := json.Unmarshal(rr.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Models) != 1 || view.Cooling != 0 {
		t.Fatalf("route view after clear: models=%d cooling=%d, want 1/0", len(view.Models), view.Cooling)
	}

	// 缺少 model 字段 → 400
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodPost, "/admin/api/route/cooling/clear-model", `{}`, tok))
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("missing model: status=%d want 400", rr2.Code)
	}
}

// TestAdminClearRouteModelCoolingMappedAndAlias：路由页清除按分组名解析——
// 渠道映射扇出的每个上游模型、以及全局别名指向的规范模型都被清到。
func TestAdminClearRouteModelCoolingMappedAndAlias(t *testing.T) {
	setupGateway(t)
	tok := adminToken(t)
	up := newUpstream(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`)
	mustPutChannel(t, &Channel{Name: "map", BaseURL: up.srv.URL, Enabled: true, CooldownScope: cooldownScopeKeyModel,
		ModelMap: ModelMap{"x": {"cn:x", "global:y"}},
		Keys:     []*UpKey{{Name: "k1", APIKey: "sk-1", Enabled: true}}})
	k1 := store.Snapshot().Channels[0].Keys[0].ID
	cool.Mark(k1, "cn:x", time.Hour)
	cool.Mark(k1, "global:y", time.Hour)
	cool.Mark(k1, "other", time.Hour)

	// 分组名 x → 两个上游变体一起清
	if n := clearCoolingReq(t, tok, "/admin/api/route/cooling/clear-model", `{"model":"x"}`); n != 2 {
		t.Fatalf("mapped fan-out cleared=%d want 2", n)
	}
	if cool.IsCooling(k1, "cn:x") || cool.IsCooling(k1, "global:y") || !cool.IsCooling(k1, "other") {
		t.Fatal("mapped fan-out clear must remove both upstream variants only")
	}

	// 全局别名 myx → x：清 myx 等价于清 x
	aliases := map[string]string{"myx": "x"}
	if err := store.PutSettings(&GatewaySettings{ModelAliases: aliases}); err != nil {
		t.Fatal(err)
	}
	applySettings(store.Settings())
	cool.Mark(k1, "cn:x", time.Hour)
	if n := clearCoolingReq(t, tok, "/admin/api/route/cooling/clear-model", `{"model":"myx"}`); n != 1 {
		t.Fatalf("alias clear cleared=%d want 1", n)
	}
	if cool.IsCooling(k1, "cn:x") {
		t.Fatal("alias group clear must resolve to the canonical model")
	}
}
