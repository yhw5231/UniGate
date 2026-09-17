// 启动探测测试：重启/重新部署后的活跃度基线恢复（从持久化请求日志）、
// 跳过规则（上游明确冷却中 / 最近成功调用过）、按 (key, model) 粒度的模型
// 选择、探测并发上限、以及「上游明确到期时间」标记的落盘恢复。
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestStartupSweepSkipsExplicitAndRecent 启动探测的跳过规则：上游明确给出到期
// 时间、仍在冷却中的账号不试探（等自然到期）；最近 2 小时内成功调用过的账号也
// 跳过；兜底冷却（上游未给明确时间）与没有任何记录的账号要核对；停用 key 与
// 未开「自动探测」的渠道不探测。
func TestStartupSweepSkipsExplicitAndRecent(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.Models = []string{"m1"}
	ch.Keys = []*UpKey{
		{ID: "k1", Name: "账号一", APIKey: "sk-1", Enabled: true},
		{ID: "k2", Name: "账号二", APIKey: "sk-2", Enabled: true},
		{ID: "k3", Name: "账号三", APIKey: "sk-3", Enabled: true},
		{ID: "k4", Name: "账号四", APIKey: "sk-4", Enabled: true},
		{ID: "k5", Name: "账号五", APIKey: "sk-5", Enabled: false},
	}
	off := testProbeChannel(false)
	off.ID, off.Name = "ch2", "无探测渠道"
	off.Keys = []*UpKey{{ID: "k9", Name: "账号九", APIKey: "sk-9", Enabled: true}}
	mustPutChannel(t, ch)
	mustPutChannel(t, off)

	cool.MarkExplicit("k1", "", time.Hour)                     // 上游明确到期时间（如 "Try again in 14h"）
	activity.noteAt("k2", "", time.Now().Add(-30*time.Minute)) // 最近（2h 窗口内）成功调用过
	cool.Mark("k3", "", time.Minute)                           // 兜底冷却：值得重新核对

	tasks, skip := probes.startupTasks(probeBootRecentWindow)
	got := map[string]string{}
	for _, tk := range tasks {
		if tk.Kind != probeKindBoot {
			t.Fatalf("startup task kind = %q, want %q", tk.Kind, probeKindBoot)
		}
		got[tk.Key.ID] = tk.Model
	}
	if len(got) != 2 || got["k3"] != "m1" || got["k4"] != "m1" {
		t.Fatalf("want startup probes for k3/k4 only, got %+v (skip %+v)", got, skip)
	}
	if skip.explicit != 1 || skip.recent != 1 {
		t.Fatalf("skip stats = %+v, want explicit=1 recent=1", skip)
	}
}

// TestStartupSweepKeyModelPick 按 (key, model) 冷却的渠道：启动探测选第一个未
// 冷却的模型；所有模型都在冷却中（兜底时长）则整个 key 跳过。
func TestStartupSweepKeyModelPick(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.CooldownScope = cooldownScopeKeyModel
	ch.Models = []string{"m1", "m2"}
	mustPutChannel(t, ch)

	cool.Mark("k1", "m1", time.Minute)
	tasks, _ := probes.startupTasks(probeBootRecentWindow)
	if len(tasks) != 1 || tasks[0].Model != "m2" {
		t.Fatalf("want probe for m2 (m1 cooling), got %+v", tasks)
	}

	cool.Mark("k1", "m2", time.Minute)
	tasks, skip := probes.startupTasks(probeBootRecentWindow)
	if len(tasks) != 0 || skip.cooling != 1 {
		t.Fatalf("all models cooling should skip the key, got %+v (skip %+v)", tasks, skip)
	}

	// 上游明确冷却中：同样跳过（等自然到期）
	cool.ClearKey("k1")
	cool.MarkExplicit("k1", "m2", time.Hour)
	if tasks, _ := probes.startupTasks(probeBootRecentWindow); len(tasks) != 0 {
		t.Fatalf("explicit cooling must be skipped, got %+v", tasks)
	}
}

// TestStartupActivityFromLog 启动时从持久化日志恢复活跃度计时线：窗口内成功
// 调用过的账号不被启动探测、计时线回到真实调用时刻；窗口外的记录不恢复，
// 账号进入启动探测。只有失败记录（429）不算可用证据。
func TestStartupActivityFromLog(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true) // 账号一@测试渠道
	mustPutChannel(t, ch)

	// request_log（含探测请求）里 30 分钟前一次成功调用
	usageDB.LogAppend(logTableRequest, RequestRecord{
		ID: "r1", Time: time.Now().Add(-30 * time.Minute), Method: http.MethodPost,
		Path: "/v1/chat/completions", Status: http.StatusOK, User: "gw",
		Channel: ch.Name, Model: "m1", Key: "账号一@" + ch.Name,
	}, 0)
	if n := seedActivityFromHistory(probeBootRecentWindow); n == 0 {
		t.Fatal("seedActivityFromHistory should restore activity lines from request log")
	}
	last, ok := activity.lastCall("k1", "")
	if !ok || time.Since(last) < 25*time.Minute {
		t.Fatalf("key activity = %v,%v, want ~30m ago", last, ok)
	}
	if _, ok := activity.lastCall("k1", "m1"); !ok {
		t.Fatal("per-model activity line missing")
	}
	if tasks, _ := probes.startupTasks(probeBootRecentWindow); len(tasks) != 0 {
		t.Fatalf("recently used key must be skipped, got %+v", tasks)
	}

	// usage_events（下游真实请求账本）同样作为基线来源：1h 前（2h 窗口内）
	resetCfgForTest()
	mustPutChannel(t, ch)
	usageDB.Append(UsageEvent{
		Time: time.Now().Add(-time.Hour), User: "gw", Channel: ch.Name,
		Model: "m1", Key: "账号一@" + ch.Name, Status: http.StatusOK,
	})
	if n := seedActivityFromHistory(probeBootRecentWindow); n == 0 {
		t.Fatal("seedActivityFromHistory should restore activity lines from usage events")
	}
	if tasks, _ := probes.startupTasks(probeBootRecentWindow); len(tasks) != 0 {
		t.Fatalf("recently used key must be skipped, got %+v", tasks)
	}

	// 3h 前的成功记录在 2h 窗口外：不恢复基线 → 进入启动探测
	resetCfgForTest()
	mustPutChannel(t, ch)
	usageDB.Append(UsageEvent{
		Time: time.Now().Add(-3 * time.Hour), User: "gw", Channel: ch.Name,
		Model: "m1", Key: "账号一@" + ch.Name, Status: http.StatusOK,
	})
	if n := seedActivityFromHistory(probeBootRecentWindow); n != 0 {
		t.Fatalf("records outside the window must not seed activity (got %d)", n)
	}
	tasks, _ := probes.startupTasks(probeBootRecentWindow)
	if len(tasks) != 1 || tasks[0].Key.ID != "k1" || tasks[0].Kind != probeKindBoot {
		t.Fatalf("want one startup probe for k1, got %+v", tasks)
	}

	// 只有失败记录不算可用证据（429 的账号仍要核对）
	resetCfgForTest()
	mustPutChannel(t, ch)
	usageDB.LogAppend(logTableRequest, RequestRecord{
		ID: "r2", Time: time.Now().Add(-time.Hour), Method: http.MethodPost,
		Path: "/v1/chat/completions", Status: http.StatusTooManyRequests, User: "gw",
		Channel: ch.Name, Model: "m1", Key: "账号一@" + ch.Name,
	}, 0)
	if n := seedActivityFromHistory(probeBootRecentWindow); n != 0 {
		t.Fatalf("failed requests must not seed activity (got %d)", n)
	}
}

// TestStartupSweepRunsProbes 端到端：StartupSweep 对需要核对的账号真实发出
// 加法题请求（后台进行），结果写入请求日志（User=probe）并刷新空闲计时。
func TestStartupSweepRunsProbes(t *testing.T) {
	setupProbeTest(t)
	var mu sync.Mutex
	var questions []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		a, b, c := parseProbeQuestion(t, body.Messages[0].Content)
		mu.Lock()
		questions = append(questions, body.Messages[0].Content)
		mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"%d"}}]}`, a+b+c)
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	mustPutChannel(t, ch)
	probes.StartupSweep()

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(questions)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("StartupSweep did not probe an account without recent activity")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if last, ok := activity.lastCall("k1", ""); !ok || time.Since(last) > time.Minute {
		t.Fatalf("startup probe should refresh key activity, got %v,%v", last, ok)
	}
	if recs := reqLog.Snapshot(); len(recs) == 0 || recs[0].User != probeUser {
		t.Fatalf("startup probe request not recorded: %+v", recs)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("just-probed key should not be probed again, got %+v", tasks)
	}
}

// TestProbeConcurrencyLimit 并发上限（PROBE_CONCURRENCY）：同时在途的探测请求
// 数不超过上限（这里设 1），即使多个探测任务并发派发。
func TestProbeConcurrencyLimit(t *testing.T) {
	setupProbeTest(t)
	probes.sem = newProbeSem(1)

	var mu sync.Mutex
	inflight, peak := 0, 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		inflight++
		if inflight > peak {
			peak = inflight
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		a, b, c := parseProbeQuestion(t, body.Messages[0].Content)
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"%d"}}]}`, a+b+c)
		mu.Lock()
		inflight--
		mu.Unlock()
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	ch.Keys = []*UpKey{
		{ID: "k1", Name: "账号一", APIKey: "sk-1", Enabled: true},
		{ID: "k2", Name: "账号二", APIKey: "sk-2", Enabled: true},
		{ID: "k3", Name: "账号三", APIKey: "sk-3", Enabled: true},
	}
	mustPutChannel(t, ch)

	var wg sync.WaitGroup
	for _, k := range ch.Keys {
		wg.Add(1)
		go func(k *UpKey) {
			defer wg.Done()
			probes.execute(probeTask{Kind: probeKindBoot, Ch: ch, Key: k, Model: "m1"})
		}(k)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if peak != 1 {
		t.Fatalf("peak concurrent probes = %d, want 1 (PROBE_CONCURRENCY=1)", peak)
	}
	if inflight != 0 {
		t.Fatalf("inflight = %d after all probes, want 0", inflight)
	}
}

// TestCooldownExplicitFlag 冷却来源标记：「上游明确到期时间」的冷却与配置兜底
// 冷却区分（启动探测据此决定是否重新核对），标记随 cooldowns.json 跨重启恢复，
// 重新 Mark 时来源被最新一次覆盖。
func TestCooldownExplicitFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cooldowns.json")
	c1 := newCooldowns()
	if err := c1.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	c1.Mark("k1", "", time.Hour)             // 兜底冷却
	c1.MarkExplicit("k2", "", 14*time.Hour)  // 上游明确（"Try again in 14h"）
	c1.MarkExplicit("k3", "m1", time.Minute) // key_model 粒度的明确冷却

	if _, ok := c1.ExplicitCooling("k1"); ok {
		t.Fatal("fallback cooldown must not count as explicit")
	}
	if _, ok := c1.ExplicitCooling("k3"); !ok {
		t.Fatal("explicit (key, model) cooldown not reported")
	}
	// 重新 Mark 用兜底时长覆盖：来源跟着变成兜底
	c1.Mark("k2", "", time.Hour)
	if _, ok := c1.ExplicitCooling("k2"); ok {
		t.Fatal("explicit flag must be replaced by the latest Mark")
	}

	// 模拟重启：标记随文件恢复
	c2 := newCooldowns()
	if err := c2.SetPersistPath(path); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, ok := c2.ExplicitCooling("k2"); ok {
		t.Fatal("fallback cooldown restored as explicit")
	}
	if _, ok := c2.ExplicitCooling("k3"); !ok {
		t.Fatal("explicit flag lost after reload")
	}
	found := false
	for _, e := range c2.CoolingList() {
		if e.KeyID == "k3" && e.Model == "m1" {
			found = e.Explicit
		}
	}
	if !found {
		t.Fatal("CoolingList should carry the explicit flag")
	}
}

// TestProbeDefaults 探测相关默认值：空闲探测间隔 2h、启动探测默认开、
// 启动探测「最近使用」窗口固定 2h、探测并发上限 4。
func TestProbeDefaults(t *testing.T) {
	setupProbeTest(t)
	t.Setenv("PROBE_IDLE_SEC", "")
	t.Setenv("PROBE_STARTUP", "")
	t.Setenv("PROBE_CONCURRENCY", "")
	resetCfgForTest()
	pol := currentPolicy()
	if pol.ProbeIdleInterval != 2*time.Hour {
		t.Fatalf("default idle probe interval = %s, want 2h", pol.ProbeIdleInterval)
	}
	if !pol.ProbeStartup {
		t.Fatal("startup probe should default to on")
	}
	if probeBootRecentWindow != 2*time.Hour {
		t.Fatalf("startup recent-usage window = %s, want 2h", probeBootRecentWindow)
	}
	if cfg.ProbeConcurrency != 4 {
		t.Fatalf("default probe concurrency = %d, want 4", cfg.ProbeConcurrency)
	}
}

// TestStartupRecentWindowFixed 启动探测的「最近使用」窗口固定 2h，不跟随空闲
// 探测间隔：把空闲间隔调成 24h 后，3 小时前用过（空闲探测认为不算空闲）的账号
// 仍要核对——启动探测判断的是「重启瞬间是否正在被使用」，与调度间隔无关。
func TestStartupRecentWindowFixed(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	mustPutChannel(t, ch)
	sec := 24 * 3600
	applySettings(GatewaySettings{ProbeIdleSec: &sec})
	activity.noteAt("k1", "", time.Now().Add(-3*time.Hour))

	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("idle scan (24h interval) should not probe a key used 3h ago, got %+v", tasks)
	}
	tasks, skip := probes.startupTasks(probeBootRecentWindow)
	if len(tasks) != 1 || tasks[0].Kind != probeKindBoot || skip.recent != 0 {
		t.Fatalf("startup sweep must re-check a key last used 3h ago (fixed 2h window), got %+v (skip %+v)", tasks, skip)
	}

	// 1 小时前用过（2h 窗口内）：跳过
	activity.noteAt("k1", "", time.Now().Add(-time.Hour))
	if tasks, skip := probes.startupTasks(probeBootRecentWindow); len(tasks) != 0 || skip.recent != 1 {
		t.Fatalf("key used 1h ago must be skipped by startup sweep, got %+v (skip %+v)", tasks, skip)
	}
}

// TestCooldownRestoreDrivesStartupSweep 冷却持久化与启动探测的衔接：从
// cooldowns.json 恢复后，上游明确到期时间的冷却被跳过（等自然到期），兜底时长
// 的那条重新核对；恢复的冷却对路由生效（不会被当成可用账号）。
func TestCooldownRestoreDrivesStartupSweep(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.Models = []string{"m1"}
	ch.Keys = []*UpKey{
		{ID: "k1", Name: "账号一", APIKey: "sk-1", Enabled: true},
		{ID: "k2", Name: "账号二", APIKey: "sk-2", Enabled: true},
	}
	mustPutChannel(t, ch)

	// 上一次运行落盘：k1 上游明确 15m、k2 兜底 5m
	path := filepath.Join(t.TempDir(), "cooldowns.json")
	c1 := newCooldowns()
	if err := c1.SetPersistPath(path); err != nil {
		t.Fatalf("SetPersistPath: %v", err)
	}
	c1.MarkExplicit("k1", "", 15*time.Minute)
	c1.Mark("k2", "", 5*time.Minute)

	// 模拟重启：全局冷却表从文件恢复
	prev := cool
	cool = newCooldowns()
	defer func() { cool = prev }()
	if err := cool.SetPersistPath(path); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if until, ok := cool.CoolingKey("k1", ""); !ok || time.Until(until) < 14*time.Minute {
		t.Fatalf("explicit cooldown not restored intact: %v,%v", until, ok)
	}
	if until, ok := cool.CoolingKey("k2", ""); !ok || time.Until(until) < 4*time.Minute {
		t.Fatalf("fallback cooldown not restored intact: %v,%v", until, ok)
	}
	if !cool.IsCooling("k1", "") || !cool.IsCooling("k2", "") {
		t.Fatal("restored cooldowns must be in effect for routing")
	}

	tasks, skip := probes.startupTasks(probeBootRecentWindow)
	if len(tasks) != 1 || tasks[0].Key.ID != "k2" || skip.explicit != 1 {
		t.Fatalf("want only the fallback-cooled key re-checked, got %+v (skip %+v)", tasks, skip)
	}
}

// TestCooldownPersistFailureKeepsMemory 落盘路径不可写时：只在日志里报错，
// 内存冷却照常生效（路由仍跳过该账号），也不留 .tmp 残留文件。
func TestCooldownPersistFailureKeepsMemory(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newCooldowns()
	if err := c.SetPersistPath(filepath.Join(blocker, "cooldowns.json")); err != nil {
		t.Logf("SetPersistPath on unwritable path (expected): %v", err)
	}
	c.MarkExplicit("k1", "", time.Hour)
	if !c.IsCooling("k1", "") {
		t.Fatal("in-memory cooldown lost after failed persist")
	}
	if until, ok := c.ExplicitCooling("k1"); !ok || time.Until(until) < 59*time.Minute {
		t.Fatalf("ExplicitCooling = %v,%v after failed persist", until, ok)
	}
	if _, err := os.Stat(blocker + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed persist must not leave a .tmp file (stat err = %v)", err)
	}
}
