// 账号自动探测测试：冷却恢复触发、默认间隔空闲触发、禁用/冷却账号跳过、加法题
// 生成与答案提取、探测请求的真实发送与 429 重新冷却。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupProbeTest(t *testing.T) {
	t.Helper()
	setupGateway(t) // 内部走 resetCfgForTest：activity/probes 均为全新实例
}

func testProbeChannel(autoProbe bool) *Channel {
	return &Channel{
		ID: "ch1", Name: "测试渠道", BaseURL: "https://upstream.example", Models: []string{"m1"},
		Enabled: true, AutoProbe: autoProbe,
		Keys: []*UpKey{{ID: "k1", Name: "账号一", APIKey: "sk-test", Enabled: true}},
	}
}

// runScan 执行一轮扫描并返回收集到的任务（按 key 分批的批内顺序拉平，不执行探测）。
func runScan(t *testing.T) []probeTask {
	t.Helper()
	var out []probeTask
	for _, batch := range probes.scanOnce() {
		out = append(out, batch...)
	}
	return out
}

// TestProbeIdleAfterDefaultInterval 启用探测的渠道：key 空闲超过默认空闲探测
// 间隔（PROBE_IDLE_SEC，默认 2h）应产出空闲探测任务；
// 停用渠道 / 停用 key / 未开启开关不产出。
func TestProbeIdleAfterDefaultInterval(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("just-seen key should not be probed, got %d task(s)", len(tasks))
	}
	// 默认间隔（2h）内不探测、超过才探测
	activity.noteAt("k1", "", time.Now().Add(-time.Hour))
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("key used 1h ago should not be probed (default interval 2h), got %d task(s)", len(tasks))
	}
	activity.noteAt("k1", "", time.Now().Add(-2*time.Hour-time.Second))
	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindIdle || tasks[0].Key.ID != "k1" {
		t.Fatalf("want one idle probe for k1, got %+v", tasks)
	}

	// 停用渠道：不探测
	ch.Enabled = false
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("disabled channel should not be probed, got %d task(s)", len(tasks))
	}

	// 停用 key：不探测
	ch.Enabled = true
	ch.Keys[0].Enabled = false
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("disabled key should not be probed, got %d task(s)", len(tasks))
	}

	// 未开启开关：不探测
	ch.Keys[0].Enabled = true
	ch.AutoProbe = false
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("auto_probe off should not be probed, got %d task(s)", len(tasks))
	}
}

// TestProbeCooldownRecovery key 冷却条目到期（冷却中 → 恢复）应产出冷却恢复
// 探测任务；冷却期间不产出任何任务，且恢复只触发一次。
func TestProbeCooldownRecovery(t *testing.T) {
	setupProbeTest(t)
	if err := store.PutChannel(testProbeChannel(true)); err != nil {
		t.Fatal(err)
	}
	cool.Mark("k1", "", 5*time.Minute)
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("cooling key must not be probed, got %d task(s)", len(tasks))
	}
	cool.ClearKey("k1") // 到期/手动解除 → 恢复
	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindRecov || tasks[0].Key.ID != "k1" {
		t.Fatalf("want one recover probe for k1, got %+v", tasks)
	}
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("recovery must fire once, got %d task(s)", len(tasks))
	}
}

// TestProbeRecoveryModelKeyModelScope key_model 粒度渠道：恢复探测的模型取触发
// 恢复的那条 (key, model) 冷却。
func TestProbeRecoveryModelKeyModelScope(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.CooldownScope = cooldownScopeKeyModel
	ch.Models = []string{"m1", "m2"}
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	cool.Mark("k1", "m2", time.Minute)
	runScan(t) // 调度器先观察到冷却中
	cool.ClearModel("k1", "m2")
	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Model != "m2" {
		t.Fatalf("want recover probe for m2, got %+v", tasks)
	}
}

// TestProbeQuestionAndAnswer 加法题生成与答案提取。
func TestProbeQuestionAndAnswer(t *testing.T) {
	for i := 0; i < 200; i++ {
		a, b, c := randomProbeOperands()
		for _, v := range []int{a, b, c} {
			if v < 10000 || v > 99999 {
				t.Fatalf("operand %d out of 5-digit range", v)
			}
		}
		q := probeQuestion(a, b, c)
		if !strings.Contains(q, fmt.Sprintf("%d + %d + %d", a, b, c)) {
			t.Fatalf("question %q missing operands", q)
		}
		ans := fmt.Sprintf("%d", a+b+c)
		got, ok := extractProbeAnswer(fmt.Sprintf(`{"choices":[{"message":{"content":"%s"}}]}`, ans))
		if !ok || probeNumbersIn(got)[0] != a+b+c {
			t.Fatalf("chat answer extract failed: %q ok=%v", got, ok)
		}
	}
	// responses 格式 / 纯文本 / 千分位
	got, _ := extractProbeAnswer(`{"output":[{"content":[{"type":"output_text","text":"The answer is 246913."}]}]}`)
	if n := probeNumbersIn(got); len(n) != 1 || n[0] != 246913 {
		t.Fatalf("responses extract: %v", n)
	}
	if n := probeNumbersIn("246,913"); len(n) != 1 || n[0] != 246913 {
		t.Fatalf("thousand separator: %v", n)
	}
	if n := probeNumbersIn("Sum: 123456, done"); len(n) != 1 || n[0] != 123456 {
		t.Fatalf("plain text: %v", n)
	}
}

// TestProbeExecuteSendsAddition 探测真实发出请求：上游应收到含「随机 5 位数
// 加法」的对话请求，答案校验通过时只记成功日志、不进错误日志。
func TestProbeExecuteSendsAddition(t *testing.T) {
	setupProbeTest(t)
	var mu sync.Mutex
	var got struct {
		model string
		msg   string
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		got.model, got.msg = body.Model, body.Messages[0].Content
		mu.Unlock()
		a, b, c := parseProbeQuestion(t, body.Messages[0].Content)
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"%d"}}]}`, a+b+c)
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	probes.execute(probeTask{Kind: probeKindIdle, Ch: ch, Key: ch.Keys[0], Model: "m1"})

	mu.Lock()
	defer mu.Unlock()
	a, b, c := parseProbeQuestion(t, got.msg)
	if a < 10000 || a > 99999 || b < 10000 || b > 99999 || c < 10000 || c > 99999 {
		t.Fatalf("question operands not 5-digit: %q", got.msg)
	}
	if got.model != "m1" {
		t.Fatalf("probe model = %q, want m1", got.model)
	}
	if errs := errLog.Snapshot(); len(errs) != 0 {
		t.Fatalf("successful probe should not enter error log, got %+v", errs)
	}
	recs := reqLog.Snapshot()
	if len(recs) == 0 || recs[0].User != probeUser || recs[0].Status != http.StatusOK {
		t.Fatalf("probe request record missing/incorrect: %+v", recs)
	}
	if last, ok := activity.lastCall("k1", ""); !ok || time.Since(last) > time.Minute {
		t.Fatal("probe should refresh key activity")
	}
}

// TestProbeRecordUsageTokens 探测响应的 usage 要写进请求日志的 Tokens 列：
// chat 响应读 usage.prompt_tokens/completion_tokens，responses 渠道的响应体
// （Responses 对象）读 input/output_tokens；上游未回 usage 时记 0（不是解析
// 失败留下的假数据，日志照旧可读）。
func TestProbeRecordUsageTokens(t *testing.T) {
	cases := []struct {
		name       string
		endpoint   string
		resp       func(sum int) string
		wantPrompt int64
		wantOut    int64
	}{
		{
			name:     "chat",
			endpoint: endpointChat,
			resp: func(sum int) string {
				return fmt.Sprintf(`{"choices":[{"message":{"content":"%d"}}],"usage":{"prompt_tokens":31,"completion_tokens":9}}`, sum)
			},
			wantPrompt: 31, wantOut: 9,
		},
		{
			name:     "responses",
			endpoint: endpointResponses,
			resp: func(sum int) string {
				return fmt.Sprintf(`{"id":"r1","object":"response","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"%d"}]}],"usage":{"input_tokens":17,"output_tokens":4,"total_tokens":21}}`, sum)
			},
			wantPrompt: 17, wantOut: 4,
		},
		{
			name:     "no usage",
			endpoint: endpointChat,
			resp: func(sum int) string {
				return fmt.Sprintf(`{"choices":[{"message":{"content":"%d"}}]}`, sum)
			},
			wantPrompt: 0, wantOut: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setupProbeTest(t)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				a, b, c := probeQuestionFromBody(t, raw)
				_, _ = w.Write([]byte(tc.resp(a + b + c)))
			}))
			defer up.Close()

			ch := testProbeChannel(true)
			ch.BaseURL = up.URL
			ch.EndpointType = tc.endpoint
			if err := store.PutChannel(ch); err != nil {
				t.Fatal(err)
			}
			probes.execute(probeTask{Kind: probeKindIdle, Ch: ch, Key: ch.Keys[0], Model: "m1"})

			recs := reqLog.Snapshot()
			if len(recs) == 0 {
				t.Fatal("probe request not logged")
			}
			if recs[0].PromptTokens != tc.wantPrompt || recs[0].CompletionTokens != tc.wantOut {
				t.Fatalf("recorded tokens = %d/%d, want %d/%d (rec %+v)",
					recs[0].PromptTokens, recs[0].CompletionTokens, tc.wantPrompt, tc.wantOut, recs[0])
			}
		})
	}
}

// probeQuestionRe 从请求体（chat 的 messages 或 responses 的 input 都适用）
// 里取加法题的操作数。
var probeQuestionRe = regexp.MustCompile(`What is (\d+) \+ (\d+) \+ (\d+)\?`)

func probeQuestionFromBody(t *testing.T, raw []byte) (int, int, int) {
	t.Helper()
	m := probeQuestionRe.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatalf("no probe question in request body: %s", truncate(string(raw), 200))
	}
	var a, b, c int
	for i, dst := range []*int{&a, &b, &c} {
		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			t.Fatalf("parse operand %q: %v", m[i+1], err)
		}
		*dst = n
	}
	return a, b, c
}

// parseProbeQuestion 从探测问题文本解析三个操作数（测试辅助）。
func parseProbeQuestion(t *testing.T, q string) (int, int, int) {
	t.Helper()
	var a, b, c int
	if _, err := fmt.Sscanf(q, "What is %d + %d + %d?", &a, &b, &c); err != nil {
		t.Fatalf("parse probe question %q: %v", q, err)
	}
	return a, b, c
}

// TestProbeStill429Recools 恢复探测撞 429 且上游给出明确到期时间：按该时间
// 重新记冷却，并记入错误日志。
func TestProbeStill429Recools(t *testing.T) {
	setupProbeTest(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limited"}`))
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	probes.execute(probeTask{Kind: probeKindRecov, Ch: ch, Key: ch.Keys[0], Model: "m1"})

	until, ok := cool.CoolingKey("k1", "")
	if !ok {
		t.Fatal("429 after probe should re-mark cooldown")
	}
	d := time.Until(until)
	if d <= 100*time.Second || d > 120*time.Second {
		t.Fatalf("cooldown = %s, want ~120s", d)
	}
	errs := errLog.Snapshot()
	if len(errs) == 0 || !strings.Contains(errs[0].ErrMsg, "rate limited") {
		t.Fatalf("429 probe should be in error log, got %+v", errs)
	}
}

// TestProbeWrongAnswerLogged 上游 2xx 但答案错误：记入错误日志。
func TestProbeWrongAnswerLogged(t *testing.T) {
	setupProbeTest(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"42"}}]}`))
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	probes.execute(probeTask{Kind: probeKindIdle, Ch: ch, Key: ch.Keys[0], Model: "m1"})
	errs := errLog.Snapshot()
	if len(errs) == 0 || !strings.Contains(errs[0].ErrMsg, "answer mismatch") {
		t.Fatalf("wrong answer should be logged, got %+v", errs)
	}
}

// TestProbeConcurrentGuard 同一 key 并发探测只允许一个在途。
func TestProbeConcurrentGuard(t *testing.T) {
	setupProbeTest(t)
	if !probes.tryBegin("k1") {
		t.Fatal("first begin should succeed")
	}
	if probes.tryBegin("k1") {
		t.Fatal("second begin for same key should fail")
	}
	if !probes.tryBegin("k2") {
		t.Fatal("different key should succeed")
	}
	probes.endProbe("k1")
	if !probes.tryBegin("k1") {
		t.Fatal("after end, begin should succeed again")
	}
}

// TestProbeRequestRecordOnNetworkError 上游不可达：探测以 status=0 记入日志。
func TestProbeRequestRecordOnNetworkError(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.BaseURL = "http://127.0.0.1:1" // 不可达端口
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	probes.execute(probeTask{Kind: probeKindIdle, Ch: ch, Key: ch.Keys[0], Model: "m1"})
	errs := errLog.Snapshot()
	if len(errs) == 0 || errs[0].Status != 0 || !strings.Contains(errs[0].ErrMsg, "network error") {
		t.Fatalf("network failure should be logged, got %+v", errs)
	}
}

// TestProbeSchedulerStart 端到端：冷却到期后 Start 的调度器自动发探测并按
// 答案确认账号正常。
func TestProbeSchedulerStart(t *testing.T) {
	setupProbeTest(t)
	var mu sync.Mutex
	var gotMsg string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		a, b, c := parseProbeQuestion(t, body.Messages[0].Content)
		mu.Lock()
		gotMsg = body.Messages[0].Content
		mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"%d"}}]}`, a+b+c)
	}))
	defer up.Close()

	ch := testProbeChannel(true)
	ch.BaseURL = up.URL
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	probes = newProbeScheduler()
	probes.interval = 20 * time.Millisecond // 测试用短扫描间隔
	probes.Start()
	cool.Mark("k1", "", 300*time.Millisecond)
	time.Sleep(700 * time.Millisecond) // 冷却到期 + 调度器扫描 + 探测完成

	mu.Lock()
	defer mu.Unlock()
	if gotMsg == "" {
		t.Fatal("scheduler did not auto-probe after cooldown expiry")
	}
	if errs := errLog.Snapshot(); len(errs) != 0 {
		t.Fatalf("auto probe should succeed, got %+v", errs)
	}
}

// TestNoteKeyCallRecords 网关链路的活动记录入口：key 级与 (key, model) 级双线记录。
func TestNoteKeyCallRecords(t *testing.T) {
	setupProbeTest(t)
	noteKeyCall("", "m1")
	if _, ok := activity.lastCall("", "m1"); ok {
		t.Fatal("empty key id must be ignored")
	}
	noteKeyCall("k9", "m1")
	if _, ok := activity.lastCall("k9", "m1"); !ok {
		t.Fatal("noteKeyCall should record (key, model) activity")
	}
	if _, ok := activity.lastCall("k9", ""); !ok {
		t.Fatal("noteKeyCall should record key-level activity")
	}
}

// TestProbeIdlePerModel 按 (key, model) 冷却的渠道：空闲探测逐模型检查，
// 只探测「连续未使用」的模型；最近用过的模型、冷却中的模型不探测。
func TestProbeIdlePerModel(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.CooldownScope = cooldownScopeKeyModel
	ch.Models = []string{"m1", "m2", "m3"}
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	// m1 最近有调用；m2 连续未使用超阈值；m3 处于冷却中
	activity.noteAt("k1", "m1", time.Now().Add(-time.Hour))
	activity.noteAt("k1", "m2", time.Now().Add(-8*time.Hour-time.Second))
	cool.Mark("k1", "m3", time.Hour)

	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindIdle || tasks[0].Model != "m2" {
		t.Fatalf("want one idle probe for m2 only, got %+v", tasks)
	}
	activity.noteAt("k1", "m2", time.Now()) // 模拟 m2 的空闲探测已执行

	// m3 冷却到期后：恢复探测应使用冷却的那个模型（而非第一个模型）
	cool.ClearKey("k1")
	tasks = runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindRecov || tasks[0].Model != "m3" {
		t.Fatalf("want recover probe for m3, got %+v", tasks)
	}
}

// TestProbeIdleKeyScopeUsesFirstModel 按 key 冷却的渠道：空闲探测用第一个模型。
func TestProbeIdleKeyScopeUsesFirstModel(t *testing.T) {
	setupProbeTest(t)
	ch := testProbeChannel(true)
	ch.CooldownScope = "key"
	ch.Models = []string{"m1", "m2"}
	if err := store.PutChannel(ch); err != nil {
		t.Fatal(err)
	}
	activity.noteAt("k1", "", time.Now().Add(-8*time.Hour-time.Second))
	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Model != "m1" {
		t.Fatalf("want idle probe with first model m1, got %+v", tasks)
	}
}

// TestProbeIdleIntervalConfigurable 空闲探测间隔可在设置页调整（probe_idle_sec）：
// 缩短后更早触发；设为 0 关闭空闲探测（冷却恢复探测不受影响）。
func TestProbeIdleIntervalConfigurable(t *testing.T) {
	setupProbeTest(t)
	if err := store.PutChannel(testProbeChannel(true)); err != nil {
		t.Fatal(err)
	}
	setIdle := func(sec int) {
		t.Helper()
		applySettings(GatewaySettings{ProbeIdleSec: &sec})
	}
	activity.noteAt("k1", "", time.Now().Add(-2*time.Hour))

	setIdle(3600) // 1h：2h 未调用 → 探测
	tasks := runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindIdle {
		t.Fatalf("1h interval should trigger idle probe, got %+v", tasks)
	}

	activity.noteAt("k1", "", time.Now().Add(-30*time.Minute))
	setIdle(0) // 0 = 关闭空闲探测
	if tasks := runScan(t); len(tasks) != 0 {
		t.Fatalf("probe_idle_sec=0 must disable idle probing, got %+v", tasks)
	}

	// 关闭空闲探测不影响冷却恢复探测
	cool.Mark("k1", "", time.Minute)
	runScan(t)
	cool.ClearKey("k1")
	tasks = runScan(t)
	if len(tasks) != 1 || tasks[0].Kind != probeKindRecov {
		t.Fatalf("recovery probe must still fire when idle probing is off, got %+v", tasks)
	}
}
