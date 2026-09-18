// 账号自动探测（渠道级开关 auto_probe）：后台调度器对渠道内的 key 发送真实
// 对话请求——一道随机 5 位数 + 5 位数 + 5 位数的加法题——用上游的真实响应
// 判断账号状态。探测模型按冷却粒度选择：
//   - 按 key 冷却（默认）：探测渠道启用模型的第一个；
//   - 按 (key, model) 冷却：冷却恢复探测用「冷却恢复的那个模型」，空闲探测
//     逐模型检查、探测「连续未使用的模型」。
//
// 三种触发时机：
//   - 启动探测：进程启动（重启/重新部署）后一次性核对账号状态——跳过「上游
//     明确给出到期时间、仍在冷却中」与「最近窗口内成功调用过」的账号，其余
//     账号各发一题（并发上限 PROBE_CONCURRENCY）。内存计时线在启动时先从
//     持久化请求日志恢复，因此重启不会把「刚用过的账号」当成空闲账号；
//   - 冷却恢复探测：key 的全部冷却到期（从冷却中恢复可路由）时发一题，确认
//     账号确实恢复正常。若上游再次 429（错误体给的到期时间偏短/额度未真正
//     重置），按上游明确给出的到期时间重新记冷却，避免路由反复撞限流；
//   - 空闲探测：正常状态（渠道与 key 均启用且不在冷却）的账号连续
//     PROBE_IDLE_SEC（WebUI 设置页可改，默认 2h）没有任何调用时发一题，确认
//     账号仍然可用；停用与冷却中的账号不探测；间隔 0 = 关闭空闲探测。
//
// 探测本身就是一次真实调用：无论结果如何都会刷新该 key（及对应模型）的空闲
// 计时（因此持续空闲的账号每个间隔探测一次）。结果写入请求日志（User 列存
// "probe"，日志页显示为「探测」，可按该列搜索/过滤查看），带真实 token 用量
// 但不计入用量统计；答案异常（上游 2xx 但回复里找不到正确的加法结果）同样
// 记入错误日志供人工核查。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	probeUser      = "probe" // 请求日志中探测请求的 User 标识（日志页显示为「探测」）
	probeScanEvery = 30 * time.Second
	probeKindIdle  = "idle"
	probeKindRecov = "recover"
	probeKindBoot  = "startup" // 启动探测（重启/重新部署后一次性核对账号状态）

	// probeBootRecentWindow 启动探测判断「最近用过」的窗口：窗口内成功调用过的
	// 账号不核对（刚验证过，不必再花一次额度）。固定 2h，不跟随空闲探测间隔
	// ——空闲探测间隔是「多久没调用值得检查一次」的调度参数，启动探测要的是
	// 「重启瞬间这个账号是不是正在被使用」的事实判断。
	probeBootRecentWindow = 2 * time.Hour
)

// ---- key 活动跟踪 ----

// keyActivity 记录每个上游 key 最近一次被调用的时刻（内存态，重启归零：
// 重启后以首次观察到该账号的时刻为空闲计时起点）。同时维护 key 级与
// (key, model) 级两条计时线：「调用」包括网关转发的真实请求、管理员测试与
// 自动探测——任何一次向上游发出的请求都刷新两条计时；按 key 冷却的渠道看
// key 级计时，按 (key, model) 冷却的渠道逐模型看各自的计时。
type keyActivity struct {
	mu   sync.Mutex
	last map[string]time.Time // 键：keyID 或 keyID+"\x00"+model
}

var activity = newKeyActivity()

func newKeyActivity() *keyActivity { return &keyActivity{last: map[string]time.Time{}} }

func activityKey(keyID, model string) string {
	if model == "" {
		return keyID
	}
	return keyID + "\x00" + model
}

// activityMaxEntries 计时线条目软阈值：越过时做一次维护（清理长时间未更新的
// 计时线，仍超则随机逐出到一半）。model 部分来自请求体里的任意字符串（下游
// 可对一个 gw key 发无限不同的模型名），不做上限的话表随请求多样性无限增长。
// 逐出到一半（而非逐出一条）保证维护以摊销 O(1)/写入 的成本触发；基线丢失
// 只是让空闲探测重新建立计时起点，无副作用。
const activityMaxEntries = 65536

func (a *keyActivity) noteAt(keyID, model string, t time.Time) {
	if keyID == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.last[keyID] = t
	if model != "" {
		a.last[activityKey(keyID, model)] = t
	}
	if len(a.last) <= activityMaxEntries {
		return
	}
	a.pruneLocked(t)
	if n := len(a.last); n > activityMaxEntries {
		target := n / 2
		for k := range a.last {
			if len(a.last) <= target {
				break
			}
			delete(a.last, k)
		}
	}
}

// pruneLocked 清理长时间未更新的计时线（调用方持锁）。
func (a *keyActivity) pruneLocked(now time.Time) {
	cut := now.Add(-24 * time.Hour)
	if idle := currentPolicy().ProbeIdleInterval; idle > 24*time.Hour {
		cut = now.Add(-2 * idle)
	}
	for k, t := range a.last {
		if t.Before(cut) {
			delete(a.last, k)
		}
	}
}

// note 刷新 key 的最近调用时刻。
func (a *keyActivity) note(keyID, model string) { a.noteAt(keyID, model, time.Now()) }

// noteMax 仅在 t 晚于已记录值时刷新（启动时从请求日志恢复计时线用：同一 key
// 的多条历史记录不保证顺序，避免较早的记录覆盖较晚的）。
func (a *keyActivity) noteMax(keyID, model string, t time.Time) {
	if keyID == "" {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, k := range []string{keyID, activityKey(keyID, model)} {
		if k == "" {
			continue
		}
		if prev, ok := a.last[k]; !ok || t.After(prev) {
			a.last[k] = t
		}
	}
}

// lastCall 返回 key（model 为空）或 (key, model) 的最近调用时刻；从未记录返回 false。
func (a *keyActivity) lastCall(keyID, model string) (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.last[activityKey(keyID, model)]
	return t, ok
}

// noteKeyCall 网关转发/测试链路刷新 key 活动的统一入口（model 为本次请求的模型）。
func noteKeyCall(keyID, model string) { activity.note(keyID, model) }

// ---- 探测调度器 ----

// probeTask 一次待执行的探测。Model 为探测用的模型：冷却恢复探测取触发恢复
// 的模型（key_model 粒度渠道），其余取渠道启用模型列表的第一个。
type probeTask struct {
	Kind  string // idle / recover
	Ch    *Channel
	Key   *UpKey
	Model string
}

// probeScheduler 周期扫描（默认 probeScanEvery，测试可调短 interval）检测冷却
// 恢复与空闲超时；任务在独立 goroutine 中执行，inFlight 保证同一 key 同时只有
// 一个探测在途。cooling 跟踪「上一轮扫描时处于冷却中的 (key, model) 对」：
// 本轮消失（到期或手动解除）即视为该 key 从冷却恢复——无论开关状态如何都
// 持续跟踪，避免关闭再打开探测后误触发历史恢复。
// sem 是全局探测并发上限（PROBE_CONCURRENCY，nil = 不限制）：启动探测一次
// 核对大量账号，空闲探测也可能在一轮扫描里同时命中很多账号（一批账号同时
// 越过空闲阈值），无限并发会瞬间打满上游额度与本机连接。
type probeScheduler struct {
	mu        sync.Mutex
	cooling   map[cooldownPair]bool
	inFlight  map[string]bool
	interval  time.Duration
	sem       chan struct{} // 在途探测请求数上限（nil = 不限制）
	startOnce sync.Once
}

var probes = newProbeScheduler()

func newProbeScheduler() *probeScheduler {
	return &probeScheduler{
		cooling:  map[cooldownPair]bool{},
		inFlight: map[string]bool{},
		interval: probeScanEvery,
	}
}

// setConcurrency 设置探测并发上限（0 = 不限制）。进程启动与测试重置时显式调用：
// 包级变量初始化先于 init() 里的 cfg 装载，构造时读环境变量只会拿到零值。
func (s *probeScheduler) setConcurrency(n int) { s.sem = newProbeSem(n) }

// newProbeSem 构造并发信号量（n <= 0 = 不限制，返回 nil）。
func newProbeSem(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

// concurrency 当前探测并发上限（0 = 不限制）。
func (s *probeScheduler) concurrency() int { return cap(s.sem) }

// acquireProbe / releaseProbe 占位/释放一个探测并发名额（不限制时为空操作）。
func (s *probeScheduler) acquireProbe() {
	if s.sem != nil {
		s.sem <- struct{}{}
	}
}

func (s *probeScheduler) releaseProbe() {
	if s.sem != nil {
		<-s.sem
	}
}

// Start 启动后台扫描循环（幂等）。扫描本身在 ticker 协程内同步做（只做状态
// 对比与任务收集，开销极小），探测任务按 key 分批派发到独立 goroutine——
// 同一 key 的多模型探测在批内串行，不同 key 并行。
func (s *probeScheduler) Start() {
	s.startOnce.Do(func() {
		go func() {
			t := time.NewTicker(s.interval)
			defer t.Stop()
			for range t.C {
				for _, batch := range s.scanOnce() {
					go s.runBatch(batch)
				}
			}
		}()
	})
}

// scanOnce 扫描一轮：更新冷却跟踪、收集命中探测条件的任务，按 key 分组返回
// （同 key 的任务在批内串行执行；不实际执行探测）。
func (s *probeScheduler) scanOnce() [][]probeTask {
	if store == nil {
		return nil
	}
	snap := store.View() // 只读视图，零拷贝

	nowCooling := map[cooldownPair]bool{}
	nowCoolingKeys := map[string]bool{}
	for _, e := range cool.CoolingList() {
		p := cooldownPair{e.KeyID, e.Model}
		nowCooling[p] = true
		nowCoolingKeys[e.KeyID] = true
	}

	s.mu.Lock()
	prev := s.cooling
	s.cooling = nowCooling
	s.mu.Unlock()

	// 冷却恢复：上轮在冷却、本轮全部条目到期（key 不再有任何生效冷却）。
	// key_model 粒度渠道记录触发恢复的模型作为探测题的模型。
	recovered := map[string]string{} // keyID → 触发恢复的模型（"" = key 级粒度）
	for p := range prev {
		if !nowCooling[p] && (!has(p.keyID, recovered) || p.model == "") {
			recovered[p.keyID] = p.model
		}
	}

	byID := map[string]struct {
		ch *Channel
		k  *UpKey
	}{}
	for _, ch := range snap.Channels {
		for _, k := range ch.Keys {
			byID[k.ID] = struct {
				ch *Channel
				k  *UpKey
			}{ch, k}
		}
	}

	var tasks []probeTask
	for keyID, model := range recovered {
		e, ok := byID[keyID]
		if !ok || !e.ch.Enabled || !e.k.Enabled || !e.ch.AutoProbe {
			continue
		}
		// 按 key 冷却：探测第一个模型；按 (key, model) 冷却：探测冷却恢复的那个模型
		if e.ch.CooldownScope == cooldownScopeKeyModel && model != "" {
			tasks = append(tasks, probeTask{Kind: probeKindRecov, Ch: e.ch, Key: e.k, Model: model})
			continue
		}
		tasks = append(tasks, probeTask{Kind: probeKindRecov, Ch: e.ch, Key: e.k, Model: probeModelFor(e.ch)})
	}

	// 空闲探测：仅正常状态账号（渠道/key 启用且不在冷却），间隔取设置页
	// probe_idle_sec（默认 2h，0 = 关闭）
	if idle := currentPolicy().ProbeIdleInterval; idle > 0 {
		for _, ch := range snap.Channels {
			if !ch.Enabled || !ch.AutoProbe {
				continue
			}
			perModel := ch.CooldownScope == cooldownScopeKeyModel && len(ch.Models) > 0
			for _, k := range ch.Keys {
				if !k.Enabled {
					continue // 禁用账号不探测
				}
				if !perModel {
					if nowCoolingKeys[k.ID] {
						continue // 按 key 冷却：冷却中的账号不探测
					}
					if t, ok := s.idleTask(ch, k, probeModelFor(ch), "", idle); ok {
						tasks = append(tasks, t)
					}
					continue
				}
				// 按 (key, model) 冷却：逐模型检查「连续未使用的模型」，冷却中
				// 的 (key, model) 不探测（到期后的恢复探测会验证它）
				for _, m := range ch.Models {
					if nowCooling[cooldownPair{k.ID, m}] {
						continue
					}
					if t, ok := s.idleTask(ch, k, m, m, idle); ok {
						tasks = append(tasks, t)
					}
				}
			}
		}
	}

	// 按 key 分组（保持收集顺序），同 key 串行、跨 key 并行
	order := make([]string, 0, 4)
	batches := map[string][]probeTask{}
	for _, t := range tasks {
		if _, seen := batches[t.Key.ID]; !seen {
			order = append(order, t.Key.ID)
		}
		batches[t.Key.ID] = append(batches[t.Key.ID], t)
	}
	out := make([][]probeTask, 0, len(order))
	for _, id := range order {
		out = append(out, batches[id])
	}
	return out
}

// idleTask 空闲检查：key（per-model=false）或 (key, model)（per-model=true，
// model 为该模型）自最近一次调用起空闲超过 idle 时，返回一个空闲探测任务；
// 首次观察到该计时线时先建立基线（从当前时刻起算）。keyLevel 为计时线的
// model 部分（"" = key 级）。
func (s *probeScheduler) idleTask(ch *Channel, k *UpKey, probeModel, keyLevelModel string, idle time.Duration) (probeTask, bool) {
	last, ok := activity.lastCall(k.ID, keyLevelModel)
	if !ok {
		activity.note(k.ID, keyLevelModel) // 首次观察到：以当前时刻为计时起点
		return probeTask{}, false
	}
	if time.Since(last) < idle {
		return probeTask{}, false
	}
	return probeTask{Kind: probeKindIdle, Ch: ch, Key: k, Model: probeModel}, true
}

func has(key string, m map[string]string) bool {
	_, ok := m[key]
	return ok
}

// tryBegin 占位某 key 的在途探测；已有探测在途时返回 false。
func (s *probeScheduler) tryBegin(keyID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight[keyID] {
		return false
	}
	s.inFlight[keyID] = true
	return true
}

func (s *probeScheduler) endProbe(keyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inFlight, keyID)
}

// ---- 探测执行 ----

// probeModelFor 探测用模型：渠道启用模型列表的第一个；未声明模型列表时用
// gpt-4o-mini（与渠道测试的兜底一致）。
func probeModelFor(ch *Channel) string {
	if len(ch.Models) > 0 {
		return ch.Models[0]
	}
	return "gpt-4o-mini"
}

// randomProbeOperands 加法题的三个随机 5 位数（10000–99999）。
func randomProbeOperands() (int, int, int) {
	return 10000 + rand.Intn(90000), 10000 + rand.Intn(90000), 10000 + rand.Intn(90000)
}

// probeQuestion 渲染加法题。用英文短句以适配任意上游模型，且最小化 token 消耗。
func probeQuestion(a, b, c int) string {
	return fmt.Sprintf("What is %d + %d + %d? Reply with only the number.", a, b, c)
}

// execute 执行单个探测任务（带在途去重；单任务路径与测试使用）。
func (s *probeScheduler) execute(t probeTask) {
	if !s.tryBegin(t.Key.ID) {
		return // 同一 key 已有探测在途
	}
	defer s.endProbe(t.Key.ID)
	s.probeOne(t)
}

// runBatch 执行同一 key 的一批探测任务（批内串行、整批共用一个在途占位）。
func (s *probeScheduler) runBatch(batch []probeTask) {
	if len(batch) == 0 {
		return
	}
	if !s.tryBegin(batch[0].Key.ID) {
		return // 同一 key 已有探测在途
	}
	defer s.endProbe(batch[0].Key.ID)
	for _, t := range batch {
		s.probeOne(t)
	}
}

// ---- 启动探测 ----

// seedActivityFromHistory 启动时从持久化请求日志恢复活跃度计时线：window 内
// 每个成功调用过的 (key, 模型) 的最近时刻写回内存（key 级计时取该 key 所有
// 模型中最晚的一次）。返回恢复的计时线条数。
// 日志里的 key 是「名称@渠道」标签，按标签匹配当前配置里的 key；改过名/删过的
// key 匹配不上，按「没有历史」处理（启动探测会核对一次，无害）。
func seedActivityFromHistory(window time.Duration) int {
	if usageDB == nil || store == nil || window <= 0 {
		return 0
	}
	uses := usageDB.RecentKeyUse(window)
	if len(uses) == 0 {
		return 0
	}
	snap := store.View() // 只读视图，零拷贝
	byLabel := map[string][]*UpKey{}
	for _, ch := range snap.Channels {
		for _, k := range ch.Keys {
			label := k.Name + "@" + ch.Name
			byLabel[label] = append(byLabel[label], k)
		}
	}
	n := 0
	for _, u := range uses {
		// 日志里的 key 是「名称@渠道」完整标签，直接按标签匹配
		for _, k := range byLabel[u.Key] {
			activity.noteMax(k.ID, u.Model, u.Last)
			n++
		}
	}
	return n
}

// startupSkip 启动探测的跳过统计（仅用于日志）。
type startupSkip struct {
	explicit int // 上游明确到期时间的冷却中：等它自然到期
	recent   int // 最近窗口内成功调用过：刚验证过
	cooling  int // 全部模型都在兜底冷却中：没有可测模型
}

func (s startupSkip) total() int { return s.explicit + s.recent + s.cooling }

// startupTasks 收集启动探测任务：渠道与 key 均启用且开启自动探测的账号，
// 跳过上游明确冷却与最近成功调用的，其余每个账号一道加法题。window 为
// 「最近使用」判定窗口（probeBootRecentWindow）。
func (s *probeScheduler) startupTasks(window time.Duration) ([]probeTask, startupSkip) {
	var (
		tasks []probeTask
		skip  startupSkip
	)
	if store == nil {
		return nil, skip
	}
	snap := store.View() // 只读视图，零拷贝
	for _, ch := range snap.Channels {
		if !ch.Enabled || !ch.AutoProbe {
			continue
		}
		perModel := ch.CooldownScope == cooldownScopeKeyModel && len(ch.Models) > 0
		for _, k := range ch.Keys {
			if !k.Enabled {
				continue
			}
			if _, ok := cool.ExplicitCooling(k.ID); ok {
				skip.explicit++
				continue
			}
			if t, ok := activity.lastCall(k.ID, ""); ok && time.Since(t) < window {
				skip.recent++
				continue
			}
			model := probeModelFor(ch)
			if perModel {
				// 按 (key, model) 冷却：探测第一个不在冷却中的模型；全在冷却中
				// 则跳过（到期后的恢复探测会验证它们）
				cooling := cool.CoolingMap(k.ID)
				model = ""
				for _, m := range ch.Models {
					if _, ok := cooling[m]; !ok {
						model = m
						break
					}
				}
				if model == "" {
					skip.cooling++
					continue
				}
			}
			tasks = append(tasks, probeTask{Kind: probeKindBoot, Ch: ch, Key: k, Model: model})
		}
	}
	return tasks, skip
}

// StartupSweep 进程启动时调用一次：先恢复活跃度计时线（重启后「最近用过」
// 不再归零：最近用过的账号不会被当空闲账号探测，空闲计时也从真实调用时刻
// 起算），再按设置（policy.ProbeStartup，默认开）做一轮启动探测——对「上游
// 明确冷却中」与「最近成功调用过」之外的账号各发一题核对状态，按并发上限
// 分发给 worker。探测在后台进行，不阻塞监听；随后由周期扫描按冷却恢复/空闲
// 规则继续探测。
func (s *probeScheduler) StartupSweep() {
	pol := currentPolicy()
	// 恢复窗口 = max(空闲探测间隔, 启动探测的「最近用过」窗口)：计时线要对齐空闲
	// 探测的阈值，同时空闲探测关闭（间隔 0）时也要能判断最近是否在用。
	seedWindow := pol.ProbeIdleInterval
	if seedWindow < probeBootRecentWindow {
		seedWindow = probeBootRecentWindow
	}
	if n := seedActivityFromHistory(seedWindow); n > 0 {
		log.Printf("probe: 启动活跃度基线：从请求日志恢复 %d 条计时线（窗口 %s）", n, seedWindow)
	}
	if !pol.ProbeStartup {
		return
	}
	tasks, skip := s.startupTasks(probeBootRecentWindow)
	if len(tasks) == 0 {
		if skip.total() > 0 {
			log.Printf("probe: 启动探测：无待核对账号（跳过 %d：明确冷却 %d、最近使用 %d、兜底冷却 %d）",
				skip.total(), skip.explicit, skip.recent, skip.cooling)
		}
		return
	}
	log.Printf("probe: 启动探测：%d 个账号待核对（跳过 %d：明确冷却 %d、最近使用 %d、兜底冷却 %d；并发上限 %d）",
		len(tasks), skip.total(), skip.explicit, skip.recent, skip.cooling, s.concurrency())
	go s.runTasks(tasks)
}

// runTasks 并发执行一批探测（worker 数 = 并发上限；不限制时为任务数），
// 阻塞到全部完成。任务本身按在途占位去重：与周期扫描撞上同一 key 时跳过。
func (s *probeScheduler) runTasks(tasks []probeTask) {
	if len(tasks) == 0 {
		return
	}
	workers := s.concurrency()
	if workers <= 0 || workers > len(tasks) {
		workers = len(tasks)
	}
	queue := make(chan probeTask)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range queue {
				s.execute(t)
			}
		}()
	}
	for _, t := range tasks {
		queue <- t
	}
	close(queue)
	wg.Wait()
}

// probeOne 执行一次探测并按结果处置：429 重新记冷却（上游明确到期时间优先），
// 鉴权失败/5xx/答案异常记入日志供人工核查，成功则仅刷新空闲计时。探测请求
// 全部写入请求日志（User=probe）。在途请求数受并发上限约束。
func (s *probeScheduler) probeOne(t probeTask) {
	activity.note(t.Key.ID, t.Model) // 探测即调用：无论结果如何都重置空闲计时

	a, b, c := randomProbeOperands()
	sum := a + b + c
	s.acquireProbe()
	res := sendProbeRequest(t.Ch, t.Key, t.Model, probeQuestion(a, b, c))
	s.releaseProbe()

	kind := "空闲探测"
	switch t.Kind {
	case probeKindRecov:
		kind = "冷却恢复探测"
	case probeKindBoot:
		kind = "启动探测"
	}
	label := t.Key.Name + "@" + t.Ch.Name

	switch {
	case res.err != nil:
		log.Printf("probe: %s %s network error: %v", kind, label, res.err)
		recordProbeRequest(t, res, fmt.Sprintf("network error: %v", res.err))
		return
	case res.status == http.StatusTooManyRequests:
		// 上游仍限流：按与路由引擎一致的优先级取到期时间重新记冷却
		d := retryAfterDuration(res.retryAfter, 0)
		if d == 0 {
			if d2, ok := bodyRetryAfter(res.body); ok {
				d = d2
			}
		}
		explicit := d != 0
		if !explicit {
			d = currentPolicy().RateLimitCooldown
		}
		if explicit {
			cool.MarkExplicit(t.Key.ID, t.Ch.cooldownModelFor(t.Model), d)
		} else {
			cool.Mark(t.Key.ID, t.Ch.cooldownModelFor(t.Model), d)
		}
		log.Printf("probe: %s %s 上游仍 429，按 %s 重新冷却", kind, label, d)
		recordProbeRequest(t, res, fmt.Sprintf("still rate limited after probe; cooldown %s", d))
		return
	case res.status == http.StatusUnauthorized || res.status == http.StatusForbidden:
		log.Printf("probe: %s %s 上游 %d：账号鉴权失败，状态异常", kind, label, res.status)
		recordProbeRequest(t, res, fmt.Sprintf("auth rejected after probe (upstream %d)", res.status))
		return
	case res.status >= 500:
		log.Printf("probe: %s %s 上游 %d：%s", kind, label, res.status, truncate(res.body, 200))
		recordProbeRequest(t, res, fmt.Sprintf("upstream %d after probe", res.status))
		return
	case res.status < 200 || res.status >= 300:
		log.Printf("probe: %s %s 上游 %d", kind, label, res.status)
		recordProbeRequest(t, res, fmt.Sprintf("upstream %d after probe", res.status))
		return
	}

	// 2xx：账号可正常对话。校验加法题答案（模型可能附带说明文字，取回复中
	// 全部整数做匹配）；找不到正确结果时账号状态存疑，记入日志供人工核查。
	if ans, ok := extractProbeAnswer(res.body); ok {
		nums := probeNumbersIn(ans)
		if len(nums) > 0 {
			correct := false
			for _, n := range nums {
				if n == sum {
					correct = true
					break
				}
			}
			if !correct {
				msg := fmt.Sprintf("answer mismatch after probe: got %q, want %d", truncate(ans, 128), sum)
				log.Printf("probe: %s %s 账号状态存疑：%s", kind, label, msg)
				recordProbeRequest(t, res, msg)
				return
			}
		}
	}
	log.Printf("probe: %s %s 正常（上游 %d）", kind, label, res.status)
	recordProbeRequest(t, res, "")
}

// probeHTTPResult 一次探测请求的原始结果。
type probeHTTPResult struct {
	start      time.Time
	target     string
	status     int
	retryAfter string
	body       string
	err        error
}

// sendProbeRequest 用指定渠道 key 发送探测对话请求（非流式，超时取测试链路
// 的 TEST_TIMEOUT；responses 渠道由 doUpstreamRequest 自动转换请求格式）。
func sendProbeRequest(ch *Channel, k *UpKey, model, question string) probeHTTPResult {
	res := probeHTTPResult{start: time.Now()}
	reqBody, _ := json.Marshal(map[string]any{
		"model":    model,
		"messages": []map[string]string{{"role": "user", "content": question}},
		"stream":   false,
	})
	cand := candidate{ch: ch, k: k}
	res.target = cand.chatTarget()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.TestTimeout)
	defer cancel()
	route, err := resolveProxy(&cand)
	if err != nil {
		res.err = fmt.Errorf("proxy resolve: %w", err)
		return res
	}
	client := newUpstreamClient(route)
	resp, err := doUpstreamRequest(ctx, client, &cand, reqBody, false, nil)
	if err != nil {
		res.err = err
		return res
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
	leaseMgr.RecordUse(cand.k.effectiveProxy(cand.ch), cand.k.ID)
	res.status = resp.StatusCode
	res.retryAfter = resp.Header.Get("Retry-After")
	res.body = string(data)
	return res
}

// recordProbeRequest 把一次探测写入请求日志（与网关请求共用日志；不计入用量
// 统计）。errMsg 非空时进错误日志。
func recordProbeRequest(t probeTask, res probeHTTPResult, errMsg string) {
	if reqLog == nil {
		return
	}
	promptTok, completionTok := parseUsageFromBody([]byte(res.body))
	rec := RequestRecord{
		ID:               strconv.FormatInt(time.Now().UnixNano(), 36),
		Time:             res.start,
		Duration:         time.Since(res.start),
		DurationMs:       time.Since(res.start).Milliseconds(),
		Method:           http.MethodPost,
		Path:             res.target,
		Status:           res.status,
		BytesOut:         int64(len(res.body)),
		PromptTokens:     promptTok,
		CompletionTokens: completionTok,
		User:             probeUser,
		Channel:          t.Ch.Name,
		Model:            t.Model,
		Key:              t.Key.Name + "@" + t.Ch.Name,
	}
	if errMsg != "" {
		rec.ErrMsg = truncate(errMsg, errMsgMax)
	}
	recordRequest(rec)
}

// ---- 答案提取与校验 ----

// probeNumberRe 从回复文本中提取全部整数。
var probeNumberRe = regexp.MustCompile(`-?\d+`)

// extractProbeAnswer 从上游响应体提取回复文本：chat 响应取 choices[0].message
// .content；responses 响应取 output_text 或 output[].content[].text；都不是
// 标准 JSON 时把整个响应体当纯文本答案（交给数字提取兜底）。
func extractProbeAnswer(body string) (string, bool) {
	body = strings.TrimSpace(body)
	if body == "" {
		return "", false
	}
	var chat struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(body), &chat); err == nil &&
		len(chat.Choices) > 0 && strings.TrimSpace(chat.Choices[0].Message.Content) != "" {
		return chat.Choices[0].Message.Content, true
	}
	var respn struct {
		OutputText string `json:"output_text"`
		Output     []struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal([]byte(body), &respn); err == nil {
		if s := strings.TrimSpace(respn.OutputText); s != "" {
			return s, true
		}
		for _, o := range respn.Output {
			for _, c := range o.Content {
				if s := strings.TrimSpace(c.Text); s != "" {
					return s, true
				}
			}
		}
	}
	return body, true
}

// probeNumbersIn 取回复文本中的全部整数（容忍千分位逗号与前后缀说明文字；
// 超长数字串溢出 int 时忽略）。
func probeNumbersIn(text string) []int {
	text = strings.NewReplacer(",", "", "，", "").Replace(text)
	matches := probeNumberRe.FindAllString(text, -1)
	out := make([]int, 0, len(matches))
	for _, m := range matches {
		if n, err := strconv.Atoi(m); err == nil {
			out = append(out, n)
		}
	}
	return out
}
