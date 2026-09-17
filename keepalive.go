// 流式转发保活（heartbeat）：下游请求为 stream=true 时，等待上游首包期间
// 以及 SSE 流静默期间，周期性向下游写 SSE 注释帧（": keepalive"）并 flush。
//
// 为什么必须有：反代/下游客户端按「连接空闲时长」掐请求（常见 60s）。上游
// 排队几分钟但没有字节流过时，下游判定超时断开，网关只能把断开误报成
// client_canceled/502——表现为「渠道测试可用、下游持续 502」。只要持续有
// 心跳字节流过，空闲计时器就不会触发，网关可以放心等上游出包；上游真出
// 故障（429/5xx 快速失败）时依旧走原有故障转移，冷却/换 key 语义不变。
//
// 代价：第一帧心跳会把 200 + text/event-stream 响应头提前提交给下游，此后
// HTTP 错误状态码与 JSON 错误体都无法再下发；路由彻底失败时改用流内错误帧
// （OpenAI 流式协议支持 data: {"error":...} + [DONE]）表达，请求日志照常
// 记录逐 key 失败轨迹。
package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const keepaliveFrame = ": keepalive\n\n"

// streamKeeper 包装下游 ResponseWriter：所有写出（含心跳）共用一把锁，与
// 响应体转发循环串行，不会撕裂 SSE 帧。newStreamKeeper 在 interval <= 0 时
// 返回 nil（关闭保活，调用方直接用原始 writer）。
type streamKeeper struct {
	w        http.ResponseWriter
	interval time.Duration

	mu        sync.Mutex
	committed bool      // 响应头已发出（首帧心跳或上游 WriteHeader）
	lastWrite time.Time // 最后一次向下游写出字节的时刻

	stopCh   chan struct{}
	stopOnce sync.Once
}

func newStreamKeeper(w http.ResponseWriter, interval time.Duration) *streamKeeper {
	if interval <= 0 {
		return nil
	}
	return &streamKeeper{w: w, interval: interval, lastWrite: time.Now(), stopCh: make(chan struct{})}
}

// start 启动心跳协程：每 interval/2 检查一次，距上次写出超过 interval 就
// 补一帧注释（兼作首包等待期与流中途的静默期）。心跳写出失败（下游停读
// 超时/断开）即停止——客户端已不可达，继续写没有意义。
func (k *streamKeeper) start() {
	per := k.interval / 2
	if per < 10*time.Millisecond {
		per = 10 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(per)
		defer t.Stop()
		for {
			select {
			case <-k.stopCh:
				return
			case now := <-t.C:
				k.mu.Lock()
				if !k.committed || now.Sub(k.lastWrite) >= k.interval {
					if err := k.keepaliveLocked(); err != nil {
						k.mu.Unlock()
						return
					}
				}
				k.mu.Unlock()
			}
		}
	}()
}

// stop 终止心跳（forwardChat 返回前 defer 兜底）。
func (k *streamKeeper) stop() {
	k.stopOnce.Do(func() { close(k.stopCh) })
}

// keepaliveLocked 确保已提交 SSE 响应头，再写一帧注释心跳。调用方须持 mu。
// 写出带下游写超时（客户端停读时不会把心跳协程永久挂在写调用上）。
func (k *streamKeeper) keepaliveLocked() error {
	if !k.committed {
		h := k.w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no") // 显式关 nginx 缓冲，心跳必须即时到达下游
		k.w.WriteHeader(http.StatusOK)
		k.committed = true
	}
	err := writeDownstream(k.w, []byte(keepaliveFrame))
	if err == nil {
		k.lastWrite = time.Now()
	}
	return err
}

// ---- http.ResponseWriter 代理：serveUpstreamResponse 经此写出 ----

func (k *streamKeeper) Header() http.Header { return k.w.Header() }

// Unwrap 暴露被包装的 writer：http.ResponseController 据此把下游写超时
// 穿透包装层设在底层连接上（本类型自身不实现 SetWriteDeadline）。
func (k *streamKeeper) Unwrap() http.ResponseWriter { return k.w }

// WriteHeader 提交上游状态码；若心跳已抢先发过 200 SSE 头则吞掉
// （同一连接不能再发第二份头，上游 body 帧仍照常透传）。
func (k *streamKeeper) WriteHeader(code int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.committed {
		return
	}
	k.w.WriteHeader(code)
	k.committed = true
	k.lastWrite = time.Now()
}

func (k *streamKeeper) Write(b []byte) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	n, err := k.w.Write(b)
	k.lastWrite = time.Now()
	return n, err
}

func (k *streamKeeper) Flush() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if f, ok := k.w.(http.Flusher); ok {
		f.Flush()
	}
}

// committedResponse 报告心跳是否已把响应头提交给下游：true 时 HTTP 错误
// 状态与 JSON 错误体都发不出去了，失败只能用流内 error 帧表达。
func (k *streamKeeper) committedResponse() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.committed
}

// finish 终态写出：先停心跳，再在同一把锁内判定响应头是否已被心跳提交。
// 未提交 → 正常 JSON 错误（可带 Retry-After 头）；已提交 → 流内 error 帧。
// 与 forwardChat 的失败返回路径共用，确保「写 HTTP 错误」与「写流错误」
// 二选一且与在途心跳互斥，不会撕裂。retryAfter>0 仅在未提交分支生效
// （响应头一旦发出就无法补写）。
func (k *streamKeeper) finish(msg, code string, status int, retryAfter int64) {
	k.stop()
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.committed {
		payload, err := json.Marshal(map[string]any{
			"error": map[string]string{"message": msg, "type": "gateway_error", "code": code},
		})
		if err == nil {
			_ = writeDownstream(k.w, append(append([]byte("data: "), payload...), '\n', '\n'))
			_ = writeDownstream(k.w, []byte("data: [DONE]\n\n"))
		}
		return
	}
	if retryAfter > 0 {
		k.w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
	}
	writeJSONError(k.w, status, msg, code)
}
