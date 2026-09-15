// 流式保活单元测试：心跳协程、响应头提交语义、终态二选一（JSON 错误 vs 流内错误帧）。
// 端到端行为（等待上游首包期间的心跳、故障转移、日志轨迹）见 route_test.go。
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestNewStreamKeeperNilWhenDisabled：interval <= 0 时返回 nil，调用方据此
// 直接使用原始 writer（功能关闭路径）。
func TestNewStreamKeeperNilWhenDisabled(t *testing.T) {
	w := httptest.NewRecorder()
	if k := newStreamKeeper(w, 0); k != nil {
		t.Fatal("interval 0 should disable keepalive (nil)")
	}
	if k := newStreamKeeper(w, -time.Second); k != nil {
		t.Fatal("negative interval should disable keepalive (nil)")
	}
	if k := newStreamKeeper(w, 15*time.Second); k == nil {
		t.Fatal("positive interval should build a keeper")
	}
}

// TestStreamKeeperEmitsHeartbeat：start 后未写出任何字节时应按间隔补心跳帧，
// 并提交 200 + event-stream 响应头。
func TestStreamKeeperEmitsHeartbeat(t *testing.T) {
	rec := httptest.NewRecorder()
	k := newStreamKeeper(rec, 20*time.Millisecond)
	k.start()
	defer k.stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(rec.Body.String(), keepaliveFrame) {
		time.Sleep(5 * time.Millisecond)
	}
	body := rec.Body.String()
	if !strings.Contains(body, keepaliveFrame) {
		t.Fatalf("no heartbeat frame emitted; body=%q", body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !k.committedResponse() {
		t.Fatal("heartbeat should mark response as committed")
	}
}

// TestStreamKeeperHeaderCommitSwallowsUpstreamStatus：心跳已提交 200 后，
// 上游状态码不能再写（同一连接只有一份响应头）。
func TestStreamKeeperHeaderCommitSwallowsUpstreamStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	k := newStreamKeeper(rec, 15*time.Second)
	k.keepaliveLocked() // 立即提交 200

	k.WriteHeader(http.StatusBadGateway)
	if rec.Code != http.StatusOK {
		t.Fatalf("status overwritten to %d after commit; want 200", rec.Code)
	}
	if _, err := k.Write([]byte("data: x\n\n")); err != nil {
		t.Fatalf("write after commit: %v", err)
	}
	if !strings.Contains(rec.Body.String(), "data: x") {
		t.Fatalf("body frames should pass through after commit: %q", rec.Body.String())
	}
}

// TestStreamKeeperFinishBeforeCommitWritesJSONError：未提交时终态是普通
// JSON 错误（可带 Retry-After），下游能拿到 HTTP 状态码。
func TestStreamKeeperFinishBeforeCommitWritesJSONError(t *testing.T) {
	rec := httptest.NewRecorder()
	k := newStreamKeeper(rec, 15*time.Second)
	k.finish("all keys cooling", "rate_limited", http.StatusTooManyRequests, 42)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "42" {
		t.Fatalf("Retry-After = %q, want 42", got)
	}
	if !strings.Contains(rec.Body.String(), "rate_limited") {
		t.Fatalf("body = %q", rec.Body.String())
	}
	if k.committedResponse() {
		t.Fatal("JSON error path should not be treated as SSE-committed")
	}
}

// TestStreamKeeperFinishAfterCommitWritesErrorFrame：心跳已提交响应头时，
// 终态改用流内 error 帧 + [DONE]，HTTP 状态无法再更改。
func TestStreamKeeperFinishAfterCommitWritesErrorFrame(t *testing.T) {
	rec := httptest.NewRecorder()
	k := newStreamKeeper(rec, 15*time.Second)
	k.keepaliveLocked() // 提交 200 SSE 头

	k.finish("upstream exhausted", "upstream_error", http.StatusBadGateway, 0)
	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (already committed)", rec.Code)
	}
	if !strings.Contains(body, `"type":"gateway_error"`) || !strings.Contains(body, "upstream_error") {
		t.Fatalf("missing in-stream error frame: %q", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("missing [DONE] terminator: %q", body)
	}
}

// TestStreamKeeperStopIdempotent：stop 可重复调用（defer + 显式两处都会走），
// 且停止后不再产生新的心跳帧。
func TestStreamKeeperStopIdempotent(t *testing.T) {
	rec := httptest.NewRecorder()
	k := newStreamKeeper(rec, 10*time.Millisecond)
	k.start()
	time.Sleep(60 * time.Millisecond) // 至少几帧
	k.stop()
	k.stop() // 第二次不得 panic（stopOnce 保护）

	after := rec.Body.Len()
	time.Sleep(80 * time.Millisecond)
	if rec.Body.Len() != after {
		t.Fatalf("heartbeat continued after stop: %d -> %d bytes", after, rec.Body.Len())
	}
}
