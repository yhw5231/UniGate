// 上游响应转发：SSE 逐帧读取、可选 reasoning -> reasoning_content 改写、用量解析。
package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ---- 下游写入（写超时兜底） ----

// writeDownstream 向下游写一段数据并 flush，返回首错。
// 写超时（配置 cfg.DownstreamWriteTimeout）由 responseRecorder 在每次 Write/
// Flush 上武装：穿透包装层设在底层连接上，客户端保持连接但停止读取（TCP
// 窗口填满）时，写会在超时后返回错误，转发循环随之中止——否则该请求的
// goroutine 与上下游连接对会永久挂住（上游侧已有读取静默超时兜底，下游侧
// 需要对称保护）。
func writeDownstream(w http.ResponseWriter, payload []byte) error {
	_, err := w.Write(payload)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return err
}

// armWriteDeadline 武装下游单次写超时（cfg.DownstreamWriteTimeout，0 = 关闭），
// 返回清除超时的恢复函数。通过 http.ResponseController 调用（穿透
// responseRecorder / streamKeeper 的 Unwrap 链设在底层连接上）。
// 底层不支持写超时（HTTP/2、httptest.ResponseRecorder 等）时退化为普通写入。
func armWriteDeadline(w http.ResponseWriter) func() {
	if cfg.DownstreamWriteTimeout <= 0 {
		return func() {}
	}
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Now().Add(cfg.DownstreamWriteTimeout)); err != nil {
		return func() {}
	}
	return func() { _ = rc.SetWriteDeadline(time.Time{}) }
}

// idleTimeoutReader 包装上游响应体：每次 Read 武装一个看门狗，连续 idle 没有
// 任何字节返回（上游失联，TCP 半开 / NAT 静默回收等）就关闭底层连接强制结束
// 阻塞中的 Read，调用方把错误当普通上游故障处理（路由层换 key，转发层结束
// 该流）。idle 来自 cfg.UpstreamReadIdle（UPSTREAM_READ_IDLE_TIMEOUT，默认
// 10m，0 = 关闭）；须大于最长上游思考时间（LLM 首包可达数分钟），流式期间
// 任何字节都会重置计时。
type idleTimeoutReader struct {
	rc   io.ReadCloser
	idle time.Duration
}

func (r *idleTimeoutReader) Read(p []byte) (int, error) {
	if r.idle <= 0 {
		return r.rc.Read(p)
	}
	timer := time.AfterFunc(r.idle, func() { _ = r.rc.Close() })
	n, err := r.rc.Read(p)
	timer.Stop()
	return n, err
}

func (r *idleTimeoutReader) Close() error { return r.rc.Close() }

// extractModel 从请求体解析 model 字段（用于按 (key, model) 计算冷却）。
func extractModel(raw []byte) string {
	var parsed struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return ""
	}
	return parsed.Model
}

// parseUsageJSON 从 OpenAI 响应体解析 usage.prompt_tokens / completion_tokens。
func parseUsageJSON(raw []byte) (int64, int64) {
	var obj struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil || obj.Usage == nil {
		return 0, 0
	}
	return obj.Usage.PromptTokens, obj.Usage.CompletionTokens
}

// parseUsageFromFrame 从 SSE 帧中解析 usage（OpenAI 流式在末尾 chunk 带 usage）。
func parseUsageFromFrame(frame []byte) (int64, int64, bool) {
	for _, line := range strings.Split(string(frame), "\n") {
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		if !strings.Contains(payload, "usage") {
			continue
		}
		p, c := parseUsageJSON([]byte(payload))
		if p > 0 || c > 0 {
			return p, c, true
		}
	}
	return 0, 0, false
}

// recordUsageToRecorder 把 token 用量写回 responseRecorder 的 reqStats。
// w 可能是 streamKeeper（流式保活）包装，先解包到底层 recorder。
// 解包链路里找不到 recorder 说明中间件被绕过（handler 未挂在 statsMiddleware
// 下），用量会静默丢失——记一条日志便于定位，不静默吞掉。
func recordUsageToRecorder(w http.ResponseWriter, prompt, completion int64) {
	if prompt <= 0 && completion <= 0 {
		return
	}
	for {
		switch t := w.(type) {
		case *responseRecorder:
			if t.rs != nil {
				t.rs.promptTokens += prompt
				t.rs.completionTokens += completion
			}
			return
		case *streamKeeper:
			w = t.w
		default:
			log.Printf("usage: response writer %T is not wrapped by statsMiddleware; token usage dropped", w)
			return
		}
	}
}

// passFrame 不改写地透传一帧 SSE（保留原字节，确保帧尾分隔符完整）。
func passFrame(frame []byte) []byte {
	if len(frame) == 0 {
		return nil
	}
	if frameEndsWithDoubleNL(frame) {
		return frame
	}
	return append(frame, '\n', '\n')
}

// serveUpstreamResponse 把上游响应转发给客户端（rewrite 时做 reasoning -> reasoning_content 改写；
// 渠道端点类型为 responses 时先做 Responses API -> chat/completions 转换）。
func serveUpstreamResponse(w http.ResponseWriter, upResp *http.Response, stream, rewrite bool, endpointType string) {
	defer upResp.Body.Close()

	if endpointType == endpointResponses {
		serveResponsesUpstream(w, upResp, stream, rewrite)
		return
	}

	if !stream {
		body, readErr := io.ReadAll(upResp.Body)
		if readErr != nil {
			writeJSONError(w, http.StatusBadGateway, "upstream read failed: "+readErr.Error(), "upstream_error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		out, ok := body, false
		if rewrite {
			out, ok = rewriteNonStream(body)
		}
		if rewrite && !ok {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		p, c := parseUsageJSON(out)
		recordUsageToRecorder(w, p, c)
		w.WriteHeader(upResp.StatusCode)
		_ = writeDownstream(w, out)
		return
	}

	// 流式：无 body 的响应（如 204/304）原样透传
	if upResp.ContentLength == 0 {
		copyHeader(w.Header(), upResp.Header)
		w.WriteHeader(upResp.StatusCode)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(upResp.StatusCode)

	br := bufio.NewReader(upResp.Body)
	for {
		frame, readErr := readFrame(br)
		if readErr != nil && readErr != io.EOF {
			// 上游读取中断（失联看门狗、帧超限、连接被取消）：
			// 结束该流，日志留痕便于定位
			log.Printf("forward: upstream stream read aborted: %v", readErr)
			break
		}
		if len(frame) > 0 {
			if p, c, ok := parseUsageFromFrame(frame); ok {
				recordUsageToRecorder(w, p, c)
			}
			var out []byte
			if rewrite {
				out = processFrame(frame)
			} else {
				out = passFrame(frame)
			}
			if out != nil {
				if err := writeDownstream(w, out); err != nil {
					log.Printf("forward: downstream write aborted: %v", err)
					break
				}
			}
		}
		if readErr != nil {
			break
		}
	}
}

// serveResponsesUpstream 转发 Responses API 上游的响应（非流式：response 对象
// 转成 chat.completion；流式：response.* 事件转成 chat.completion.chunk 帧）。
// 非 response 对象（如上游错误体）原样透传。
func serveResponsesUpstream(w http.ResponseWriter, upResp *http.Response, stream, rewrite bool) {
	defer upResp.Body.Close()

	if !stream {
		body, readErr := io.ReadAll(upResp.Body)
		if readErr != nil {
			writeJSONError(w, http.StatusBadGateway, "upstream read failed: "+readErr.Error(), "upstream_error")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		out, ok := responsesToChatCompletion(body)
		if !ok {
			out = body // 错误体等非 response 对象，保持上游原样
		}
		p, c := parseUsageJSON(out)
		recordUsageToRecorder(w, p, c)
		w.WriteHeader(upResp.StatusCode)
		_ = writeDownstream(w, out)
		return
	}

	if upResp.ContentLength == 0 {
		copyHeader(w.Header(), upResp.Header)
		w.WriteHeader(upResp.StatusCode)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(upResp.StatusCode)

	conv := newResponsesStreamConv()
	br := bufio.NewReader(upResp.Body)
	for {
		frame, readErr := readFrame(br)
		if readErr != nil && readErr != io.EOF {
			log.Printf("forward: upstream stream read aborted: %v", readErr)
			break
		}
		if len(frame) > 0 {
			// usage 已在 response.completed 事件里映射，无需再从帧解析
			if out := conv.convertFrame(frame); len(out) > 0 {
				if p, c2, ok := parseUsageFromFrame(out); ok {
					recordUsageToRecorder(w, p, c2)
				}
				if err := writeDownstream(w, out); err != nil {
					log.Printf("forward: downstream write aborted: %v", err)
					return
				}
			}
		}
		if readErr != nil {
			break
		}
	}
	// 上游正常结束但未发 completed/incomplete/error 事件（或异常断流）：
	// 补发 finish_reason + [DONE]，避免下游悬挂
	if !conv.done {
		if out := conv.finalize(); len(out) > 0 {
			_ = writeDownstream(w, out)
		}
		_ = writeDownstream(w, []byte("data: [DONE]\n\n"))
	}
}
