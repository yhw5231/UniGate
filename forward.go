// 上游响应转发：SSE 逐帧读取、可选 reasoning -> reasoning_content 改写、用量解析。
package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

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
		_, _ = w.Write(out)
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
	flusher, _ := w.(http.Flusher)

	br := bufio.NewReader(upResp.Body)
	for {
		frame, readErr := readFrame(br)
		if readErr != nil && readErr != io.EOF {
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
				_, _ = w.Write(out)
				if flusher != nil {
					flusher.Flush()
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
		_, _ = w.Write(out)
		return
	}

	if upResp.ContentLength == 0 {
		copyHeader(w.Header(), upResp.Header)
		w.WriteHeader(upResp.StatusCode)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(upResp.StatusCode)
	flusher, _ := w.(http.Flusher)

	conv := newResponsesStreamConv()
	br := bufio.NewReader(upResp.Body)
	for {
		frame, readErr := readFrame(br)
		if readErr != nil && readErr != io.EOF {
			break
		}
		if len(frame) > 0 {
			// usage 已在 response.completed 事件里映射，无需再从帧解析
			if out := conv.convertFrame(frame); len(out) > 0 {
				if p, c2, ok := parseUsageFromFrame(out); ok {
					recordUsageToRecorder(w, p, c2)
				}
				_, _ = w.Write(out)
				if flusher != nil {
					flusher.Flush()
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
			_, _ = w.Write(out)
			if flusher != nil {
				flusher.Flush()
			}
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		if flusher != nil {
			flusher.Flush()
		}
	}
}
