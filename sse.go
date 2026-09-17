// SSE 改写与通用辅助：帧读取、delta.reasoning -> reasoning_content 复制、错误输出。
// （自 cline2api 移植版原样保留，含与 JS 原版对齐的容错语义。）
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode"
)

// copyHeader 把源 header 复制到目标（用于无 body 响应的整体透传）。
func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

// isStreamRequest：请求体里 stream === false 才算非流式；
// 缺省 / stream 为 true / null / 其他值时按流式处理（与 JS `parsed.stream === false` 一致）。
func isStreamRequest(raw []byte) bool {
	var parsed struct {
		Stream *bool `json:"stream"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return true
	}
	if parsed.Stream == nil {
		return true
	}
	return *parsed.Stream
}

// isBearerToken：是否已带 "Bearer " 前缀（大小写不敏感，对应 JS 正则 /^Bearer /i）。
func isBearerToken(auth string) bool {
	return len(auth) >= len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ")
}

// ---- SSE 处理 ----

// maxSSEFrameBytes 单帧字节上限。上游若发出一条始终没有换行的超长「行」，
// readFrame 会沿 bufio.ErrBufferFull 分支持续累积帧缓冲，单连接就能吃掉大量
// 内存（下游可重复触发）。正常 LLM 帧（含 reasoning 全文）远小于该值。
// var 而非 const：测试可调小以快速构造超限场景。
var maxSSEFrameBytes = 8 << 20

// errSSEFrameTooLarge 帧超限：按上游协议异常中止该流（调用方停止转发）。
var errSSEFrameTooLarge = errors.New("sse frame exceeds size limit")

// readFrame 以 "\n\n" 为帧边界读取一帧（对应 JS 的 TransformStream），返回不含分隔符的帧内容。
// 到达 EOF 时：若有残余且非全空白，作为最后一帧返回 (frame, nil)；否则返回 (nil, io.EOF)。
// 行超长（超出 bufio 缓冲区）时继续累积（上限 maxSSEFrameBytes）。
func readFrame(br *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		line, err := br.ReadSlice('\n')
		frame = append(frame, line...)
		if frameEndsWithDoubleNL(frame) {
			return frame[:len(frame)-2], nil
		}
		if len(frame) > maxSSEFrameBytes {
			return nil, errSSEFrameTooLarge
		}
		if err != nil {
			if err == bufio.ErrBufferFull {
				continue
			}
			if err == io.EOF {
				if len(bytes.TrimSpace(frame)) == 0 {
					return nil, io.EOF
				}
				return frame, nil // 非空残余帧（对应 JS flush 分支）
			}
			return nil, err
		}
	}
}

func frameEndsWithDoubleNL(frame []byte) bool {
	n := len(frame)
	return n >= 2 && frame[n-2] == '\n' && frame[n-1] == '\n'
}

// processFrame 处理一帧 SSE：把 data 行中的 delta.reasoning 复制到 delta.reasoning_content。
// 与 JS 一致：data 行与非 data 行分开再拼接；无有效行时返回 nil（不输出）。
func processFrame(frame []byte) []byte {
	lines := strings.Split(string(frame), "\n")
	var dataLines, otherLines []string
	for _, line := range lines {
		if strings.HasPrefix(line, "data:") {
			// 对应 JS 的 line.slice(5).trimStart()：剥掉 slice 后左侧全部 Unicode whitespace
			// （含 \t \v \f \r 及 NBSP 等），避免 \v 等残留在 JSON 前面导致解析失败。
			dataLines = append(dataLines, strings.TrimLeftFunc(line[len("data:"):], unicode.IsSpace))
		} else {
			otherLines = append(otherLines, line)
		}
	}

	parts := make([]string, 0, len(otherLines)+len(dataLines))
	for _, l := range otherLines {
		if l != "" {
			parts = append(parts, l)
		}
	}
	for _, d := range dataLines {
		if d == "[DONE]" {
			parts = append(parts, "data: [DONE]")
			continue
		}
		// 快路径：不含 "reasoning" 的 chunk 大概率无需改写，直接透传，
		// 跳过 JSON 解析/序列化，显著降低长流式下的 CPU 消耗。
		// 注：JSON 字符串值内的引号会被转义为 \"，不会误匹配字段名 "reasoning"。
		if strings.Contains(d, `"reasoning"`) {
			if out, changed := rewriteDelta([]byte(d)); changed {
				parts = append(parts, "data: "+string(out))
				continue
			}
			// 不可解析或未改写则原样透传（对应 JS 的 catch / 未 changed 分支）
		}
		parts = append(parts, "data: "+d)
	}

	if len(parts) == 0 {
		return nil
	}
	return []byte(strings.Join(parts, "\n") + "\n\n")
}

// rewriteDelta 改写流式 chunk：delta.reasoning -> delta.reasoning_content。
// 返回改写后的字节与是否确实发生了改写（未改 / 不可解析时返回原字节 + false）。
// 用 map[string]json.RawMessage 保留各字段原始字节，只重排键序、不改数字精度与转义。
func rewriteDelta(raw []byte) ([]byte, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw, false
	}
	rawChoices, ok := obj["choices"]
	if !ok {
		return raw, false
	}
	var choices []json.RawMessage
	if err := json.Unmarshal(rawChoices, &choices); err != nil {
		return raw, false
	}

	changed := false
	for i := range choices {
		// 逐元素解码：null / 原始值 / 脏对象一律跳过（对应 JS `const d = c && c.delta` 的宽松处理），
		// 其它带 delta.reasoning 的对象元素照常改写 —— 避免整个数组因单个脏元素一起解析失败而放弃改写
		var choice map[string]json.RawMessage
		if err := json.Unmarshal(choices[i], &choice); err != nil {
			continue
		}
		rawDelta, ok := choice["delta"]
		if !ok {
			continue
		}
		var delta map[string]json.RawMessage
		if err := json.Unmarshal(rawDelta, &delta); err != nil {
			continue
		}
		reasoning, hasReasoning := delta["reasoning"]
		// 对应 JS `d.reasoning != null`：字段存在且值非 JSON null
		if !hasReasoning || jsonIsNull(reasoning) {
			continue
		}
		// 对应 JS `!d.reasoning_content`：缺失或为 falsy 值才覆盖
		if rc, hasRC := delta["reasoning_content"]; hasRC && !jsonFalsy(rc) {
			continue
		}
		delta["reasoning_content"] = reasoning
		newDelta, err := json.Marshal(delta)
		if err != nil {
			continue
		}
		choice["delta"] = newDelta
		newChoice, err := json.Marshal(choice)
		if err != nil {
			continue
		}
		choices[i] = newChoice
		changed = true
	}

	if !changed {
		return raw, false
	}
	newChoices, err := json.Marshal(choices)
	if err != nil {
		return raw, false
	}
	obj["choices"] = newChoices
	out, err := json.Marshal(obj)
	if err != nil {
		return raw, false
	}
	return out, true
}

// rewriteNonStream 改写非流式响应：message.reasoning -> message.reasoning_content。
// 返回 (改写后字节, 是否解析成功)。解析失败返回 (原字节, false)，外层据此原样透传（不设 JSON Content-Type）。
func rewriteNonStream(raw []byte) ([]byte, bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw, false
	}
	rawChoices, ok := obj["choices"]
	if !ok {
		return raw, true // 解析成功但无 choices（如上游错误体），原样返回
	}
	var choices []json.RawMessage
	if err := json.Unmarshal(rawChoices, &choices); err != nil {
		return raw, true
	}

	changed := false
	for i := range choices {
		var choice map[string]json.RawMessage
		if err := json.Unmarshal(choices[i], &choice); err != nil {
			continue // 跳过 null / 原始值元素，与 JS 逐元素容错一致
		}
		rawMsg, ok := choice["message"]
		if !ok {
			continue
		}
		var msg map[string]json.RawMessage
		if err := json.Unmarshal(rawMsg, &msg); err != nil {
			continue
		}
		reasoning, hasReasoning := msg["reasoning"]
		if !hasReasoning || jsonIsNull(reasoning) {
			continue
		}
		if rc, hasRC := msg["reasoning_content"]; hasRC && !jsonFalsy(rc) {
			continue
		}
		msg["reasoning_content"] = reasoning
		newMsg, err := json.Marshal(msg)
		if err != nil {
			continue
		}
		choice["message"] = newMsg
		newChoice, err := json.Marshal(choice)
		if err != nil {
			continue
		}
		choices[i] = newChoice
		changed = true
	}

	if !changed {
		return raw, true
	}
	newChoices, err := json.Marshal(choices)
	if err != nil {
		return raw, true
	}
	obj["choices"] = newChoices
	out, err := json.Marshal(obj)
	if err != nil {
		return raw, true
	}
	return out, true
}

// jsonFalsy：对应 JS 的宽松 truthiness 检查（!value 为 true 的情况）。
// JS 里 null / "" / 0（含 -0、0.0）/ false / undefined 均为 falsy。
func jsonFalsy(rm json.RawMessage) bool {
	t := bytes.TrimSpace(rm)
	// 只有数字字面量（以 - 或数字开头）才尝试数值判断。
	// 不能直接对 json.Number 调 Unmarshal：其底层是 string，会顺手接收 JSON 字符串 "0"，
	// 使 jsonFalsy(`"0"`) 误判为 true（JS 里 "0" 是 truthy）。
	if len(t) > 0 && (t[0] == '-' || (t[0] >= '0' && t[0] <= '9')) {
		var n json.Number
		if err := json.Unmarshal(t, &n); err == nil {
			f, err := n.Float64()
			return err == nil && f == 0
		}
		return false
	}
	// 注：JSON null 解码到 string/bool 时 Go 不报错而置零值，恰好落入 falsy 分支。
	var s string
	if err := json.Unmarshal(t, &s); err == nil {
		return s == ""
	}
	var b bool
	if err := json.Unmarshal(t, &b); err == nil {
		return !b
	}
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

func jsonIsNull(rm json.RawMessage) bool {
	t := bytes.TrimSpace(rm)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

func writeJSONError(w http.ResponseWriter, status int, message, typ string) {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    typ,
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
