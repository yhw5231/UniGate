// bodyRetryAfter 单测：从 429 错误体解析上游明确给出的重试等待时间
//（相对时长短语 / 绝对时间戳），与误匹配防护。
package main

import (
	"testing"
	"time"
)

func TestBodyRetryAfterRelative(t *testing.T) {
	cases := []struct {
		name string
		body string
		want time.Duration
	}{
		{"用户上报的 INFERENCE_CAP_ERROR 原文", `{"error":{"code":"INFERENCE_CAP_ERROR","message":"Error 429: Daily free limit reached on model deepseek/deepseek-v4-flash-0731. Try again in 14h 23m"}}`, 14*time.Hour + 23*time.Minute},
		{"h+m", "Try again in 2h 5m", 2*time.Hour + 5*time.Minute},
		{"无空格紧凑 1h23m", "Try again in 1h23m", 83 * time.Minute},
		{"无空格紧凑 2h30m", "in 2h30m", 2*time.Hour + 30*time.Minute},
		{"hours", "Rate limit exceeded. Try again in 9 hours", 9 * time.Hour},
		{"minutes", "Daily free limit reached. Try again in 5 minutes", 5 * time.Minute},
		{"seconds", "retry after 120 seconds", 2 * time.Minute},
		{"单 s", "Try again in 45s", 45 * time.Second},
		{"d+h", "in 2d 3h", 51 * time.Hour},
		{"day", "in 1 day", 24 * time.Hour},
		{"and 连接", "in 1 hour and 30 minutes", 90 * time.Minute},
		{"逗号连接", "in 1 hour, 30 minutes", 90 * time.Minute},
		{"小数", "in 1.5h", 90 * time.Minute},
		{"全大写", "TRY AGAIN IN 14H 23M", 14*time.Hour + 23*time.Minute},
		{"hour 缩写 hrs", "in 2 hrs", 2 * time.Hour},
		{"短语后跟其他文本", "Try again in 14h 23m (window resets at midnight UTC)", 14*time.Hour + 23*time.Minute},
		{"短语后空格接普通单词", "Try again in 5m extra", 5 * time.Minute},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ok := bodyRetryAfter(c.body)
			if !ok || d != c.want {
				t.Fatalf("bodyRetryAfter(%q) = %v,%v want %v", c.body, d, ok, c.want)
			}
		})
	}
}

func TestBodyRetryAfterAbsolute(t *testing.T) {
	// RFC3339 带 Z
	fut := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	if d, ok := bodyRetryAfter("Try again at " + fut.Format(time.RFC3339)); !ok || d < 2*time.Hour-time.Minute || d > 2*time.Hour+time.Minute {
		t.Fatalf("absolute Z: d=%v ok=%v, want ~2h", d, ok)
	}
	// RFC3339 带时区偏移
	loc := time.FixedZone("CST", 8*3600)
	fut2 := time.Now().Add(3 * time.Hour).In(loc).Truncate(time.Second)
	if d, ok := bodyRetryAfter(`{"reset_at":"` + fut2.Format(time.RFC3339) + `"}`); !ok || d < 3*time.Hour-time.Minute || d > 3*time.Hour+time.Minute {
		t.Fatalf("absolute +08:00: d=%v ok=%v, want ~3h", d, ok)
	}
	// 无时区空格分隔（按 UTC 理解）
	fut3 := time.Now().Add(90 * time.Minute).UTC().Truncate(time.Minute)
	if d, ok := bodyRetryAfter(`{"reset_at":"` + fut3.Format("2006-01-02 15:04") + `"}`); !ok || d < 90*time.Minute-time.Minute || d > 90*time.Minute+time.Minute {
		t.Fatalf("absolute naive: d=%v ok=%v, want ~90m", d, ok)
	}
	// 小写 t/z
	fut4 := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	if d, ok := bodyRetryAfter("try again at " + fut4.Format(time.RFC3339)); !ok || d < 2*time.Hour-time.Minute || d > 2*time.Hour+time.Minute {
		t.Fatalf("absolute lowercase: d=%v ok=%v, want ~2h", d, ok)
	}
}

func TestBodyRetryAfterNoMatch(t *testing.T) {
	for _, s := range []string{
		"",
		`{"error":"rate limited"}`,
		"429 Too Many Requests",
		"quota exceeded",
		"in the request",
		"wait a moment",
		"limit reached on model deepseek-v4-flash-0731",
		"limit used since 2020-01-01T00:00:00Z", // 过去时刻不作为冷却依据
		"Try again in about 2 hours",            // 数字与单位间夹词不算
		"Your limit resets in 2 months",         // 单位被截断（"2 m"+onths）不算
	} {
		if d, ok := bodyRetryAfter(s); ok {
			t.Fatalf("bodyRetryAfter(%q) = %v, want no match", s, d)
		}
	}
}
