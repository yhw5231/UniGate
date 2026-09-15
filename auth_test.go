// 登录验证、token 签发与验证测试。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVerifyLogin(t *testing.T) {
	// 默认管理员
	if !verifyLogin("admin", "admin") {
		t.Error("admin/admin should be valid")
	}
	if verifyLogin("admin", "wrong") {
		t.Error("admin/wrong should be invalid")
	}
	if verifyLogin("", "") {
		t.Error("empty should be invalid")
	}
}

func TestVerifyLoginExtraUsers(t *testing.T) {
	t.Setenv("EXTRA_USERS", "user1:pass1,user2:pass2")
	t.Setenv("LOGIN_REQUIRED", "true") // 确保 reloadConfig 能正确读取
	resetCfgForTest()

	if !verifyLogin("user1", "pass1") {
		t.Error("user1/pass1 should be valid")
	}
	if !verifyLogin("user2", "pass2") {
		t.Error("user2/pass2 should be valid")
	}
	if verifyLogin("user1", "wrong") {
		t.Error("user1/wrong should be invalid")
	}
	// 恢复
	resetCfgForTest()
}

func TestIssueAndVerifyToken(t *testing.T) {
	token, err := issueToken("admin")
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("token should not be empty")
	}

	user, err := verifyToken(token)
	if err != nil {
		t.Fatalf("verifyToken: %v", err)
	}
	if user != "admin" {
		t.Fatalf("got user %q, want admin", user)
	}
}

func TestVerifyTokenMalformed(t *testing.T) {
	if _, err := verifyToken(""); err == nil {
		t.Error("empty token should fail")
	}
	if _, err := verifyToken("no-dot"); err == nil {
		t.Error("no-dot should fail")
	}
	if _, err := verifyToken("bad.base64.here"); err == nil {
		t.Error("should fail")
	}
}

func TestVerifyTokenExpired(t *testing.T) {
	t.Setenv("TOKEN_TTL", "1s")
	resetCfgForTest()
	defer resetCfgForTest()

	token, err := issueToken("admin")
	if err != nil {
		t.Fatal(err)
	}
	// token 应该有效
	if _, err := verifyToken(token); err != nil {
		t.Fatalf("fresh token should be valid: %v", err)
	}
	// 等待过期（留足秒级截断余量）
	time.Sleep(2100 * time.Millisecond)
	if _, err := verifyToken(token); err == nil {
		t.Error("expired token should fail")
	}
}

func TestHandleLogin(t *testing.T) {
	resetLoginThrottle()
	defer resetLoginThrottle()
	tests := []struct {
		name       string
		method     string
		body       string
		wantStatus int
		wantToken  bool
	}{
		{"有效登录", http.MethodPost, `{"username":"admin","password":"admin"}`, http.StatusOK, true},
		{"错误密码", http.MethodPost, `{"username":"admin","password":"wrong"}`, http.StatusUnauthorized, false},
		{"空用户名", http.MethodPost, `{"username":"","password":"admin"}`, http.StatusUnauthorized, false},
		{"GET 不支持", http.MethodGet, ``, http.StatusMethodNotAllowed, false},
		{"非 JSON", http.MethodPost, `not json`, http.StatusBadRequest, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/login", strings.NewReader(tc.body))
			rr := httptest.NewRecorder()
			handleLogin(rr, req)

			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tc.wantStatus, rr.Body.String())
			}
			if tc.wantToken {
				var resp struct {
					Token     string `json:"token"`
					TokenType string `json:"token_type"`
				}
				if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if resp.Token == "" {
					t.Error("token field should not be empty")
				}
				if resp.TokenType != "Bearer" {
					t.Errorf("token_type = %q, want Bearer", resp.TokenType)
				}
			}
		})
	}
}

// TestLoginThrottleLocksAfterFailures：连续失败达阈值后锁定（429 + Retry-After）；
// 锁定期间即使密码正确也拒绝；成功登录会清零计数。
func TestLoginThrottleLocksAfterFailures(t *testing.T) {
	t.Setenv("LOGIN_FAIL_LOCKOUT", "3")
	t.Setenv("LOGIN_FAIL_WINDOW", "1m")
	resetCfgForTest()
	defer resetCfgForTest()

	login := func(password string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/login",
			strings.NewReader(`{"username":"admin","password":"`+password+`"}`))
		req.RemoteAddr = "10.0.0.9:1234"
		rr := httptest.NewRecorder()
		handleLogin(rr, req)
		return rr
	}

	// 前 2 次失败仅 401（阈值 3）
	for i := 0; i < 2; i++ {
		if rr := login("wrong"); rr.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status=%d want 401", i+1, rr.Code)
		}
	}
	// 第 3 次失败触发锁定，本次仍是 401（锁定从下一次请求生效）
	if rr := login("wrong"); rr.Code != http.StatusUnauthorized {
		t.Fatalf("3rd failure status=%d want 401", rr.Code)
	}
	// 第 4 次：已锁定 → 429，且正确密码也拒绝
	rr := login("admin")
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("locked status=%d want 429; body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}

	// 解锁（模拟窗口过期）后可用正确密码登录
	resetLoginThrottle()
	if rr := login("admin"); rr.Code != http.StatusOK {
		t.Fatalf("after unlock status=%d want 200; body=%s", rr.Code, rr.Body.String())
	}
}

// TestLoginThrottleDisabledByZero：LOGIN_FAIL_LOCKOUT=0 关闭防爆破。
func TestLoginThrottleDisabledByZero(t *testing.T) {
	t.Setenv("LOGIN_FAIL_LOCKOUT", "0")
	resetCfgForTest()
	defer resetCfgForTest()

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodPost, "/login",
			strings.NewReader(`{"username":"admin","password":"wrong"}`))
		req.RemoteAddr = "10.0.0.10:1234"
		rr := httptest.NewRecorder()
		handleLogin(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status=%d want 401 (throttle disabled)", i+1, rr.Code)
		}
	}
}

func TestRequireAuth(t *testing.T) {
	// 默认 LOGIN_REQUIRED=true
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	_, ok := requireAuth(rr, req)
	if ok {
		t.Error("should reject missing token")
	}
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}

	// 有效 token
	token, _ := issueToken("admin")
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req2.Header.Set("Authorization", "Bearer "+token)
	user, ok := requireAuth(rr2, req2)
	if !ok {
		t.Error("should accept valid token")
	}
	if user != "admin" {
		t.Errorf("user = %q, want admin", user)
	}

	// 过期 token
	t.Setenv("TOKEN_TTL", "1s")
	resetCfgForTest()
	defer resetCfgForTest()
	expToken, _ := issueToken("admin")
	time.Sleep(2100 * time.Millisecond)
	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req3.Header.Set("Authorization", "Bearer "+expToken)
	if _, ok := requireAuth(rr3, req3); ok {
		t.Error("should reject expired token")
	}
}

func TestBearerToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer abc123")
	if b := bearerToken(req); b != "abc123" {
		t.Errorf("got %q, want abc123", b)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "BEARER xyz")
	if b := bearerToken(req2); b != "xyz" {
		t.Errorf("got %q, want xyz", b)
	}

	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	if b := bearerToken(req3); b != "" {
		t.Errorf("got %q, want empty", b)
	}
}
