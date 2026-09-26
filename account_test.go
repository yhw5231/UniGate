// 设置页「账号」子页：PUT /admin/api/account 修改管理员用户名/密码的测试。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

// setupAccountTest 给每次测试独立的 accounts.json 路径 + 固定追加用户。
func setupAccountTest(t *testing.T) {
	t.Helper()
	t.Setenv("ACCOUNTS_PATH", filepath.Join(t.TempDir(), "accounts.json"))
	t.Setenv("EXTRA_USERS", "alice:alicepass")
	resetCfgForTest()
}

// loginUser 用指定凭据登录并返回 token（失败直接 fatal）。
func loginUser(t *testing.T, username, password string) string {
	t.Helper()
	rr := httptest.NewRecorder()
	rootHandler(rr, httptest.NewRequest(http.MethodPost, "/login",
		strings.NewReader(`{"username":"`+username+`","password":"`+password+`"}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("login %s/%s status=%d body=%s", username, password, rr.Code, rr.Body.String())
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Token
}

// putAccount 以指定 token 调用 PUT /admin/api/account，返回响应记录器。
func putAccount(t *testing.T, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/account", body, token))
	return rr
}

func TestAdminPutAccountRequiresAuth(t *testing.T) {
	setupAccountTest(t)
	defer resetCfgForTest()
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodPut, "/admin/api/account",
		`{"current_password":"admin","password":"newpass456"}`, ""))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr.Code)
	}
}

// TestAdminPutAccountPassword：修改密码后旧密码失效、新密码可登录，且落盘。
func TestAdminPutAccountPassword(t *testing.T) {
	setupAccountTest(t)
	defer resetCfgForTest()

	if rr := putAccount(t, adminToken(t), `{"current_password":"admin","password":"newpass456"}`); rr.Code != http.StatusOK {
		t.Fatalf("change password status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !verifyLogin("admin", "newpass456") {
		t.Error("new password should be valid")
	}
	if verifyLogin("admin", "admin") {
		t.Error("old password should be invalid")
	}

	// 完整登录链路：新密码 200，旧密码 401
	for _, tc := range []struct {
		pw   string
		want int
	}{
		{"newpass456", http.StatusOK},
		{"admin", http.StatusUnauthorized},
	} {
		rr := httptest.NewRecorder()
		rootHandler(rr, httptest.NewRequest(http.MethodPost, "/login",
			strings.NewReader(`{"username":"admin","password":"`+tc.pw+`"}`)))
		if rr.Code != tc.want {
			t.Errorf("login with %q status=%d want %d", tc.pw, rr.Code, tc.want)
		}
	}

	// 已落盘 + 追加用户保留
	saved, err := loadPersistedAccounts(cfg.AccountsPath)
	if err != nil {
		t.Fatalf("load accounts: %v", err)
	}
	if saved.AdminPassword != "newpass456" {
		t.Errorf("persisted password=%q want newpass456", saved.AdminPassword)
	}
	if saved.AdminUsername != "admin" {
		t.Errorf("persisted username=%q want admin", saved.AdminUsername)
	}
	if saved.ExtraUsers == nil || saved.ExtraUsers["alice"] != "alicepass" {
		t.Errorf("persisted extra users=%v want alice preserved", saved.ExtraUsers)
	}
}

// TestAdminPutAccountUsername：修改用户名后旧用户/旧 token 失效，新用户可登录并访问管理 API。
func TestAdminPutAccountUsername(t *testing.T) {
	setupAccountTest(t)
	defer resetCfgForTest()

	oldTok := adminToken(t)
	if rr := putAccount(t, oldTok, `{"current_password":"admin","username":"boss"}`); rr.Code != http.StatusOK {
		t.Fatalf("change username status=%d body=%s", rr.Code, rr.Body.String())
	}
	if !verifyLogin("boss", "admin") {
		t.Error("new username should be valid")
	}
	if verifyLogin("admin", "admin") {
		t.Error("old username should be invalid")
	}
	if !isAdmin("boss") {
		t.Error("new username should be admin")
	}

	// 旧 token 在管理 API 上失效（token 里用户名还是 admin，已非管理员 → 403）
	rr := httptest.NewRecorder()
	rootHandler(rr, adminReq(http.MethodGet, "/admin/api/state", "", oldTok))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("old token status=%d want 403", rr.Code)
	}
	// 新用户名的 token 可正常访问
	tok2 := loginUser(t, "boss", "admin")
	rr2 := httptest.NewRecorder()
	rootHandler(rr2, adminReq(http.MethodGet, "/admin/api/state", "", tok2))
	if rr2.Code != http.StatusOK {
		t.Fatalf("new token status=%d want 200; body=%s", rr2.Code, rr2.Body.String())
	}

	saved, err := loadPersistedAccounts(cfg.AccountsPath)
	if err != nil {
		t.Fatalf("load accounts: %v", err)
	}
	if saved.AdminUsername != "boss" {
		t.Errorf("persisted username=%q want boss", saved.AdminUsername)
	}
}

// TestAdminPutAccountWrongPassword：当前密码错误一律拒绝且不改动。
func TestAdminPutAccountWrongPassword(t *testing.T) {
	setupAccountTest(t)
	defer resetCfgForTest()

	if rr := putAccount(t, adminToken(t), `{"current_password":"wrong","password":"newpass456"}`); rr.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d want 401", rr.Code)
	}
	if !verifyLogin("admin", "admin") {
		t.Error("original credentials should be unchanged")
	}
}

// TestAdminPutAccountValidation：缺字段 / 密码过短 / 用户名冲突 / 非法字符。
func TestAdminPutAccountValidation(t *testing.T) {
	setupAccountTest(t)
	defer resetCfgForTest()

	tok := adminToken(t)
	tests := []struct {
		name string
		body string
	}{
		{"两项都缺", `{"current_password":"admin"}`},
		{"密码过短", `{"current_password":"admin","password":"12345"}`},
		{"密码过长", `{"current_password":"admin","password":"` + strings.Repeat("x", 129) + `"}`},
		{"用户名与已有用户冲突", `{"current_password":"admin","username":"alice"}`},
		{"用户名含冒号", `{"current_password":"admin","username":"a:b"}`},
		{"用户名与当前相同", `{"current_password":"admin","username":"admin"}`},
	}
	for _, tc := range tests {
		if rr := putAccount(t, tok, tc.body); rr.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d want 400; body=%s", tc.name, rr.Code, rr.Body.String())
		}
	}
	if !verifyLogin("admin", "admin") {
		t.Error("credentials should be unchanged after rejected attempts")
	}
}

// TestAdminPutAccountEnvWarning：环境变量存在时响应带 warning。
func TestAdminPutAccountEnvWarning(t *testing.T) {
	t.Setenv("ADMIN_PASSWORD", "envpass")
	t.Setenv("ADMIN_USERNAME", "envadmin")
	t.Setenv("ACCOUNTS_PATH", filepath.Join(t.TempDir(), "accounts.json"))
	resetCfgForTest()
	defer resetCfgForTest()

	tok := loginUser(t, "envadmin", "envpass")
	var resp struct {
		Warning string `json:"warning"`
	}
	rr := putAccount(t, tok, `{"current_password":"envpass","username":"uiadmin","password":"newpass456"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Warning, "ADMIN_USERNAME") || !strings.Contains(resp.Warning, "ADMIN_PASSWORD") {
		t.Errorf("warning=%q want mention of both env vars", resp.Warning)
	}
	// 环境变量要重启才生效：内存里立刻是 UI 改的值
	if !verifyLogin("uiadmin", "newpass456") {
		t.Error("UI changes should take effect immediately")
	}
}
