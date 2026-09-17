// 登录验证：POST /login 换取 token，受保护端点需带 Authorization: Bearer <token>。
// token 为 HMAC-SHA256 签名（payload 带用户名与过期时间），无状态可验证。
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type tokenClaims struct {
	User string `json:"u"`
	Exp  int64  `json:"exp"` // unix 秒
}

// verifyLogin 校验用户名密码：先查 EXTRA_USERS 追加用户，再查管理员（默认 admin/admin）。
func verifyLogin(username, password string) bool {
	if username == "" || password == "" {
		return false
	}
	if pw, ok := cfg.ExtraUsers[username]; ok {
		return subtleEqual(pw, password)
	}
	if subtleEqual(cfg.AdminUser, username) && subtleEqual(cfg.AdminPass, password) {
		return true
	}
	return false
}

// subtleEqual 常量时间字符串比较（防时序侧信道；长度不同提前返回，可接受）。
func subtleEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// issueToken 签发 HMAC 签名 token。
func issueToken(username string) (string, error) {
	claims := tokenClaims{User: username, Exp: time.Now().Add(cfg.TokenTTL).Unix()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret()))
	mac.Write([]byte(enc))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return enc + "." + sig, nil
}

// verifyToken 校验 token，返回用户名。
func verifyToken(token string) (string, error) {
	i := strings.IndexByte(token, '.')
	if i < 0 {
		return "", errors.New("malformed token")
	}
	enc, sig := token[:i], token[i+1:]
	mac := hmac.New(sha256.New, []byte(secret()))
	mac.Write([]byte(enc))
	want := mac.Sum(nil)
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, want) {
		return "", errors.New("invalid signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return "", errors.New("invalid payload")
	}
	var claims tokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", errors.New("invalid payload json")
	}
	if time.Now().Unix() > claims.Exp {
		return "", errors.New("token expired")
	}
	return claims.User, nil
}

// secret 返回签名密钥：优先 TOKEN_SECRET，否则进程内随机密钥。
// 文件密钥只读一次并缓存（密钥文件运行期不会变化；此前每次验证 token 都读
// 一次盘，高 QPS 下是无谓的磁盘 IO 放大）。
func secret() string {
	secretOnce.Do(func() {
		if s := getenv("TOKEN_SECRET", ""); s != "" {
			cachedSecret = s
			return
		}
		if s, err := loadOrCreateTokenSecret(cfg.TokenSecretPath); err == nil {
			cachedSecret = s
			return
		}
		cachedSecret = defaultTokenSecret
	})
	return cachedSecret
}

var (
	secretOnce   sync.Once
	cachedSecret string
)

// handleLogin 处理 POST /login。连续失败达阈值后按用户名/IP 锁定（429）。
func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), "bad_request")
		return
	}
	ip := clientIP(r)
	if locked, retryIn := loginBlocked(body.Username, ip); locked {
		log.Printf("login throttled: user=%q ip=%s retry_in=%s", body.Username, ip, retryIn.Round(time.Second))
		writeLoginThrottled(w, retryIn)
		return
	}
	if !verifyLogin(body.Username, body.Password) {
		loginFailed(body.Username, ip)
		log.Printf("login failed: user=%q ip=%s", body.Username, ip)
		writeJSONError(w, http.StatusUnauthorized, "invalid username or password", "unauthorized")
		return
	}
	loginSucceeded(body.Username, ip)
	token, err := issueToken(body.Username)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "token issue failed", "internal_error")
		return
	}
	resp := map[string]any{
		"token":      token,
		"token_type": "Bearer",
		"expires_in": int64(cfg.TokenTTL.Seconds()),
		"user":       body.Username,
	}
	writeJSON(w, http.StatusOK, resp)
}

// requireAuth 中间件：校验请求身份。LOGIN_REQUIRED=false 时放行。
// 已登录用户（token 验证通过）返回其用户名；未通过时写入 401 并返回 false。
func requireAuth(w http.ResponseWriter, r *http.Request) (string, bool) {
	if !cfg.LoginRequired {
		return "", true
	}
	token := bearerToken(r)
	if token == "" {
		writeJSONError(w, http.StatusUnauthorized, "missing bearer token", "unauthorized")
		return "", false
	}
	user, err := verifyToken(token)
	if err != nil {
		writeJSONError(w, http.StatusUnauthorized, "invalid or expired token", "unauthorized")
		return "", false
	}
	return user, true
}

// bearerToken 从 Authorization 头提取 Bearer token；兼容大小写不敏感的前缀。
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):])
	}
	return ""
}

// writeJSON 写 JSON 响应。
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "marshal failed", "internal_error")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
