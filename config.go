// 配置：来自环境变量（部署级参数）。业务配置（渠道/key/下游密钥）在 gateway.json，
// 由 WebUI / Admin API 管理。
package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config 进程级配置（环境变量）。
type Config struct {
	Port            string // 网关监听端口（/v1/* 转发）
	WebUIPort       string // 管理监听端口（WebUI/Admin API；空 = 与网关同端口合并监听）
	DataDir         string
	GWPath          string // gateway.json 路径
	AccountsPath    string
	TokenSecretPath string

	// 登录验证（WebUI / Admin API）
	LoginRequired bool
	AdminUser     string
	AdminPass     string
	ExtraUsers    map[string]string
	TokenTTL      time.Duration

	// 下游网关
	GWKeyAuth bool // 是否校验下游通用 key

	// 故障转移与冷却
	MaxRouteTries     int           // 单请求最多尝试的 key 数（0 = 全部）
	RateLimitCooldown time.Duration // 429 冷却（无 Retry-After 时）
	AuthFailCooldown  time.Duration // 401/403 冷却
	ServerErrCooldown time.Duration // 5xx 冷却
	NetErrCooldown    time.Duration // 网络错误冷却

	// 上游传输
	UpstreamHeaderTimeout time.Duration

	// 可观察性
	ReqLogSize         int
	UsageDBPath        string
	UsageRetentionDays int
	UsageMaxRecords    int
}

// defaultTokenSecret 进程内兜底签名密钥（无 TOKEN_SECRET 且密钥文件不可写时用）。
var defaultTokenSecret = randomHex(32)

var cfg Config

func init() {
	cfg = loadConfig()
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func loadConfig() Config {
	dataDir := getenv("DATA_DIR", "data")
	gwPath := getenv("GATEWAY_CONFIG_PATH", filepath.Join(dataDir, "gateway.json"))
	accountsPath := getenv("ACCOUNTS_PATH", filepath.Join(dataDir, "accounts.json"))
	tokenSecretPath := getenv("TOKEN_SECRET_PATH", filepath.Join(dataDir, "token-secret"))
	usageDBPath := getenv("USAGE_DB_PATH", filepath.Join(dataDir, "usage.db"))

	persisted, _ := loadPersistedAccounts(accountsPath)
	adminUser := persisted.AdminUsername
	if adminUser == "" {
		adminUser = "admin"
	}
	adminPass := persisted.AdminPassword
	if adminPass == "" {
		adminPass = "admin"
	}
	adminUser = getenv("ADMIN_USERNAME", adminUser)
	adminPass = getenv("ADMIN_PASSWORD", adminPass)

	extra := map[string]string{}
	for user, password := range persisted.ExtraUsers {
		extra[user] = password
	}
	for _, pair := range splitCSV(getenv("EXTRA_USERS", "")) {
		if u, p, ok := cutPair(pair); ok {
			extra[u] = p
		}
	}

	loginReq, _ := parseBoolEnv("LOGIN_REQUIRED", true)
	gwKeyAuth, _ := parseBoolEnv("GW_KEY_AUTH", true)

	return Config{
		Port:            getenv("PORT", defaultPort),
		WebUIPort:       strings.TrimSpace(os.Getenv("WEBUI_PORT")),
		DataDir:         dataDir,
		GWPath:          gwPath,
		AccountsPath:    accountsPath,
		TokenSecretPath: tokenSecretPath,

		LoginRequired: loginReq,
		AdminUser:     adminUser,
		AdminPass:     adminPass,
		ExtraUsers:    extra,
		TokenTTL:      durationEnv("TOKEN_TTL", 24*time.Hour),

		GWKeyAuth: gwKeyAuth,

		MaxRouteTries:     intEnv("MAX_ROUTE_TRIES", 0),
		RateLimitCooldown: durationEnv("RATE_LIMIT_COOLDOWN", time.Hour),
		AuthFailCooldown:  durationEnv("AUTH_FAIL_COOLDOWN", 10*time.Minute),
		ServerErrCooldown: durationEnv("SERVER_ERR_COOLDOWN", 30*time.Second),
		NetErrCooldown:    durationEnv("NET_ERR_COOLDOWN", 15*time.Second),

		UpstreamHeaderTimeout: durationEnv("UPSTREAM_HEADER_TIMEOUT", 10*time.Minute),

		ReqLogSize:         intEnv("REQ_LOG_SIZE", 1000),
		UsageDBPath:        usageDBPath,
		UsageRetentionDays: intEnv("USAGE_RETENTION_DAYS", 30),
		UsageMaxRecords:    intEnv("USAGE_MAX_RECORDS", 100000),
	}
}

// resetCfgForTest 测试辅助：重置全部运行时状态。
func resetCfgForTest() {
	cfg = loadConfig()
	cool = newCooldowns()
	leaseMgr = newLeaseManager()
	globalTransportCache = &transportCache{trs: map[string]*http.Transport{}}
	initStats()
	initUsageDB()
}

const defaultPort = "10010"

func parseBoolEnv(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, err
	}
	return b, nil
}

func intEnv(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("warning: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

func durationEnv(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("warning: invalid %s=%q, using default %s", key, v, def)
		return def
	}
	return d
}

// splitCSV 按逗号切分并去空白、去空项。
func splitCSV(s string) []string {
	raw := strings.Split(s, ",")
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// cutPair 把 "user:pass" 切成用户名与密码。
func cutPair(s string) (string, string, bool) {
	idx := strings.IndexByte(s, ':')
	if idx < 0 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}
