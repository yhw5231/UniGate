# UniGate · 通用 AI 网关（原 cline2api-go）

一个 **OpenAI 兼容的多渠道 API 网关**：集中管理多个上游渠道的账号（key），每个 key
可绑定独立代理（固定 HTTP/SOCKS5 代理，或对接 [ipv6-proxy-pool](../ipv6-proxy-pool)
动态 IPv6 租约，代理池连接信息独立配置、渠道 key 只引用池并设置租约策略），对下游
签发通用 key，按「渠道顺序 → key 顺序」做**故障转移**转发，内置请求日志、用量统计
（SQLite）与 **WebUI**。

由 cline2api（Cline 反代）演进而来：保留了其登录鉴权、SSE `reasoning →
reasoning_content` 改写（现改为**渠道级开关**）、请求日志与用量库；移除了 Cline
专属的正向代理/端口池监听器，重构为通用网关。参考了 newapi / sub2api 的核心思路，
只保留基础必要功能。

纯 Go 标准库 + SQLite（modernc.org/sqlite，无 CGO），单二进制，WebUI 嵌入二进制。

## 功能总览

- **多渠道**：每个渠道一个 OpenAI 兼容上游（`BaseURL` + 多个账号 key）。
  渠道可配置自定义请求头（模拟特定客户端指纹）、静态模型列表、模型列表端点，
  并支持**自定义分组**（WebUI 按分组归类/过滤）。
- **模型列表**：渠道编辑页可**用渠道 key（含其代理）从上游拉取模型列表**，弹出候选
  清单**勾选要启用的模型**（默认勾选当前已启用项，可选全选/全不选，确定后写入列表）；
  带价格信息的上游会在编辑器里为输入/输出价格均为 0 的模型标注**免费**（仅展示标记，
  不额外存储）。手工添加且不在候选中的模型保留。Admin API `?replace=1` 仍保留全量
  替换旧行为。
- **每 key 独立代理**：
  - `直连`
  - `固定代理`：`http(s)://user:pass@host:port` 或 `socks5://...`
  - `IPv6 代理池`：连接信息（管理端 URL、Token、SOCKS5 地址）在「代理池」页**独立
    配置**，渠道 key 只**选择池**并设置租约行为；支持自动申请/绑定/释放/换 IP
    （详见下文）
- **下游通用 key**：`sk-gw-...`，客户端用它调用本网关；启用/停用即生效。
- **故障转移**：单请求内自动换下一个 key/渠道；按渠道冷却粒度（默认按 key，可选按
  (key, model)）跳过故障 key。
- **reasoning 改写**（渠道可选）：把上游 `reasoning` 复制为 `reasoning_content`，
  流式/非流式都支持（Cline 渠道需要，供 sub2api 等下游识别 thinking）。
- **请求日志**：仅记录**大模型网关接口**请求（`/v1/*`，如 chat/completions、models、
  responses；Admin/WebUI 等系统后台请求不记），环形缓冲保留最近 N 条，含接口/渠道/
  key/模型/token 数/状态/耗时/错误。**渠道测试请求例外**：无论成败都会写入请求
  记录留痕，`user` 为发起测试的管理员，`path` 为实际上游路径。
- **用量统计**：SQLite 逐条记录输入/输出 token，支持 今日/24h/7d/30d/全部 窗口，
  按下游 key、渠道、模型、上游 key 聚合。
- **WebUI**：渠道与 key 管理（支持关键字搜索 + 分组过滤）、通用密钥签发、日志、
  用量、代理池租约搜索、连通性测试、手动换 IP / 释放租约、单 key 连通性测试，
  以及**「渠道测试」独立页面**（按渠道/模型批量测试，逐项返回结果）。
- **版本标识**：后台登录页与顶栏显示构建版本号（`GET /api/version`），CI 按 Git
  tag（`v*`）注入，本地构建显示 `dev`；启动日志与上游请求 User-Agent 同样携带版本。

## 快速开始

```bash
go run .
# 网关 http://localhost:10080/v1/*，WebUI http://localhost:10070 ，默认 admin/admin（务必修改）
```

Docker（克隆仓库后在本地构建镜像并启动，无需宿主机安装 Go）：

```bash
git clone https://github.com/yhw5231/UniGate.git
cd UniGate
docker compose up -d --build
```

### WebUI 里配一个渠道的最小流程

1. 「渠道」→ 新建：填名称、`BaseURL`（如 `https://api.cline.bot/api/v1`），
   Cline 渠道勾选「reasoning→reasoning_content 改写」。
2. 渠道内「+ 添加 Key」填上游 key；需要代理的 key 选择代理类型：
   - 固定代理：填 URL；
   - IPv6 代理池：先在「代理池」页**新建池**（填管理端地址如 `http://1.2.3.4:8080`
     与 token（可空）），回到 key 的代理设置里**选择该池**；租约 ID 留空自动按
     `gw-<keyID>` 申请。池可用「测试」按钮验证连通性。
3. 「通用密钥」→ 生成密钥，复制给下游。
4. 下游以 OpenAI 兼容方式调用：

```bash
curl http://localhost:10080/v1/chat/completions \
  -H "Authorization: Bearer sk-gw-xxxx" \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}'
```

`GET /v1/models` 聚合所有启用渠道的模型列表（静态列表 ∪ 已拉取列表 ∪ models 端点
实时拉取，去重）。

## 部署说明

### 方式一：Docker Compose（推荐，本地构建部署）

容器部署即**本地重编译**：克隆仓库后由 Dockerfile 在容器内编译 Go 源码并打包镜像
（多阶段构建，构建阶段按目标架构自动交叉编译，支持 `linux/amd64` 与 `linux/arm64`，
无需 qemu 模拟）。镜像不在公共仓库发布，必须从源码构建。

```bash
# 1. 克隆仓库
git clone https://github.com/yhw5231/UniGate.git
cd UniGate

# 2. 构建镜像并启动
docker compose up -d --build

# 3. 查看日志确认启动成功
docker compose logs -f
# 出现 "gateway on :10080, webui on :10070" 即正常，Ctrl+C 退出跟踪
```

启动后浏览器打开 WebUI `http://<主机IP>:10070`（网关 API 在 `:10080`），默认账号
`admin` / `admin`，**生产环境务必通过环境变量修改管理员账号密码**（见下文安全清单）。
本地构建未注入版本号时，后台版本标识显示 `dev`（见上文「版本标识」）。

**更新版本**（拉取最新代码后重新构建、重建容器）：

```bash
./upgrade.sh                                  # 一键升级：git pull → 重新构建 → 重建容器 → 备份 data/ → 跟踪日志
# 或手动执行：
git pull && docker compose up -d --build
docker image prune -f    # 可选：清理悬空的旧镜像
```

常用运维命令：

```bash
docker compose restart          # 重启
docker compose down             # 停止（data/ 目录保留）
docker compose logs -f --tail=100
```

### 方式二：纯 Docker（不用 Compose）

先在仓库根目录构建镜像，再用构建产物运行：

```bash
git clone https://github.com/yhw5231/UniGate.git
cd UniGate
docker build -t unigate:local .

mkdir -p ./data

docker run -d --name unigate \
  --restart unless-stopped \
  -p 10080:10080 -p 10070:10070 \
  -v "$(pwd)/data:/data" \
  -e ADMIN_USERNAME=admin \
  -e ADMIN_PASSWORD=改成强密码 \
  unigate:local

# 数据目录属主想匹配宿主机某用户时，指定运行身份（可选）：
docker run -d --name unigate \
  --restart unless-stopped \
  -p 10080:10080 -p 10070:10070 \
  -v "$(pwd)/data:/data" \
  -e PUID=1000 -e PGID=1000 \
  unigate:local
```

> 说明：镜像基于 alpine。entrypoint 以 root 启动，会自动把 `/data` 属主修正为
> `PUID:PGID`（默认取镜像内 app 用户的 uid/gid，通常 100:100），随后立即降权为该身份
> 运行——因此**宿主机挂载目录属主任意均可直接部署**；进程实际不以 root 运行。
> 显式 `--user=<uid>:<gid>` 启动时跳过 chown，属主由调用方保证。
>
> 后续升级可用仓库自带的 `./upgrade.sh`（未用 compose 时自动走 `docker build` +
> `docker run` 路径），见下文「升级」。

### 常见问题（容器反复重启）

**启动日志报 `open /data/gateway.json.tmp: permission denied` 并循环重启**：数据目录
写入权限不足。容器 entrypoint 已自动 `chown /data`，一般不再出现；若仍遇到：

- 挂载了 NFS/SMB 等网络存储导致 root 无权 chown（root-squash）：改用 `-e PUID=<宿主UID> -e PGID=<宿主GID>` 匹配存储属主，或手动在宿主机 `chown -R 100:100 ./data`；
- 显式指定了 `--user`：改为以默认身份运行，或使 `--user` 与目录属主一致；
- 旧版镜像（无 entrypoint 自动 chown）：在宿主机执行 `chown -R 100:100 ./data` 后重新构建镜像并重建容器。

### 方式三：源码编译部署

前置要求：Go 1.26+（仅编译期需要，无需 CGO 与 gcc）。

```bash
# 编译（WebUI 已通过 go:embed 嵌入二进制，产物单文件即可运行）
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o unigate .

# 前台运行
DATA_DIR=./data ./unigate

# 或安装为 systemd 服务（Linux）
sudo useradd -r -s /usr/sbin/nologin unigate 2>/dev/null || true
sudo mkdir -p /opt/unigate && sudo cp unigate /opt/unigate/
sudo chown -R unigate:unigate /opt/unigate

sudo tee /etc/systemd/system/unigate.service <<'EOF'
[Unit]
Description=UniGate AI Gateway
After=network-online.target
Wants=network-online.target

[Service]
User=unigate
WorkingDirectory=/opt/unigate
ExecStart=/opt/unigate/unigate
Environment=PORT=10080
Environment=WEBUI_PORT=10070
Environment=DATA_DIR=/opt/unigate/data
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && sudo systemctl enable --now unigate
sudo systemctl status unigate        # 查看状态
journalctl -u unigate -f             # 跟踪日志
```

### 数据目录与持久化

所有状态集中在一个目录（容器内 `/data`，本地默认 `./data`），**部署时必须持久化
该目录**，否则重启后渠道配置、密钥、用量全部丢失：

| 文件 | 内容 |
| --- | --- |
| `gateway.json` | 渠道、上游 key 与代理配置、下游通用 key（WebUI 管理，原子写入） |
| `accounts.json` | 管理员与额外用户账号的持久化文件（可选；启动时读取，环境变量优先级更高，手动编辑可固定账号） |
| `token-secret` | 登录 token 签名密钥（首次启动自动生成；固定后重启不影响已登录状态） |
| `usage.db` | 用量统计 SQLite 数据库 |
| `lease-assignments.json` | 代理池「key→租约」分配表（保证重启后一号一 IP 不变） |

备份即备份该目录；迁移到新机器：停服 → 拷贝整个目录 → 启动，配置自动加载。
自定义路径可用 `DATA_DIR` 与 `GATEWAY_CONFIG_PATH` 等环境变量（见下文配置表）。

### 反向代理与 HTTPS

网关本身只提供 HTTP，生产环境建议套 Nginx / Caddy 提供 TLS。SSE 流式响应需关闭缓冲：

Nginx（网关转发，反代到 10080）：

```nginx
location / {
    proxy_pass http://127.0.0.1:10080;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_buffering off;        # SSE 流式必需
    proxy_cache off;
    proxy_read_timeout 600s;    # LLM 长响应
}
```

Caddy（自动 HTTPS，反代到 10080）：

```
gw.example.com {
    reverse_proxy 127.0.0.1:10080 {
        flush_interval -1       # SSE 流式必需
    }
}
```

WebUI/Admin（10070）不建议直接暴露公网；确需域名访问时同样按上面方式反代 10070。

### 验证与健康检查

```bash
# 模型列表（网关端口 10080；GW_KEY_AUTH=true 时需带下游通用 key）
curl http://127.0.0.1:10080/v1/models -H "Authorization: Bearer sk-gw-xxxx"

# 未配渠道时也可先登录验证服务可用（管理端口 10070，返回 token）
curl -X POST http://127.0.0.1:10070/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'

# 对话转发冒烟测试
curl http://127.0.0.1:10080/v1/chat/completions \
  -H "Authorization: Bearer sk-gw-xxxx" -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"ping"}]}'
```

容器可加健康检查（compose，探 WebUI 端口根路径）：

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://127.0.0.1:10070/"]
  interval: 30s
  timeout: 5s
  retries: 3
```

### 安全清单（生产部署必读）

- **修改默认管理员密码**：通过 `ADMIN_USERNAME`/`ADMIN_PASSWORD` 环境变量设置（或手动编辑 `data/accounts.json`）；
- **保持 `GW_KEY_AUTH=true`**（默认）：否则 `/v1/*` 完全开放，任何能访问端口的人都能消耗你的上游额度；
- **收紧网络暴露**：网关（10080）与 WebUI（10070）已默认分离——公网部署只暴露
  10080（务必套反代 + HTTPS + 防火墙白名单），10070 仅限内网/本机访问，compose
  映射写成 `127.0.0.1:10070:10070`；仅本机使用时网关映射同理改 `127.0.0.1:10080:10080`；
- **使用强下游 key**：WebUI 生成的 `sk-gw-...` 即为凭证，泄露后可停用再换发；
- **`EXTRA_USERS` 仅用于预留多用户登录**：额外用户可登录换取 token，但 WebUI 数据均经
  Admin API 拉取（仅主管理员可见），额外用户目前登录后看不到内容；
- 上游 key、代理凭证均明文存于 `gateway.json`，请确保数据目录权限（`chmod 600` 各文件或目录 `700`）。

### 升级

容器部署与源码部署都一样：**更新代码后必须重新编译再重启**——容器是本地构建的，
镜像里打包的是构建时的二进制，`git pull` 后不重建容器不会生效。

Compose 部署直接用仓库自带的一键升级脚本 `upgrade.sh`（拉代码 → 备份 `data/` →
重新构建镜像 → 重建容器 → 输出启动日志；非 compose 环境也能用，走 `docker build` +
`docker run` 路径）：

```bash
./upgrade.sh
```

或手动更新：

```bash
git pull && docker compose up -d --build       # Compose（本地构建）
git pull && docker build -t unigate:local . && docker rm -f unigate
# 然后按「方式二」的 docker run 命令用新镜像重建容器
# 或源码部署：git pull 重新 go build 后重启服务
```

- 数据格式向后兼容：旧 `gateway.json` / `usage.db` 会自动迁移（如用量库自动补
  `channel` 列），升级前照常保留 `data/` 即可；保险起见升级前备份一份。
- 破坏性变更仅存在于 cline2api → UniGate 那一次（见文末差异说明），此后均为常规升级。

## IPv6 代理池集成（ipv6-proxy-pool）

代理池（连接信息：管理端 URL、Token、SOCKS5 地址）作为**独立实体**在 WebUI「代理池」
页或 Admin API 统一配置；渠道 key 只保存池引用（`pool_id`）与租约行为（跨渠道复用、
换 IP 策略、常驻等），不重复填写连接信息。旧版本内联在 key 上的连接信息（`pool_url`/
`pool_token`/`socks_host`）在启动加载时自动迁移为代理池实体并按连接信息合并去重。

每个绑定代理池的 key 对应池子里一个**租约**（lease = 一个出口 IPv6 + SOCKS5 端口）。

**跨渠道复用**：key 代理配置开启 `share` 后，网关在本地维护一个共享代理池并自动做
「一号一 IP」分配（分配表持久化于 `data/lease-assignments.json`，重启不变、IP 稳定）：

- **同一渠道**（生效 BaseURL 相同，即上游同一站点）的不同 key **保证使用不同 IP**——
  同站多账号共用 IP 容易触发风控；
- **不同渠道**的 key **可以共用同一个 IP**——对每个上游来说依然是一个账号一个 IP，
  例如 A1,A2（渠道 A）与 B1,B2（渠道 B）只需 2 个 IP：A1+B1 一个、A2+B2 一个；
- 分配采用最优装填：新 key 优先复用「已承载分组数最多且不含本分组」的租约，
  池容量不够时才新申请；WebUI「代理池」页可见每个租约占用的分组；
- 共享租约换 IP 时所有使用方一起切换；key 删除/解绑后其租约在最后一个使用方
  消失时才释放（`Reconcile` 随渠道配置变更自动执行）；
- 显式配置 `lease_id` 时跳过自动分配（手动绑定，约束由用户自担）。

| 动作 | 行为 |
|---|---|
| 申请/绑定 | 该 key 首次使用时调 `POST /v1/leases`（幂等，同 ID 复用现有租约）；网关重启无需恢复 |
| 使用 | 请求经池子的 SOCKS5 出口转发（自动探测 `per_ipv6` / `multiplex` 模式） |
| 换 IP | `POST /v1/leases/{id}/rotate`。自动触发：网络失败、上游返回指定状态码（如 403/429）、按时间间隔、按请求次数 |
| 释放 | `DELETE /v1/leases/{id}`。手动（WebUI/Admin API）或删除/改绑 key 时自动释放不再使用的租约 |

- `per_ipv6` 模式：每个租约独立 SOCKS5 端口，SOCKS 地址默认取池管理端同机（可在
  代理池实体上用 `socks_host` 覆盖）。
- `multiplex` 模式：共用池基础端口，网关自动以 `user:<租约ID>` 作为 SOCKS5 用户名。

## 故障转移与冷却策略

候选顺序 = 渠道在配置中的顺序（即优先级）→ 渠道内 key 顺序。单请求最多尝试
`MAX_ROUTE_TRIES` 个候选（默认全部）。命中冷却的候选直接跳过：

| 故障 | 冷却时长（环境变量） | 默认 |
|---|---|---|
| 上游 429 | `Retry-After` 优先，否则 `RATE_LIMIT_COOLDOWN` | 1h |
| 上游 401/403（鉴权失败） | `AUTH_FAIL_COOLDOWN` | 10m |
| 上游 5xx | `SERVER_ERR_COOLDOWN` | 30s |
| 网络/代理错误 | `NET_ERR_COOLDOWN` | 15s |
| 全部候选失败 | 有 429 记录则返回 429 + `Retry-After`，否则 502 | |

冷却粒度按渠道配置（`cooldown_scope`）：默认**按 key 跨模型共享**——某模型故障即冷停
该 key 的全部模型（适合 key 配额共享的上游）；渠道可选 `key_model` 按 `(key, model)`
独立记录——同一账号不同模型的额度互不影响。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `10080` | 网关监听端口（`/v1/*` 转发接口） |
| `WEBUI_PORT` | `10070` | WebUI/Admin API 独立监听端口；显式置空 = 与 `PORT` 合并单端口 |
| `DATA_DIR` | `data`（容器内 `/data`） | 数据目录，**容器部署必须挂载** |
| `GATEWAY_CONFIG_PATH` | `${DATA_DIR}/gateway.json` | 渠道/密钥配置文件（WebUI 管理） |
| `LEASE_ASSIGN_PATH` | `${DATA_DIR}/lease-assignments.json` | 代理池「key→租约」分配表（跨渠道复用，持久化保证 IP 稳定） |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | `admin` / `admin` | WebUI 管理员账号；Admin API 仅此账号可用 |
| `EXTRA_USERS` | 空 | 额外 WebUI 登录用户，`user:pass,user2:pass2`（仅登录 WebUI，无 Admin API 权限） |
| `TOKEN_TTL` | `24h` | 登录 token 有效期 |
| `TOKEN_SECRET` | 空 | 登录 token 签名密钥（缺省自动持久化到 data/token-secret） |
| `GW_KEY_AUTH` | `true` | 下游是否必须携带通用 key |
| `MAX_ROUTE_TRIES` | 0（全部） | 单请求最多尝试的 key 数 |
| `RATE_LIMIT_COOLDOWN` | `1h` | 429 冷却（无 `Retry-After` 时；有则优先） |
| `AUTH_FAIL_COOLDOWN` | `10m` | 401/403 冷却 |
| `SERVER_ERR_COOLDOWN` | `30s` | 5xx 冷却 |
| `NET_ERR_COOLDOWN` | `15s` | 网络错误冷却 |
| `UPSTREAM_HEADER_TIMEOUT` | `10m` | 等待上游响应头超时（LLM 非流式可能较慢，勿设过小） |
| `REQ_LOG_SIZE` | `1000` | 请求日志环形缓冲容量 |
| `USAGE_DB_PATH` | `${DATA_DIR}/usage.db` | 用量 SQLite 路径（空 = 纯内存） |
| `USAGE_RETENTION_DAYS` | `30` | 用量保留天数 |
| `USAGE_MAX_RECORDS` | `100000` | 用量最大条数 |

## Admin API（需管理员登录 token）

`POST /login` 换 token 后调用（`Authorization: Bearer <token>`）：

| 端点 | 说明 |
| --- | --- |
| `GET /admin/api/state` | 渠道、下游 key、代理池列表、租约缓存总览 |
| `PUT /admin/api/channels` | 新增/整体更新渠道（含内嵌 keys） |
| `POST /admin/api/channels/{id}/fetch-models` | 用渠道 key 拉取上游模型列表，默认 dry-run 返回候选（`fetched`/`free_models`/`enabled`）供 WebUI 勾选启用，不写回渠道；`?replace=1` 全量替换写回（兼容旧脚本）；免费清单仅随响应展示，不持久化 |
| `DELETE /admin/api/channels/{id}` | 删除渠道（自动释放其池租约） |
| `PUT /admin/api/pools` | 新增/更新代理池（连接信息：`{name, pool_url, pool_token?, socks_host?}`；被渠道 key 引用的池不可删除） |
| `DELETE /admin/api/pools/{id}` | 删除代理池（仍被引用时返回 400，成功后释放其遗留租约） |
| `PUT /admin/api/gwkeys` | 新增/更新下游 key（key 留空自动生成） |
| `DELETE /admin/api/gwkeys/{id}` | 删除下游 key |
| `POST /admin/api/pool/test` | 测试池子连通性 `{pool_id}` 或旧格式 `{pool_url, pool_token}` |
| `POST /admin/api/pool/rotate` | 手动换 IP `{channel_id, key_id}` 或 `{pool_url, lease_id}`（直连本地代理池条目） |
| `POST /admin/api/pool/release` | 手动释放租约 `{channel_id, key_id}` 或 `{pool_url, lease_id}` |
| `GET /admin/api/pool/leases` | 网关持有的租约列表 |
| `POST /admin/api/testkey` | 用指定 key 发一条测试请求 `{channel_id, key_id, model?}` |
| `POST /admin/api/channels/{id}/test-model` | 渠道级批量测试 `{key_id?, first_only?, models?}`（models 每行/逗号分隔，空 = 渠道已启用模型）；逐 key × 逐模型执行，HTTP 层不报错，成败均通过结果项的 `ok` 表达，且每次测试（含失败）都写入请求记录 |
| `GET /admin/api/requests?limit=` | 最近请求日志 |
| `GET /admin/api/usage?window=today\|24h\|7d\|30d\|all` | 用量统计（支持 `?user=&channel=&model=&key=`） |

## 渠道自定义请求头示例（Cline 渠道）

渠道级 headers（JSON）可模拟任意客户端指纹；Cline 渠道建议：

```json
{
  "http-referer": "https://cline.bot",
  "x-title": "Cline",
  "User-Agent": "Cline/4.1.16",
  "x-core-version": "4.1.16",
  "x-platform-version": "1.106.0",
  "x-client-version": "4.1.16",
  "x-platform": "vscode",
  "x-client-type": "cline-vscode"
}
```

## 测试

```bash
go test ./...
```

覆盖：配置存储、池客户端（申请幂等/换IP/释放/两种 SOCKS 模式/跨渠道复用分配约束
[同组互斥、跨组共享、最优装填]、分配持久化与 Reconcile 回收）、路由故障转移
（429/401/网络错误/冷却/模型过滤）、ipv6pool 端到端（真实 SOCKS5 stub 隧道 +
状态码触发换 IP）、下游鉴权、模型聚合、Admin API、用量库。

## 与原 cline2api 的差异（破坏性变更）

- 移除：`PROXY_PORT`/`SOCKS_PORT` 正向代理监听器、`PROXY_POOL_*` 端口池监听器、
  `UPSTREAM_URL`/`UPSTREAM_KEYS`/`KEY_SELECT_MODE`/`CLIENT_*` 全局环境变量配置
  （对应能力全部移入 WebUI 的渠道/代理配置）。
- `POST /`（根路径聊天转发）不再提供，统一走 `/v1/chat/completions`。
- 下游鉴权从登录 token 改为通用 key（`GW_KEY_AUTH=false` 可关闭校验）。
- reasoning 改写从全局行为改为渠道级开关（`rewrite_reasoning`）。
- 用量库增加 `channel` 维度（旧库自动迁移，兼容升级）。
