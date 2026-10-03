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
- **模型列表**（对标 go-gateway 的模型选择器）：渠道编辑页的模型区是**一行一个模型**的
  勾选列表——`[✓] 对外名  [上游模型名]`，**勾选=启用**，行内「上游模型名」留空 = 与对外名
  相同（填上游真正认识的写法即可，含**与上游名无关的渠道别名**，如自定义行 `my-gpt` →
  `upstream/model-x`）；工具栏有**获取模型**、**筛选**、**已选/对外名计数**、以及**手动添加
  自定义模型**（进「自定义」分组）。列表**按分组显示**：`免费` / 上游给的分组（Cline 的
  `recommended` / `clinePass` / `clineCloud` …）/ `自定义` / `已保存`（上游本次未返回的旧模型，
  标「上游未返回」徽标），每组有**全选 / 全不选**，点组名可折叠。**免费标识按模型判定**：
  免费模型行带绿色 `免费` 徽标，整组免费的分组头标 `免费`、只含部分免费的分组头标 `含免费 N`
  （不会因为组里有免费模型就把整组都标成免费）。
  - **获取模型**：用渠道 key（含其代理）请求上游模型列表，候选按上游分组铺进列表，
    **已生效的上游写法预勾选**（渠道未声明模型 = 对全部放行 → 全选），新增行不勾选；
    勾选/取消即生效，勾完直接点「保存」即可。免费模型（分组名含 `free`、`:free`/`-free`
    后缀、或输入输出价格均为 0）标「免费」徽标（仅展示，不额外存储；重新打开编辑器时
    上游写法自带 `:free`/`-free` 的行仍标免费）。
  - **拉取并直接重建**：重新拉取上游模型列表，**忽略当前勾选**，以上游返回的全部模型
    重建列表与映射并立即保存（上游已下线的残留模型及其映射一并清除）。
  - 模型列表端点（`/models`，可用 `models_url` 覆盖）的响应**兼容多种形态**：OpenAI 的
    `{"data":[…]} `、裸数组（对象或字符串）、`models`/`result`/`items`/`list` 包装字段、
    `data` 内再嵌一层分组对象，以及**按分组返回的 JSON**——Cline 的
    `/api/v1/ai/cline/recommended-models` 就是这种形态（`recommended` / `free` /
    `clinePass` / `clineCloud`，**字段名即分组名**，元素含 `id`/`name`/`description`/`tags`）。
    分组顺序按响应原文保留。
  - **编辑器里的按钮都以当前填写内容为准**（`POST /admin/api/fetch-models` 内联渠道拉取、
    `/admin/api/testkey` 内联测试、`/admin/api/pool/{rotate,release}` 内联代理），新建渠道
    不必先保存即可拉模型 / 测试 key / 换 IP；点「保存」成功后编辑弹窗自动关闭。
  - Admin API：`?replace=1` 走上面的「拉取并直接重建」语义（列表存对外名、上游写法写进
    `model_map`，并丢弃已不在列表里的旧映射键）；dry-run 响应返回 `fetched`（上游写法）、
    `groups`（`{name, free, free_total, models, total}`，按上游分组；`free` = 该组含免费模型、`free_total` = 该组免费模型数）、`free_models`、
    `enabled_upstream`（候选里当前已生效的写法，供预勾选）与 `stale`（上游已不再返回、
    重建将移除的旧模型）。
- **模型名称处理**（对标 go-gateway 的 pattern / ModelMapping / source_model）：
  - **模型列表支持模式**：每项可为精确名、通配（`claude-*` / `gpt-4?`）或正则（`re:^gpt-4.*$`）；
  - **对外名归一化**：保存渠道（含拉取模型）时，精确名统一归一化为**对外名**——小写、去掉
    供应商前缀（`cline-free/x` → `x`）与变体后缀（`x:free` / `x-free` → `x`）——原写法自动
    记进下面的**渠道级名称映射**。于是路由页、`/v1/models`、渠道模型列表只出现干净名字
    （不会带 `cline-free/`、`:free` 之类前缀后缀），而发往上游的仍是**上游自己的写法**
    （对标 go-gateway：路由用规范名，渠道记 `source_model`）。通配/正则模式原样保留，
    不参与归一化；**历史配置加载时自动迁移**（一次性归一化并写回）。
  - **同一对外名的多个上游写法 = 多个候选**：上游把同一个模型报了多种写法（如 `x` 与
    `x:free`、`DeepSeek-V4.1-Flash` 与 `cline-free/DeepSeek-V4.1-Flash:free`）时，这些写法
    **各自是一个转发候选**：请求按顺序故障转移、**冷却各自独立计算**（同一个渠道内也分别
    计算），而对外始终只暴露一个名字（`/v1/models` 与路由页的分组名都去重为一个）。
    路由页会为每个上游写法单独列一行并标注实际发往上游的名字。
  - **归一化匹配**：精确名按归一化后比较——上游报 `cline-free/deepseek-v4.1-flash:free`、
    下游写 `deepseek-v4.1-flash` 命中同一渠道，并按上游原写法改写请求体的 `model` 字段；
  - **渠道级名称映射**（`model_map`）：`下游模型名=上游模型名`，把同一模型在各上游的不同叫法
    映射到该上游实际使用的名字（JSON 与 multipart 表单请求都改写）。**一个下游模型名可以
    对应上游的多个模型**（多个上游名用逗号分隔，如
    `deepseek-v4.1-flash=cn:deepseek-v4.1-flash,global:deepseek-v4.1-flash`）：路由时每个
    上游模型各成一个候选，转发时分别改写 `model`，**冷却也按上游模型分别计算**（同一 key 下
    `cn:` 线路被限流不会牵连 `global:`）。映射的**键**（下游名）即该渠道「声明支持」的模型，
    无需再写进模型列表，会一并出现在 `/v1/models` 与路由页（编辑页打开时映射里的每个上游写法
    都会铺成一行，保存时归一化进模型列表；路由页为每个上游模型
    单独列一行，标注实际发往上游的名字）；映射的**目标是上游真正认识的写法**，所以按字面
    判重（`x=cline-free/x:free,x` 保留两个候选，而 `x=x` 这种只映射到自身的不产生改写）；
  - **全局模型别名**（设置页 `model_aliases`）：`别名=规范模型名`，下游用别名调用即按规范名
    路由（再按渠道映射改写上游名）；别名出现在 `/v1/models`，路由页为其单独列出分组。
    **渠道级映射优先于全局别名**：渠道对某个下游名配了映射时，该渠道按渠道映射发往上游，
    不受全局别名影响（同一别名在不同渠道可以落到各自不同的上游模型）。
- **下游通用 key**：`sk-gw-...`，客户端用它调用本网关；启用/停用即生效。每个 key 可配置
  **可用模型白名单**（支持通配/正则，留空不限）：不在白名单的请求返回 403，
  `/v1/models` 按白名单过滤。
- **每 key 独立代理**：
  - `直连`
  - `固定代理`：`http(s)://user:pass@host:port` 或 `socks5://...`
  - `IPv6 代理池`：连接信息（管理端 URL、Token、SOCKS5 地址）在「代理池」页**独立
    配置**，渠道 key 只**选择池**并设置租约行为；支持自动申请/绑定/释放/换 IP
    （详见下文）
- **下游通用 key**：`sk-gw-...`，客户端用它调用本网关；启用/停用即生效。
- **账号调度**：按渠道可选**故障转移**（默认，按 key 顺序靠前的用满才换）或**顺序轮询**
  （每次请求从下一个 key 开始轮流分配，均摊账号用量）；未显式配置的渠道跟随全局默认
  （WebUI「设置」/「路由」页或 `DEFAULT_SCHEDULE` 环境变量）。**「路由」页按
  「模型 → 渠道（优先级 / 权限 / 分组 / 调度）→ 映射出的真实上游模型名 → key」逐级展开**：
  模型头显示可用/冷却/候选数与渠道数（点击展开），渠道头显示优先级、权重、启用状态与
  账号调度（点击可单独收起），上游模型节点标出该候选实际发往的名字（「映射」/「直发」/
  「改名」徽标），key 行显示 可用 / 冷却中剩多久 / 停用 并带**网关实际尝试顺序序号 `#n`**
  （分层展示后仍能看出下一个请求会用谁）。支持逐 (key, 模型) 精确解除冷却、
  按模型清该模型在所有渠道所有 key 的冷却，以及一键清空全部冷却。
  **已停用的渠道整体不参与路由**：它的模型不出现在路由页（模型分组与候选行都跳过）、
  也不出现在 `/v1/models`，与转发时的候选选择保持一致——停用即摘除，而不是显示成「已停用」。
- **故障转移与熔断**：单请求内自动换下一个 key/渠道；按渠道冷却粒度（默认按 key，可选按
  (key, model)）跳过故障 key。除 429 冷却外，**网络/代理错误与 5xx 连续失败达阈值即按
  指数退避冷却该 key**（熔断，设置页可调阈值/基础与最大冷却/倍数，成功即清零并解除）。
- **固定模型渠道**（渠道级 `model_pins`）：上游网关的一个模型可能内置多个上游
  渠道（如 Cline Pass 的 glm 背后随机路由到 alibaba/baseten 等）。为渠道的指定模型
  固定内部渠道：请求时注入 `provider.only/order`（OpenRouter 型）或
  `providerOptions.gateway.*`（AI Gateway 型），不再让上游随机路由；支持**探测**
  （自动发现模型背后的全部内部渠道与管线类型）与**逐渠道验证**（详见
  「故障转移与冷却策略」）。
- **账号自动探测**（渠道可选）：开启后网关定时向上游发一道**随机 5 位数加法题**
  验证账号状态——**进程启动（重启/重新部署）时**对状态未知的账号核对一次（跳过上游
  明确冷却中与最近 2 小时成功调用过的），key **冷却恢复时**探测一次（按 (key, model)
  冷却的渠道探测恢复的那个模型；上游仍 429 则按其明确到期时间重新冷却），正常状态
  **连续无调用达到空闲探测间隔**（设置页可配，默认 2 小时）再探测一次（按 (key, model)
  冷却的渠道逐模型检查，只探测连续未使用的模型；停用/冷却中的账号不探测）；结果写入
  请求日志（用户列显示「探测」，存 `user=probe`），不计入用量统计。
- **reasoning 改写**（渠道可选）：把上游 `reasoning` 复制为 `reasoning_content`，
  流式/非流式都支持（Cline 渠道需要，供 sub2api 等下游识别 thinking）。
- **渠道级代理**：渠道可统一设置代理（固定代理或 IPv6 代理池），未单独配置代理的
  key 全部继承——同一渠道共用同一套池设置，但**每个 key 仍是独立租约/出口 IP**
  （同设置不同 IP）；key 块内可选择「跟随渠道」（默认）或单独覆盖为直连/固定代理/其它池。
- **请求日志**：仅记录**大模型网关接口**请求（`/v1/*`，如 chat/completions、models、
  responses；Admin/WebUI 等系统后台请求不记），**持久化在 `usage.db` 中**（重启不丢），
  每张表保留最近 `REQ_LOG_SIZE` 条，含接口/渠道/key/模型/token 数/状态/耗时/错误，
  WebUI 分页展示并支持**关键字过滤当前页**与**点击行展开全部字段**；**行默认收起**，
  错误列只显示单行摘要、超长失败轨迹省略号截断（悬停看全文），展开后才展示完整详情。
  **渠道测试请求例外**：
  无论成败都会写入请求记录留痕，`user` 为发起测试的管理员，`path` 为实际上游路径。
- **错误日志**：失败请求（状态 ≥400 或带失败原因）**单独存一张表**，不会被海量成功
  请求挤出，便于事后排查；错误信息含**逐 key 失败轨迹**（哪个 key 因什么失败、
  冷却跳过/穿透试探），同样的轨迹也会写进下游 502/429 错误体；**错误记录自动携带
  「请求内容 + 返回内容」**（各截断 4KB/8KB，WebUI 展开详情可见，发出去什么、上游回了
  什么一目了然），保留上限 = **条数**（`ERR_LOG_SIZE`，超出自动清理最旧）与**天数**
  （`ERR_LOG_RETENTION_DAYS`，默认 7，超期自动清理）双保险，可在 WebUI「设置 → 日志与错误」调整天数。
- **用量统计**：SQLite 逐条记录输入/输出 token，支持 今日/24h/7d/30d/全部 窗口，
  按下游 key、渠道、模型、上游 key 聚合（key 列存「名称@渠道」标签，非凭证，见下）。
- **WebUI**：渠道与 key 管理（支持关键字搜索 + 分组过滤；渠道/路由页卡片**默认收起详情、点击展开**）、通用密钥签发、日志
  （列宽自适应、行默认收起、错误列单行省略号截断、行内展开完整详情、当前页关键字过滤、
  自动刷新、默认每页 50 条）、
  用量、代理池租约搜索、连通性测试、手动换 IP / 释放租约、单 key 连通性测试，
  以及**「渠道测试」独立页面**（按渠道/模型批量测试，模型清单可点击切换加入/移除、
  可**一键清空**并实时显示待测模型条数，逐项返回结果；「渠道 / key」列同时给出渠道名与
  key 名，便于分辨是哪个渠道在报错）；弹窗支持 Esc 关闭，**点击遮罩不会关闭**（弹窗里常已填了不少内容，
  防误点丢数据；用右上角 × 或底部「取消 / 关闭」按钮退出），保存成功后编辑弹窗自动
  关闭，标题栏滚动时固定；**WebUI 内所有
  时间统一显示北京时间**（与浏览器/服务器时区无关）。渠道编辑弹窗宽度调高，
  多 key 列表内部滚动，便于大量 key 时的编辑操作。
- **版本标识**：后台登录页与顶栏显示构建版本号（`GET /api/version`），构建时自动
  从 `.git` 推导——有 tag 显示 tag（如 `v1.2.3`），无 tag 显示短提交哈希（如 `6a53e28`），
  可用构建参数 `VERSION=xxx` 强制覆盖；本地 `go build` 在 git 仓库内同样显示短提交
  哈希（有未提交改动加 `-dirty` 后缀）；启动日志与上游请求 User-Agent 同样携带版本。

## 快速开始

```bash
go run .
# 网关与 WebUI 同端口：http://localhost:10010 ，默认 admin/admin（务必修改）
```

Docker（克隆仓库后在本地构建镜像并启动，无需宿主机安装 Go）：

```bash
git clone https://github.com/yhw5231/UniGate.git
cd UniGate
docker compose up -d --build
```

### WebUI 里配一个渠道的最小流程

界面外观：顶栏右侧「🌙 夜间 / ☀ 日间」按钮切换**日间/夜间模式**（登录页右下角有同样的
悬浮按钮）。选择存在浏览器 `localStorage`（`unigate_theme`）里、下次打开直接生效；没手动
选过时跟随系统偏好（`prefers-color-scheme`），系统切换会跟着变，手动选过就以你的选择为准。
两套配色是同一组 CSS 变量，正文对比度均 ≥ 4.5:1（WCAG AA）。

1. 「渠道」→ 新建：填名称、`BaseURL`（如 `https://api.cline.bot/api/v1`）。
   `BaseURL` 是 **OpenAI 兼容前缀，默认自适应补全版本段**：只填站点根
   （`https://api.deepseek.com`）或自定义前缀（`https://host/openai`）会自动补成
   `…/v1`；已含版本段（`/v1`、`/api/v1`、`/v4`、`/v1beta`…）或已指向具体端点
   （`/chat/completions`、`/responses`、`/models`…）时原样使用，不会补成 `/v1/v1`。
   Cline 渠道勾选「reasoning→reasoning_content 改写」。
2. 渠道内「+ 添加 Key」填上游 key；需要代理的 key 选择代理类型：
   - 固定代理：填 URL；
   - IPv6 代理池：先在「代理池」页**新建池**（填管理端地址如 `http://1.2.3.4:8080`
     与 token（可空）），回到 key 的代理设置里**选择该池**；租约 ID 留空自动按
     `gw-<keyID>` 申请。池可用「测试」按钮验证连通性。
   > 新建渠道时编辑器会自动补一个空的 key 行；用**批量导入**粘贴 key 时这类**完全空白**
   > 的行（无名称、无 API Key、代理跟随渠道）会被自动清掉，保存时也不写进配置。若渠道里
   > 残留了这样的空行，它会排在被测/被路由的第一个 key 位置、发不出 `Authorization` 头，
   > 上游回 `401 Header of type \`authorization\` was missing`，看起来像「网关没带认证」。
   > **渠道测试**会跳过这些空白占位行（结果区提示「已跳过 N 个空白占位 key」）；无鉴权
   > 渠道只有一个空白 key 行时照常测试，并在 401/403 时直接点明「该 key 未配置 API Key」。
3. 点「**获取模型**」：上游模型按分组铺进列表（`免费` / `recommended` / `clinePass` /
   `clineCloud` …，每行 = `[✓] 对外名  [上游模型名]`），**默认勾选当前已生效的上游写法**
   （渠道未声明模型 = 对全部放行时全选），可用**筛选框**按关键字过滤、用工具栏或组头的
   **全选 / 全不选**批量勾选、点组名折叠分组；**勾选即启用**（勾完直接「保存」即可；
   同一对外名勾了多个写法时，请求按顺序尝试、冷却分别计算）。
   **所见即所存**：保存的就是勾选状态——状态栏实时显示「已勾选 N 行 · 保存后启用 K 个
   对外名」，筛选/折叠都**不会藏起已勾选的行**（出现过「看不见的行仍被启用」）；只有
   勾选框与对外名能切换勾选（点「上游模型名」输入框只改上游名，不会误切换）。
   **一个都不勾 = 不限制模型**（该渠道对全部模型放行，上游返回的全部模型都会进路由页与
   `/v1/models`），保存前会再确认一次，保存后的提示也直接说明启用了几个模型、是哪些。
   想顺手清理上游已下线的残留模型时，点「**拉取并直接重建**」：重新拉取并**忽略当前勾选**，
   以上游返回的全部模型重建列表与映射并立即保存（旧模型及其映射一并清除）。
   手工补充模型用工具栏右侧的「添加自定义模型名」（进「自定义」分组）。
   **拉取、测试、换IP 等按钮都以弹窗里当前填写的内容为准**（Base URL / key / 代理 /
   模型清单），新建渠道**不必先保存**就能用；点「保存」成功后弹窗自动关闭。
4. 「通用密钥」→ 生成密钥，复制给下游（可在该页为密钥限定可用模型白名单）。
5. 下游以 OpenAI 兼容方式调用：

```bash
curl http://localhost:10010/v1/chat/completions \
  -H "Authorization: Bearer sk-gw-xxxx" \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}'
```

`GET /v1/models` 聚合所有启用渠道的模型列表（静态列表 ∪ 已拉取列表 ∪ models 端点
实时拉取，去重），并附上设置页配置的**全局模型别名**；下游 key 配置了模型白名单时
只返回其允许的模型。**暴露的名字统一为对外名**（小写、去供应商前缀与 `:free`/`-free`
类变体后缀）：同一个模型在上游的多种写法合并成一个对外名，上游原写法只用于发往上游
（见渠道模型映射），不会出现在 `/v1/models` 与路由页分组名里；
也可在渠道模型列表里直接写通配（`claude-*`）或正则（`re:^gpt-4.*$`）。
**已停用的渠道不参与**：它的模型不出现在 `/v1/models`（转发与路由页同理）。

除对话外，网关还支持 OpenAI 兼容的**非对话端点**，路由 / 故障转移 / 冷却 / 用量
记账与 chat/completions 完全一致（按请求体的 `model` 字段匹配渠道模型列表，
`{BaseURL}` 为渠道 `BaseURL`，key 单独配置 `BaseURL` 时优先；两者都按上面的规则
自适应补全版本段）：

| 网关端点 | 上游路径 |
| --- | --- |
| `POST /v1/embeddings` | `{BaseURL}/embeddings` |
| `POST /v1/images/generations` | `{BaseURL}/images/generations` |
| `POST /v1/images/edits`（multipart 或 JSON） | `{BaseURL}/images/edits` |
| `POST /v1/images/variations` | `{BaseURL}/images/variations` |
| `POST /v1/videos/generations` | `{BaseURL}/videos/generations` |

multipart 请求（图片 edits / variations）连同 Content-Type（含 boundary）原样
透传上游，`model` 从表单字段提取。embedding 模型（如 `text-embedding-3-small`）、
图片生成模型（如 `dall-e-3`、`gpt-image-1`、`flux`）、视频生成模型（如 `kling-v1`）
与对话模型一样声明在渠道的模型列表里即可参与路由；自动探测会跳过明显非对话的
模型名。

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
# 出现 "unigate listening on :10010 (gateway+webui ...)" 即正常，Ctrl+C 退出跟踪
```

启动后浏览器打开 `http://<主机IP>:10010`（网关 API 与 WebUI 同端口），默认账号
`admin` / `admin`，**生产环境务必通过环境变量修改管理员账号密码**（见下文安全清单）。
WebUI 登录页与顶栏的版本标识即当前代码的仓库版本（tag 或短提交哈希，见上文「版本标识」）。

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
  -p 10010:10010 \
  -v "$(pwd)/data:/data" \
  -e ADMIN_USERNAME=admin \
  -e ADMIN_PASSWORD=改成强密码 \
  unigate:local

# 数据目录属主想匹配宿主机某用户时，指定运行身份（可选）：
docker run -d --name unigate \
  --restart unless-stopped \
  -p 10010:10010 \
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
Environment=PORT=10010
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
| `usage.db` | 用量统计 + 请求/错误日志 SQLite 数据库（请求/错误日志持久化，重启不丢）。运行在 **WAL + synchronous=NORMAL** 下：提交只追加 WAL 而不每次 fsync，每个请求的两笔写入（请求日志 + 用量）不再被磁盘串行化；同目录会出现 `usage.db-wal`/`usage.db-shm`（属正常现象，最近写入暂存在 WAL 里，下次启动自动恢复合并）。**备份/迁移请停服后拷贝整个数据目录**，只拷 `usage.db` 会丢掉仍在 WAL 中的最新记录。库暂时不可用（磁盘/权限/被占用/目录不存在）时**不阻断启动**：遥测降级为空操作并在日志里报错（`usage db: open ...`），随后每次读写按 5s 退避自动重开，条件恢复后继续记账（`usage db: reopened ...`），不需要重启进程 |
| `lease-assignments.json` | 代理池「key→租约」分配表（保证重启后一号一 IP 不变） |
| `cooldowns.json` | 429 冷却状态（key/model → 重试到期时间；重启、重新部署后自动恢复，不会因重启清零而立刻冲击限流中的上游账号） |

备份即备份该目录；迁移到新机器：停服 → 拷贝整个目录 → 启动，配置自动加载。
自定义路径可用 `DATA_DIR` 与 `GATEWAY_CONFIG_PATH` 等环境变量（见下文配置表）。

### 反向代理与 HTTPS

网关本身只提供 HTTP，生产环境建议套 Nginx / Caddy 提供 TLS。SSE 流式响应需关闭缓冲。
套反代后真实客户端 IP 自动生效（`TRUST_PROXY_HEADERS` 默认 `true`：优先
`X-Real-IP`，其次 `X-Forwarded-For`），请求记录的「出口」列与登录防爆破的按 IP
计数都取真实 IP 而非反代地址：

Nginx（网关转发，反代到 10010）：

```nginx
location / {
    proxy_pass http://127.0.0.1:10010;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_buffering off;        # SSE 流式必需
    proxy_cache off;
    proxy_read_timeout 600s;    # LLM 长响应
}
```

Caddy（自动 HTTPS，反代到 10010）：

```
gw.example.com {
    reverse_proxy 127.0.0.1:10010 {
        flush_interval -1       # SSE 流式必需
    }
}
```

网关与 WebUI 同在 10010：反代即同时覆盖 API 与管理后台。设置了 `WEBUI_PORT`
拆分时，WebUI/Admin（如 10070）不建议直接暴露公网，确需域名访问再单独反代该端口。

### 验证与健康检查

```bash
# 模型列表（GW_KEY_AUTH=true 时需带下游通用 key）
curl http://127.0.0.1:10010/v1/models -H "Authorization: Bearer sk-gw-xxxx"

# 未配渠道时也可先登录验证服务可用（返回 token）
curl -X POST http://127.0.0.1:10010/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'

# 对话转发冒烟测试
curl http://127.0.0.1:10010/v1/chat/completions \
  -H "Authorization: Bearer sk-gw-xxxx" -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"ping"}]}'
```

容器可加健康检查（compose，探服务根路径）：

```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://127.0.0.1:10010/"]
  interval: 30s
  timeout: 5s
  retries: 3
```

### 安全清单（生产部署必读）

- **修改默认管理员密码**：通过 `ADMIN_USERNAME`/`ADMIN_PASSWORD` 环境变量设置（或手动编辑
  `data/accounts.json`）；登录后也可在 WebUI「设置 → 账号」直接修改用户名/密码（须验证当前
  密码，改动立即生效并落盘，重启后仍保留；若对应环境变量已设置，重启后会被环境变量覆盖，
  页面会给出提示）；
- **保持 `GW_KEY_AUTH=true`**（默认）：否则 `/v1/*` 完全开放，任何能访问端口的人都能消耗你的上游额度；
- **收紧网络暴露**：默认单端口 10010 同时服务网关与 WebUI——公网部署务必套反代 +
  HTTPS + 防火墙白名单，仅本机使用时端口映射改 `127.0.0.1:10010:10010`；
  如需管理面与网关分离，设 `WEBUI_PORT`（如 10070）后只把网关端口暴露公网，
  WebUI 端口仅限内网/本机；
- **使用强下游 key**：WebUI 生成的 `sk-gw-...` 即为凭证，泄露后可停用再换发；
- **`EXTRA_USERS` 仅用于预留多用户登录**：额外用户可登录换取 token，但 WebUI 数据均经
  Admin API 拉取（仅主管理员可见），额外用户目前登录后看不到内容；
- 上游 key、代理凭证均明文存于 `gateway.json`，请确保数据目录权限（`chmod 600` 各文件或目录 `700`）。
- **日志与用量库不落真实上游 key**：`usage.db` 的 `usage_events.key` 与请求/错误日志的
  key 列存的是「key 名称@渠道」用户标签（非凭证，WebUI 完整显示），不存在凭证泄漏面；
  升级时会按现存 api_key 精确匹配，把更早版本落盘的历史明文 key 行就地脱敏（幂等）。
  上游 key 本身仍只在 `gateway.json` 中明文保存。
- **登录防爆破默认开启**：连续 `LOGIN_FAIL_LOCKOUT`（默认 5）次失败后，按用户名与来源 IP
  分别锁定（指数退避，封顶 1h）；即使密码正确，锁定期间也返回 429 + `Retry-After`。
  失败与限流均记入服务日志（不含密码）。

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

| 故障 | 处理 |
|---|---|
| 上游 429 | 记冷却：冷却时长优先取上游明确给出的到期时间（`Retry-After` 头，或错误体里的 "Try again in 14h 23m" 类文本/时间戳），上游没给明确时间才用 `RATE_LIMIT_COOLDOWN`（默认 1h） |
| 上游 5xx | 切换到下一个 key；按 key 记连续次数（正常请求清零），**连续超过 `ROTATE_AFTER_5XX`（默认 3）自动换出口 IP**；同时计入熔断（连续失败达阈值即冷却该 key） |
| 网络/代理错误 | ipv6pool key **立即自动换出口 IP 并同 key 重试一次**（出口被拒/瞬断常只影响单个 IP，新出口可立即恢复），仍失败再切到下一个 key；同时计入熔断 |
| 上游 401/403 | 只切换到下一个 key，不冷却、不换出口（鉴权失败不是上游容量/健康信号，不计入熔断） |
| 上游其他 4xx（如 400） | 不切换，原样透传给下游 |
| 全部候选失败 | 有 429 记录则返回 429 + `Retry-After`，否则 502（错误体与请求日志含逐 key 失败原因） |

**熔断（连续失败冷却）**：网络/代理错误与上游 5xx 按 `(key, model 粒度)` 累计**连续失败**
次数（任意一次成功即清零，4xx 业务语义与 429 不清零也不累加），达到阈值（`BREAKER_THRESHOLD`，
默认 3）即冷却该 key；冷却时长按 **基础冷却 × 倍数^(触发次数-1)** 指数退避
（`BREAKER_BASE_COOLDOWN` 默认 30s、`BREAKER_COOLDOWN_MULTIPLIER` 默认 2、上限
`BREAKER_MAX_COOLDOWN` 默认 15m），触发次数与失败计数一并持久化到 `cooldowns.json`
（重启后不丢），只有成功或手工解除才清零。效果是上游整体故障时停止持续冲击，避免把
「上游挂了」放大成「网关把所有请求都打过去」；关闭开关（设置页「熔断开关」或
`BREAKER_ENABLED=false`）即恢复「只换 key 不冷却」的旧行为。冷却中的 key 在「路由」页
可见并可手动解除；全部候选都在冷却时仍会对**最早到期者做一次穿透试探**，成功即自愈。

换出口 IP 时网关同时关闭旧出口上的空闲连接隧道——否则 transport 连接池会复用
换 IP 前建立的旧连接，导致「换了 IP 但请求仍从旧出口发出」。

冷却粒度按渠道配置（`cooldown_scope`）：默认**按 key 跨模型共享**——某模型触发 429 即冷却
该 key 的全部模型（适合 key 配额共享的上游）；渠道可选 `key_model` 按 `(key, model)`
独立记录——同一账号不同模型的额度互不影响。渠道测试成功会自动解除该 key 的存量冷却。
渠道卡片按粒度展示明细：`key_model` 渠道逐模型显示「冷却 模型 · 剩 X」（每个徽标带 `×`
可只解除该条），并给出该 key **可用模型的个数**（完整名单在悬浮提示里——冷却那一行只随
冷却条数增长，不会把渠道的全部模型都铺出来），key 粒度显示整体冷却；「路由」页按模型
列出每个候选 key 的状态，可逐条或一键解除冷却。
**渠道编辑弹窗底部的冷却清单**把该渠道冷却中的 key / `(key, 模型)` 列成列表（限高、
清单内部滚动：条目再多也不会把弹窗撑高；整体冷却排最前，其余按剩余时间升序，
先到期先恢复的在前），逐条「解除」即恢复路由，不必等冷却到期或跑一次渠道测试。
**层级清除冷却**：渠道卡片头「清除冷却」弹窗按冷却键的模型分组列出该渠道冷却中的 key——
可「按模型」一次清掉本渠道所有 key 的该冷却（渠道映射扇出的多个上游模型按分组名一次清完），
也可「清除全部」清空本渠道所有模型所有 key；路由页每个模型分组头的「清除冷却」清该模型在
**全部渠道所有 key** 上的冷却（按分组名解析，别名与渠道映射同样适用）。key 粒度渠道的
共享条目在弹窗里以「整 key（跨模型共享）」单列一行。
**例外：渠道名称映射出的候选始终按上游模型独立冷却**（见「模型名称处理」）——一个下游名
映射到 `cn:x`/`global:x` 时，`cn:x` 被限流不会牵连 `global:x`，也不受 key 粒度设置影响；
路由页为每个上游模型单列一行，解除冷却按该行自己的冷却键精确解除。

账号调度按渠道配置（`schedule`）：默认**故障转移**——按 key 顺序，靠前的用满/失败才换
下一个；可选**顺序轮询**（`round_robin`）——每次请求从下一个 key 开始轮流分配（配置顺序
不变、保序轮转，冷却 key 依旧跳过），均摊账号用量。渠道未显式配置时跟随全局默认：
WebUI「设置」页「默认账号调度」或「路由」页顶部下拉（保存立即生效），环境变量
`DEFAULT_SCHEDULE` 提供默认值。

**渠道优先级与权重**（渠道级设置，WebUI 渠道编辑弹窗）：`priority`（默认 0，越小越先被
路由）决定渠道间的先后；同一优先级内，配置了 `weight`（>0）的渠道按**权重比例轮流优先**
（平滑加权轮询：请求多时各渠道承担比例 ≈ 权重比，权重未设置的渠道不获得额外分配），
未配置权重的组保持配置顺序（纯故障转移）。权重决定的是「每请求从哪个渠道先手试」，失败
仍按候选顺序继续转移。

**失败转移模式**（渠道级设置，`failover_mode`）：默认**同渠道切换**——渠道内一个 key 失败
继续尝试下一个 key，全部失败才切到下一渠道；可选**强制切换渠道**（`force_channel`）——
渠道内任一 key 失败立即跳到下一渠道（适合每个渠道只希望一个 key 生效的场景）。

**固定模型渠道**（渠道级设置，独立设置页：WebUI 顶部「**渠道固定**」页——每个渠道一块、
每个模型一行，可单独探测/验证/编辑，所有改动立即落盘生效；渠道页卡片的「内部渠道固定」
按钮会跳转到该页并定位对应渠道。持久化为 `gateway.json` 中渠道的 `model_pins`）：上游网关
（如 Cline Pass）的一个模型背后常内置多个上游渠道（provider），请求时由上游**随机路由**。
此设置把指定模型钉到固定内部渠道，语义对齐 dsh-cline-pass 的 per-model pin
（`pinMode=strict/preferred` + `exclude` + `sort`）：

| 模式 | 注入行为 |
| --- | --- |
| **严格固定**（`strict`，默认） | 按固定列表顺序，每个内部渠道一次独占尝试（注入 `only=[渠道]`），失败切换下一个固定渠道（再失败按正常故障转移换 key/渠道）——绝不路由到列表之外的渠道 |
| **固定优先**（`preferred`） | 单次请求注入完整优先序 `order=[固定列表…]`，由上游按序自选（对标 `provider.order`） |

注入写法按上游管线自动选择：**direct**（OpenRouter 型）写顶层 `provider.only/order/sort`；
**planner**（Vercel AI Gateway 型）写 `providerOptions.gateway.only/order/sort`；管线未知时
两种写法都注入（各管线忽略不认识的字段）。**排序**（`sort`，可选）：cost（价格）/ttft（首字
延迟）/tps（吞吐），direct 管线自动映射为 price/latency/throughput。

- **探测**（每个模型行的「探测」按钮）：发两到四条小请求——正常请求（推理模型把
  `max_tokens` 配额烧成"空内容"类报错时自动去掉上限重试一次）从响应的
  `provider_metadata.gateway.routing` 识别管线类型与实际服务的渠道（真实网关把路由块挂在
  响应**顶层**，message/choice 级一并兼容）；再把 only 钉到不存在的渠道（`__probe__`），
  上游在花费 token 前报错并**点名全部可用渠道**（planner 文本
  "Available providers are: …"，direct 为错误 JSON 的 `error.metadata.available_providers`；
  每条收割请求只带当前管线那一种写法，管线未知时按 planner → direct 各发一次干净请求）。
  direct 管线另从 OpenRouter 公开目录补充该模型的全部渠道；planner 的 tier-0 提示
  （`planningReasoning` 里 "… won tier 0 over …" 点名的渠道）一并并入已知渠道。
  上游对正常小请求明确报错（模型不存在/限流/鉴权失败）时探测直接失败并展示上游错误。
  探测产物（管线/渠道清单/最近实际渠道）立即落盘到渠道的 `model_pins`，在「已知渠道」里
  点击即可切换 固定→排除→移除；
- **验证**（每个模型行的「验证」按钮）：对每个已知内部渠道发一条固定小请求，按上游响应分类
  （可用 / 限流 / 不可用 / 鉴权失败 / 未知）；
- **排除**（`exclude`）编译进 allowlist（上游不认 exclude 字段）：需要先探测到渠道清单才
  生效；固定与排除互斥（同一渠道不会既固定又排除）；
- 独立「渠道固定」页上的所有改动立即落盘生效（路由每请求实时取快照），无需重启；
  「编辑渠道」弹窗保存渠道时原样保留固定配置，不会清空；「渠道测试」等链路不受影响；
- responses 端点渠道不支持（请求体会被 Responses API 转换重建，注入字段无法保留）。

Admin API（均需管理员 token）：`PUT /admin/api/channels/{id}/model-pin` body
`{"model":"<模型ID>","mode":"strict"|"preferred","upstreams":["<slug>"],"exclude":["<slug>"],"sort":"cost"}`
（upstreams/exclude/sort 全空 = 清除固定，探测产物保留）；`POST
/admin/api/channels/{id}/probe-upstreams` body `{"model":"<模型ID>","key_id?"}`；
`POST /admin/api/channels/{id}/validate-upstreams` body `{"model":"<模型ID>","upstreams"?}`。
渠道的 `model_pins` 随 `GET /admin/api/state` 一并返回。

**账号自动探测**（渠道编辑页「自动探测」开关）：开启后后台调度器（每 30s 扫描）对渠道内
的 key 发送真实对话请求——一道随机 5 位数 + 5 位数 + 5 位数的加法题（如
`What is 12345 + 67890 + 13579?`）——用上游的真实响应判断账号状态。探测模型按渠道冷却
粒度选择：**按 key 冷却**的渠道用启用模型的第一个；**按 (key, model) 冷却**的渠道探测
「冷却恢复的那个模型」/「连续未使用的模型」。三种触发时机：

- **启动探测**：进程启动（重启/重新部署）后一次性核对账号状态，避免重启后「谁还能用」
  只能等下一个探测周期才知道。跳过两类账号——**上游明确给出到期时间、仍在冷却中**的
  （等它自然到期，试探只会白撞一次 429；兜底时长的冷却不跳过，重启后值得重新核对）与
  **最近 2 小时内成功调用过**的（固定 2h 窗口，不跟随空闲探测间隔——启动探测要判断的是
  「重启瞬间这个账号是不是正在被使用」），其余账号各发一题核对，并发上限
  `PROBE_CONCURRENCY`（默认 4）。可在 WebUI「设置」页关闭（`probe_startup`，
  环境变量 `PROBE_STARTUP` 提供默认值）；
- **冷却恢复探测**：key 的全部冷却到期（从冷却恢复可路由）时探测一次，确认账号确实
  恢复正常。若上游再次 429（错误体给的到期时间偏短、额度未真正重置），按上游明确的
  到期时间**重新记冷却**，避免路由反复撞限流；
- **空闲探测**：正常状态（渠道与 key 均启用且不在冷却）的账号**连续「空闲探测间隔」
  没有任何调用**时探测一次，确认账号仍可用；停用与冷却中的账号不探测。间隔在 WebUI
  「设置」页调整（`probe_idle_sec`，默认 7200 秒 = 2 小时，0 = 关闭空闲探测，环境变量
  `PROBE_IDLE_SEC` 提供默认值）。按 key 冷却的渠道按 key 计时；按 (key, model) 冷却的
  渠道逐模型计时、只探测连续未使用的模型（冷却中的模型到期后由恢复探测验证）。探测
  本身也是一次调用，会重置对应计时线，因此持续空闲的账号每个间隔探测一次。

探测请求超时取 `TEST_TIMEOUT`；回复中提取不到正确的加法结果（答案错误/答非所问）会
记入错误日志供人工核查，鉴权失败（401/403）与 5xx 同样留痕。全部探测写入请求日志
（`user` 列存 `probe`、日志页显示为「探测」，可在日志页搜索该列过滤查看），**不计入
用量统计**；日志里的 Tokens 列是这次探测的真实用量（上游未回 `usage` 时为 0）。

活动计时（用于「最近是否在用」判断）在启动时从持久化的请求日志恢复（`usage.db` 的
`request_log` 与 `usage_events`，取窗口内每个 (key, 模型) 最近一次**成功**调用的时刻；
恢复窗口取「空闲探测间隔」与启动探测的 2 小时窗口中较大者）：重启不再把刚在用过的账号
当成空闲账号，空闲计时也从真实调用时刻起算——否则重启后会先静默一个完整间隔才恢复
空闲探测。日志里没有记录（或只在窗口外、只有失败记录）的账号视为状态未知，由启动探测
核对一次。

429 冷却时长、5xx 换出口阈值、单请求最大尝试数、启动探测开关、默认账号调度均可在 WebUI
「**设置**」页调整（保存到 `gateway.json`，保存后立即生效）；环境变量仅提供默认值，
前端显式设置优先。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `10010` | 监听端口（网关 `/v1/*` 与 WebUI/Admin 同端口） |
| `WEBUI_PORT` | 空（同 `PORT`） | 可选：为 WebUI/Admin API 设独立监听端口（如 10070），网关与管理面分离 |
| `DATA_DIR` | `data`（容器内 `/data`） | 数据目录，**容器部署必须挂载** |
| `TZ` | 镜像内 `Asia/Shanghai` | 服务进程时区（影响启动/运行日志时间戳）；容器已装 tzdata，改为 `UTC` 等即可。WebUI 显示的时间不受此影响，始终按北京时间渲染 |
| `GATEWAY_CONFIG_PATH` | `${DATA_DIR}/gateway.json` | 渠道/密钥配置文件（WebUI 管理） |
| `LEASE_ASSIGN_PATH` | `${DATA_DIR}/lease-assignments.json` | 代理池「key→租约」分配表（跨渠道复用，持久化保证 IP 稳定） |
| `COOLDOWN_PATH` | `${DATA_DIR}/cooldowns.json` | 429 冷却状态持久化文件（原子写入；启动时恢复未过期冷却） |
| `ADMIN_USERNAME` / `ADMIN_PASSWORD` | `admin` / `admin` | WebUI 管理员账号；Admin API 仅此账号可用。启动日志只打印用户名，不打印密码 |
| `LOGIN_FAIL_LOCKOUT` | `5` | 连续登录失败达到该次数后按「用户名」与「来源 IP」分别锁定（指数退避，封顶 1h）。0 = 关闭防爆破 |
| `LOGIN_FAIL_WINDOW` | `5m` | 登录失败计数窗口，同时是锁定基础时长（超过阈值后每多失败一次翻倍） |
| `EXTRA_USERS` | 空 | 额外 WebUI 登录用户，`user:pass,user2:pass2`（仅登录 WebUI，无 Admin API 权限） |
| `TOKEN_TTL` | `24h` | 登录 token 有效期 |
| `TOKEN_SECRET` | 空 | 登录 token 签名密钥（缺省自动持久化到 data/token-secret） |
| `GW_KEY_AUTH` | `true` | 下游是否必须携带通用 key |
| `TRUST_PROXY_HEADERS` | `true` | 反代部署取真实客户端 IP：优先 `X-Real-IP`，其次 `X-Forwarded-For` 最左合法段（脏值跳过，回退 TCP 对端地址）。影响请求记录的「出口/客户端 IP」列与登录防爆破的按 IP 计数。网关不经反代直接暴露给不可信客户端时改为 `false`，防伪造头污染日志/绕过按 IP 限流 |
| `MAX_ROUTE_TRIES` | 0（全部） | 单请求最多尝试的 key 数（也可在 WebUI「设置」页修改） |
| `RATE_LIMIT_COOLDOWN` | `1h` | 429 冷却（上游未给出明确到期时间时才用；`Retry-After` 头或错误体文本里的明确时间优先；也可在 WebUI「设置」页修改） |
| `ROTATE_AFTER_5XX` | `3` | 同一 key 连续 5xx 超过该次数自动换出口 IP（0 = 关闭；仅 ipv6pool key 生效；也可在 WebUI「设置」页修改） |
| `BREAKER_ENABLED` | `true` | 熔断开关：网络/代理错误与 5xx 连续失败达阈值即冷却该 key（对照「故障转移与冷却策略」；也可在 WebUI「设置」页修改） |
| `BREAKER_THRESHOLD` | `3` | 熔断阈值：同一 (key, 模型) 连续失败达该次数触发冷却（也可在 WebUI「设置」页修改） |
| `BREAKER_BASE_COOLDOWN` | `30s` | 熔断首次触发的冷却时长（也可在 WebUI「设置」页修改） |
| `BREAKER_MAX_COOLDOWN` | `15m` | 熔断冷却时长上限（指数退避封顶；也可在 WebUI「设置」页修改） |
| `BREAKER_MULTIPLIER` | `2` | 每次触发熔断时冷却时长的增长倍数（>= 1；也可在 WebUI「设置」页修改） |
| `DEFAULT_SCHEDULE` | `failover` | 默认账号调度（渠道未显式配置时使用）：`failover` 故障转移 / `round_robin` 顺序轮询；也可在 WebUI「设置」/「路由」页修改 |
| `UPSTREAM_HEADER_TIMEOUT` | `10m` | 等待上游响应头超时（LLM 非流式可能较慢，勿设过小） |
| `UPSTREAM_READ_IDLE_TIMEOUT` | `10m` | 上游响应**读取静默超时**：连续该时长未从上游读到任何字节（连接失联：TCP 半开、NAT 静默回收等）即关闭连接中止该候选，避免读取永久阻塞导致 goroutine/连接泄漏累积；须大于最长的上游思考时间，流式期间任何字节都会重置计时；0 = 关闭 |
| `DOWNSTREAM_WRITE_TIMEOUT` | `60s` | 下游**写出静默超时**（与上游读取静默超时对称）：客户端保持连接但停止读取（TCP 窗口填满）时，单次「写 + flush」阻塞超过该时长即中止该请求，释放 goroutine 与上下游连接对；正常慢速客户端不受影响（只约束单次写的阻塞时长，不限制流的总时长）；0 = 关闭 |
| `KEEPALIVE_INTERVAL` | `15s` | 流式转发心跳：等待上游首包/流静默期间，每该间隔向下游写一帧 SSE 注释（`: keepalive`），防下游反代按空闲超时（常见 60s）掐连接；0 = 关闭。首帧心跳会提前提交 200 + event-stream 头，此后路由彻底失败改用流内 `data: {"error":...}` 帧表达（也可在 WebUI「设置」页修改） |
| `PROBE_IDLE_SEC` | `7200`（2h） | 自动探测的空闲探测间隔秒数：开启「自动探测」的渠道内，正常状态账号连续无调用该时长后发加法题验证账号状态（按 (Key,模型) 冷却的渠道逐模型检查）；0 = 关闭空闲探测，冷却恢复探测不受影响（也可在 WebUI「设置」页修改） |
| `PROBE_STARTUP` | `true` | 启动探测：进程启动（重启/重新部署）后对「无上游明确冷却、最近 2 小时内没有成功调用」的账号各发一题核对状态；0 = 关闭（活跃度计时线仍会从请求日志恢复）（也可在 WebUI「设置」页修改） |
| `PROBE_CONCURRENCY` | `4` | 探测并发上限（同时进行中的上游探测请求数，含启动探测与空闲/恢复探测；0 = 不限制）。成批账号同时命中探测条件时限制压向上游的请求数 |
| `TEST_TIMEOUT` | `45s` | WebUI 渠道/key 测试的整体超时（默认低于常见反代 60s，避免测试被反代掐断成 504） |
| `REQ_LOG_SIZE` | `1000` | 请求日志保留条数（持久化在 `usage.db`，重启不丢） |
| `ERR_LOG_SIZE` | `1000` | 错误日志保留条数（独立表存储，不被成功请求挤出） |
| `ERR_LOG_RETENTION_DAYS` | `7` | 错误日志按天保留上限：只保留最近 N 天的错误记录，超期自动清理（0 = 关闭天数裁剪，只按条数）。错误记录携带「请求内容 + 返回内容」（截断），占用较快，推荐保留 7–30 天。可在 WebUI「设置 → 日志与错误」修改 |
| `USAGE_DB_PATH` | `${DATA_DIR}/usage.db` | 用量 SQLite 路径（空 = 纯内存） |
| `USAGE_RETENTION_DAYS` | `30` | 用量保留天数 |
| `USAGE_MAX_RECORDS` | `100000` | 用量最大条数 |

## Admin API（需管理员登录 token）

`POST /login` 换 token 后调用（`Authorization: Bearer <token>`）：

| 端点 | 说明 |
| --- | --- |
| `GET /admin/api/state` | 渠道、下游 key、代理池列表、租约缓存总览 |
| `PUT /admin/api/channels` | 新增/整体更新渠道（含内嵌 keys） |
| `POST /admin/api/channels/{id}/fetch-models` | 用渠道 key 拉取上游模型列表，默认 dry-run 返回候选（`fetched` 上游写法 / `groups` 按上游分组 `{name, free, free_total, models, total}` / `free_models` / `enabled` / `enabled_ids` / `enabled_upstream` 预勾选 / `stale`）供 WebUI 选择器铺开勾选，不写回渠道；`?replace=1` 全量替换写回（列表存对外名、上游写法写进 `model_map`，并丢弃已不在列表里的旧映射键）；免费清单仅随响应展示，不持久化 |
| `POST /admin/api/fetch-models` | **按编辑器当前内容**拉取模型列表（`{channel: {...}}` 内联渠道定义，未保存也能用）：只读不落盘，返回同上字段（WebUI 新增/编辑弹窗「获取模型」用，候选按上游写法逐个返回、按上游分组展示，同一对外名的多个写法各一条）；响应兼容 OpenAI `{"data":[…]}`、裸数组、`models` 等包装字段与**按分组返回的 JSON**（Cline `recommended-models` 的 `recommended`/`free`/`clinePass`/`clineCloud`，字段名即分组名） |
| `DELETE /admin/api/channels/{id}` | 删除渠道（自动释放其池租约） |
| `PUT /admin/api/pools` | 新增/更新代理池（连接信息：`{name, pool_url, pool_token?, socks_host?}`；被渠道 key 引用的池不可删除） |
| `DELETE /admin/api/pools/{id}` | 删除代理池（仍被引用时返回 400，成功后释放其遗留租约） |
| `PUT /admin/api/gwkeys` | 新增/更新下游 key（key 留空自动生成） |
| `DELETE /admin/api/gwkeys/{id}` | 删除下游 key |
| `POST /admin/api/pool/test` | 测试池子连通性 `{pool_id}` 或旧格式 `{pool_url, pool_token}` |
| `POST /admin/api/pool/rotate` | 手动换 IP `{channel_id, key_id}`、`{proxy, key_id, key_name?}`（编辑器内联代理，未保存的 key 也能换）或 `{pool_url, lease_id}`（直连本地代理池条目） |
| `POST /admin/api/pool/release` | 手动释放租约 `{channel_id, key_id}`、`{proxy, key_id, key_name?}`（内联代理）或 `{pool_url, lease_id}` |
| `GET /admin/api/pool/leases` | 网关持有的租约列表 |
| `POST /admin/api/testkey` | 用指定 key 发一条测试请求 `{channel_id, key_id, model?}` |
| `POST /admin/api/channels/{id}/test-model` | 渠道级批量测试 `{key_id?, first_only?, models?}`（models 每行/逗号分隔，空 = 渠道已启用模型）；逐 key × 逐模型执行，HTTP 层不报错，成败均通过结果项的 `ok` 表达，且每次测试（含失败）都写入请求记录 |
| `GET /admin/api/requests?page=&page_size=` | 请求日志（分页，兼容旧 `?limit=`；数据持久化在 `usage.db`，重启不丢） |
| `GET /admin/api/errors?page=&page_size=` | 错误日志（失败请求独立存储，分页） |
| `POST /admin/api/cooling/clear` | 手动解除 key 冷却 `{key_id, channel_id?}`（key 级与按模型冷却全部清除） |
| `POST /admin/api/cooling/clear-model` | 按 (key, 模型) 精确解除一条冷却 `{key_id, model}` |
| `POST /admin/api/cooling/clear-all` | 一键清空全部冷却（所有 key、所有模型粒度） |
| `POST /admin/api/channels/{id}/cooling/clear-all` | 清除该渠道全部 key 的全部冷却（所有模型粒度） |
| `POST /admin/api/channels/{id}/cooling/clear-model` | 按模型清除该渠道所有 key 的冷却 `{model}`（模型映射扇出的上游名一并清除；`model` 为空串 = 该渠道按 key 跨模型共享的冷却条目） |
| `POST /admin/api/route/cooling/clear-model` | 清除某模型在全部渠道所有 key 上的冷却 `{model}`（路由页分组名，支持全局别名与渠道映射；空串 = 「对全部模型放行」分组） |
| `GET /admin/api/route?model=` | 路由视图：按模型聚合候选 (渠道, key) 与实时状态（`ok`/`cooling`/`disabled`，含剩余冷却毫秒），候选顺序即网关转发顺序 |
| `PUT /admin/api/channels/{id}/model-pin` | 保存/清除渠道上某模型的内部渠道固定 `{model, mode: "strict"\|"preferred", upstreams: [slug], exclude: [slug], sort}`；upstreams/exclude/sort 全空 = 清除固定（探测产物保留）。保存立即生效 |
| `POST /admin/api/channels/{id}/probe-upstreams` | 探测某模型的内部渠道 `{model, key_id?}`：识别管线（direct/planner）、实际服务渠道，并把 only 钉到 `__probe__` 让上游报出全部可用渠道；产物写回渠道 `model_pins` |
| `POST /admin/api/channels/{id}/validate-upstreams` | 逐个内部渠道发固定小请求验证可用性 `{model, key_id?, upstreams?}`（缺省用探测到的清单），返回逐渠道分类（ok/limited/bad/auth/unknown） |
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
（429/401/网络错误/冷却/模型过滤）、流式保活（心跳、提交后错误帧、JSON 错误二选一、
非流式跳过）、ipv6pool 端到端（真实 SOCKS5 stub 隧道 + 状态码触发换 IP）、下游鉴权、
模型聚合、Admin API、用量库（含日志持久化与 key 脱敏迁移）、登录防爆破。

## 与原 cline2api 的差异（破坏性变更）

- 移除：`PROXY_PORT`/`SOCKS_PORT` 正向代理监听器、`PROXY_POOL_*` 端口池监听器、
  `UPSTREAM_URL`/`UPSTREAM_KEYS`/`KEY_SELECT_MODE`/`CLIENT_*` 全局环境变量配置
  （对应能力全部移入 WebUI 的渠道/代理配置）。
- `POST /`（根路径聊天转发）不再提供，统一走 `/v1/chat/completions`。
- 下游鉴权从登录 token 改为通用 key（`GW_KEY_AUTH=false` 可关闭校验）。
- reasoning 改写从全局行为改为渠道级开关（`rewrite_reasoning`）。
- 用量库增加 `channel` 维度（旧库自动迁移，兼容升级）。
