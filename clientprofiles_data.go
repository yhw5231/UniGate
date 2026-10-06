package main

// clientProfiles 内置客户端指纹预设清单（顺序即 WebUI 下拉顺序，与 README
// 「内置客户端协议头预设」的支持清单一致）。每条的头值与可信度见下方字段；
// 鉴权头（Authorization / x-api-key / x-dsh-auth-token 等）一律不进预设——
// 网关按渠道 key 注入，写进预设反而会顶掉鉴权。
var clientProfiles = []clientProfile{
	{
		ID: "claude-code", Name: "Claude Code", Starred: true, Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":        "claude-cli/2.1.100 (external, cli)",
			"x-app":             "cli",
			"anthropic-version": "2023-06-01",
			"anthropic-dangerous-direct-browser-access": "true",
			"x-stainless-lang":                          "js",
			"x-stainless-package-version":               "0.81.0",
			"x-stainless-os":                            "Windows",
			"x-stainless-arch":                          "x64",
			"x-stainless-runtime":                       "node",
			"x-stainless-runtime-version":               "v22.14.0",
			"x-stainless-retry-count":                   "0",
			"x-stainless-timeout":                       "600",
		},
		Dynamic: map[string]string{
			"X-Claude-Code-Session-Id": "uuid",
		},
		Evidence: []string{
			"https://unpkg.com/@anthropic-ai/claude-code@2.1.100/cli.js",
			"https://unpkg.com/@anthropic-ai/claude-code@2.0.0/cli.js",
			"https://registry.npmjs.org/@anthropic-ai/claude-code/latest",
		},
		Note: "从官方 npm 包 cli.js 内联字符串取证（2.0.0 / 2.1.0 / 2.1.100 三版一致）：UA 模板 claude-cli/<VERSION> (external, <entrypoint>)，entrypoint 默认 cli、后台模式 cli-bg；x-app: cli；anthropic-version 常量 2023-06-01；anthropic-dangerous-direct-browser-access 来自 CLI 以 dangerouslyAllowBrowser:true 构造客户端时 SDK 自动附加；x-stainless-* 由内置 @anthropic-ai/sdk（2.1.100 为 0.81.0）生成，os/arch/runtime-version 随运行环境变化（上面按 Windows + Node 22 取值），retry-count 首次为 0、timeout 取 API_TIMEOUT_MS 默认 600000ms → 600。X-Claude-Code-Session-Id 在真实客户端是进程启动时生成、跨请求固定的会话 id（这里按每请求 UUID 生成）。条件头未写进预设：anthropic-beta 按模型与环境拼装（默认含 claude-code-20250219；OAuth 时加 oauth-2025-04-20；[1m] 模型加 context-1m-2025-08-07；另有 interleaved-thinking-2025-05-14 / fine-grained-tool-streaming-2025-05-14 / context-management-2025-06-27 / redact-thinking-2026-02-12 等）；鉴权为 x-api-key（API key 模式）或 Authorization: Bearer（OAuth 模式），由网关按渠道 key 注入；x-claude-remote-container-id 等仅在容器/远端会话出现。版本说明：最新 2.1.290 已改为平台原生二进制、无 JS 可读，故取 2.1.100 作为可核实版本。",
	},
	{
		ID: "codex", Name: "Codex CLI", Starred: true, Confidence: confidenceVerified,
		Headers: map[string]string{
			"originator": "codex_cli_rs",
			"User-Agent": "codex_cli_rs/0.160.1 (Windows 11; x86_64) WindowsTerminal",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/openai/codex/main/codex-rs/login/src/auth/default_client.rs",
			"https://raw.githubusercontent.com/openai/codex/main/codex-rs/terminal-detection/src/lib.rs",
			"https://raw.githubusercontent.com/openai/codex/rust-v0.160.1/codex-rs/Cargo.toml",
		},
		Note: "官方 Rust 源码取证：DEFAULT_ORIGINATOR = codex_cli_rs（其它首方取值 codex-tui / codex_vscode / codex_atlas / codex_chatgpt_desktop），UA 模板 {originator}/{CARGO_PKG_VERSION} ({os_type} {os_version}; {arch}) {terminal_token}，terminal_token 由 TERM_PROGRAM/TERM 探测（上面取 WindowsTerminal），版本取工作区版本（发布 tag rust-v0.160.1 → 0.160.1）。鉴权头 Authorization: Bearer 与 chatgpt-account-id 由网关按渠道 key 注入，不写进预设。条件头未写进预设：OpenAI-Beta: responses_websockets=2026-02-06 只在 WebSocket 握手发送；x-codex-window-id / x-codex-turn-metadata / x-codex-turn-state / x-codex-beta-features / x-codex-parent-thread-id / x-openai-subagent 属会话与子代理态；session_id / thread_id 在普通 HTTP POST /responses 上走请求体 client_metadata（只有 WebSocket 握手与 Realtime 才作为 session-id / thread-id 头）。",
	},
	{
		ID: "openclaw", Name: "OpenClaw", Confidence: confidencePartial,
		Headers: map[string]string{
			"user-agent": "claude-cli/2.1.280",
			"x-app":      "cli",
			"accept":     "application/json",
			"anthropic-dangerous-direct-browser-access": "true",
			"anthropic-beta": "claude-code-20250219,oauth-2025-04-20",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/openclaw/openclaw/main/packages/ai/src/providers/anthropic-model-contract.ts",
			"https://raw.githubusercontent.com/openclaw/openclaw/main/packages/ai/src/transports/anthropic-transport-stream.ts",
			"https://raw.githubusercontent.com/openclaw/openclaw/main/packages/ai/src/anthropic-client-identity-parity.test.ts",
		},
		Note: "官方源码（openclaw/openclaw）证实的是 Anthropic OAuth 路径的指纹：user-agent 硬编码下限 claude-cli/2.1.280（只有调用方传入主版本更大的 claude-cli/x.y.z 才上浮，预发布版本会退回下限）、x-app: cli、anthropic-beta 前缀 claude-code-20250219,oauth-2025-04-20。非 OAuth（纯 API key）分支的完整头集合未取到证据，故标 partial；anthropic-version 与会话亲和头的确切头名亦未证实。鉴权头（authorization/x-api-key）由网关按 key 注入。",
	},
	{
		ID: "pi", Name: "pi (Earendil Works)", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent": "pi (win32 10.0.26100; x64)",
			"accept":     "application/json",
			"originator": "pi",
		},
		Dynamic: map[string]string{
			"session-id":          "uuid",
			"x-client-request-id": "same:session-id",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/earendil-works/pi/main/packages/ai/src/utils/pi-user-agent.ts",
			"https://raw.githubusercontent.com/earendil-works/pi/main/packages/ai/src/api/openai-codex-responses.ts",
			"https://registry.npmjs.org/@earendil-works/pi-ai",
		},
		Note: "pi = Earendil Works（作者 Mario Zechner / badlogic）的编码 agent，仓库 github.com/earendil-works/pi，LLM 客户端库 @earendil-works/pi-ai。核心静态指纹是 User-Agent: pi (<os.platform()> <os.release()>; <arch>)，不含 pi 自身版本号——上面的 win32 取值是本机 Windows 形态，换平台请照此格式改。已验证它不发 HTTP-Referer / X-Title。另有两条按凭据切换的伪装路径（未写进预设，需要时用渠道自定义头覆盖）：走 Anthropic OAuth（sk-ant-oat…）时 user-agent 换成 claude-cli/2.1.280 + x-app: cli + anthropic-beta: claude-code-20250219,oauth-2025-04-20；走 Codex 时 originator: pi + OpenAI-Beta: responses=experimental + chatgpt-account-id。鉴权头由网关注入。",
	},
	{
		ID: "opencode", Name: "opencode", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent": "opencode/1.18.34",
		},
		Dynamic: map[string]string{
			"x-opencode-session-id": "uuid",
			"x-session-affinity":    "same:x-opencode-session-id",
			"X-Session-Id":          "same:x-opencode-session-id",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/sst/opencode/dev/packages/opencode/src/session/llm/request.ts",
			"https://raw.githubusercontent.com/sst/opencode/dev/packages/opencode/src/provider/provider.ts",
		},
		Note: "官方源码取证（默认分支 dev）：模型请求的全局 UA 常量是 opencode/<InstallationVersion>（本仓 package.json 为 1.18.34），注意它与 installation/index.ts 里用于安装升级类请求的 opencode/<channel>/<version>/<client> 不是同一个。会话头 x-opencode-session-id 每个请求都发，第三方 provider 另发 x-session-affinity 与 X-Session-Id（真实取值形如 ses_xxx，这里统一按每请求 UUID 生成且三头同值）。仅自家 provider（providerID 以 opencode 开头）才发 x-opencode-project / x-opencode-session / x-opencode-request / x-opencode-client，子代理才发 x-opencode-parent-session-id / x-parent-session-id，github-copilot 才发 x-initiator，故均未写进预设；第三方 provider 的归因头也按线路不同（llmgateway 用 HTTP-Referer: https://opencode.ai/ + X-Title: opencode + X-Source: opencode，openrouter/zenmux 用前两个，nvidia 再加 X-BILLING-INVOKE-ORIGIN: OpenCode，vercel 用小写 http-referer/x-title）。另注意 x-opencode-directory / x-opencode-workspace / x-opencode-ticket 等是 opencode HTTP server 的内部头，不发给模型 API。",
	},
	{
		ID: "ohmyopencode", Name: "oh-my-opencode", Confidence: confidencePartial,
		Headers: map[string]string{
			"User-Agent": "opencode/1.18.34",
		},
		Dynamic: map[string]string{
			"x-opencode-session-id": "uuid",
			"x-session-affinity":    "same:x-opencode-session-id",
			"X-Session-Id":          "same:x-opencode-session-id",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/code-yeongyu/oh-my-openagent/dev/packages/omo-opencode/src/plugin/chat-headers.ts",
			"https://raw.githubusercontent.com/code-yeongyu/oh-my-openagent/dev/docs/reference/omo-ai-publishing.md",
			"https://registry.npmjs.org/oh-my-opencode",
		},
		Note: "oh-my-opencode（npm 包 oh-my-opencode，最新 5.1.19；仓库 code-yeongyu/oh-my-openagent，自称 OMO）不是独立客户端：它主要以 OpenCode 插件形态运行，模型请求由 OpenCode 本体发出，因此指纹就是 opencode 的那一套（含 User-Agent: opencode/<版本>）——本预设照抄 opencode 预设，符合事实而非猜测。OMO 唯一自己加的头是插件 chat.headers 钩子：仅当 provider 为 github-copilot / github-copilot-enterprise 且该 user 消息带 OMO 内部标记时写 x-initiator: agent，故未写进预设。另有原生形态 omo-ai（spawn 被钉死的 @code-yeongyu/senpi 引擎），品牌 profile 声明 userAgent/originator 为 omo，但引擎不在公开仓库内、具体落到哪个头名未取证，故整体标 partial。",
	},
	{
		ID: "kilo-code", Name: "Kilo Code", Confidence: confidenceVerified,
		Headers: map[string]string{
			"HTTP-Referer": "https://kilocode.ai",
			"X-Title":      "Kilo Code",
			"User-Agent":   "Kilo-Code/7.8.3",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/Kilo-Org/kilocode/main/packages/opencode/src/kilocode/const.ts",
			"https://raw.githubusercontent.com/Kilo-Org/kilocode/main/packages/core/src/plugin/provider/kilo.ts",
			"https://raw.githubusercontent.com/Kilo-Org/kilocode/main/packages/kilo-gateway/src/api/constants.ts",
		},
		Note: "官方源码取证（HEAD = v7.8.3）。重要前提：当前 Kilo Code 已不是 Roo Code 分支，而是 opencode 的 fork——packages/opencode 是 CLI 引擎（模型请求由它发出），packages/kilo-vscode 只是 spawn `kilo serve` 的外壳，故 Roo 分支时期（v4/v5）的头无法取证。存在两套并存的 referer：引擎 const.ts 的 DEFAULT_HEADERS 用 https://kilocode.ai（本预设取这一套），packages/core 的 provider 插件用 https://kilo.ai/，X-Title 都是 Kilo Code。仅特定线路才发的头未写进预设：X-KILOCODE-TASKID / PARENT-TASKID / ORGANIZATIONID / PROJECTID / MACHINEID / EDITORNAME（如 Visual Studio Code 1.105.1）/ FEATURE: vscode-extension、nvidia 的 X-BILLING-INVOKE-ORIGIN: KiloCode、llmgateway 的 X-Source: kilo、cerebras 的 X-Cerebras-3rd-Party-Integration: kilo；Kilo Gateway 自身 UA 为 opencode-kilo-provider/<KILOCODE_VERSION>。",
	},
	{
		ID: "roocode", Name: "Roo Code", Confidence: confidenceVerified,
		Headers: map[string]string{
			"HTTP-Referer": "https://github.com/RooVetGit/Roo-Cline",
			"X-Title":      "Roo Code",
			"User-Agent":   "RooCode/3.53.0",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/RooCodeInc/Roo-Code/main/src/api/providers/constants.ts",
			"https://raw.githubusercontent.com/RooCodeInc/Roo-Code/main/src/api/providers/__tests__/constants.spec.ts",
			"https://raw.githubusercontent.com/RooCodeInc/Roo-Code/main/src/api/providers/qwen-code.ts",
		},
		Note: "官方源码取证：DEFAULT_HEADERS 只有 HTTP-Referer / X-Title / User-Agent 三个键（constants.spec.ts 断言 headerKeys 恰好是这三个），UA 为 RooCode/<package.json 版本>（HEAD 为 3.53.0），被 openrouter、openai（含 Azure）、requesty 与 base-openai-compatible-provider 作为 defaultHeaders 注入。仅特定线路才发的头未写进预设：anthropic provider 的 anthropic-beta（可缓存模型加 prompt-caching-2024-07-31）、qwen-code provider 的 X-DashScope-CacheControl: enable / X-DashScope-UserAgent / X-DashScope-AuthType: qwen-oauth、unbound 的 X-Unbound-Metadata。注意 X-Roo-Task-ID 在 HEAD 上只剩注释、无实现，故不收录。",
	},
	{
		ID: "cline", Name: "Cline", Confidence: confidenceVerified,
		Headers: map[string]string{
			"HTTP-Referer":       "https://cline.bot",
			"X-Title":            "Cline",
			"User-Agent":         "Cline/4.1.22",
			"X-CLIENT-TYPE":      "VSCode Extension",
			"X-CLIENT-VERSION":   "4.1.22",
			"X-PLATFORM":         "Visual Studio Code",
			"X-PLATFORM-VERSION": "1.105.1",
			"X-CORE-VERSION":     "4.1.22",
			"X-IS-MULTIROOT":     "false",
		},
		Dynamic: map[string]string{
			"X-Task-ID": "uuid",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/cline/cline/main/sdk/packages/llms/src/providers/request-headers.ts",
			"https://raw.githubusercontent.com/cline/cline/main/sdk/packages/llms/src/providers/cline-client-headers.ts",
			"https://raw.githubusercontent.com/cline/cline/main/apps/vscode/src/shared/cline/index.ts",
		},
		Note: "官方源码取证（VS Code 扩展 apps/vscode/package.json 版本 4.1.22）：User-Agent = Cline/<clientVersion>，X-CLIENT-TYPE = VSCode Extension（CLI 为 cline-cli、JetBrains 为 Cline for JetBrains，SDK 回退 cline-sdk），X-PLATFORM = vscode.env.appName、X-PLATFORM-VERSION = vscode.version、X-CORE-VERSION = 扩展版本、X-IS-MULTIROOT = 工作区根目录数 > 1。关键限制：这套 DEFAULT_CLINE_REQUEST_HEADERS 只在自家计费 provider（providerId 为 cline / cline-pass）时合并，直连 OpenRouter / Anthropic 时不注入 HTTP-Referer 与 X-Title。注意：本项目 README 早先的示例写的是 x-client-type: cline-vscode，源码里并非如此（cline-vscode 只是 ClientName 类型联合与示例 app 的取值），本预设按源码取值。X-Task-ID 为当前任务 id（无任务的单次调用如生成 commit message 时整个头不发送，这里按每请求 UUID 生成）；仅特定线路才发的头未写进预设：openai-codex 路径的 originator: cline / session_id / ChatGPT-Account-Id，opencode-go 的 x-opencode-session，以及 openai-codex 路径 UA 回退值 Cline/1.0.0。",
	},
	{
		ID: "crush", Name: "Crush", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent": "Charm-Crush/0.97.1 (https://charm.land/crush)",
			"originator": "crush",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/charmbracelet/crush/main/internal/agent/agent.go",
			"https://raw.githubusercontent.com/charmbracelet/crush/main/internal/agent/coordinator.go",
			"https://api.github.com/repos/charmbracelet/crush/releases/latest",
		},
		Note: "官方源码取证：internal/agent/agent.go 里 userAgent = fmt.Sprintf(\"Charm-Crush/%s (https://charm.land/crush)\", version.Version)，经 fantasy.WithUserAgent 注入 anthropic / openai / openrouter 等所有 provider（上面版本取最新 release v0.97.1；go install 场景回退模块版本，源码构建为 devel）。originator: crush 仅在 ChatGPT/Codex 线路（coordinator.go）出现。crush 不发 HTTP-Referer / X-Title。仅特定线路才发的头未写进预设：anthropic thinking 模型追加的 anthropic-beta: interleaved-thinking-2025-05-14、ChatGPT 登录时的 chatgpt-account-id、GitHub Copilot 的固定伪装头（User-Agent: GitHubCopilotChat/0.32.4、Editor-Version: vscode/1.105.1、Editor-Plugin-Version: copilot-chat/0.32.4、Copilot-Integration-Id: vscode-chat 与动态 X-Initiator）；Hyper OAuth 设备流另用 User-Agent: crush。用户可用 crush.json 的 extra_headers 追加任意头。",
	},
	{
		ID: "droid", Name: "Factory Droid", Confidence: confidencePartial,
		Headers: map[string]string{
			"X-Factory-Client": "cli",
			"X-Client-Version": "0.233.0",
		},
		Evidence: []string{
			"https://registry.npmjs.org/droid",
			"https://registry.npmjs.org/@factory/cli-win32-x64",
			"https://github.com/Factory-AI/droid-sdk-typescript",
		},
		Note: "Droid CLI 闭源，但官方在 npm 公开发布薄壳包 droid（0.233.0，optionalDependencies 指向 @factory/cli-<platform> 二进制）与 @factory/droid-sdk（0.9.1）。从 bin/droid.exe 提取的字符串常量确认了 X-Factory-Client / X-Factory-Sdk / X-Client-Version / X-Factory-Org-Id / X-Factory-Whoami-Extended 等头名，X-Factory-Client 的取值域（sdk / web-desktop / web-app / web-workspace / daemon / cli / backend）来自公开 SDK 的 ClientRequestAttributionSchema，CLI 对应 cli。注意两点：一是这些头针对 Factory 自家 API / droid 协议链路，直连模型 API 的链路需实机抓包确认；二是 droid 发往模型 API 的 User-Agent 未取证（275MB 二进制里没有任何 droid 专属 UA 字面量），推测沿用打包 SDK 默认 UA，故不写进预设。未写进预设的还有 X-Factory-Sdk（SDK 形态为 <language>/<version>，如 typescript/0.9.1）、X-Factory-Org-Id / X-Factory-Whoami-Extended（账号态）、x-factoryd-proxy-token / x-factory-sf-snapshot-token（一次性令牌）与整套 X-Stainless-*（由打包的 OpenAI/Anthropic SDK 注入）。",
	},
	{
		ID: "qwencode", Name: "Qwen Code", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":        "QwenCode/0.25.0 (win32; x64)",
			"anthropic-version": "2023-06-01",
		},
		Evidence: []string{
			"https://github.com/QwenLM/qwen-code/blob/main/packages/core/src/core/anthropicContentGenerator/anthropicContentGenerator.ts",
			"https://github.com/QwenLM/qwen-code/blob/main/docs/design/2026-09-03-outbound-session-id-header.md",
		},
		Note: "官方源码 + npm 发布包确认：四条协议路径（Anthropic / Gemini / OpenAI 兼容 / OpenAI Responses）都发 User-Agent: QwenCode/<cliVersion> (<process.platform>; <process.arch>)，win32 形态如上；anthropic-version: 2023-06-01 由内置 Anthropic 官方 SDK 注入。条件头未写进预设：仅当 baseURL 指向非 api.anthropic.com 的第三方 Anthropic 兼容代理时，它把 UA 换成 claude-cli/<cliVersion> (external, cli) 并加 x-app: cli；session_id 仅在出站主机为 routify*.alibaba-inc.com（HTTPS）时注入。x-stainless-* 由官方 SDK 自动注入，未逐个取值。",
	},
	{
		ID: "hermes", Name: "Hermes Agent", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":   "HermesAgent/2026.9.24",
			"HTTP-Referer": "https://hermes-agent.nousresearch.com",
			"X-Title":      "Hermes Agent",
			"originator":   "hermes-agent",
		},
		Evidence: []string{
			"https://github.com/NousResearch/hermes-agent/blob/main/agent/anthropic_adapter.py",
			"https://github.com/NousResearch/hermes-agent/blob/main/agent/codex_headers.py",
			"https://github.com/NousResearch/hermes-agent/blob/main/tests/agent/test_provider_attribution_headers.py",
		},
		Note: "hermes = Nous Research 的 Hermes Agent（NousResearch/hermes-agent）。官方源码 _attribution_headers() 给出三件套 HTTP-Referer / X-Title / User-Agent: HermesAgent/<base_version>（calver，最新 release v2026.9.24），测试逐条断言其值；Codex 线路用 originator: hermes-agent。按线路切换的头未写进预设：Anthropic OAuth（Claude 订阅）时 user-agent 改为 claude-code/<版本> (external, cli) + x-app: cli（版本由 `claude --version` 探测，失败回退 2.1.74）；OpenRouter 缓存头 X-OpenRouter-Cache/TTL 需显式开启；ChatGPT-Account-ID / x-openai-internal-codex-residency 从 OAuth JWT 解出（账号态，不写进预设）。",
	},
	{
		ID: "zcode", Name: "ZCode", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":          "ZCode/3.14.3",
			"HTTP-Referer":        "https://zcode.z.ai",
			"X-Title":             "Z Code@cli",
			"X-ZCode-App-Version": "3.14.3",
			"X-ZCode-Agent":       "glm",
			"X-Release-Channel":   "production",
			"X-Client-Language":   "zh-CN",
			"X-Client-Timezone":   "Asia/Shanghai",
			"X-Platform":          "win32-x64",
			"X-Os-Category":       "windows",
			"X-Os-Version":        "10.0.26100",
			"X-Device-Mid":        "3f7c1d92-4a5b-4c6d-8e7f-9012345678ab",
		},
		Dynamic: map[string]string{
			"x-request-id":     "uuid",
			"x-zcode-trace-id": "uuid",
			"x-session-id":     "uuid",
		},
		Evidence: []string{
			"https://github.com/zai-org/ZCode/blob/main/apps/zcode-cli/packages/bootstrap/src/model-config.ts",
			"https://github.com/zai-org/ZCode/blob/main/packages/shared/src/zcode-source-headers.ts",
			"https://github.com/zai-org/ZCode/blob/main/apps/zcode-cli/packages/bootstrap/src/runtime-platform-headers.ts",
		},
		Note: "智谱 / Z.ai 的 ZCode（zai-org/ZCode），官方仓库确认模型补全的 defaultHeaders：User-Agent: ZCode/<appVersion>、HTTP-Referer: https://zcode.z.ai、X-Title: Z Code@<source>（CLI 为 cli，desktop/app-server 为 electron）、X-ZCode-Agent: glm、X-Release-Channel、X-Client-Language/Timezone（Intl 取本机，这里给 zh-CN / Asia/Shanghai）与 X-Platform / X-Os-Category / X-Os-Version（process.platform-arch 与 os.release()）。X-Device-Mid 在真实客户端是首次生成后永久复用、存于 ~/.zcode/v2/telemetry-state.json 的 UUID（上面给固定示例值）。未写进预设：coding-plan 请求的签名头（x-client-ts/version/sig/nonce/pow/app-id，Ed25519 + 8 bit PoW，需服务端下发开关）、x-zcode-session-type、x-query-id、OpenRouter 归因头（仅 openrouter.ai 域名）。",
	},
	{
		ID: "qoder", Name: "Qoder", Confidence: confidencePartial,
		Headers: map[string]string{
			"user-agent":       "Go-http-client/2.0",
			"accept":           "text/event-stream",
			"accept-encoding":  "identity",
			"cache-control":    "no-cache",
			"cosy-data-policy": "AGREE",
			"cosy-clienttype":  "5",
			"cosy-clientip":    "169.254.198.161",
			"cosy-version":     "1.1.64",
			"login-version":    "v2",
			"x-model-source":   "system",
		},
		Dynamic: map[string]string{
			"cosy-date": "ts_s",
		},
		Evidence: []string{
			"https://github.com/cubk1/qoder2api/blob/master/src/main/java/us/cubk/BearerApiClient.java",
			"https://github.com/shuishuipingan/qoder2api-hub/blob/main/qoder_sign.py",
			"https://github.com/shuishuipingan/qoder2api-hub/blob/main/qoder_fingerprint.py",
		},
		Note: "Qoder（阿里，闭源，COSY 协议）没有官方源码，头来自两个互相独立的逆向项目（cubk1/qoder2api 的 Java 头表与 shuishuipingan/qoder2api-hub 的 Python 头表逐条一致），故标 partial。官方客户端是 Go 实现，user-agent 直接用 Go 默认的 Go-http-client/2.0；cosy-clientip 是客户端写死的占位 IP；cosy-version 取新版 qoder-agent-sdk 内置常量 1.1.64（旧实现为 0.1.43）。未写进预设的是整套签名与账号头：authorization: Bearer COSY.<payload>.<sig>（payload 为 base64 的紧凑 JSON，sig = md5(payload + cosyKey + date + body + path)）、cosy-key（RSA 加密的会话临时密钥）、cosy-user / cosy-machineid / cosy-machinetoken / cosy-machinetype（按账号 uid 稳定派生）——这些需要按 Qoder 的签名算法现算，网关按 key 注入的 Bearer 无法替代，仅靠本预设无法通过 Qoder 的鉴权。",
	},
	{
		ID: "kimi-code", Name: "Kimi Code", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":         "kimi-code-cli/2.1.1",
			"X-Msh-Platform":     "kimi_code_cli",
			"X-Msh-Version":      "2.1.1",
			"X-Msh-Device-Id":    "8c2f4b6a-1d3e-4f50-9a71-2b3c4d5e6f70",
			"X-Msh-Device-Name":  "DESKTOP-7F3K9Q2",
			"X-Msh-Device-Model": "Windows 10.0.26100 x64",
			"X-Msh-Os-Version":   "10.0.26100",
			"anthropic-version":  "2023-06-01",
		},
		Evidence: []string{
			"https://github.com/MoonshotAI/kimi-code/blob/main/packages/oauth/src/identity.ts",
			"https://github.com/MoonshotAI/kimi-code/blob/main/apps/kimi-code/src/constant/app.ts",
			"https://github.com/MoonshotAI/kimi-code/blob/main/packages/node-sdk/src/sdk-rpc-client-v2.ts",
		},
		Note: "月之暗面 Kimi Code CLI（MoonshotAI/kimi-code），官方源码确认 createKimiDefaultHeaders() 一次性产出 User-Agent: kimi-code-cli/<version> 与 6 个 X-Msh-* 头，并合并进 anthropic / openai / openai-responses / google-genai 四条协议路径的 defaultHeaders。X-Msh-Platform 固定 kimi_code_cli；X-Msh-Version 取 CLI 版本；X-Msh-Device-Model 与 X-Msh-Os-Version 由 os.release() 拼出（这里是 Windows 形态）；X-Msh-Device-Name 是 os.hostname()；X-Msh-Device-Id 在真实客户端首次启动生成并持久化到 <KIMI_CODE_HOME>/device_id（0600），上面给固定示例值。anthropic-version 由 Anthropic 官方 SDK 注入。另有 KIMI_CODE_CUSTOM_HEADERS 环境变量可追加头。",
	},
	{ID: "craft-agent", Name: "Craft Agent", Confidence: confidenceUnverified,
		Evidence: []string{
			"https://raw.githubusercontent.com/craft-ai-agents/craft-agents-oss/main/packages/core/package.json",
			"https://raw.githubusercontent.com/craft-ai-agents/craft-agents-oss/main/packages/pi-agent-server/package.json",
		},
		Note: "Craft Agent（craft-ai-agents/craft-agents-oss，@craft-agent/core 0.14.0）没有自有的模型 API 指纹：它不直连模型 API，而是内置两个 agent 运行时——@anthropic-ai/claude-agent-sdk@0.3.280 与 @earendil-works/pi-ai@0.87.1，链路上的头完全来自上游客户端。要模拟它请选 claude-code 或 pi 预设（走 Claude Agent SDK 时发 claude-cli/<版本> + x-app: cli；走 pi 时发 pi (<platform> <release>; <arch>)）。未定位到它是否另加自有 x-craft-* 头，故 headers 留空、不编造。",
	},
	{
		ID: "trae", Name: "Trae", Confidence: confidencePartial,
		Headers: map[string]string{
			"User-Agent":           "Trae/0.1.52",
			"Accept":               "text/event-stream",
			"X-App-Id":             "6eefa01c-1036-4c7e-9ca5-d891f63bfcd8",
			"X-App-Version":        "default",
			"X-App-Version-Code":   "20260811",
			"X-Ide-Version":        "0.1.52",
			"X-Ide-Version-Code":   "20260811",
			"X-Ide-Version-Type":   "stable",
			"X-Device-Type":        "windows",
			"X-OS-Version":         "Windows 11 Pro",
			"X-Device-Brand":       "83DG",
			"X-Device-Cpu":         "AMD",
			"X-System-Type":        "Windows",
			"Request-Traffic-Type": "prod",
			"X-User-Region":        "CN",
		},
		Evidence: []string{
			"https://github.com/connectedGraph/trae2api-web/blob/main/internal/upstream/headers.go",
			"https://github.com/autumnsentiment/Trae2api-cn/blob/main/src/trae_client.py",
		},
		Note: "Trae / TRAE SOLO（字节跳动，闭源 IDE）没有官方源码，头来自两个独立三方项目（trae2api-web 的 Go 头表标注「实测必须」、Trae2api-cn 的 Python build_headers()），两处 UA 规则与 X-App-Id 完全一致可交叉印证，但同一客户端不同版本/线路的头并不统一，故标 partial；这里按 SOLO 网关那一组做默认（UA Trae/0.1.52，IDE 线路为 Trae/3.3.67）。未写进预设的是账号与设备态：Authorization: Cloud-IDE-JWT <jwt>、X-Cloudide-Token / X-Ide-Token（同 JWT）、X-Uid、X-Machine-Id / X-Device-Id（设备指纹）——由网关按 key 注入或按需用自定义头补。注意字节开源的 bytedance/trae-agent 是另一款产品，不设任何自有 UA，不能代表 Trae IDE。",
	},
	{
		ID: "github-copilot", Name: "GitHub Copilot", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":                          "GitHubCopilotChat/0.34.0",
			"Editor-Version":                      "vscode/1.104.0",
			"Editor-Plugin-Version":               "copilot-chat/0.34.0",
			"Copilot-Language-Server-Version":     "1.408.0",
			"Copilot-Integration-Id":              "vscode-chat",
			"X-GitHub-Api-Version":                "2025-05-01",
			"Openai-Organization":                 "github-copilot",
			"OpenAI-Intent":                       "conversation-panel",
			"X-Initiator":                         "user",
			"X-Interaction-Type":                  "conversation-agent",
			"X-VSCode-User-Agent-Library-Version": "node-fetch",
		},
		Dynamic: map[string]string{
			"X-Request-Id": "uuid",
		},
		Evidence: []string{
			"https://raw.githubusercontent.com/microsoft/vscode-copilot-chat/main/src/platform/networking/common/networking.ts",
			"https://raw.githubusercontent.com/microsoft/vscode-copilot-chat/main/src/platform/networking/node/baseFetchFetcher.ts",
			"https://github.com/github/copilot.vim（release 分支内置 @github/copilot-language-server 1.408.0 打包产物）",
		},
		Note: "两处公开源码取证：(1) microsoft/vscode-copilot-chat 扩展的 CAPI 头——User-Agent: GitHubCopilotChat/<扩展版本>、X-Request-Id、OpenAI-Intent、X-GitHub-Api-Version、X-Interaction-Id/Type、X-Agent-Task-Id、X-Initiator、Copilot-Vision-Request，以及 X-VSCode-User-Agent-Library-Version: <fetcher 库名>；(2) copilot.vim 内置的 copilot-language-server 1.408.0 产物里 editorVersionHeaders 生成的 Editor-Version / Editor-Plugin-Version / Copilot-Language-Server-Version（格式均为 name/version）与 Copilot-Integration-Id（取值 vscode-chat / vscode-chat-dev / vscode-nl / code-oss / jetbrains-chat 等）。Editor-Version 与 Editor-Plugin-Version 随编辑器版本变化（上面取 VS Code 1.104.0 + copilot-chat 0.34.0）；X-Request-Id 每请求一个 UUID；鉴权 Authorization: Bearer <copilot_token> 由网关按渠道 key 注入。未写进预设的条件头：Copilot-Vision-Request: true（仅含图片的请求）、X-Interaction-Id / X-Agent-Task-Id（每轮交互与子代理）、VScode-SessionId / Vscode-MachineId（会话与设备态）、Request-Hmac（仅 dev 构建）。端点：api.githubcopilot.com、copilot-proxy.githubusercontent.com。",
	},
	{
		ID: "workbuddy", Name: "WorkBuddy", Confidence: confidencePartial,
		Headers: map[string]string{
			"User-Agent":       "WorkBuddy/5.5.6",
			"Accept":           "application/json",
			"X-Product":        "SaaS",
			"X-IDE-Type":       "WorkBuddy",
			"X-Requested-With": "XMLHttpRequest",
			"X-Agent-Intent":   "craft",
			"X-Agent-Type":     "main",
			"X-IDE-Name":       "WorkBuddy",
			"X-IDE-Version":    "5.5.6",
		},
		Dynamic: map[string]string{
			"X-Conversation-ID":         "rand32",
			"X-Conversation-Request-ID": "same:X-Conversation-ID",
			"X-Conversation-Message-ID": "rand32",
			"X-Request-ID":              "same:X-Conversation-Message-ID",
		},
		Evidence: []string{
			"https://unpkg.com/@magpie-community/opencode-workbuddy-auth@0.1.9/README.md",
			"https://unpkg.com/@magpie-community/opencode-workbuddy-auth@0.1.9/index.mjs",
		},
		Note: "WorkBuddy = 腾讯桌面 AI 编码 agent（出自 CodeBuddy 产品线），国内版 https://copilot.tencent.com/v2、国际版 https://www.workbuddy.ai/v2，对话是 OpenAI 兼容的 <endpoint>/chat/completions 且只接受流式。User-Agent 是服务端强校验项（非 WorkBuddy/<版本> 直接报 10085）；5.5.6 取自第三方插件常量，官方客户端版本可能更高，需跟随升级。上面的头是国际版（workbuddy-ai）并集；只模拟国内版请删掉 X-Requested-With 及其后的头。证据来源是公开 npm 包对官方客户端的复刻（非腾讯官方仓库），故标 partial；X-User-Id / X-Domain 属账号态、Authorization 属鉴权，均不写进预设。拉取模型清单（GET /v3/config）用另一个 UA：CLI/2.0.0 WorkBuddy/5.5.6。",
	},
	{
		ID: "cursor", Name: "Cursor", Confidence: confidenceUnverified,
		Headers: map[string]string{
			"User-Agent":                  "connect-es/1.6.1",
			"connect-protocol-version":    "1",
			"x-cursor-client-version":     "2.6.22",
			"x-cursor-client-type":        "ide",
			"x-cursor-client-os":          "linux",
			"x-cursor-client-arch":        "x64",
			"x-cursor-client-os-version":  "unknown",
			"x-cursor-client-device-type": "desktop",
			"x-cursor-timezone":           "UTC",
			"x-ghost-mode":                "false",
			"x-new-onboarding-completed":  "false",
		},
		Dynamic: map[string]string{
			"x-request-id": "uuid",
		},
		Evidence: []string{
			"https://github.com/standardagents/composer-api/blob/main/worker/cursor.ts",
			"https://github.com/R44VC0RP/cursor-opencode-auth/blob/main/RESEARCH.md",
		},
		Note: "未验证：Cursor 完全闭源，官方仓库与文档都不公开请求头，以下全部来自第三方逆向/中继项目的实测实现（standardagents/composer-api 的 cursorInternalHeaders() 与 R44VC0RP/cursor-opencode-auth 的 RESEARCH.md），故标 unverified。Content-Type: application/connect+proto 是它的真实取值，但本网关发的是 JSON，写进预设会把上游的请求体类型改坏，因此刻意不收录。同样未收录的还有整套鉴权与设备指纹头：Authorization: Bearer <jwt>（网关注入）、x-client-key（token 的 SHA-256）、x-cursor-checksum（base64url(6 字节混淆时间戳) + sha256Hex(machineId)）、x-cursor-config-version、x-session-id、x-amzn-trace-id: Root=<request-id>。已收录值的说明：x-cursor-client-os/arch/os-version 与 x-cursor-timezone 随运行环境变化（上面是第三方实现里的取值）；x-cursor-client-version 的真实当前取值未证实（2.6.22 取自第三方默认值，CLI 形态为 cli-2026.01.09-231024f 且必须与真实版本一致否则 permission_denied）；Cursor 的真实 User-Agent 无抓包证据（推测即 Connect 客户端库的 connect-es/1.6.1）。",
	},
	{
		ID: "deepseek-harness", Name: "DeepSeek Harness", Confidence: confidenceVerified,
		Headers: map[string]string{
			"user-agent":                 "deepseek-harness/0.2.0-rc.2 (+https://github.com/deepseek-ai/deepseek-harness)",
			"accept":                     "text/event-stream",
			"anthropic-version":          "2023-06-01",
			"anthropic-beta":             "files-api-2025-04-14",
			"x-deepseek-harness-user-id": "9f1c2a34-5b6d-4e7f-8a90-1b2c3d4e5f60",
		},
		Dynamic: map[string]string{
			"x-deepseek-harness-session-id": "ts_ms",
		},
		Evidence: []string{
			"D:\\Program Files\\DSH\\resources\\app.asar → /dsh/node_modules/@deepseek-ai/dsh-llm/lib/index.js（APP_IDENTITY/userAgent/attributionHeaders）",
			"D:\\Program Files\\DSH\\resources\\app.asar → /dsh/node_modules/@deepseek-ai/dsh-llm/package.json（version 0.2.0-rc.2）",
			"D:\\Program Files\\DSH\\resources\\app.asar → /dsh/node_modules/@deepseek-ai/dsh-llm-deepseek/lib/index.js（Messages 请求头拼装）",
			"D:\\Program Files\\DSH\\resources\\app.asar → /dsh/node_modules/@deepseek-ai/dsh-anonymous-user-id/lib/index.js（.anonymous-user-id）",
		},
		Note: "本机 DSH 安装包（asar）内取证：user-agent 是 DSH 所有 provider 请求强制携带的自有指纹，模板 deepseek-harness/<dsh-llm 包版本> (+https://github.com/deepseek-ai/deepseek-harness)，本机 dsh-llm 版本 0.2.0-rc.2，升级 DSH 后版本号随包变化。x-deepseek-harness-user-id 在真实客户端是每台机器随机生成一次并持久化在 $DSH_HOME/.anonymous-user-id 的 UUID v4（上面是示例形态，建议每个网关实例固定一个）。x-deepseek-harness-session-id 是会话 id 的十进制字符串（这里按毫秒时间戳生成，真实客户端无会话时不发该头）；x-deepseek-harness-compact: 1 只在上下文压缩请求时发送，故不进预设。anthropic-beta 为条件头：含 Files API file id 时为 files-api-2025-04-14，含 tool_addition/tool_removal 块时为 mid-conversation-tool-changes-2026-07-01（都没有则不发送）。DSH 不发 originator / x-app / HTTP-Referer / X-Title；鉴权（x-api-key 或 x-dsh-auth-token）由网关注入。",
	},
	{
		ID: "mimo-code", Name: "MiMo Code", Confidence: confidenceVerified,
		Headers: map[string]string{
			"User-Agent":     "mimocode/0.1.15",
			"anthropic-beta": "interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14",
		},
		Dynamic: map[string]string{
			"x-session-affinity": "uuid",
		},
		Evidence: []string{
			"https://github.com/XiaomiMiMo/MiMo-Code/blob/main/packages/cli/src/session/llm.ts",
			"https://github.com/XiaomiMiMo/MiMo-Code/blob/main/packages/cli/src/provider/provider.ts",
			"https://github.com/XiaomiMiMo/MiMo-Code/blob/main/packages/cli/src/installation/version.ts",
		},
		Note: "小米 MiMo Code（XiaomiMiMo/MiMo-Code，opencode 系衍生），官方源码确认模型请求最后硬覆盖 User-Agent: mimocode/<InstallationVersion>（构建期注入，未注入时为 mimocode/local；上面取 0.1.15），anthropic 提供商默认带 anthropic-beta: interleaved-thinking-2025-05-14,fine-grained-tool-streaming-2025-05-14，非临时请求带 x-session-affinity（会话 id，这里按每请求 UUID 生成）。条件头未写进预设：HTTP-Referer: https://mimo.xiaomi.com/coder/ 与 X-Title: mimocode / X-Source: mimocode / X-OpenRouter-Categories 只在 llmgateway、openrouter、nvidia、vercel 这类第三方网关提供商分支出现，自建小米 API 分支不带；x-parent-session-id 只在子代理请求带；另有按提供商特化的 UA（cloudflare-workers-ai / cloudflare-ai-gateway / gitlab-ai-provider 后缀）。",
	},
}
