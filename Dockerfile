# syntax=docker/dockerfile:1
# 构建阶段固定在宿主平台运行，按 TARGETARCH 交叉编译（多架构构建无需 qemu 模拟 Go 编译）
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
# git：构建阶段读取 .git 生成版本号（见下方 VERSION 逻辑）
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party ./third_party
ARG GOPROXY=https://goproxy.cn,direct
RUN GOPROXY=${GOPROXY} go mod download
# .dockerignore 已排除 data/、backups/、*.exe，.git 保留用于版本推导
COPY . .
# 版本号：默认自动从 .git 推导（git describe --tags --always：
# 有 tag 显示 tag，如 v1.2.3；无 tag 显示短提交哈希，如 6a53e28）；
# 可用 --build-arg VERSION=xxx 强制覆盖（如 CI 固定版本）；无 .git 时回落 dev。
# 不加 --dirty：Windows 换行符差异会让复制进容器的 .git 误报 dirty。
ARG VERSION=""
RUN git config --global --add safe.directory '*' && \
	if [ -z "$VERSION" ]; then VERSION="$(git describe --tags --always 2>/dev/null || true)"; fi && \
	: "${VERSION:=dev}" && \
	echo "==> build version: $VERSION" && \
	CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false \
	-ldflags="-s -w -X main.version=${VERSION}" -o /out/unigate .

FROM alpine:3.22
# su-exec：entrypoint 修正 /data 属主后降权运行（避免 bind mount 属主不匹配导致启动失败）
# 不强制 app 的 uid/gid（显式 -u 100 在部分 alpine 版本会与既有 ID 冲突构建失败），
# entrypoint 默认动态取 app 的实际 uid/gid
RUN apk add --no-cache ca-certificates su-exec && addgroup -S app && adduser -S -G app app
WORKDIR /app
COPY --from=builder /out/unigate /usr/local/bin/unigate
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# 防 Windows CRLF 混入：剔除 \r，否则 busybox sh 报 '\r' 未找到
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh && chmod +x /usr/local/bin/entrypoint.sh && mkdir -p /data && chown app:app /data
# 以 root 启动 entrypoint（chown /data 需 root），进程随后降权为 app
# 默认单端口：网关与 WebUI 同在 10010；如需分离管理面，加 WEBUI_PORT（如 10070）
ENV PORT=10010 DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 10010
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
