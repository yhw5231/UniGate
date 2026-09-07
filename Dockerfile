# syntax=docker/dockerfile:1
# 构建阶段固定在宿主平台运行，按 TARGETARCH 交叉编译（多架构构建无需 qemu 模拟 Go 编译）
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
COPY third_party ./third_party
ARG GOPROXY=https://goproxy.cn,direct
RUN GOPROXY=${GOPROXY} go mod download
COPY *.go ./
COPY web ./web
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath \
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
