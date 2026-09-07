#!/usr/bin/env bash
# UniGate 容器升级脚本：拉取最新代码 → 重新构建镜像 → 重建容器（数据目录挂载保留）。
# 用法：在仓库目录执行 ./upgrade.sh [镜像标签]，默认镜像 unigate:latest。
# 前置要求：docker、git；数据持久化在 ./data（docker-compose.yml 默认挂载），升级不影响。
set -euo pipefail

IMAGE="${1:-unigate:latest}"
CONTAINER=unigate
COMPOSE_FILE=docker-compose.yml

cd "$(dirname "$0")"

command -v docker >/dev/null 2>&1 || { echo "错误：未安装 docker"; exit 1; }

# 有 compose 插件且存在 compose 文件时走 compose 路径
use_compose=false
if docker compose version >/dev/null 2>&1 && [ -f "$COMPOSE_FILE" ]; then
    use_compose=true
fi

echo "==> 1/4 拉取最新代码"
if [ -d .git ]; then
    git pull --ff-only
else
    echo "警告：当前目录不是 git 仓库，跳过拉取，将用现有源码构建"
fi

# 升级前备份数据目录（gateway.json、usage.db 等一律向后兼容，备份仅作保险）
backup_data() {
    if [ -d data ]; then
        local dest="backups/data-$(date +%Y%m%d-%H%M%S)"
        echo "==> 备份数据目录 → $dest"
        mkdir -p backups
        cp -a data "$dest" || echo "警告：备份失败，继续升级"
    fi
}

if [ "$use_compose" = true ]; then
    echo "==> 2/4 重新构建镜像并重建容器（docker compose）"
    backup_data
    docker compose up -d --build
    echo "==> 3/4 清理悬空旧镜像"
    docker image prune -f >/dev/null 2>&1 || true
    echo "==> 4/4 查看启动日志（确认出现 \"unigate listening on :8080\"，Ctrl+C 退出）"
    docker compose logs -f --tail=50
else
    echo "==> 2/4 构建镜像 $IMAGE（docker build）"
    backup_data
    docker build -t "$IMAGE" .
    echo "==> 3/4 重建容器"
    # 容器已存在则先删除（数据都在宿主机 ./data，删容器不丢数据）
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    # 沿用 compose 的默认配置：8080 端口、./data 挂载、unless-stopped
    docker run -d --name "$CONTAINER" \
        --restart unless-stopped \
        -p 8080:8080 \
        -v "$(pwd)/data:/data" \
        "$IMAGE"
    echo "==> 4/4 启动日志"
    docker logs -f --tail 50 "$CONTAINER"
fi
