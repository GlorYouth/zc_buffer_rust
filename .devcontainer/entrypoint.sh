#!/bin/sh
set -e # 如果任何命令失败，立即退出脚本

# 从环境变量获取 CARGO_HOME (应由 Dockerfile 设置)
if [ -z "$CARGO_HOME" ]; then
  echo "错误: CARGO_HOME 环境变量未设置。" >&2
  exit 1
fi

# 目标用户和组 (应与 Dockerfile 中 adduser/addgroup 创建的一致)
TARGET_USER="devuser"
TARGET_GROUP="devgroup" # chown 时使用，su-exec 会使用用户的主组

echo "Entrypoint: 正在检查并设置 Cargo 目录权限..."

# 确保 Cargo registry 目录存在并具有正确的所有权
mkdir -p "$CARGO_HOME/registry"
chown -R "$TARGET_USER:$TARGET_GROUP" "$CARGO_HOME/registry"
echo "Entrypoint: 已为 $CARGO_HOME/registry 设置所有权。"

# 确保 Cargo git 依赖目录存在并具有正确的所有权
mkdir -p "$CARGO_HOME/git"
chown -R "$TARGET_USER:$TARGET_GROUP" "$CARGO_HOME/git"
echo "Entrypoint: 已为 $CARGO_HOME/git 设置所有权。"

# 处理项目 target 目录的权限 (如果它作为一个具名卷挂载)
PROJECT_WORKDIR="/home/devuser/project"
PROJECT_TARGET_DIR="$PROJECT_WORKDIR/target"

if [ -d "$PROJECT_TARGET_DIR" ]; then
    echo "Entrypoint: 正在为项目 target 目录 ($PROJECT_TARGET_DIR) 设置所有权..."
    chown -R "$TARGET_USER:$TARGET_GROUP" "$PROJECT_TARGET_DIR"
    echo "Entrypoint: 已为 $PROJECT_TARGET_DIR 设置所有权。"
else
    echo "Entrypoint: 项目 target 目录 ($PROJECT_TARGET_DIR) 未找到，跳过权限设置 (cargo build 应该会创建它)。"
fi

echo "Entrypoint: 权限设置完成。将以用户 $TARGET_USER 执行命令: $@"

# 使用 su-exec 切换到 TARGET_USER 并执行传递给 entrypoint 的 CMD
# "$@" 代表 Dockerfile 中的 CMD 或 docker run/compose 中指定的命令
exec su-exec "$TARGET_USER" "$@"
