# 使用最新的 Alpine Linux 作为基础镜像
FROM alpine:latest

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive

# 安装依赖
RUN apk add --no-cache \
    curl \
    bash \
    build-base \
    pkgconf \
    git \
    openssl-dev \
    # libressl-dev \
    ca-certificates

# 创建用户和组 (作为 root)
RUN addgroup -S devgroup && adduser -S -G devgroup -h /home/devuser -s /bin/ash devuser

# 设置 Rustup 和 Cargo 的环境变量
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    LD_LIBRARY_PATH=/usr/local/cargo/lib

# 下载并安装 rustup (作为 root)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
    -y --default-toolchain stable --profile minimal --no-modify-path

# 安装 rust-analyzer (作为 root)
RUN "$CARGO_HOME/bin/rustup" component add rust-analyzer

# 对镜像内的 RUSTUP_HOME 和 CARGO_HOME 设置基础所有权 (可选，但良好实践)
# 主要的权限修复现在由 entrypoint 在运行时处理卷挂载点
RUN chown -R devuser:devgroup "$RUSTUP_HOME" "$CARGO_HOME" /home/devuser

# 复制 entrypoint 脚本到镜像中并赋予执行权限
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# 设置 entrypoint
# entrypoint 脚本将默认以 root 用户运行
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# USER devuser # <--- 此行被移除或注释掉，entrypoint 会使用 gosu 切换用户

# 设置默认工作目录 (当 entrypoint 切换到 devuser 后会使用此目录)
WORKDIR /home/devuser/project

# 默认命令，将作为参数传递给 entrypoint 脚本 (例如 "$@")
CMD ["sleep", "infinity"]