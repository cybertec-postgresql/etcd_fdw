FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PG_MAJOR=17

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg apt-transport-https \
        build-essential git pkg-config \
        libreadline-dev zlib1g-dev flex bison \
        libxml2-dev libxslt-dev libssl-dev \
        libxml2-utils xsltproc ccache \
        clang libclang-dev \
        protobuf-compiler libprotobuf-dev \
        sudo \
    && update-ca-certificates \
    && sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-$PG_MAJOR \
        postgresql-server-dev-$PG_MAJOR \
        postgresql-common \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create vscode user with sudo access
RUN groupadd --gid 1000 vscode \
    && useradd --uid 1000 --gid 1000 -m -s /bin/bash vscode \
    && echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode \
    && chown -R vscode:vscode /usr/share/postgresql /usr/lib/postgresql

# Switch to vscode user
USER vscode
WORKDIR /home/vscode

# Set up Rust environment for vscode user
ENV RUSTUP_HOME=/home/vscode/.rustup
ENV CARGO_HOME=/home/vscode/.cargo
ENV PATH="/home/vscode/.cargo/bin:/usr/lib/postgresql/$PG_MAJOR/bin:${PATH}"

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable \
    && . "$CARGO_HOME/env" \
    && rustup component add clippy rustfmt rust-src

# Install cargo-pgrx
ENV CARGO_PGRX_VERSION=0.16.1
RUN . "$CARGO_HOME/env" \
    && cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}"

# Initialize pgrx
RUN . "$CARGO_HOME/env" \
    && cargo pgrx init --pg$PG_MAJOR $(which pg_config)

