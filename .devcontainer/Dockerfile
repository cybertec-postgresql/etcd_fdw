FROM rust:1-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    bison \
    flex \
    clang \
    protobuf-compiler \
    libreadline8 \
    libreadline-dev \
    git \
    curl \
    pkg-config \
    libssl-dev \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for running PostgreSQL
RUN useradd -m -s /bin/bash -u 1000 vscode && \
    echo "vscode ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Switch to non-root user
USER vscode

# Set up Rust and cargo for vscode user
ENV PATH="/home/vscode/.cargo/bin:${PATH}"
ENV CARGO_HOME="/home/vscode/.cargo"

# Install pgrx as vscode user
ENV CARGO_PGRX_VERSION=0.16.1
RUN cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}"

# Set working directory
WORKDIR /workspace

# Keep container running
CMD ["sleep", "infinity"]
