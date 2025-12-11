# --- base image ---
FROM postgres:17-bookworm AS base

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && update-ca-certificates \
    && sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
    && apt-get update

# --- pgrx builder base image
FROM base AS pgrx_builder_base

ENV DEBIAN_FRONTEND=noninteractive
ENV PG_MAJOR=17

RUN apt-get -qy install curl gnupg apt-transport-https \
    libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev \
    libssl-dev libxml2-utils xsltproc ccache \
    build-essential git pkg-config libssl-dev clang libclang-dev \
    postgresql-server-dev-$PG_MAJOR postgresql-common \
    postgresql-common-dev protobuf-compiler libprotobuf-dev \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# --- etcd_fdw image ---
FROM pgrx_builder_base AS etcd_fdw_builder

ENV CARGO_PGRX_VERSION=0.16.1
ENV PATH="/root/.cargo/bin:${PATH}"

RUN . $HOME/.cargo/env \
    && rustup component add clippy rustfmt rust-src \
    && cargo install --force --locked cargo-pgrx@"${CARGO_PGRX_VERSION}" \
    && cargo pgrx init --pg$PG_MAJOR $(which pg_config)
