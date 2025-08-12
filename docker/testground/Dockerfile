#FROM rust:1-buster AS builder
FROM ubuntu:24.04 AS base
RUN apt update && apt install -y \
  binutils \
  iproute2 \
  clang \
  curl \
  make \
  ca-certificates \
  pkg-config \
  gnupg \
  software-properties-common \
  wget \
  git \
  python-is-python3 \
  cgroup-tools \
  libstdc++-13-dev \
  gcc-13 \
  g++-13

RUN apt update && apt install -y \
  libboost-atomic1.83.0 \
  libboost-container1.83.0 \
  libboost-fiber1.83.0 \
  libboost-filesystem1.83.0 \
  libboost-graph1.83.0 \
  libboost-json1.83.0 \
  libboost-regex1.83.0 \
  libboost-stacktrace1.83.0

RUN apt update && apt install -y \
  libabsl-dev \
  libarchive-dev \
  libbenchmark-dev \
  libbrotli-dev \
  libcap-dev \
  libcgroup-dev \
  libcli11-dev \
  libgmock-dev \
  libgmp-dev \
  libgtest-dev \
  libmimalloc-dev \
  libtbb-dev \
  liburing-dev \
  libzstd-dev

FROM base AS builder

RUN apt update && apt install -y \
  cmake \
  clang \
  libssl-dev

RUN apt update && apt install -y \
  libboost-fiber1.83-dev \
  libboost-graph1.83-dev \
  libboost-json1.83-dev \
  libboost-stacktrace1.83-dev \
  libboost1.83-dev

# install cargo
ARG CARGO_ROOT="/root/.cargo"
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="${CARGO_ROOT}/bin:$PATH"
RUN rustup toolchain install 1.85.0-x86_64-unknown-linux-gnu

WORKDIR /usr/src/monad-bft
RUN apt update
RUN apt install -y clang

# Builder
COPY . .
RUN cargo build --release --bin monad-testground && \
    mv target/release/monad-testground testground
RUN cp monad-scripts/testground/topology-gen.py .
RUN cp monad-scripts/testground/tc-gen.py .
RUN cp monad-scripts/testground/run_testground.sh .

# Runner
#FROM debian:buster-slim
FROM ubuntu:24.04
SHELL ["/bin/bash", "-c"]
WORKDIR /usr/src/monad-bft

RUN apt update
RUN apt install -y ethtool iproute2 net-tools python3 tcpdump
RUN apt clean
COPY --from=builder /usr/src/monad-bft/testground /usr/local/bin/monad-testground
COPY --from=builder /usr/src/monad-bft/topology-gen.py .
COPY --from=builder /usr/src/monad-bft/tc-gen.py .
COPY --from=builder /usr/src/monad-bft/run_testground.sh .
RUN chmod u+x run_testground.sh
CMD ./run_testground.sh -l -n "5,5,5,5,5" -d INFO -s 300
