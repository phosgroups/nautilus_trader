# syntax=docker/dockerfile:1.7
#
# Build a Python 3.14 wheel for the current checkout:
#
#   docker buildx build \
#     -f .docker/build-wheel.dockerfile \
#     --target wheel \
#     --output type=local,dest=dist \
#     .
#
# The wheel will be written to dist/.

ARG RUST_IMAGE=public.ecr.aws/docker/library/rust:1.97.1-slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.33@sha256:77280f2f771df71f90786c314fe1bbc1e023feac652969bbf139c280babf2eb7

FROM ${UV_IMAGE} AS uv

FROM ${RUST_IMAGE} AS wheel-builder

ARG PYTHON_VERSION=3.14
ARG BUILD_MODE=release

ENV DEBIAN_FRONTEND=noninteractive \
    BUILD_MODE=${BUILD_MODE} \
    CARGO_INCREMENTAL=0 \
    RUST_BACKTRACE=1 \
    CC=clang \
    CXX=clang++ \
    LDSHARED="clang -shared" \
    UV_LINK_MODE=copy \
    UV_MANAGED_PYTHON=1 \
    UV_PYTHON=${PYTHON_VERSION} \
    VIRTUAL_ENV=/opt/nautilus-python \
    CAPNP_PREFIX=/opt/capnp \
    LD_LIBRARY_PATH=/opt/capnp/lib \
    PKG_CONFIG_PATH=/opt/capnp/lib/pkgconfig \
    PATH="/opt/capnp/bin:/opt/nautilus-python/bin:/usr/local/cargo/bin:/usr/local/bin:${PATH}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      clang \
      curl \
      git \
      lld \
      make \
      pkg-config \
      xz-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/

WORKDIR /workspace

COPY scripts/install-capnp.sh scripts/tool-version.sh scripts/
COPY tools.toml ./

RUN bash scripts/install-capnp.sh

RUN --mount=type=cache,target=/root/.cache/uv \
    uv python install "${PYTHON_VERSION}" && \
    uv venv --managed-python --python "${PYTHON_VERSION}" "${VIRTUAL_ENV}" && \
    python --version && \
    uv --version && \
    rustc --version && \
    capnp --version

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    --mount=type=cache,target=/root/.cache/uv \
    PYTHON_LIB_DIR="$("${VIRTUAL_ENV}/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')" && \
    PYO3_PYTHON="${VIRTUAL_ENV}/bin/python" \
    LD_LIBRARY_PATH="/opt/capnp/lib:${PYTHON_LIB_DIR}" \
      uv build --wheel \
        --python "${VIRTUAL_ENV}/bin/python" \
        --managed-python

FROM scratch AS wheel

COPY --from=wheel-builder /workspace/dist/ /
