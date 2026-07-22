# IOWarp + Mofka Combined Development Container
# Inherits from deps-cpu (IOWarp dependencies), then adds Mofka via Spack
#
FROM iowarp/deps-cpu:latest
USER root
RUN userdel -r ubuntu || true

# The deps-cpu image already includes:
# - All apt + source-built dependencies (boost, hdf5, yaml-cpp, zeromq, cereal, etc.)
# - Compression libraries (zlib, bzip2, lzo, zstd, lz4, xz, brotli, snappy, c-blosc2)
# - Lossy compression (ZFP, SZ3, FPZIP, LibPressio)
# - ADIOS2 with HDF5 and ZeroMQ support
# - Docker-in-Docker support
# - Jarvis (IOWarp runtime deployment tool)
# - MPI (OpenMPI)
# - libaio (for bdev ChiMod)

# Remap iowarp user UID/GID to match the host user.
# This avoids file permission issues with bind-mounted volumes.
# Override at build time: --build-arg HOST_UID=$(id -u) --build-arg HOST_GID=$(id -g)
ARG HOST_UID=1000
ARG HOST_GID=1000

USER root
# Ubuntu 24.04 ships a default "ubuntu" user at UID 1000 which blocks
# updateRemoteUserUID and manual UID remapping (see
# https://github.com/microsoft/vscode-remote-release/issues/10030).
# Move it out of the way so iowarp can claim the host user's UID.
RUN if id ubuntu >/dev/null 2>&1; then \
      usermod -u 59999 ubuntu && \
      groupmod -g 59999 ubuntu; \
    fi && \
    OLD_UID=$(id -u iowarp) && OLD_GID=$(id -g iowarp) && \
    if [ "${HOST_UID}" != "${OLD_UID}" ] || [ "${HOST_GID}" != "${OLD_GID}" ]; then \
      groupmod -o -g "${HOST_GID}" iowarp && \
      usermod  -o -u "${HOST_UID}" -g "${HOST_GID}" iowarp && \
      chown -R "${HOST_UID}:${HOST_GID}" /home/iowarp; \
    fi

# ── Additional system packages needed by Spack ─────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    file \
    gfortran \
    patchelf \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# ── Install Spack (pinned release) ─────────────────────────────────
ENV SPACK_ROOT=/opt/spack
ENV PATH="${SPACK_ROOT}/bin:${PATH}"

RUN git clone --depth=1 --branch=v0.22.2 \
    https://github.com/spack/spack.git ${SPACK_ROOT} && \
    chown -R iowarp:iowarp ${SPACK_ROOT}

# ── Clone mochi-spack-packages ─────────────────────────────────────
RUN git clone https://github.com/mochi-hpc/mochi-spack-packages.git \
    /opt/mochi-spack-packages && \
    chown -R iowarp:iowarp /opt/mochi-spack-packages

# ── Create the spack environment directory ─────────────────────────
RUN mkdir -p /opt/spack-env && \
    chown -R iowarp:iowarp /opt/spack-env

# Copy spack.yaml into the environment directory
COPY --chown=iowarp:iowarp spack.yaml /opt/spack-env/spack.yaml

# ── Build Mofka (as iowarp to avoid root permission issues) ────────
USER iowarp

# Initialize spack for this shell, create env, and install
RUN . ${SPACK_ROOT}/share/spack/setup-env.sh && \
    spack env activate /opt/spack-env && \
    spack concretize -f 2>&1 | tail -50 && \
    spack install --fail-fast -j$(nproc) 2>&1 | tail -100

# ── Copy env.sh for Mofka environment activation ──────────────────
COPY --chown=iowarp:iowarp env.sh /opt/spack-env/env.sh

# Add Mofka env.sh sourcing to bashrc (after IOWarp's existing setup)
RUN echo '' >> /home/iowarp/.bashrc && \
    echo '# Activate Mofka Spack environment' >> /home/iowarp/.bashrc && \
    echo 'source /opt/spack-env/env.sh' >> /home/iowarp/.bashrc

WORKDIR /workspace

# Install Claude Code for AI-assisted development
RUN curl -fsSL https://claude.ai/install.sh | bash

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/bin/bash"]
