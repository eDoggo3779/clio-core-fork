# Mofka Development Container
# Installs Mofka via Spack with mochi-spack-packages
#
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV SPACK_ROOT=/opt/spack
ENV PATH="${SPACK_ROOT}/bin:${PATH}"

# ── System dependencies ──────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    file \
    g++ \
    gcc \
    gfortran \
    git \
    gnupg2 \
    iproute2 \
    locales \
    lsb-release \
    make \
    openssh-client \
    patch \
    patchelf \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    sudo \
    tar \
    unzip \
    vim \
    wget \
    xz-utils \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Set locale
RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# ── Create user ──────────────────────────────────────────────────────
ARG HOST_UID=1000
ARG HOST_GID=1000
RUN groupadd -g ${HOST_GID} mofka-dev && \
    useradd -m -u ${HOST_UID} -g ${HOST_GID} -s /bin/bash mofka-dev && \
    echo "mofka-dev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# ── Install Spack (pinned release) ──────────────────────────────────
RUN git clone --depth=1 --branch=v0.22.2 \
    https://github.com/spack/spack.git ${SPACK_ROOT} && \
    chown -R mofka-dev:mofka-dev ${SPACK_ROOT}

# ── Clone mochi-spack-packages ──────────────────────────────────────
RUN git clone https://github.com/mochi-hpc/mochi-spack-packages.git \
    /opt/mochi-spack-packages && \
    chown -R mofka-dev:mofka-dev /opt/mochi-spack-packages

# ── Create the spack environment directory ──────────────────────────
RUN mkdir -p /opt/spack-env && \
    chown -R mofka-dev:mofka-dev /opt/spack-env

# Copy spack.yaml into the environment directory
COPY --chown=mofka-dev:mofka-dev spack.yaml /opt/spack-env/spack.yaml

# ── Build Mofka (as mofka-dev to avoid root permission issues) ──────
USER mofka-dev

# Initialize spack for this shell, create env, and install
RUN . ${SPACK_ROOT}/share/spack/setup-env.sh && \
    spack env activate /opt/spack-env && \
    spack concretize -f 2>&1 | tail -50 && \
    spack install --fail-fast -j$(nproc) 2>&1 | tail -100

# ── Generate env.sh ─────────────────────────────────────────────────
COPY --chown=mofka-dev:mofka-dev env.sh /opt/spack-env/env.sh

# Add env.sh sourcing to bashrc
RUN echo '' >> /home/mofka-dev/.bashrc && \
    echo '# Activate Mofka Spack environment' >> /home/mofka-dev/.bashrc && \
    echo 'source /opt/spack-env/env.sh' >> /home/mofka-dev/.bashrc

WORKDIR /workspace
CMD ["/bin/bash"]
