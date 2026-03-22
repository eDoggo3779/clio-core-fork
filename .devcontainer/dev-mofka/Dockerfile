FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies required by Spack
RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    file \
    g++ \
    gcc \
    gfortran \
    git \
    gnupg2 \
    make \
    python3 \
    python3-pip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Clone and set up Spack
RUN git clone --depth=2 https://github.com/spack/spack.git /opt/spack
ENV SPACK_ROOT=/opt/spack
ENV PATH="${SPACK_ROOT}/bin:${PATH}"

# Create a Spack environment and install Mofka
RUN spack env create mofka_env \
    && spack env activate mofka_env \
    && git clone https://github.com/mochi-hpc/mochi-spack-packages.git /opt/mochi-spack-packages \
    && spack repo add /opt/mochi-spack-packages \
    && spack add mofka \
    && spack install

# Activate the Spack environment on shell startup
RUN echo '. /opt/spack/share/spack/setup-env.sh && spack env activate mofka_env' >> /etc/bash.bashrc
