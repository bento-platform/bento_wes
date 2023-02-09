FROM --platform=$BUILDPLATFORM debian:bullseye-slim AS downloaded-deps

# Install VCF2MAF
# TODO: I don't like /opt as a home for these

WORKDIR /tmp/vcf2maf
ENV VCF2MAF_VERSION=1.6.21
RUN apt-get update -y && \
    apt-get install -y curl git unzip && \
    echo "https://github.com/mskcc/vcf2maf/archive/refs/tags/v${VCF2MAF_VERSION}.zip" && \
    curl -L "https://github.com/mskcc/vcf2maf/archive/refs/tags/v${VCF2MAF_VERSION}.zip" -o vcf2maf.zip && \
    unzip vcf2maf.zip && \
    mv "vcf2maf-${VCF2MAF_VERSION}" vcf2maf && \
    mkdir -p /opt/data && \
    cp vcf2maf/*.pl /opt && \
    cp -r vcf2maf/data /opt/data && \
    rm -rf vcf2maf

# Install Cromwell
ENV CROMWELL_VERSION=84
WORKDIR /
RUN curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar

# Clone (but don't install yet) Ensembl-VEP
ENV VEP_ENSEMBL_RELEASE_VERSION=104.3
RUN git clone --depth 1 -b "release/${VEP_ENSEMBL_RELEASE_VERSION}" https://github.com/Ensembl/ensembl-vep.git

FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.02.09 AS base-deps

# Copy Ensembl-VEP from downloaded-deps
COPY --from=downloaded-deps /ensembl-vep /ensembl-vep

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell
# Perl/libdbi-perl/lib*-dev/cpanminus/unzip are for cBioPortal scripts / caches / utilities
RUN apt-get update -y && \
    apt-get install -y \
        samtools \
        tabix \
        bcftools \
        curl \
        jq \
        openjdk-17-jre \
        perl \
        libdbi-perl \
        libperl-dev \
        cpanminus \
        unzip \
        libbz2-dev \
        liblzma-dev \
        zlib1g-dev \
    && \
    rm -rf /var/lib/apt/lists/*

# Boostrap dependencies for setting up and running the Python application
RUN pip install --no-cache-dir poetry==1.3.2 gunicorn==20.1.0 "pysam>=0.20.0,<0.21.0"

# Install Ensembl-VEP from cloned source
WORKDIR /
RUN cpanm --installdeps --with-recommends --notest --cpanfile ensembl-vep/cpanfile . && \
    cd ensembl-vep && \
    # Build vep in /ensembl-vep
    perl INSTALL.pl -a a --NO_TEST --NO_UPDATE

# Install Ensembl-VEP
ENV VEP_ENSEMBL_RELEASE_VERSION=104.3
WORKDIR /
RUN git clone --depth 1 -b "release/${VEP_ENSEMBL_RELEASE_VERSION}" https://github.com/Ensembl/ensembl-vep.git && \
    cpanm --installdeps --with-recommends --notest --cpanfile ensembl-vep/cpanfile . && \
    cd ensembl-vep && \
    # Build vep in /ensembl-vep
    perl INSTALL.pl -a a --NO_TEST --NO_UPDATE

FROM base-deps AS build-install

# Copy VCF2MAF
COPY --from=downloaded-deps /opt /opt

# Copy Cromwell
COPY --from=downloaded-deps /cromwell.jar /cromwell.jar

# Backwards-compatible with old BentoV2 container layout
RUN mkdir -p /wes/tmp && mkdir -p /data
WORKDIR /wes

COPY pyproject.toml pyproject.toml
COPY poetry.toml poetry.toml
COPY poetry.lock poetry.lock

# Install production dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --without dev --no-root

# Manually copy only what's relevant
# (Don't use .dockerignore, which allows us to have development containers too)
COPY bento_wes bento_wes
COPY entrypoint.bash entrypoint.bash
COPY LICENSE LICENSE
COPY README.md README.md

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

CMD [ "bash", "./entrypoint.bash" ]
