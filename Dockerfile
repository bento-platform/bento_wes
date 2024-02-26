FROM --platform=$BUILDPLATFORM debian:bullseye-slim AS downloaded-deps

SHELL ["/bin/bash", "-c"]

# Install VCF2MAF
# TODO: I don't like /opt as a home for these

WORKDIR /tmp/vcf2maf
ENV VCF2MAF_VERSION=1.6.21
RUN apt-get update -y && \
    apt-get install -y curl git unzip wget && \
    echo "https://github.com/mskcc/vcf2maf/archive/refs/tags/v${VCF2MAF_VERSION}.zip" && \
    curl -L "https://github.com/mskcc/vcf2maf/archive/refs/tags/v${VCF2MAF_VERSION}.zip" -o vcf2maf.zip && \
    unzip vcf2maf.zip && \
    mv "vcf2maf-${VCF2MAF_VERSION}" vcf2maf && \
    mkdir -p /opt/data && \
    cp vcf2maf/*.pl /opt && \
    cp -r vcf2maf/data /opt/data && \
    rm -rf vcf2maf

# Install Cromwell
ENV CROMWELL_VERSION=86
WORKDIR /
RUN curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar

# Clone (but don't install yet) Ensembl-VEP
ENV VEP_ENSEMBL_RELEASE_VERSION=111.0
RUN git clone --depth 1 -b "release/${VEP_ENSEMBL_RELEASE_VERSION}" https://github.com/Ensembl/ensembl-vep.git && \
    chmod u+x ensembl-vep/*.pl

# Clone ensembl-variation git repository
WORKDIR /ensembl-vep/
RUN git clone --depth 1 https://github.com/Ensembl/ensembl-variation.git && \
    mkdir var_c_code && \
    cp ensembl-variation/C_code/*.c ensembl-variation/C_code/Makefile var_c_code/
RUN git clone --depth 1 https://github.com/bioperl/bioperl-ext.git
RUN curl -L https://github.com/Ensembl/ensembl-xs/archive/2.3.2.zip -o ensembl-xs.zip && \
    unzip -q ensembl-xs.zip && \
    mv ensembl-xs-2.3.2 ensembl-xs && \
    rm -rf ensembl-xs.zip

WORKDIR /

FROM ghcr.io/bento-platform/bento_base_image:python-debian-2024.02.01 AS base-deps

SHELL ["/bin/bash", "-c"]

WORKDIR /

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell

RUN apt-get update -y && \
    apt-get install -y \
        samtools \
        tabix \
        bcftools \
        curl \
        jq \
        openjdk-17-jre \
    && \
    rm -rf /var/lib/apt/lists/*

# Install system packages for VEP
# Perl/libdbi-perl/lib*-dev/cpanminus/unzip are for cBioPortal scripts / caches / utilities
RUN apt-get update -y && \
    apt-get install -y \
        curl \
        perl \
        libdbd-mysql-perl \
        libdbi-perl \
        libjson-perl \
        libperl-dev \
        cpanminus \
        unzip \
        libbz2-dev \
        liblzma-dev \
        zlib1g-dev \
    && \
    rm -rf /var/lib/apt/lists/*

# Install Perl packages for VEP
RUN curl "https://raw.githubusercontent.com/Ensembl/ensembl/release/111/cpanfile" -o "ensembl_cpanfile" && \
    curl "https://raw.githubusercontent.com/Ensembl/ensembl-vep/release/111/cpanfile" -o "ensembl_vep_cpanfile" && \
    cpanm --installdeps --with-recommends --notest --cpanfile ensembl_cpanfile . && \
    cpanm --installdeps --with-recommends --notest --cpanfile ensembl_vep_cpanfile . && \
    rm ensembl_cpanfile ensembl_vep_cpanfile && \
    rm -rf /root/.cpanm

# Then, install dependencies for running the Python server + Python workflow dependencies
COPY container.requirements.txt .
RUN pip install --no-cache-dir -r /container.requirements.txt && \
    rm /container.requirements.txt

FROM base-deps AS build-install

# Backwards-compatible with old BentoV2 container layout
RUN mkdir -p /wes/tmp && mkdir -p /data
WORKDIR /wes

COPY pyproject.toml .
COPY poetry.lock .

# Install production dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry install --without dev --no-root

# Manually copy only what's relevant
# (Don't use .dockerignore, which allows us to have development containers too)
COPY bento_wes bento_wes
COPY entrypoint.bash .
COPY run.bash .
COPY LICENSE .
COPY README.md .

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

# Copy from other stages last, since it means the stages can be built in parallel

# - Copy VCF2MAF
COPY --from=downloaded-deps /opt /opt

# - Copy Cromwell
COPY --from=downloaded-deps /cromwell.jar /cromwell.jar

# - Copy Ensembl-VEP
COPY --from=ensemblorg/ensembl-vep:release_111.0 /opt/vep /opt/vep

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.bash" ]
