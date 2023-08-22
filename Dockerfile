FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.08.16 AS base-deps

SHELL ["/bin/bash", "-c"]

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell
# Then, boostrap dependencies for setting up and running the Python application
RUN apt-get update -y && \
    apt-get install -y samtools tabix bcftools curl jq openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir poetry==1.3.2 gunicorn==21.2.0 "pysam>=0.21.0,<0.22.0"

WORKDIR /
ENV CROMWELL_VERSION=85
RUN curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar

FROM base-deps AS build-install

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
COPY entrypoint.bash .
COPY run.bash .
COPY LICENSE .
COPY README.md .

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.bash" ]
