FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest AS base-deps

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell
RUN apt-get update -y && \
    apt-get install -y samtools tabix bcftools curl jq openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/*

# Boostrap dependencies for setting up and running the Python application
RUN pip install --no-cache-dir poetry==1.3.2 gunicorn==20.1.0 "pysam>=0.20.0,<0.21.0"

WORKDIR /
ENV CROMWELL_VERSION=84
RUN curl \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar

FROM base-deps AS install

# Backwards-compatible with old BentoV2 container layout
RUN mkdir -p /wes/tmp && mkdir -p /data
WORKDIR /wes

COPY pyproject.toml pyproject.toml
COPY poetry.toml poetry.toml
COPY poetry.lock poetry.lock

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --no-root

CMD [ "bash", "./entrypoint.dev.bash" ]
