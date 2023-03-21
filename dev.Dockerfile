FROM ghcr.io/bento-platform/bento_base_image:python-debian-2023.02.27 AS base-deps

SHELL ["/bin/bash", "-c"]

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell
# Then, bootstrap dependencies for setting up and running the Python application
RUN apt-get update -y && \
    apt-get install -y samtools tabix bcftools curl jq openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/* && \
    source /env/bin/activate && \
    pip install --no-cache-dir gunicorn==20.1.0 "pysam>=0.20.0,<0.21.0"

WORKDIR /
ENV CROMWELL_VERSION=84
RUN curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar

FROM base-deps AS install

# Backwards-compatible with old BentoV2 container layout
RUN mkdir -p /wes/tmp && mkdir -p /data
WORKDIR /wes

COPY pyproject.toml .
COPY poetry.toml .
COPY poetry.lock .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry install --no-root

# Copy in the entrypoint & run script so we have somewhere to start
COPY entrypoint.bash .
COPY run.dev.bash .

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.dev.bash" ]
