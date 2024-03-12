FROM ghcr.io/bento-platform/bento_base_image:python-debian-2024.03.01 AS base-deps

LABEL org.opencontainers.image.description="Local development image for Bento WES."
LABEL devcontainer.metadata='[{ \
  "remoteUser": "bento_user", \
  "customizations": { \
    "vscode": { \
      "extensions": ["ms-python.python", "eamodio.gitlens"], \
      "settings": {"workspaceFolder": "/wes"} \
    } \
  } \
}]'

SHELL ["/bin/bash", "-c"]

# Install system packages for HTSLib + SAMtools + curl and jq for workflows
# OpenJDK is for running WOMtool/Cromwell
# Then, install dependencies for running the Python server + Python workflow dependencies
COPY container.requirements.txt .
RUN apt-get update -y && \
    apt-get install -y samtools tabix bcftools curl jq openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r /container.requirements.txt && \
    rm /container.requirements.txt

WORKDIR /
ENV CROMWELL_VERSION=86
RUN curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/cromwell-${CROMWELL_VERSION}.jar \
    -o cromwell.jar && \
    curl -L \
    https://github.com/broadinstitute/cromwell/releases/download/${CROMWELL_VERSION}/womtool-${CROMWELL_VERSION}.jar \
    -o womtool.jar

FROM base-deps AS install

# Backwards-compatible with old BentoV2 container layout
RUN mkdir -p /wes/tmp && mkdir -p /data
WORKDIR /wes

COPY pyproject.toml .
COPY poetry.lock .

# Install production + development dependencies
# Without --no-root, we get errors related to the code not being copied in yet.
# But we don't want the code here, otherwise Docker cache doesn't work well.
RUN poetry config virtualenvs.create false && \
    poetry --no-cache install --no-root

# Copy in the entrypoint & run script so we have somewhere to start
COPY entrypoint.bash .
COPY run.dev.bash .

# Tell the service that we're running a local development container
ENV BENTO_CONTAINER_LOCAL=true

ENTRYPOINT [ "bash", "./entrypoint.bash" ]
CMD [ "bash", "./run.dev.bash" ]
