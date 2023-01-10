FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

# Install system packages for HTSLib + SAMtools
RUN apt-get update -y && apt-get install -y samtools tabix bcftools

# Boostrap dependencies for setting up and running the Python application
RUN pip install --no-cache-dir poetry==1.3.2 gunicorn==20.1.0 "pysam>=0.20.0,<0.21.0"

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
COPY entrypoint.sh entrypoint.sh
COPY LICENSE LICENSE
COPY README.md README.md

# Install the module itself, locally (similar to `pip install -e .`)
RUN poetry install --without dev

CMD [ "bash", "./entrypoint.sh" ]
