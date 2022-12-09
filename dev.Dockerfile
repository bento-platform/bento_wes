FROM ghcr.io/bento-platform/bento_base_image:python-debian-latest

# Install system packages for HTSLib + SAMtools
RUN apt-get install -y htslib samtools

# Boostrap dependencies for setting up and running the Python application
RUN pip install --no-cache-dir poetry==1.2.2 gunicorn==20.1.0

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

CMD [ "sh", "./entrypoint.dev.sh" ]
