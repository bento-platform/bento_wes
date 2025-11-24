#!/bin/bash

cd /wes || exit

# Create bento_user + home
source /create_service_user.bash

# Fix permissions on /wes
chown -R bento_user:bento_user /wes
chmod -R o-rwx /wes/tmp  # Remove all access from others for /wes/tmp

# Configure git from entrypoint, since we've overwritten the base image entrypoint
gosu bento_user /bin/bash -c '/set_gitconfig.bash'

# Set up PATH for VEP
export PATH="/opt/vep/src/ensembl-vep:/opt/vep/src/var_c_code:${PATH}"

# Drop into bento_user from root and execute the CMD specified for the image
exec gosu bento_user "$@"
