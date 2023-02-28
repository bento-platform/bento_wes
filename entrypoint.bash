#!/bin/bash

cd /wes || exit

# Create bento_user + home
source /create_service_user.bash

# Fix permissions on /wes
chown -R bento_user:bento_user /wes
chmod -R o-rwx /wes/tmp  # Remove all access from others for /wes/tmp
if [[ -d /env ]]; then
  chown -R bento_user:bento_user /env
fi

# Drop into bento_user from root and execute the CMD specified for the image
exec gosu bento_user "$@"
