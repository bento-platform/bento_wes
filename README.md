# Bento Workflow Execution Service (WES)

![Build Status](https://api.travis-ci.com/bento-platform/bento_wes.svg?branch=master)
[![codecov](https://codecov.io/gh/bento-platform/bento_wes/branch/master/graph/badge.svg)](https://codecov.io/gh/bento-platform/bento_wes)

## Overview

Workflow execution service for the Bento platform. This service implements the
[GA4GH WES API schema](https://github.com/ga4gh/workflow-execution-service-schemas)
with additional Bento-specific features.


## `/service-info` Type

```
ca.c3g.bento:wes:VERSION
```


## Environment Variables

```bash
# Bento instance or service base URL, used for generating absolute URLs within
# the service, for making requests, and for re-writing internal URLS in the case
# of Singularity-based Bento instances
CHORD_URL=http://127.0.0.1:5000/

# Celery configuration
CELERY_RESULT_BACKEND=redis://
CELERY_BROKER_URL=redis://

# Run/task database location
DATABASE=bento_wes.db

# Service configuration
# - unique ID service within for Bento instance
SERVICE_ID=
# - temporary data directory
SERVICE_TEMP=tmp
# - base path for service endpoints
SERVICE_URL_BASE_PATH=/

# Location of WOMtool, used to validate WDL files
WOM_TOOL_LOCATION=/path/to/womtool.jar

# Configuration / behaviour options:
# - Where the DRS instance to use is located. Technically, if not set this 
#   defaults to using a different environment variable, NGINX_INTERNAL_SOCKET,
#   as a path to a Bento instance's internal NGINX UNIX socket and adding a 
#   path to the actual DRS instance
DRS_URL=http+unix:///chord/tmp/nginx_internal.sock/api/drs
# - Whether to redirect file outputs to the DRS instance specified above
WRITE_OUTPUT_TO_DRS=False
# - Whether to de-duplicate / consolidate identical files within DRS
DRS_DEDUPLICATE=True
# - File extensions to skip when ingesting into DRS - this is a SUPER HACKY way
#   of not ingesting JSON into DRS used for stuff like katsu
DRS_SKIP_TYPES=
```


## Events

`wes_run_updated`: TODO

`wes_run_completed`: TODO


## Running Tests

```bash
python3 -m tox
```
