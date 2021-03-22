# Bento Workflow Execution Service (WES)

![Test Status](https://github.com/bento-platform/bento_wes/workflows/Test/badge.svg)
![Lint Status](https://github.com/bento-platform/bento_wes/workflows/Lint/badge.svg)
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

# Debug mode for the service - falls back to FLASK_ENV (development = true, 
# any other value = false) if not set
# SECURITY NOTE: This SHOULD NOT EVER be enabled in production, as it removes
# checks for TLS certificate validity!
CHORD_DEBUG=False

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
# - If not set, no WDL validation will be done
# - SECURITY: If not set, WDL_HOST_ALLOW_LIST must contain a comma-separated 
#   list of hosts workflow files can be downloaded from
WOM_TOOL_LOCATION=/path/to/womtool.jar

# Allow-list (comma-separated) for hosts that workflow files can be downloaded
# from - prevents possibly insecure WDLs from being ran
WORKFLOW_HOST_ALLOW_LIST=

# DRS configuration options:
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



## Development

### Setting up a Virtual Environment

After cloning the repository, set up a virtual environment with Python 3 and
install the development dependencies:

```bash
virtualenv -p python3 ./env
source env/bin/activate
pip install -r requirements.txt
```


### Running Tests

To run all tests and linting, use the following command:

```bash
python3 -m tox
```


### Releases

#### Release Checklist

  * [ ] All tests pass

  * [ ] Package version has been updated (following semver) in 
    `bento_lib/package.cfg`
    
  * [ ] A release can then be created, tagged in the format of `v#.#.#` and named
    in the format of `Version #.#.#`, listing any changes made, in the GitHub 
    releases page **tagged from the master branch!**
    

#### Note on Versioning

The `bento_wes` project uses [semantic versioning](https://semver.org/) for
releasing. If the API is broken in any way, including minor differences in the
way a function behaves given an identical set of parameters (excluding bugfixes
for unintentional behaviour), the MAJOR version must be incremented. In this 
way, we guarantee that projects relying on this API do not accidentally break
upon upgrading.



## Deploying

The `bento_wes` service can be deployed with a WSGI server like Gunicorn or
UWSGI, specifying `bento_wes.app:application` as the WSGI application.

It is best to then put an HTTP server software such as NGINX in front of 
Gunicorn. 

**Flask applications should NEVER be deployed in production via the Flask 
development server, i.e. `flask run`!**
