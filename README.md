# Bento Workflow Execution Service (WES)

![Test Status](https://github.com/bento-platform/bento_wes/workflows/Test/badge.svg)
![Lint Status](https://github.com/bento-platform/bento_wes/workflows/Lint/badge.svg)
[![codecov](https://codecov.io/gh/bento-platform/bento_wes/branch/master/graph/badge.svg)](https://codecov.io/gh/bento-platform/bento_wes)



## Overview

Workflow execution service for the Bento platform. This service implements the
[GA4GH WES API schema](https://github.com/ga4gh/workflow-execution-service-schemas)
with additional Bento-specific features.

### Workflow definition
A workflow is based on a `.wdl` file which defines the different tasks with
their related I/O dependencies (i.e. which variables or files are required as
input, and what is the output of the workflow). See the [Workflow Definition Language Specs](https://github.com/openwdl/wdl/blob/main/versions/draft-2/SPEC.md) for more information.
A mandatory JSON file containing the
required metadata (variables values, file names, etc... to be used by the workflow) is also provided.

### Where are workflows defined in Bento?
In Bento, each data related service (e.g. Katsu, Gohan) stores its own workflows
in a `/**/workflows/` directory. The workflows can be requested from the
`workflows` API endpoints exposed by these microservices (e.g. list all workflows, 
show details or download `.wdl` file for a specific workflow).
On the other hand, the metadata is generated by the `bento-web` (!not `bento-wes`!) 
service when the execution is triggered, including the reference to the
workflow.

### Workflows execution
The WES container may receive a `/runs` POST request to execute a given workflow
with specified metadata. The WES service then queries the worflow provider
to get the relevant `.wdl` file which is copied over in a temporary execution folder,
along with the metadata as JSON.
The [Toil library](https://toil.readthedocs.io/en/latest/running/wdl.html)
is used to generate a script from the workflow definition and a process is
spawned to run it. In a first step, the dependencies such as
input files are copied over locally. Note that in DEV mode, the temporary files
are not cleaned up after completion.
Each run is monitored and its state is stored in a local database.

Note that some metadata may contain callback urls which are called once the
workflow described in the wdl file has been executed. This is the case for 
Katsu ingestions workflows.

### File sharing between services
The WES needs to access the files used as input. It may also pass references to
files to other services as part of the workflow. For example during an
ingestion workflow, a file must be passed to the relevant data service
for ingestion in its internal database. This file transfer is based on
mounted volumes shared between the containers.

Of note, the `wes/tmp` directory is mounted in some data service container (with the exception of Gohan which mounts the dropbox data directory instead).
When a workflow is executed, this is where the necessary input files are stagged.
This side effect is used to pass files for ingestion to the relevant containers.
Some workflows (ingestions workflows in Katsu) contain an "identity" task which
only takes a path to a dropbox file as input and returns a local path to a temp file. Note that the `/wes/tmp` volume must be mounted to the same
path in every container for this to work seamlessly.

## REST API
### `/service-info` Type

```
ca.c3g.bento:wes:VERSION
```

### `/runs` POST
Parameter:
```JSON
{
  "workflow_params",    // unused?
  "workflow_type": "WDL",
  "worflow_type_version": "1.0",
  "workflow_engine_parameters": {}, // unused
  "workflow_url":       // where the WES can fetch the wdl file,
  "tags": {
    "workflow_id":      // must correspond to the worflow_params namespace
    "workflow_metadata": {
      "inputs": [{}],   // must correspond to the .wdl input section
      "outputs": [{}]   // must correspond to the .wdl output section
    }
    "ingestion_path"?,  // optional callback
    "ingestion_url"?,
    "table_id"          // used for ingestion callbacks
  }
}
```
Note: this diverges from GA4GH recommendations: `tags.workflow_metadata` should
be in `workflow_params`. The usage of the `tags` property is Bento specific
and the callback mechanism should probably be part of the tasks definition.

### `/runs` GET
Parameter Optional `with_details` (BOOL)
Lists all runs.

### `/runs/<uuid>` GET
Details of the run corresponding to the uuid

### `/runs/<uuid>/stdout` GET ; `/runs/<uuid>/stderr` GET
Stream of run's stdout or sterr respectively
### `/runs/<uuid>/cancel` POST
Cancel run
### `/runs/<uuid>/status` GET
Get run state



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
# - temporary data directory - the service currently does not make this by 
#   itself, so this must be created prior to startup
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

To run the Celery worker (required to actually run jobs), the following command
(or similar) can be used:

```bash
nohup celery -A bento_wes.app worker --loglevel=INFO &> celery.log &
```

## About the implementation
This service is built around a [Flask](https://flask.palletsprojects.com/) application.
It uses [Celery](https://github.com/celery/celery) to monitor and run workflows executed
by [Toil](http://toil.ucsc-cgl.org/).
The workflows are downloaded from local services.
There are no checks on the workflows validity in that case
(assumption that workflows coming from configured hosts are correct,
see above `WORKFLOW_HOST_ALLOW_LIST` env variable).
For now the [WOMtool](https://cromwell.readthedocs.io/en/stable/WOMtool/) utility used for checking `.wdl` files
validity is disabled in Bento (see the corresponding Dockerfile).

### WDL files syntax tips
Toil support for WDL files is considered in alpha stage. Currently the Toil library version
used in bento-wes is 3.2 which only supports draft-2 version of the WDL specs.
This makes it error-prone to rely on WOMtools for syntax checks.
Here is a list of the limitations found:
- Chaning tasks in scatter blocks is unsupported (i.e. tasks which inputs depend on previous tasks outputs)
- Input block is unsupported (WDL-draft2 limitation)
- Interpolation of internal variables with `~{}` syntax is unsupported, even in `<<<>>>` delimited command blocks. Use `${}` instead.
- If the shell script command block makes use of local variables defined in
  the shell script (as opposed to WDL variables declared externally), the
  `${VARNAME}` conflicts with WDL variables. A trick is to define a `dollar`
  WDL variable `String dollar = "$"` and use it to "escape" shell variables
  (e.g. `${dollar}{VARNAME}`).
  ```wdl
  String dollar = "$"

  command <<<
    PI=3.14
    echo ${dollar}{PI}
  >>>
  ```
- File names based on WDL variables used in the `output` block must be enclosed
  in quotes and use string interpolation
  ```wdl
  String output_file = "myfilename.txt"

  output {
    File out = "${output_file}"
  }
  ```

### runs.py
This script contains the routes definitions as [Flask's Blueprints](https://flask.palletsprojects.com/en/2.0.x/blueprints/)

### runner.py
This script contains the implementation of the workflows execution.
Of interest is the code handling the callbacks and
some service specific routines (i.e. code paths specific to Gohan or Katsu ingestions).
