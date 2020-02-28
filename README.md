# CHORD Workflow Execution Service (WES)

## Overview

Workflow execution service for CHORD. This service implements the
[GA4GH WES API schema](https://github.com/ga4gh/workflow-execution-service-schemas)
with additional CHORD-specific features.


## `/service-info` Type

```
ca.c3g.chord:wes:VERSION
```


## Environment Variables

```bash
CHORD_SERVICES=/path/to/chord_services.json
CHORD_URL=http://127.0.0.1:5000/
CELERY_RESULT_BACKEND=redis://
CELERY_BROKER_URL=redis://
DATABASE=chord_wes.db
SERVICE_ID=
SERVICE_TEMP=tmp
SERVICE_URL_BASE_PATH=/
WOM_TOOL_LOCATION=/path/to/womtool.jar
```


## Events

`wes_run_updated`: TODO

`wes_run_completed`: TODO


## Running Tests

```bash
python3 -m pytest --cov=chord_wes --cov-branch
```
