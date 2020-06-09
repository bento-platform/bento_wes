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
CHORD_SERVICES=/path/to/chord_services.json
CHORD_URL=http://127.0.0.1:5000/
CELERY_RESULT_BACKEND=redis://
CELERY_BROKER_URL=redis://
DATABASE=bento_wes.db
SERVICE_ID=
SERVICE_TEMP=tmp
SERVICE_URL_BASE_PATH=/
WOM_TOOL_LOCATION=/path/to/womtool.jar
WRITE_OUTPUT_TO_DRS=False
DRS_SKIP_TYPES=
```


## Events

`wes_run_updated`: TODO

`wes_run_completed`: TODO


## Running Tests

```bash
python3 -m tox
```
