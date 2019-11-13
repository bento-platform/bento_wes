# CHORD Workflow Execution Service (WES)

## Overview

Workflow execution service for CHORD. This service implements the
[GA4GH WES API schema](https://github.com/ga4gh/workflow-execution-service-schemas)
with additional CHORD-specific features.


## Environment Variables

```bash
CHORD_SERVICES=/path/to/chord_services.json
CHORD_URL=http://127.0.0.1:5000/
CELERY_RESULT_BACKEND=redis://
CELERY_BROKER_URL=redis://
DATABASE=chord_wes.db
SERVICE_BASE_URL=/
SERVICE_ID=
SERVICE_TEMP=tmp
WOM_TOOL_LOCATION=/path/to/womtool.jar
```
