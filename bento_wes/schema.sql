DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS run_requests;
DROP TABLE IF EXISTS run_logs;
DROP TABLE IF EXISTS task_logs;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,    -- UUID
    state TEXT NOT NULL,    -- enum
    outputs TEXT,           -- Outputs from the run  TODO: formal spec for this?

    request__workflow_params TEXT NOT NULL,                          -- JSON
    request__workflow_type TEXT NOT NULL DEFAULT 'WDL',              -- CWL or >>WDL<<
    request__workflow_type_version TEXT NOT NULL DEFAULT '1.0',      -- Version of workflow language specification
    request__workflow_engine_parameters TEXT NOT NULL DEFAULT '{}',  -- JSON
    request__workflow_url TEXT NOT NULL,                             -- URL to WDL file
    request__tags TEXT NOT NULL DEFAULT '{}'                         -- JSON

    run_log__name TEXT NOT NULL,                   -- Workflow name
    run_log__cmd TEXT NOT NULL DEFAULT '',         -- Command used to execute the workflow
    run_log__start_time TEXT NOT NULL DEFAULT '',  -- Time started (ISO 8601, UTC)
    run_log__end_time TEXT NOT NULL DEFAULT '',    -- Completed, failed, or canceled  (ISO 8601, UTC)
    run_log__stdout TEXT NOT NULL DEFAULT '',      -- Contents
    run_log__stderr TEXT NOT NULL DEFAULT '',      -- Contents
    run_log__exit_code INTEGER DEFAULT NULL,       -- Exit code

    -- Non-standard columns
    run_log__celery_id INTEGER DEFAULT NULL        -- UUID task ID from Celery
);

CREATE TABLE task_logs (
    id TEXT PRIMARY KEY,
    run_id TEXT,                 -- Reference to run (many task logs to one run)
    name TEXT NOT NULL,          -- Workflow name
    cmd TEXT NOT NULL,           -- Command used to execute the workflow
    start_time TEXT NOT NULL,    -- Time started (ISO 8601, UTC)
    end_time TEXT NOT NULL,      -- Completed, failed, or canceled  (ISO 8601, UTC)
    stdout TEXT NOT NULL,        -- URL
    stderr TEXT NOT NULL,        -- URL
    exit_code INTEGER NOT NULL,  -- Exit code
    FOREIGN KEY (run_id) REFERENCES runs ON DELETE CASCADE
);
