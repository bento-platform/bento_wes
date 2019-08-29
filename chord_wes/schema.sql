DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS run_requests;
DROP TABLE IF EXISTS run_logs;
DROP TABLE IF EXISTS task_logs;

CREATE TABLE runs (
    id TEXT PRIMARY KEY,    -- UUID
    request TEXT NOT NULL,  -- Original request message used to initiate execution
    state TEXT NOT NULL,    -- enum
    run_log TEXT,           -- Foreign key to run log UUID
    outputs TEXT,           -- TODO
    FOREIGN KEY (request) REFERENCES run_requests ON DELETE CASCADE,
    FOREIGN KEY (run_log) REFERENCES run_logs ON DELETE CASCADE
);

CREATE TABLE run_requests (
    id TEXT PRIMARY KEY,                        -- UUID
    workflow_params TEXT NOT NULL,              -- JSON
    workflow_type TEXT NOT NULL DEFAULT 'WDL',  -- CWL or >>WDL<<
    workflow_type_version TEXT NOT NULL DEFAULT '1.0',
    workflow_url TEXT NOT NULL                  -- URL to WDL file
);

CREATE TABLE run_logs (
    id TEXT PRIMARY KEY,                  -- UUID
    name TEXT NOT NULL,                   -- Workflow name
    cmd TEXT NOT NULL DEFAULT '',         -- Command used to execute the workflow
    start_time TEXT NOT NULL DEFAULT '',  -- Time started (ISO 8601, UTC)
    end_time TEXT NOT NULL DEFAULT '',    -- Completed, failed, or canceled  (ISO 8601, UTC)
    stdout TEXT NOT NULL DEFAULT '',      -- Contents
    stderr TEXT NOT NULL DEFAULT '',      -- Contents
    exit_code INTEGER DEFAULT NULL,       -- Exit code

    -- Non-standard columns
    celery_id INTEGER DEFAULT NULL        -- UUID task ID from Celery
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
