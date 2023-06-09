import json
import logging
import os
import requests
import shutil
import traceback
import uuid

from bento_lib.auth.flask_decorators import flask_permissions_owner
from bento_lib.responses.flask_errors import (
    flask_bad_request_error,
    flask_internal_server_error,
    flask_not_found_error,
)
from flask import Blueprint, current_app, jsonify, request
from urllib.parse import urljoin, urlparse
from werkzeug.utils import secure_filename

from collections import defaultdict

from . import states
from .celery import celery
from .events import get_flask_event_bus
from .runner import run_workflow
from .workflows import (
    WorkflowType,
    UnsupportedWorkflowType,
    WorkflowDownloadError,
    WorkflowManager,
    parse_workflow_host_allow_list,
    count_bento_workflow_file_outputs,
)

from .db import get_db, run_request_dict,run_request_dict_public, run_log_dict, run_log_dict_public, get_task_logs, get_run_details, update_run_state_and_commit


bp_runs = Blueprint("runs", __name__)

logger = logging.getLogger(__name__)


def _create_run(db, c):
    try:
        assert "workflow_params" in request.form
        assert "workflow_type" in request.form
        assert "workflow_type_version" in request.form
        assert "workflow_engine_parameters" in request.form
        assert "workflow_url" in request.form
        assert "tags" in request.form

        workflow_params = json.loads(request.form["workflow_params"])
        workflow_type = request.form["workflow_type"].upper().strip()
        workflow_type_version = request.form["workflow_type_version"].strip()
        workflow_engine_parameters = json.loads(request.form["workflow_engine_parameters"])  # TODO: Unused
        workflow_url = request.form["workflow_url"].lower()  # TODO: This can refer to an attachment
        workflow_attachment_list = request.files.getlist("workflow_attachment")  # TODO: Use this fully
        tags = json.loads(request.form["tags"])

        # TODO: Move CHORD-specific stuff out somehow?

        # Only "turn on" CHORD-specific features if specific tags are present

        chord_mode = all((
            "workflow_id" in tags,
            "workflow_metadata" in tags,
            tags["workflow_metadata"].get("action", "ingestion") == "ingestion",

            # Allow either a path to be specified for ingestion (for the 'classic'
            # Bento singularity architecture) or
            "ingestion_path" in tags or "ingestion_url" in tags,

            "table_id" in tags,
        ))

        workflow_id = tags.get("workflow_id", workflow_url)
        workflow_metadata = tags.get("workflow_metadata", {})
        workflow_ingestion_path = tags.get("ingestion_path", None)
        workflow_ingestion_url = tags.get(
            "ingestion_url",
            (f"{current_app.config['CHORD_URL'].rstrip('/')}/{workflow_ingestion_path.lstrip('/')}"
             if workflow_ingestion_path else None))
        table_id = tags.get("table_id", None)
        if chord_mode:
            table_id = str(uuid.UUID(table_id))  # Check and standardize table ID

        not_ingestion_mode = workflow_metadata.get("action") in ["export", "analysis"]

        # Don't accept anything (ex. CWL) other than WDL TODO: CWL support
        assert workflow_type == "WDL"
        assert workflow_type_version == "1.0"

        assert isinstance(workflow_params, dict)
        assert isinstance(workflow_engine_parameters, dict)
        assert isinstance(tags, dict)

        # TODO: Refactor (Gohan)
        # - Extract filenames from workflow_params and inject them back into workflow_params
        #  as an array-of-strings alongside the original array-of-files
        # - Pass workflow ingestion URL in as a parameter to the workflow (used in the .wdl file directly)
        if workflow_ingestion_url and "gohan" in workflow_ingestion_url:
            workflow_params["vcf_gz.original_vcf_gz_file_paths"] = workflow_params["vcf_gz.vcf_gz_file_names"]
            gohan_url = urlparse(workflow_ingestion_url)
            workflow_params["vcf_gz.gohan_url"] = (f"{gohan_url.scheme}" +
                                                   f"://{gohan_url.netloc}" +
                                                   f"{gohan_url.path.replace('/private/ingest', '')}")

        # Some workflow parameters depend on the WES application configuration
        # and need to be added from there.
        # The reserved keyword `FROM_CONFIG` is used to detect those inputs.
        # All parameters in config are upper case. e.g. drs_url --> DRS_URL
        for i in workflow_metadata["inputs"]:
            if i.get("value") != "FROM_CONFIG":
                continue
            param_name = i["id"]
            workflow_params[f"{workflow_id}.{param_name}"] = current_app.config.get(param_name.upper(), "")

        # TODO: Use JSON schemas for workflow params / engine parameters / tags

        # Get list of allowed workflow hosts from configuration for any checks inside the runner
        # If it's blank, assume that means "any host is allowed" and pass None to the runner
        workflow_host_allow_list = parse_workflow_host_allow_list(current_app.config["WORKFLOW_HOST_ALLOW_LIST"])

        # Download workflow file, potentially using passed auth headers if they're present
        # and we're querying our own node.

        # TODO: Move this back to runner, since we'll need to handle the callback anyway with local URLs...

        chord_url = current_app.config["CHORD_URL"]

        wm = WorkflowManager(
            current_app.config["SERVICE_TEMP"],
            chord_url,
            logger=current_app.logger,
            workflow_host_allow_list=workflow_host_allow_list,
            validate_ssl=current_app.config["BENTO_VALIDATE_SSL"],
            debug=current_app.config["BENTO_DEBUG"],
        )

        # Optional Authorization HTTP header to forward to nested requests
        # TODO: Move X-Auth... constant to bento_lib
        auth_header = request.headers.get("X-Authorization", request.headers.get("Authorization"))
        auth_header_dict = {"Authorization": auth_header} if auth_header else {}

        try:
            wm.download_or_copy_workflow(
                workflow_url,
                WorkflowType(workflow_type),
                auth_headers=auth_header_dict)
        except UnsupportedWorkflowType:
            return flask_bad_request_error(f"Unsupported workflow type: {workflow_type}")
        except (WorkflowDownloadError, requests.exceptions.ConnectionError) as e:
            return flask_bad_request_error(f"Could not access workflow file: {workflow_url} (Python error: {e})")

        # Generate one-time tokens for ingestion purposes if in Bento mode
        one_time_tokens = []
        drs_url: str = current_app.config["DRS_URL"]
        use_otts_for_drs: bool = chord_url in drs_url and urlparse(drs_url).scheme != "http+unix"
        ott_endpoint_namespace: str = current_app.config["OTT_ENDPOINT_NAMESPACE"]  # TODO: py3.9: walrus operator
        tt_endpoint_namespace: str = current_app.config["TT_ENDPOINT_NAMESPACE"]  # TODO: py3.9: walrus operator

        token_host = urlparse(chord_url).netloc

        ott = AuthorizationToken(
            {**auth_header_dict},  # TODO: Host?
            ott_endpoint_namespace
        )

        tt = AuthorizationToken(
            {**auth_header_dict},
            tt_endpoint_namespace
        )

        if chord_mode and ott_endpoint_namespace:
            # Generate the correct number of one-time tokens for the DRS and ingest scopes
            # to allow for the callback to ingest files
            # Skip doing this for DRS if the DRS URL is an internal (container) URL
            # TODO: Remove this ^ bit

            if use_otts_for_drs:
                # TODO: This sort of assumes DRS is on the same domain as WES, which isn't necessarily correct
                #  An error should be thrown if there's a mismatch and we're still trying to do OTT stuff, probably
                scope = f"/{drs_url.replace(chord_url, '').rstrip('/')}/"
                nb_tokens = count_bento_workflow_file_outputs(workflow_id, workflow_params, workflow_metadata)
                try:
                    one_time_tokens.extend(ott.get(scope, nb_tokens))
                except Exception as err:
                    return flask_internal_server_error(err)

            # Request an additional OTT for the service ingest request
            scope = ("/" if chord_url in workflow_ingestion_url else "") + workflow_ingestion_url.replace(
                chord_url, "").rsplit("/", 1)[0] + "/"
            one_time_tokens.extend(ott.get(scope, 1))

        # Generate temporary tokens for polling purposes during ingestion
        # (Gohan specific)
        if chord_mode and tt_endpoint_namespace and "gohan" in workflow_ingestion_url:

            scope = ("/" if chord_url in workflow_ingestion_url else "") + workflow_ingestion_url.replace(
                chord_url, "").rsplit("/", 1)[0] + "/"
            try:
                token = tt.get(scope, 1)
            except Exception as err:
                return flask_internal_server_error(err)

            # TODO: Refactor (Gohan)
            # - Pass TT in as a parameter to the workflow (used in the .wdl file directly)
            #    (current purpose for this is only to get automatic Gohan workflow requests through)
            workflow_params[f"{workflow_id}.temp_token"] = token[0]
            workflow_params[f"{workflow_id}.temp_token_host"] = token_host

            # TODO: Refactor (Gohan)
            # - Include table_id as part of the input parameters so Gohan can affiliate variants with a table
            workflow_params["vcf_gz.table_id"] = table_id

        if "auth" in workflow_metadata:
            for tok in workflow_metadata["auth"]:
                tokenFactory: AuthorizationToken = tt if tok.get("type", "tt") == "tt" else ott
                scope = tok.get("scope", "//")
                try:
                    token = tokenFactory.get(scope, 1)
                except Exception as err:
                    return flask_internal_server_error(str(err))

                workflow_params[f"{workflow_id}.{tok['id']}"] = token[0]
                workflow_params[f"{workflow_id}.auth_host"] = token_host
        # ---

        # Begin creating the job after validating the request
        req_id = uuid.uuid4()
        run_id = uuid.uuid4()
        log_id = uuid.uuid4()

        # Create run directory

        run_dir = os.path.join(current_app.config["SERVICE_TEMP"], str(run_id))

        if os.path.exists(run_dir):
            return flask_internal_server_error("UUID collision")

        os.makedirs(run_dir, exist_ok=True)
        # TODO: Delete run dir if something goes wrong...

        # In export/analysis mode, as we rely on services located in different containers
        # there is a need to have designated folders on shared volumes between
        # WES and the other services, to write files to.
        # This is possible because /wes/tmp is a volume mounted with the same
        # path in each data service (except Gohan which mounts the dropbox
        # data-x directory directly instead, to avoid massive duplicates).
        # Also, the Toil library creates directories in the generic /tmp/
        # directory instead of the one that is configured for the job execution,
        # to create the current working directories where tasks are executed.
        # These files are inaccessible to other containers in the context of a
        # task unless they are written arbitrarily to run_dir
        if not_ingestion_mode:
            workflow_params[f"{workflow_id}.run_dir"] = run_dir

        # Move workflow attachments to run directory

        for attachment in workflow_attachment_list:
            # TODO: Check and fix input if filename is non-secure
            # TODO: Do we put these in a subdirectory?
            # TODO: Support WDL uploads for workflows
            attachment.save(os.path.join(run_dir, secure_filename(attachment.filename)))

        # Will be updated to STATE_ QUEUED once submitted
        c.execute("INSERT INTO run_requests (id, workflow_params, workflow_type, workflow_type_version, "
                  "workflow_engine_parameters, workflow_url, tags) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (str(req_id), json.dumps(workflow_params), workflow_type, workflow_type_version,
                   json.dumps(workflow_engine_parameters), workflow_url, json.dumps(tags)))
        c.execute("INSERT INTO run_logs (id, name) VALUES (?, ?)", (str(log_id), workflow_id))
        c.execute("INSERT INTO runs (id, request, state, run_log, outputs) VALUES (?, ?, ?, ?, ?)",
                  (str(run_id), str(req_id), states.STATE_UNKNOWN, str(log_id), json.dumps({})))
        db.commit()

        # TODO: figure out timeout
        # TODO: retry policy
        c.execute("UPDATE runs SET state = ? WHERE id = ?", (states.STATE_QUEUED, str(run_id)))
        db.commit()

        run_workflow.delay(run_id, chord_mode, workflow_metadata, workflow_ingestion_url, table_id, one_time_tokens,
                           use_otts_for_drs)

        return jsonify({"run_id": str(run_id)})

    except ValueError:
        return flask_bad_request_error("Value error")

    except AssertionError:  # TODO: Better error messages
        logger.error(f"Encountered assertion error: {traceback.format_exc()}")
        return flask_bad_request_error("Assertion error: bad run request format")


def fetch_run_details(c, public_endpoint=False):
    c.execute(
        "SELECT r.id AS run_id, r.state AS state, rr.*, rl.* "
        "FROM runs AS r, run_requests AS rr, run_logs AS rl "
        "WHERE r.request = rr.id AND r.run_log = rl.id"
    )

    runs = []
    for r in c.fetchall():
        request = run_request_dict(r) if not public_endpoint else run_request_dict_public(r)
        run_log = run_log_dict(r["run_id"], r) if not public_endpoint else run_log_dict_public(r["run_id"], r)
        end_time = run_log['end_time']
        state = r['state']

        if public_endpoint:
            if state == 'COMPLETE':
                runs.append({
                    'end_time': end_time,
                    'state': state,
                    'request': request,
                    'run_log': run_log,
                    'run_id': r['run_id'],
                })
        else:
            runs.append({
                'run_id': r["run_id"],
                'state': state,
                'details': {
                    'run_id': r["run_id"],
                    'state': state,
                    'request': request,
                    'run_log': run_log,
                    'task_logs': get_task_logs(c, r["run_id"])
                }
            })

    return runs if not public_endpoint else get_latest_runs_by_data_type(runs)

def get_latest_runs_by_data_type(runs):
    runs_by_data_type = defaultdict(list)
    for run in runs:
        runs_by_data_type[run['request']['tags']['workflow_metadata']['data_type']].append(run)

    latest_runs_by_data_type = {}
    for data_type, runs in runs_by_data_type.items():
        latest_runs_by_data_type[data_type] = max(runs, key=lambda r: r['end_time'])

    return [run for run in latest_runs_by_data_type.values()]

@bp_runs.route("/runs", methods=["GET", "POST"])
@flask_permissions_owner  # TODO: Allow others to submit analysis runs?
def run_list():
    db = get_db()
    c = db.cursor()

    if request.method == "POST":
        return _create_run(db, c)

    # GET
    # CHORD Extension: Include run details with /runs request
    with_details = request.args.get("with_details", "false").lower() == "true"

    if not with_details:
        c.execute("SELECT * FROM runs")

        return jsonify([{
            "run_id": run["id"],
            "state": run["state"]
        } for run in c.fetchall()])

    return jsonify(fetch_run_details(c))

@bp_runs.route("/runs-public", methods=["GET"])
def run_list_public():
    db = get_db()
    c = db.cursor()
    return jsonify(fetch_run_details(c, public_endpoint=True))


@bp_runs.route("/runs/<uuid:run_id>", methods=["GET"])
@flask_permissions_owner
def run_detail(run_id):
    run_details, err = get_run_details(get_db().cursor(), run_id)
    return jsonify(run_details) if run_details is not None else flask_not_found_error(f"Run {run_id} not found ({err})")


def get_stream(c, stream, run_id):
    c.execute("SELECT * FROM runs AS r, run_logs AS rl WHERE r.id = ? AND r.run_log = rl.id", (str(run_id),))
    run = c.fetchone()
    return (current_app.response_class(
        headers={
            # If we've finished, we allow long-term (24h) caching of the stdout/stderr responses.
            # Otherwise, no caching allowed!
            "Cache-Control": (
                "private, max-age=86400" if run["state"] in states.TERMINATED_STATES
                else "no-cache, no-store, must-revalidate, max-age=0"
            ),
        },
        response=run[stream],
        mimetype="text/plain",
        status=200,
    ) if run is not None else flask_not_found_error(f"Stream {stream} not found for run {run_id}"))


@bp_runs.route("/runs/<uuid:run_id>/stdout", methods=["GET"])
@flask_permissions_owner
def run_stdout(run_id):
    return get_stream(get_db().cursor(), "stdout", run_id)


@bp_runs.route("/runs/<uuid:run_id>/stderr", methods=["GET"])
@flask_permissions_owner
def run_stderr(run_id):
    return get_stream(get_db().cursor(), "stderr", run_id)


@bp_runs.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
@flask_permissions_owner
def run_cancel(run_id):
    # TODO: Check if already completed
    # TODO: Check if run log exists
    # TODO: from celery.task.control import revoke; revoke(celery_id, terminate=True)
    db = get_db()
    c = db.cursor()
    event_bus = get_flask_event_bus()

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return flask_not_found_error(f"Run {run_id} not found")

    if run["state"] in (states.STATE_CANCELING, states.STATE_CANCELED):
        return flask_bad_request_error("Run already canceled")

    if run["state"] in states.FAILURE_STATES:
        return flask_bad_request_error("Run already terminated with error")

    if run["state"] in states.SUCCESS_STATES:
        return flask_bad_request_error("Run already completed")

    c.execute("SELECT * FROM run_logs WHERE id = ?", (run["run_log"],))
    run_log = c.fetchone()

    if run_log is None:
        return flask_internal_server_error(f"No run log present for run {run_id}")

    if run_log["celery_id"] is None:
        # Never made it into the queue, so "cancel" it
        return flask_internal_server_error(f"No Celery ID present for run {run_id}")

    # TODO: terminate=True might be iffy
    update_run_state_and_commit(db, c, event_bus, run["id"], states.STATE_CANCELING)
    celery.control.revoke(run_log["celery_id"], terminate=True)  # Remove from queue if there, terminate if running

    # TODO: wait for revocation / failure and update status...

    # TODO: Generalize clean-up code / fetch from back-end
    run_dir = os.path.join(current_app.config["SERVICE_TEMP"], run["run_id"])
    if not current_app.config["BENTO_DEBUG"]:
        shutil.rmtree(run_dir, ignore_errors=True)

    update_run_state_and_commit(db, c, event_bus, run["id"], states.STATE_CANCELED)

    return current_app.response_class(status=204)  # TODO: Better response


@bp_runs.route("/runs/<uuid:run_id>/status", methods=["GET"])
@flask_permissions_owner
def run_status(run_id):
    c = get_db().cursor()

    c.execute("SELECT * FROM runs WHERE id = ?", (str(run_id),))
    run = c.fetchone()

    if run is None:
        return flask_not_found_error(f"Run {run_id} not found")

    return jsonify({
        "run_id": run["id"],
        "state": run["state"]
    })


class AuthorizationToken():
    """Encapsulation of requests for authorization tokens (one time or temp)"""

    def __init__(self, headers, endpoint_namespace):
        self.headers = headers
        self.generate_url = urljoin(endpoint_namespace.rstrip("/") + "/", "generate")

    def get(self, scope, number):
        tr = requests.post(self.generate_url, headers=self.headers, json={
            "scope": scope,
            "number": number,
        }, verify=current_app.config["BENTO_VALIDATE_SSL"])

        if not tr.ok:
            # An error occurred while requesting authorization token, so we cannot complete the run request
            raise Exception(
                f"Got error while requesting authorization tokens: {tr.content} "
                f"(Scope: {scope}, URL: {self.generate_url}, headers included: {list(self.headers.keys())})")

        return tr.json()
