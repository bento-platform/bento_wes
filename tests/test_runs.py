import json
import os
import responses
import uuid

from bento_lib.events import EventBus

from .constants import EXAMPLE_RUN, EXAMPLE_RUN_BODY

from bento_wes.db import get_db, run_request_dict_public, update_run_state_and_commit
from bento_wes.states import STATE_QUEUED, STATE_COMPLETE


def _add_workflow_response(r):
    with open(os.path.join(os.path.dirname(__file__), "phenopackets_json.wdl"), "r") as wf:
        r.add(
            responses.GET,
            "http://metadata.local/workflows/phenopackets_json.wdl",
            body=wf.read(),
            status=200,
            content_type="text/plain")


def _create_valid_run(client):
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    data = rv.get_json()
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)
    return data


def test_runs_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)

    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data) == json.dumps([])

    cr_data = _create_valid_run(client)
    assert "run_id" in cr_data

    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data, sort_keys=True) == json.dumps([{
        **cr_data,
        "state": STATE_QUEUED
    }], sort_keys=True)

    rv = client.get("/runs?with_details=true")
    assert rv.status_code == 200
    data = rv.get_json()

    run = data[0]

    assert run["run_id"] == cr_data["run_id"]
    assert run["state"] == STATE_QUEUED
    assert isinstance(run["details"], dict)
    assert run["details"]["run_id"] == cr_data["run_id"]
    assert run["details"]["state"] == STATE_QUEUED
    assert json.dumps(run["details"]["request"], sort_keys=True) == json.dumps(EXAMPLE_RUN, sort_keys=True)

    assert run["details"]["run_log"]["name"] == "phenopackets_json"
    assert run["details"]["run_log"]["cmd"] == ""
    assert run["details"]["run_log"]["start_time"] is None
    assert run["details"]["run_log"]["end_time"] is None
    assert run["details"]["run_log"]["stdout"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stdout"
    assert run["details"]["run_log"]["stderr"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stderr"
    assert run["details"]["run_log"]["exit_code"] is None

    assert tuple(sorted(run.keys())) == ("details", "run_id", "state")


def test_run_create_errors(client):
    bad_body_1 = EXAMPLE_RUN_BODY.copy()
    del bad_body_1["workflow_params"]

    rv = client.post("/runs", data=bad_body_1)
    assert rv.status_code == 400
    error = rv.get_json()
    assert len(error["errors"]) == 1
    assert error["errors"][0]["message"].startswith("Validation error")


def test_run_detail_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)

    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}")
    assert rv.status_code == 200
    run = rv.get_json()

    assert run["run_id"] == cr_data["run_id"]
    assert run["state"] == STATE_QUEUED

    assert json.dumps(run["request"], sort_keys=True) == json.dumps(EXAMPLE_RUN, sort_keys=True)
    assert json.dumps(run["task_logs"], sort_keys=True) == json.dumps([], sort_keys=True)  # TODO: Tasks impl

    assert run["run_log"]["name"] == "phenopackets_json"
    assert run["run_log"]["cmd"] == ""
    assert run["run_log"]["start_time"] is None
    assert run["run_log"]["end_time"] is None
    assert run["run_log"]["stdout"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stdout"
    assert run["run_log"]["stderr"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stderr"
    assert run["run_log"]["exit_code"] is None

    assert json.dumps(run["outputs"]) == json.dumps({})

    assert tuple(sorted(run.keys())) == ("outputs", "request", "run_id", "run_log", "state", "task_logs")


def test_run_status_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)

    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}/status")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}/status")
    assert rv.status_code == 200
    assert json.dumps(rv.get_json(), sort_keys=True) == json.dumps({**cr_data, "state": STATE_QUEUED}, sort_keys=True)


def test_run_streams(client, mocked_responses):
    _add_workflow_response(mocked_responses)

    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}/stdout")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{uuid.uuid4()}/stderr")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}/stdout")
    assert rv.status_code == 200
    assert rv.data == b""

    rv = client.get(f"/runs/{cr_data['run_id']}/stderr")
    assert rv.status_code == 200
    assert rv.data == b""


def test_run_cancel_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)

    cr_data = _create_valid_run(client)

    rv = client.post(f"/runs/{uuid.uuid4()}/cancel")
    assert rv.status_code == 404

    rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    assert rv.status_code == 500
    error = rv.get_json()
    assert len(error["errors"]) == 1
    assert error["errors"][0]["message"].startswith("No Celery ID present")


event_bus = EventBus(allow_fake=True)  # mock event bus


def test_runs_public_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)
    _add_ott_response(mocked_responses)

    # first, create a run, so we have something to fetch
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)

    # make sure the run is complete, otherwise the public endpoint won't list it
    db = get_db()
    c = db.cursor()
    update_run_state_and_commit(db, c, event_bus, rv.get_json()["run_id"], STATE_COMPLETE)

    # validate the public runs endpoint
    rv = client.get("/runs?with_details=true&public=true")
    assert rv.status_code == 200
    data = rv.get_json()

    expected_keys = ["run_id", "state", "details"]
    expected_details_keys = ["request", "run_id", "run_log", "state", "task_logs"]
    expected_request_keys = ["tags", "workflow_type"]
    expected_tags_keys = ["table_id", "workflow_id", "workflow_metadata"]
    expected_metadata_keys = ["data_type", "id"]
    expected_run_log_keys = ["end_time", "id", "start_time"]

    for run in data:
        assert set(run.keys()) == set(expected_keys)
        details = run["details"]
        assert set(details.keys()) == set(expected_details_keys)
        request = details["request"]
        assert set(request.keys()) == set(expected_request_keys)
        tags = request["tags"]
        assert set(tags.keys()) == set(expected_tags_keys)
        metadata = tags["workflow_metadata"]
        assert set(metadata.keys()) == set(expected_metadata_keys)
        run_log = details["run_log"]
        assert set(run_log.keys()) == set(expected_run_log_keys)

        # Testing run_request_dict_public function
        mock_run_request = {
            "workflow_type": request["workflow_type"],
            "tags": json.dumps(tags)
        }
        expected_request = run_request_dict_public(mock_run_request)
        assert request == expected_request

    # TODO: Get celery running for tests

    # rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    # print(rv.get_json(), flush=True)
    # assert rv.status_code == 204
    #
    # rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    # assert rv.status_code == 400
    # error = rv.get_json()
    # assert len(error["errors"]) == 1
    # assert error["errors"][0]["message"] == "Run already canceled"
