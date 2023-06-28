import json
import os
import responses
import uuid

from bento_wes.states import STATE_QUEUED

from .constants import EXAMPLE_RUN, EXAMPLE_RUN_BODY


def _add_workflow_response(r):
    with open(os.path.join(os.path.dirname(__file__), "phenopackets_json.wdl"), "r") as wf:
        r.add(
            responses.GET,
            "http://metadata.local/workflows/phenopackets_json.wdl",
            body=wf.read(),
            status=200,
            content_type="text/plain")


def _add_ott_response(r):
    r.add(
        responses.POST,
        "http://auth.local/ott/generate",
        json=["t1"],
        status=200)


def test_runs_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)
    _add_ott_response(mocked_responses)

    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data) == json.dumps([])

    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)
    cr_data = rv.get_json()
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

    assert "id" in run["details"]["run_log"]
    assert run["details"]["run_log"]["name"] == "phenopackets_json"
    assert run["details"]["run_log"]["cmd"] == ""
    assert run["details"]["run_log"]["start_time"] == ""
    assert run["details"]["run_log"]["end_time"] == ""
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
    assert error["errors"][0]["message"].startswith("Assertion error")


def test_run_detail_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)
    _add_ott_response(mocked_responses)

    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

    rv = client.get(f"/runs/{uuid.uuid4()}")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}")
    run = rv.get_json()

    assert run["run_id"] == cr_data["run_id"]
    assert run["state"] == STATE_QUEUED

    assert json.dumps(run["request"], sort_keys=True) == json.dumps(EXAMPLE_RUN, sort_keys=True)
    assert json.dumps(run["task_logs"], sort_keys=True) == json.dumps([], sort_keys=True)  # TODO: Tasks impl

    assert "id" in run["run_log"]
    assert run["run_log"]["name"] == "phenopackets_json"
    assert run["run_log"]["cmd"] == ""
    assert run["run_log"]["start_time"] == ""
    assert run["run_log"]["end_time"] == ""
    assert run["run_log"]["stdout"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stdout"
    assert run["run_log"]["stderr"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stderr"
    assert run["run_log"]["exit_code"] is None

    assert json.dumps(run["outputs"]) == json.dumps({})

    assert tuple(sorted(run.keys())) == ("outputs", "request", "run_id", "run_log", "state", "task_logs")


def test_run_status_endpoint(client, mocked_responses):
    _add_workflow_response(mocked_responses)
    _add_ott_response(mocked_responses)

    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

    rv = client.get(f"/runs/{uuid.uuid4()}/status")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}/status")
    assert rv.status_code == 200
    assert json.dumps(rv.get_json(), sort_keys=True) == json.dumps({**cr_data, "state": STATE_QUEUED}, sort_keys=True)


def test_run_streams(client, mocked_responses):
    _add_workflow_response(mocked_responses)
    _add_ott_response(mocked_responses)

    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

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
    _add_ott_response(mocked_responses)

    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

    rv = client.post(f"/runs/{uuid.uuid4()}/cancel")
    assert rv.status_code == 404

    rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    assert rv.status_code == 500
    error = rv.get_json()
    assert len(error["errors"]) == 1
    assert error["errors"][0]["message"].startswith("No Celery ID present")


def test_runs_public_endpoint(client, mocked_responses):
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    assert rv.status_code == 200
    created_run_data = rv.get_json()
    assert "run_id" in created_run_data

    rv = client.get("/runs_public")
    assert rv.status_code == 200
    data = rv.get_json()

    assert len(data) > 0

    expected_keys = ["run_id", "request", "run_log", "end_time", "state"]
    for run in data:
        assert set(run.keys()) == set(expected_keys)

    created_run = next((run for run in data if run["run_id"] == created_run_data["run_id"]), None)
    assert created_run is not None

    assert "workflow_type" in created_run["request"]
    assert "tags" in created_run["request"]
    assert "table_id" in created_run["request"]["tags"]
    assert "workflow_id" in created_run["request"]["tags"]
    assert "workflow_metadata" in created_run["request"]["tags"]
    assert "data_type" in created_run["request"]["tags"]["workflow_metadata"]
    assert "id" in created_run["request"]["tags"]["workflow_metadata"]

    assert "id" in created_run["run_log"]
    assert "start_time" in created_run["run_log"]
    assert "end_time" in created_run["run_log"]

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
