import json
import uuid
from urllib.parse import urlparse

from constants import EXAMPLE_RUN, EXAMPLE_RUN_BODY

from bento_wes.states import STATE_QUEUED, STATE_COMPLETE


def _create_valid_run(client):
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    data = rv.json()
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)
    return data


def test_runs_endpoint(client, _mocked_responses_with_workflow):
    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.json()
    assert json.dumps(data) == json.dumps([])

    cr_data = _create_valid_run(client)
    assert "run_id" in cr_data

    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.json()
    assert json.dumps(data, sort_keys=True) == json.dumps([{**cr_data, "state": STATE_QUEUED}], sort_keys=True)

    rv = client.get("/runs?with_details=true")
    assert rv.status_code == 200
    data = rv.json()

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
    assert urlparse(run["details"]["run_log"]["stdout"]).path == f"/api/wes/runs/{cr_data['run_id']}/stdout"
    assert urlparse(run["details"]["run_log"]["stderr"]).path == f"/api/wes/runs/{cr_data['run_id']}/stderr"
    assert run["details"]["run_log"]["exit_code"] is None

    assert tuple(sorted(run.keys())) == ("details", "run_id", "state")


def test_run_create_errors(client):
    bad_body_1 = EXAMPLE_RUN_BODY.copy()
    del bad_body_1["workflow_params"]

    rv = client.post("/runs", data=bad_body_1)
    assert rv.status_code == 400
    error = rv.json()
    assert len(error["errors"]) == 1
    assert "Field required" in error["errors"][0]["message"]


def test_run_detail_endpoint(client, _mocked_responses_with_workflow):
    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}")
    assert rv.status_code == 200
    run = rv.json()

    assert run["run_id"] == cr_data["run_id"]
    assert run["state"] == STATE_QUEUED

    assert json.dumps(run["request"], sort_keys=True) == json.dumps(EXAMPLE_RUN, sort_keys=True)
    assert json.dumps(run["task_logs"], sort_keys=True) == json.dumps([], sort_keys=True)  # TODO: Tasks impl

    assert run["run_log"]["name"] == "phenopackets_json"
    assert run["run_log"]["cmd"] == ""
    assert run["run_log"]["start_time"] is None
    assert run["run_log"]["end_time"] is None
    assert run["run_log"]["stdout"] == f"https://bentov2.local/api/wes/runs/{cr_data['run_id']}/stdout"
    assert run["run_log"]["stderr"] == f"https://bentov2.local/api/wes/runs/{cr_data['run_id']}/stderr"
    assert run["run_log"]["exit_code"] is None

    assert json.dumps(run["outputs"]) == json.dumps({})

    assert tuple(sorted(run.keys())) == ("outputs", "request", "run_id", "run_log", "state", "task_logs")


def test_run_status_endpoint(client, _mocked_responses_with_workflow):
    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}/status")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}/status")
    assert rv.status_code == 200
    assert json.dumps(rv.json(), sort_keys=True) == json.dumps({**cr_data, "state": STATE_QUEUED}, sort_keys=True)


def test_run_streams(client, _mocked_responses_with_workflow):
    cr_data = _create_valid_run(client)

    rv = client.get(f"/runs/{uuid.uuid4()}/stdout")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{uuid.uuid4()}/stderr")
    assert rv.status_code == 404

    rv = client.get(f"/runs/{cr_data['run_id']}/stdout")
    assert rv.status_code == 200
    assert rv.content == b""

    rv = client.get(f"/runs/{cr_data['run_id']}/stderr")
    assert rv.status_code == 200
    assert rv.content == b""


def test_run_cancel_endpoint(client, _mocked_responses_with_workflow):
    cr_data = _create_valid_run(client)

    rv = client.post(f"/runs/{uuid.uuid4()}/cancel")
    assert rv.status_code == 404

    rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    assert rv.status_code == 500
    error = rv.json()
    assert len(error["errors"]) == 1
    assert error["errors"][0]["message"].startswith("No Celery ID present")

    # TODO: Get celery running for tests

    # rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    # print(rv.json(), flush=True)
    # assert rv.status_code == 204
    #
    # rv = client.post(f"/runs/{cr_data['run_id']}/cancel")
    # assert rv.status_code == 400
    # error = rv.json()
    # assert len(error["errors"]) == 1
    # assert error["errors"][0]["message"] == "Run already canceled"


def test_runs_public_endpoint(client, _mocked_responses_with_workflow, db_session):
    from bento_lib.events import EventBus

    event_bus = EventBus(allow_fake=True)  # mock event bus

    # first, create a run, so we have something to fetch
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)

    # make sure the run is complete, otherwise the public endpoint won't list it
    db_session.update_run_state_and_commit(rv.json()["run_id"], STATE_COMPLETE, event_bus)

    # validate the public runs endpoint
    rv = client.get("/runs?with_details=true&public=true")
    assert rv.status_code == 200
    data = rv.json()

    expected_keys = ["run_id", "state", "details"]
    expected_details_keys = ["request", "run_id", "run_log", "state"]
    expected_request_keys = ["tags", "workflow_type"]
    expected_tags_keys = ["workflow_id", "workflow_metadata"]
    expected_metadata_keys = ["data_type"]
    expected_run_log_keys = ["end_time", "start_time"]

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
