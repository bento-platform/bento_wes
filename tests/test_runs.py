import json

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
    assert run["details"]["run_log"]["stdout"] == f"https://bentov2.local/api/wes/runs/{cr_data['run_id']}/stdout"
    assert run["details"]["run_log"]["stderr"] == f"https://bentov2.local/api/wes/runs/{cr_data['run_id']}/stderr"
    assert run["details"]["run_log"]["exit_code"] is None

    assert tuple(sorted(run.keys())) == ("details", "run_id", "state")
