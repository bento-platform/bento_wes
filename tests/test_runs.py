import json
from bento_wes.states import STATE_QUEUED


EXAMPLE_TABLE_ID = "ef9da1da-ef7f-43d6-ace3-456bf8e58431"

EXAMPLE_RUN = {
    "workflow_params": {
        "json_document": "http://my-server.local/test.json",
    },
    "workflow_type": "WDL",
    "workflow_type_version": "1.0",
    "workflow_engine_parameters": {},
    "workflow_url": "http://metadata.local/workflows/ingest.wdl",
    "tags": {
        "workflow_id": "ingest",
        "workflow_metadata": {
            "name": "Bento Phenopackets-Compatible JSON",
            "description": "This ingestion workflow will validate and import a Phenopackets schema-compatible "
                           "JSON document.",
            "data_type": "phenopacket",
            "file": "phenopackets_json.wdl",
            "inputs": [
                {
                    "id": "json_document",
                    "type": "file",
                    "required": True,
                    "extensions": [".json"]
                }
            ],
            "outputs": [
                {
                    "id": "json_document",
                    "type": "file",
                    "value": "{json_document}"
                }
            ],
        },
        "ingestion_url": "http://metadata.local/private/ingest",
        "table_id": EXAMPLE_TABLE_ID,
    },
}

EXAMPLE_RUN_BODY = {
    **EXAMPLE_RUN,
    "workflow_params": json.dumps(EXAMPLE_RUN["workflow_params"]),
    "workflow_engine_parameters": json.dumps(EXAMPLE_RUN["workflow_engine_parameters"]),
    "tags": json.dumps(EXAMPLE_RUN["tags"]),
}


def test_runs_endpoint(client):
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
    assert run["details"]["run_log"]["name"] == "ingest"
    assert run["details"]["run_log"]["cmd"] == ""
    assert run["details"]["run_log"]["start_time"] == ""
    assert run["details"]["run_log"]["end_time"] == ""
    assert run["details"]["run_log"]["stdout"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stdout"
    assert run["details"]["run_log"]["stderr"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stderr"
    assert run["details"]["run_log"]["exit_code"] is None

    assert tuple(sorted(run.keys())) == ("details", "run_id", "state")


def test_run_detail_endpoint(client):
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

    rv = client.get(f"/runs/{cr_data['run_id']}")
    run = rv.get_json()

    assert run["run_id"] == cr_data["run_id"]
    assert run["state"] == STATE_QUEUED

    assert json.dumps(run["request"], sort_keys=True) == json.dumps(EXAMPLE_RUN, sort_keys=True)
    assert json.dumps(run["task_logs"], sort_keys=True) == json.dumps([], sort_keys=True)  # TODO: Tasks impl

    assert "id" in run["run_log"]
    assert run["run_log"]["name"] == "ingest"
    assert run["run_log"]["cmd"] == ""
    assert run["run_log"]["start_time"] == ""
    assert run["run_log"]["end_time"] == ""
    assert run["run_log"]["stdout"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stdout"
    assert run["run_log"]["stderr"] == f"http://127.0.0.1:5000/runs/{cr_data['run_id']}/stderr"
    assert run["run_log"]["exit_code"] is None

    assert json.dumps(run["outputs"]) == json.dumps({})

    assert tuple(sorted(run.keys())) == ("outputs", "request", "run_id", "run_log", "state", "task_logs")


def test_run_status_endpoint(client):
    rv = client.post("/runs", data=EXAMPLE_RUN_BODY)
    cr_data = rv.get_json()

    rv = client.get(f"/runs/{cr_data['run_id']}/status")
    assert json.dumps(rv.get_json(), sort_keys=True) == json.dumps({**cr_data, "state": STATE_QUEUED}, sort_keys=True)
