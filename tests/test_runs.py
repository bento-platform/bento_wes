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
        "workflow_id": "ingest",  # TODO
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
    print(rv.get_json(), flush=True)
    assert rv.status_code == 200  # 200 is WES spec, even though 201 would be better (?)
    cr_data = rv.get_json()
    assert "run_id" in cr_data

    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data) == json.dumps([{**cr_data, "state": STATE_QUEUED}])

    # TODO: Test with_details


def test_run_finish():
    pass
