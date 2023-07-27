import json

__all__ = [
    "EXAMPLE_TABLE_ID",
    "EXAMPLE_RUN",
    "EXAMPLE_RUN_BODY",
]


EXAMPLE_TABLE_ID = "ef9da1da-ef7f-43d6-ace3-456bf8e58431"

EXAMPLE_RUN = {
    "workflow_params": {
        "phenopackets_json.json_document": "http://my-server.local/test.json",
    },
    "workflow_type": "WDL",
    "workflow_type_version": "1.0",
    "workflow_engine_parameters": {},
    "workflow_url": "http://metadata.local/workflows/phenopackets_json.wdl",
    "tags": {
        "workflow_id": "phenopackets_json",
        "workflow_metadata": {
            "id": "phenopackets_json",
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
