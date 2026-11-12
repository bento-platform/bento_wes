import json

__all__ = [
    "METADATA_URL",
    "GOHAN_URL",
    "REFERENCE_URL",
    "DROP_BOX_URL",
    "TEST_SERVICES",
    "WORKFLOW_PROVIDER_URLS",
    "EXAMPLE_DATASET_ID",
    "EXAMPLE_WORKFLOW_DEFINITION",
    "EXAMPLE_RUN",
    "EXAMPLE_RUN_BODY",
]


METADATA_URL = "http://metadata.local"
GOHAN_URL = "http://gohan.local"
REFERENCE_URL = "http://reference.local"
DROP_BOX_URL = "http://drop-box.local"

# Service-info records as returned by the service registry's /services endpoint (only the fields WES uses.)
# Like a real Bento instance: katsu (metadata) and gohan are data services, only reference is a workflow provider.
TEST_SERVICES = [
    {"url": METADATA_URL, "bento": {"serviceKind": "metadata", "dataService": True}},
    {"url": GOHAN_URL, "bento": {"serviceKind": "variant", "dataService": True}},
    {"url": REFERENCE_URL, "bento": {"serviceKind": "reference", "workflowProvider": True}},
    {"url": DROP_BOX_URL, "bento": {"serviceKind": "drop-box"}},
]
WORKFLOW_PROVIDER_URLS = [METADATA_URL, GOHAN_URL, REFERENCE_URL]

EXAMPLE_PROJECT_ID = "2b98aae0-d67a-48ae-8419-3f3e2b10629b"
EXAMPLE_DATASET_ID = "ef9da1da-ef7f-43d6-ace3-456bf8e58431"

EXAMPLE_RUN = {
    "workflow_params": {
        "phenopackets_json.project_dataset": f"{EXAMPLE_PROJECT_ID}:{EXAMPLE_DATASET_ID}",
        "phenopackets_json.json_document": "http://my-server.local/test.json",
    },
    "workflow_type": "WDL",
    "workflow_type_version": "1.0",
    "workflow_engine_parameters": {},
    "workflow_url": "http://metadata.local/workflows/phenopackets_json.wdl",
    "tags": {
        "workflow_id": "phenopackets_json",
        "workflow_metadata": {
            "name": "Bento Phenopackets-Compatible JSON",
            "description": (
                "This ingestion workflow will validate and import a Phenopackets schema-compatible JSON document."
            ),
            "type": "ingestion",
            "data_type": "phenopacket",
            "tags": ["phenopacket"],
            "file": "phenopackets_json.wdl",
            "inputs": [
                {
                    "id": "project_dataset",
                    "help": "",
                    "type": "project:dataset",
                    "required": True,
                },
                {
                    "id": "json_document",
                    "help": "",
                    "type": "file",
                    "required": True,
                    "pattern": r"^.json$",
                },
            ],
        },
    },
}

# The definition the (mocked) workflow provider serves for phenopackets_json; identical to what the client sends.
EXAMPLE_WORKFLOW_DEFINITION = EXAMPLE_RUN["tags"]["workflow_metadata"]

EXAMPLE_RUN_BODY = {
    **EXAMPLE_RUN,
    "workflow_params": json.dumps(EXAMPLE_RUN["workflow_params"]),
    "workflow_engine_parameters": json.dumps(EXAMPLE_RUN["workflow_engine_parameters"]),
    "tags": json.dumps(EXAMPLE_RUN["tags"]),
}
