import json

import pytest

from bento_wes.models import RunRequest

from .constants import EXAMPLE_RUN

DROP_BOX_INPUTS = [
    {"id": "file_in", "type": "file", "required": False},
    {"id": "files_in", "type": "file[]", "required": False},
    {"id": "dir_in", "type": "directory", "required": False},
]


def _run_request(params: dict) -> RunRequest:
    tags = json.loads(json.dumps(EXAMPLE_RUN["tags"]))
    tags["workflow_metadata"]["inputs"] = [
        i for i in tags["workflow_metadata"]["inputs"] if i["type"] == "project:dataset"
    ] + DROP_BOX_INPUTS
    return RunRequest(
        **{
            **EXAMPLE_RUN,
            "workflow_params": json.dumps({"phenopackets_json.project_dataset": "p:d", **params}),
            "workflow_engine_parameters": "{}",
            "tags": json.dumps(tags),
        }
    )


@pytest.mark.parametrize(
    "params",
    [
        {"phenopackets_json.file_in": "/project/a.json"},
        {"phenopackets_json.files_in": ["", "/project/a.json"]},
        {"phenopackets_json.dir_in": "/"},
    ],
)
def test_has_drop_box_inputs(params):
    assert _run_request(params).has_drop_box_inputs()


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"phenopackets_json.file_in": ""},
        {"phenopackets_json.files_in": []},
        {"phenopackets_json.files_in": ["", ""]},
        {"phenopackets_json.dir_in": ""},
    ],
)
def test_has_no_drop_box_inputs(params):
    assert not _run_request(params).has_drop_box_inputs()
