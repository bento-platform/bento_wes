from .constants import EXAMPLE_RUN
from bento_wes.workflows import (
    count_bento_workflow_file_outputs,
    parse_workflow_host_allow_list,
)


def test_count_file_outputs():
    assert count_bento_workflow_file_outputs(
        "phenopackets_json",
        EXAMPLE_RUN["workflow_params"],
        EXAMPLE_RUN["tags"]["workflow_metadata"])


def test_parse_allow_list():
    assert parse_workflow_host_allow_list("") is None
    assert parse_workflow_host_allow_list("a") == {"a"}
    assert parse_workflow_host_allow_list("a,") == {"a"}
    assert parse_workflow_host_allow_list("a,a") == {"a"}
    assert parse_workflow_host_allow_list("a, a") == {"a"}
    assert parse_workflow_host_allow_list("a,b") == {"a", "b"}
    assert parse_workflow_host_allow_list("a, b") == {"a", "b"}
