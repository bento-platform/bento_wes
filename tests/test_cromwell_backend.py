import pytest
from pathlib import Path

from bento_wes.backends.cromwell_local import CromwellLocalBackend


FAKE_TMP_DIR = Path("/tmp/cromwell-executions")
FAKE_OUTPUT_DIR = Path("/o")

OUTPUT_TYPES = {
    "k1": "Array[Path]",
    "k2": "Path",
    "k3": "String",
    "k4": "Array[String]",
    "k5": "Int",
    "k6": "Array[Array[Path]]",
}


@pytest.mark.parametrize(
    "outputs, expected_final",
    [
        (
            {
                "k1": ["/tmp/cromwell-executions/asdf", "/tmp/cromwell-executions/qwerty"],
                "k2": "/tmp/cromwell-executions/uiop",
                "k3": "/tmp/cromwell-executions/no-rewrite",
                "k4": ["/tmp/cromwell-executions/no-rewrite", "hello world"],
                "k5": 5,
                "k6": [["/tmp/cromwell-executions/asdf"], ["/tmp/cromwell-executions/qwerty"]],
            },
            {
                "k1": {"type": OUTPUT_TYPES["k1"], "value": ["/o/asdf", "/o/qwerty"]},
                "k2": {"type": OUTPUT_TYPES["k2"], "value": "/o/uiop"},
                "k3": {"type": OUTPUT_TYPES["k3"], "value": "/tmp/cromwell-executions/no-rewrite"},
                "k4": {"type": OUTPUT_TYPES["k4"], "value": ["/tmp/cromwell-executions/no-rewrite", "hello world"]},
                "k5": {"type": OUTPUT_TYPES["k5"], "value": 5},
                "k6": {"type": OUTPUT_TYPES["k6"], "value": [["/o/asdf"], ["/o/qwerty"]]},
            },
        ),
        (
            {
                "k1": None,
                "k2": "/tmp/cromwell-executions/uiop",
                "k3": "/tmp/cromwell-executions/no-rewrite",
                "k4": None,
                "k5": 5,
                "k6": [None, ["/tmp/cromwell-executions/qwerty"]],
            },
            {
                "k1": {"type": OUTPUT_TYPES["k1"], "value": None},
                "k2": {"type": OUTPUT_TYPES["k2"], "value": "/o/uiop"},
                "k3": {"type": OUTPUT_TYPES["k3"], "value": "/tmp/cromwell-executions/no-rewrite"},
                "k4": {"type": OUTPUT_TYPES["k4"], "value": None},
                "k5": {"type": OUTPUT_TYPES["k5"], "value": 5},
                "k6": {"type": OUTPUT_TYPES["k6"], "value": [None, ["/o/qwerty"]]},
            },
        ),
    ],
)
def test_process_workflow_outputs(outputs: dict, expected_final: dict):
    assert (
        CromwellLocalBackend.process_workflow_outputs(outputs, OUTPUT_TYPES, FAKE_TMP_DIR, FAKE_OUTPUT_DIR)
        == expected_final
    )
