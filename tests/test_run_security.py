"""
Regression tests for client-controlled workflow metadata on POST /runs: the server must use the workflow provider's
definition (not tags.workflow_metadata) for authz, parameter handling and secret injection.
"""

import asyncio
import json

import httpx
import pytest
import respx
from bento_lib.auth.permissions import P_INGEST_DATA
from bento_lib.auth.resources import RESOURCE_EVERYTHING, build_resource
from bento_lib.workflows.models import WorkflowDefinition

from bento_wes.models import InvalidRunRequest, RunRequest
from bento_wes.workflow_resolver import WorkflowResolutionError, WorkflowResolver

from .conftest import FakeServiceManager, mock_workflow_provider
from .constants import (
    EXAMPLE_DATASET_ID,
    EXAMPLE_PROJECT_ID,
    EXAMPLE_RUN,
    EXAMPLE_WORKFLOW_DEFINITION,
    METADATA_URL,
    TEST_SERVICES,
)

WID = "phenopackets_json"
OWN_PD = f"{EXAMPLE_PROJECT_ID}:{EXAMPLE_DATASET_ID}"
OTHER_PD = "11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222"
ATTACKER_URL = "https://attacker.example"

# Mirrors Katsu's real phenopackets_json workflow: the service URL and token are injected by the server.
KATSU_LIKE_DEFINITION = {
    **EXAMPLE_WORKFLOW_DEFINITION,
    "inputs": [
        *EXAMPLE_WORKFLOW_DEFINITION["inputs"],
        {"id": "katsu_url", "type": "service-url", "service_kind": "metadata", "injected": True},
        {"id": "access_token", "type": "secret", "key": "access_token", "injected": True},
    ],
}


def _body(workflow_params: dict, workflow_metadata: dict, workflow_url: str | None = None) -> dict:
    return {
        "workflow_params": json.dumps(workflow_params),
        "workflow_type": "WDL",
        "workflow_type_version": "1.0",
        "workflow_engine_parameters": json.dumps({}),
        "workflow_url": workflow_url or f"{METADATA_URL}/workflows/{WID}.wdl",
        "tags": json.dumps({"workflow_id": WID, "workflow_metadata": workflow_metadata}),
    }


@pytest.fixture
def katsu_provider():
    with respx.mock as respx_mock:
        mock_workflow_provider(respx_mock, WID, KATSU_LIKE_DEFINITION)
        yield respx_mock


@pytest.fixture
def authz_calls(client):
    """Record (permission, resource) authz checks made by POST /runs."""
    from bento_wes.routers.runs.deps import evaluate_run_permissions_function

    calls: list[tuple] = []

    async def _check(permission, resource):
        calls.append((permission, resource))

    client.app.dependency_overrides[evaluate_run_permissions_function] = lambda: _check
    return calls


# The attack from the report: the client re-declares the workflow so that the service URL is no longer "injected",
# declares the token as a secret input, and points katsu_url at their own host.
ATTACKER_METADATA = {
    **EXAMPLE_WORKFLOW_DEFINITION,
    "type": "analysis",  # chosen to match whatever permission the caller happens to hold
    "inputs": [
        {"id": "project_dataset", "type": "project:dataset"},
        {"id": "json_document", "type": "file"},
        {"id": "access_token", "type": "secret", "key": "access_token", "injected": True},
        # katsu_url deliberately NOT declared, so the server would not overwrite it
    ],
}


def test_injected_service_url_cannot_be_overridden(client, katsu_provider, authz_calls):
    params = {
        f"{WID}.project_dataset": OWN_PD,
        f"{WID}.json_document": "test.json",
        f"{WID}.katsu_url": ATTACKER_URL,
    }
    rv = client.post("/runs", data=_body(params, ATTACKER_METADATA))
    assert rv.status_code == 200, rv.text
    create_authz_calls = list(authz_calls)

    run = client.get(f"/runs/{rv.json()['run_id']}").json()
    stored = run["request"]

    # The server-resolved definition is what got stored (and what the runner will use to inject secrets)
    assert stored["tags"]["workflow_metadata"] == WorkflowDefinition.model_validate(KATSU_LIKE_DEFINITION).model_dump(
        mode="json"
    )
    # The service URL comes from the service registry, never from the client
    assert stored["workflow_params"][f"{WID}.katsu_url"] == METADATA_URL
    # Permission comes from the trusted definition type (ingestion), not the client's claimed "analysis"
    assert create_authz_calls == [
        (P_INGEST_DATA, build_resource(EXAMPLE_PROJECT_ID, EXAMPLE_DATASET_ID, "phenopacket")),
    ]


def test_client_workflow_url_is_ignored(client, katsu_provider, authz_calls):
    params = {f"{WID}.project_dataset": OWN_PD, f"{WID}.json_document": "test.json"}
    # Same allow-listed host, different WDL. respx would raise if WES tried to download it.
    rv = client.post("/runs", data=_body(params, ATTACKER_METADATA, f"{METADATA_URL}/workflows/other.wdl"))
    assert rv.status_code == 200, rv.text

    run = client.get(f"/runs/{rv.json()['run_id']}").json()
    assert run["request"]["workflow_url"] == f"{METADATA_URL}/workflows/{WID}.wdl"


def test_undeclared_param_rejected(client, katsu_provider, authz_calls):
    params = {
        f"{WID}.project_dataset": OWN_PD,
        f"{WID}.json_document": "test.json",
        f"{WID}.some_wdl_default": ATTACKER_URL,
    }
    rv = client.post("/runs", data=_body(params, ATTACKER_METADATA))
    assert rv.status_code == 400
    assert "some_wdl_default" in rv.text


def test_decoy_project_dataset_input_ignored(client, katsu_provider, authz_calls):
    # Client declares an extra project:dataset input pointing at its own dataset, and uses the real one for another.
    metadata = {
        **ATTACKER_METADATA,
        "inputs": [{"id": "decoy", "type": "project:dataset"}, *ATTACKER_METADATA["inputs"]],
    }
    params = {f"{WID}.decoy": OWN_PD, f"{WID}.project_dataset": OTHER_PD, f"{WID}.json_document": "test.json"}
    rv = client.post("/runs", data=_body(params, metadata))

    # "decoy" isn't an input of the real workflow -> rejected, but only after authz was checked on the REAL input
    assert rv.status_code == 400
    assert authz_calls == [
        (P_INGEST_DATA, build_resource(*OTHER_PD.split(":"), data_type="phenopacket")),
    ]


@pytest.mark.parametrize("bad_pd", ["no-colon", "a:b:c", ":ds", "proj:"])
def test_malformed_project_dataset_rejected(client, katsu_provider, authz_calls, bad_pd):
    params = {f"{WID}.project_dataset": bad_pd, f"{WID}.json_document": "test.json"}
    rv = client.post("/runs", data=_body(params, KATSU_LIKE_DEFINITION))
    assert rv.status_code == 400
    assert authz_calls == []


def test_missing_required_project_dataset_rejected(client, katsu_provider, authz_calls):
    params = {f"{WID}.json_document": "test.json"}
    rv = client.post("/runs", data=_body(params, KATSU_LIKE_DEFINITION))
    assert rv.status_code == 400
    assert authz_calls == []  # must not fall back to checking against RESOURCE_EVERYTHING-and-proceed


def test_unknown_workflow_rejected(client, authz_calls):
    with respx.mock as respx_mock:
        respx_mock.get(f"{METADATA_URL}/workflows/{WID}").mock(return_value=httpx.Response(404))
        rv = client.post("/runs", data=_body({}, ATTACKER_METADATA))
    assert rv.status_code == 400
    assert authz_calls == []


# -- RunRequest unit tests -------------------------------------------------------------------------------------------


def _run_request(params: dict, definition: dict) -> RunRequest:
    return RunRequest.model_validate(
        {
            **{
                k: json.dumps(v) if k in ("workflow_params", "workflow_engine_parameters", "tags") else v
                for k, v in EXAMPLE_RUN.items()
            },
            "workflow_params": json.dumps(params),
            "tags": json.dumps({"workflow_id": WID, "workflow_metadata": definition}),
        }
    )


def test_authz_resources_cover_every_project_dataset_input():
    definition = {
        **EXAMPLE_WORKFLOW_DEFINITION,
        "inputs": [{"id": "src", "type": "project:dataset"}, {"id": "dst", "type": "project:dataset"}],
    }
    req = _run_request({f"{WID}.src": OWN_PD, f"{WID}.dst": OTHER_PD}, definition)
    assert req.get_authz_resources() == [
        build_resource(EXAMPLE_PROJECT_ID, EXAMPLE_DATASET_ID, data_type="phenopacket"),
        build_resource(*OTHER_PD.split(":"), data_type="phenopacket"),
    ]


def test_authz_resources_optional_project_dataset():
    definition = {**EXAMPLE_WORKFLOW_DEFINITION, "inputs": [{"id": "pd", "type": "project:dataset", "required": False}]}
    assert _run_request({}, definition).get_authz_resources() == [RESOURCE_EVERYTHING]


def test_user_workflow_params_drops_injected_inputs():
    req = _run_request(
        {f"{WID}.project_dataset": OWN_PD, f"{WID}.json_document": "x", f"{WID}.access_token": "fake"},
        KATSU_LIKE_DEFINITION,
    )
    assert req.user_workflow_params() == {f"{WID}.project_dataset": OWN_PD, f"{WID}.json_document": "x"}

    with pytest.raises(InvalidRunRequest):
        _run_request({"other_workflow.x": "1"}, KATSU_LIKE_DEFINITION).user_workflow_params()


# -- WorkflowResolver unit tests -------------------------------------------------------------------------------------


@pytest.fixture
def resolver(settings, logger):
    return WorkflowResolver(FakeServiceManager([dict(s) for s in TEST_SERVICES]), settings, logger)


def test_resolver_resolves_from_provider_only(resolver):
    with respx.mock as respx_mock:  # respx raises if the non-provider drop-box service is queried
        mock_workflow_provider(respx_mock, WID, KATSU_LIKE_DEFINITION)
        resolved = asyncio.run(resolver.resolve(WID))
    assert resolved.wdl_url == f"{METADATA_URL}/workflows/{WID}.wdl"
    assert resolved.definition.type == "ingestion"


@pytest.mark.parametrize("bad_id", ["../evil", "a/b", "x?y=z", ""])
def test_resolver_rejects_bad_ids(resolver, bad_id):
    with pytest.raises(WorkflowResolutionError):
        asyncio.run(resolver.resolve(bad_id))


def test_resolver_rejects_ambiguous_ids(settings, logger):
    other = "http://gohan.local"
    sm = FakeServiceManager(
        [
            *TEST_SERVICES,
            {"url": other, "bento": {"serviceKind": "gohan", "workflowProvider": True}},
        ]
    )
    with respx.mock as respx_mock:
        mock_workflow_provider(respx_mock, WID, KATSU_LIKE_DEFINITION)
        mock_workflow_provider(respx_mock, WID, KATSU_LIKE_DEFINITION, provider_url=other)
        with pytest.raises(WorkflowResolutionError, match="multiple services"):
            asyncio.run(WorkflowResolver(sm, settings, logger).resolve(WID))
