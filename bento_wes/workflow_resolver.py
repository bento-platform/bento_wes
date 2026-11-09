"""
Server-side resolution of workflow definitions.

Run requests carry a client-supplied copy of the workflow definition (tags.workflow_metadata) and a client-supplied
workflow URL. The definition cannot be trusted: it decides which permission/resource is checked, which parameters
are injected by the server, and which inputs receive secrets (e.g., the WES access token).

The workflow URL is only used as a pointer to the provider: it must exactly match {provider}/workflows/{id}.wdl for a
service the registry lists as a workflow provider (bento.workflowProvider). The definition is then fetched by WES from
that provider and used for everything, instead of the client's copy.
"""

import logging
import re
from dataclasses import dataclass
from typing import Annotated

import httpx
from bento_lib.service_info.manager import ServiceManager
from bento_lib.workflows.models import WorkflowDefinition
from fastapi import Depends, status
from pydantic import AnyUrl, ValidationError

from .config import Settings, SettingsDep
from .logger import LoggerDep
from .service_registry import ServiceManagerDep

__all__ = [
    "WorkflowResolutionError",
    "ResolvedWorkflow",
    "WorkflowResolver",
    "get_workflow_resolver",
    "WorkflowResolverDep",
]

# Workflow IDs are interpolated into provider URLs, so restrict them to a safe character set.
WORKFLOW_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")

WORKFLOW_FETCH_TIMEOUT = 10  # seconds


class WorkflowResolutionError(Exception):
    pass


@dataclass(frozen=True)
class ResolvedWorkflow:
    definition: WorkflowDefinition
    wdl_url: str


class WorkflowResolver:
    def __init__(self, service_manager: ServiceManager, settings: Settings, logger: logging.Logger):
        self._service_manager = service_manager
        self._validate_ssl: bool = settings.bento_validate_ssl
        self._logger = logger

    async def _get_provider_urls(self) -> list[str]:
        services = await self._service_manager.fetch_service_list()
        return [s["url"].rstrip("/") for s in services if s.get("bento", {}).get("workflowProvider") and s.get("url")]

    async def _find_provider(self, workflow_id: str, workflow_url: AnyUrl) -> str:
        """
        Find the registered workflow provider whose WDL URL for this workflow ID is exactly workflow_url.
        Both sides are normalized through AnyUrl, so e.g. host casing doesn't matter, but scheme/port/path must match.
        """
        if workflow_url.query or workflow_url.fragment:
            raise WorkflowResolutionError("workflow_url must not include a query string or fragment")

        provider_urls = await self._get_provider_urls()
        if not provider_urls:
            raise WorkflowResolutionError("No workflow providers found in the service registry")

        requested = str(workflow_url)
        for provider_url in provider_urls:
            if str(AnyUrl(f"{provider_url}/workflows/{workflow_id}.wdl")) == requested:
                return provider_url

        raise WorkflowResolutionError(
            f"workflow_url {requested} is not a registered workflow provider's URL for workflow {workflow_id}"
        )

    async def _fetch_definition(self, provider_url: str, workflow_id: str) -> WorkflowDefinition:
        url = f"{provider_url}/workflows/{workflow_id}"
        try:
            async with httpx.AsyncClient(verify=self._validate_ssl, timeout=WORKFLOW_FETCH_TIMEOUT) as client:
                res = await client.get(url)
        except httpx.RequestError as e:
            self._logger.warning("Could not reach workflow provider %s: %s", provider_url, e)
            raise WorkflowResolutionError(f"Could not reach workflow provider for {workflow_id}")

        if res.status_code == status.HTTP_404_NOT_FOUND:
            raise WorkflowResolutionError(f"Unknown workflow ID: {workflow_id}")
        if res.status_code != status.HTTP_200_OK:
            self._logger.warning("Workflow provider %s returned status %d for %s", provider_url, res.status_code, url)
            raise WorkflowResolutionError(f"Could not fetch workflow definition for {workflow_id}")

        try:
            return WorkflowDefinition.model_validate(res.json())
        except (ValueError, ValidationError) as e:
            self._logger.warning("Workflow provider %s returned an invalid definition for %s: %s", provider_url, url, e)
            raise WorkflowResolutionError(f"Invalid workflow definition for {workflow_id}")

    async def resolve(self, workflow_id: str, workflow_url: AnyUrl) -> ResolvedWorkflow:
        """
        Fetch a workflow definition from the registered provider that workflow_url points to.
        :raises WorkflowResolutionError: if the ID is malformed, workflow_url isn't a registered provider's WDL URL for
                                         this ID, or the provider doesn't serve a valid definition for it.
        """

        if not WORKFLOW_ID_PATTERN.fullmatch(workflow_id):
            raise WorkflowResolutionError(f"Invalid workflow ID: {workflow_id!r}")

        provider_url = await self._find_provider(workflow_id, workflow_url)
        definition = await self._fetch_definition(provider_url, workflow_id)
        return ResolvedWorkflow(definition=definition, wdl_url=f"{provider_url}/workflows/{workflow_id}.wdl")


def get_workflow_resolver(
    service_manager: ServiceManagerDep, settings: SettingsDep, logger: LoggerDep
) -> WorkflowResolver:
    return WorkflowResolver(service_manager, settings, logger)


WorkflowResolverDep = Annotated[WorkflowResolver, Depends(get_workflow_resolver)]
