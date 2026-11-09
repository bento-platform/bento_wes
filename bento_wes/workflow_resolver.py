"""
Server-side resolution of workflow definitions.

Run requests carry a client-supplied copy of the workflow definition (tags.workflow_metadata) and a client-supplied
workflow URL. Neither can be trusted: the definition decides which permission/resource is checked, which parameters
are injected by the server, and which inputs receive secrets (e.g., the WES access token). Instead, we look the
workflow up by ID in the Bento services which declare themselves as workflow providers, and use that definition and
its canonical WDL URL for everything.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Annotated

import httpx
from bento_lib.service_info.manager import ServiceManager
from bento_lib.workflows.models import WorkflowDefinition
from fastapi import Depends, status
from pydantic import ValidationError

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

    async def _fetch_from_provider(
        self, client: httpx.AsyncClient, provider_url: str, workflow_id: str
    ) -> ResolvedWorkflow | None:
        url = f"{provider_url}/workflows/{workflow_id}"
        try:
            res = await client.get(url)
        except httpx.RequestError as e:
            self._logger.warning("Could not reach workflow provider %s: %s", provider_url, e)
            return None

        if res.status_code == status.HTTP_404_NOT_FOUND:
            return None
        if res.status_code != status.HTTP_200_OK:
            self._logger.warning("Workflow provider %s returned status %d for %s", provider_url, res.status_code, url)
            return None

        try:
            definition = WorkflowDefinition.model_validate(res.json())
        except (ValueError, ValidationError) as e:
            self._logger.warning("Workflow provider %s returned an invalid definition for %s: %s", provider_url, url, e)
            return None

        return ResolvedWorkflow(definition=definition, wdl_url=f"{url}.wdl")

    async def resolve(self, workflow_id: str) -> ResolvedWorkflow:
        """
        Look up a workflow definition by ID across all Bento workflow providers.
        :raises WorkflowResolutionError: if the ID is malformed, unknown, or provided by more than one service.
        """

        if not WORKFLOW_ID_PATTERN.fullmatch(workflow_id):
            raise WorkflowResolutionError(f"Invalid workflow ID: {workflow_id!r}")

        provider_urls = await self._get_provider_urls()
        if not provider_urls:
            raise WorkflowResolutionError("No workflow providers found in the service registry")

        async with httpx.AsyncClient(verify=self._validate_ssl, timeout=WORKFLOW_FETCH_TIMEOUT) as client:
            results = await asyncio.gather(*(self._fetch_from_provider(client, u, workflow_id) for u in provider_urls))

        matches = [r for r in results if r is not None]

        if not matches:
            raise WorkflowResolutionError(f"Unknown workflow ID: {workflow_id}")
        if len(matches) > 1:
            # Refuse to guess - otherwise, the permission/resource check could be done against the wrong definition.
            raise WorkflowResolutionError(
                f"Workflow ID {workflow_id} is provided by multiple services: {[m.wdl_url for m in matches]}"
            )

        return matches[0]


def get_workflow_resolver(
    service_manager: ServiceManagerDep, settings: SettingsDep, logger: LoggerDep
) -> WorkflowResolver:
    return WorkflowResolver(service_manager, settings, logger)


WorkflowResolverDep = Annotated[WorkflowResolver, Depends(get_workflow_resolver)]
