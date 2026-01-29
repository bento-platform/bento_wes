import asyncio

import httpx
import shutil

from base64 import urlsafe_b64encode
from fastapi import Depends, status
from pathlib import Path
from pydantic import AnyUrl
from structlog.stdlib import BoundLogger
from typing import NewType, Annotated
from urllib.parse import urlparse

from bento_wes import states
from bento_wes.config import Settings, SettingsDep
from bento_wes.logger import LoggerDep

__all__ = [
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
    "WORKFLOW_IGNORE_FILE_PATH_INJECTION",
    "parse_workflow_host_allow_list",
    "UnsupportedWorkflowType",
    "WorkflowDownloadError",
    "WorkflowManager",
    "get_workflow_manager",
    "WorkflowManagerDep",
]

WorkflowType = NewType("WorkflowType", str)

WES_WORKFLOW_TYPE_WDL = WorkflowType("WDL")
WES_WORKFLOW_TYPE_CWL = WorkflowType("CWL")

# Currently, only WDL is supported
WES_SUPPORTED_WORKFLOW_TYPES = frozenset({WES_WORKFLOW_TYPE_WDL})

WORKFLOW_EXTENSIONS: dict[WorkflowType, str] = {
    WES_WORKFLOW_TYPE_WDL: "wdl",
    WES_WORKFLOW_TYPE_CWL: "cwl",
}

ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 50000  # 50 KB

# Workflow IDs for which input file(s) must be a URL reference, instead of an injected temp file.
# TODO: find a way for WES to get this info from the workflow/service, instead of hard-coding
WORKFLOW_IGNORE_FILE_PATH_INJECTION = frozenset({"vcf_gz"})


def parse_workflow_host_allow_list(allow_list: str | None) -> set[str] | None:
    """
    Get set of allowed workflow hosts from a configuration string for any
    checks while downloading workflows. If it's blank, assume that means
    "any host is allowed" and set to None (as opposed to empty, i.e. no hosts
    allowed to provide workflows.)
    :param allow_list: Comma-separated list of allowed workflow hosts, or None.
    :return:
    """
    return {a.strip() for a in (allow_list or "").split(",") if a.strip()} or None


class UnsupportedWorkflowType(Exception):
    pass


class WorkflowDownloadError(Exception):
    pass


class WorkflowManager:
    def __init__(self, settings: Settings, logger: BoundLogger):
        self.tmp_dir: Path = settings.service_temp
        self.service_base_url: str = settings.service_base_url
        self.bento_url: str = str(settings.bento_url)
        self.logger: BoundLogger = logger
        self.workflow_host_allow_list: set[str] | None = parse_workflow_host_allow_list(
            settings.workflow_host_allow_list
        )
        self._validate_ssl: bool = settings.bento_validate_ssl
        self._debug_mode: bool = settings.bento_debug

        self.logger.debug("instantiating WorkflowManager", debug_mode=self._debug_mode)

    def workflow_path(self, workflow_uri: AnyUrl, workflow_type: WorkflowType) -> Path:
        """
        Generates a unique filesystem path name for a specified workflow URI.
        """
        if workflow_type not in WES_SUPPORTED_WORKFLOW_TYPES:
            raise UnsupportedWorkflowType(f"Unsupported workflow type: {workflow_type}")

        workflow_name = str(
            urlsafe_b64encode(bytes(str(workflow_uri), encoding="utf-8")),
            encoding="utf-8",
        ).replace("=", "")
        return self.tmp_dir / f"workflow_{workflow_name}.{WORKFLOW_EXTENSIONS[workflow_type]}"

    async def download_or_copy_workflow(
        self,
        workflow_uri: AnyUrl,
        workflow_type: WorkflowType,
        auth_headers: dict,
    ) -> str | None:
        """
        Given a URI, downloads the specified workflow via its URI, or copies it over if it's on the local
        file system. # TODO: Local file system = security issue?
        :param workflow_uri: The workflow URI to download/copy
        :param workflow_type: The type of the workflow being downloaded
        :param auth_headers: Authorization headers to pass while requesting the workflow file.
        """

        # TODO: Handle references to attachments

        logger = self.logger.bind(workflow_uri=workflow_uri)

        workflow_path = self.workflow_path(workflow_uri, workflow_type)

        if workflow_uri.scheme not in ALLOWED_WORKFLOW_REQUEST_SCHEMES:  # file://
            # TODO: Other else cases
            # TODO: Handle exceptions
            await asyncio.to_thread(shutil.copyfile, str(workflow_uri.path), workflow_path)
            return None

        if self.workflow_host_allow_list is not None:
            # We need to check that the workflow in question is from an
            # allowed set of workflow hosts
            if workflow_uri.scheme != "file" and workflow_uri.host not in self.workflow_host_allow_list:
                # Dis-allowed workflow URL
                await logger.aerror(
                    "dis-allowed workflow host",
                    allow_list=self.workflow_host_allow_list,
                )
                return states.STATE_EXECUTOR_ERROR

        await logger.ainfo("fetching workflow file")

        # SECURITY: We cannot pass our auth token outside the Bento instance. Validate that BENTO_URL is
        # a) a valid URL and b) a prefix of our workflow's URI before downloading.

        use_auth_headers: bool = False
        if workflow_uri.path is not None:
            parsed_bento_url = urlparse(self.bento_url)
            use_auth_headers = all(
                (
                    self.bento_url,
                    parsed_bento_url.scheme == workflow_uri.scheme,
                    parsed_bento_url.netloc == workflow_uri.host,
                    workflow_uri.path.startswith(parsed_bento_url.path),
                )
            )

        logger = logger.bind(use_auth_headers=use_auth_headers)

        # TODO: Better auth? May only be allowed to access specific workflows
        try:
            url = str(workflow_uri)
            async with httpx.AsyncClient(verify=self._validate_ssl, timeout=None, follow_redirects=True) as client:
                resp = await client.get(
                    url,
                    headers={
                        "Host": urlparse(url or "").netloc or "",
                        **(auth_headers if use_auth_headers else {}),
                    },
                )
        except httpx.RequestError as e:
            if workflow_path.exists():
                return None
            raise e

        content = await resp.aread()

        if resp.status_code == status.HTTP_200_OK and len(content) < MAX_WORKFLOW_FILE_BYTES:
            if workflow_path.exists():
                await asyncio.to_thread(workflow_path.unlink)

            await asyncio.to_thread(workflow_path.write_bytes, content)

            await logger.ainfo("workflow file downloaded")

        elif not workflow_path.exists():  # Use cached version if needed, otherwise error
            # Request issues
            await logger.aerror("error downloading workflow", status_code=resp.status_code)
            raise WorkflowDownloadError(f"WorkflowDownloadError: {workflow_path} does not exist")

        return None


def get_workflow_manager(settings: SettingsDep, logger: LoggerDep) -> WorkflowManager:
    # BoundLogger isn't hashable; cannot memoize this
    return WorkflowManager(settings, logger)


WorkflowManagerDep = Annotated[WorkflowManager, Depends(get_workflow_manager)]
