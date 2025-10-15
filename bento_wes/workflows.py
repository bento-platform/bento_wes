import logging
import shutil
import httpx
import asyncio

from base64 import urlsafe_b64encode
from pathlib import Path
from pydantic import AnyUrl
from typing import NewType
from urllib.parse import urlparse

from bento_wes import states

__all__ = [
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
    "WORKFLOW_IGNORE_FILE_PATH_INJECTION",
    "parse_workflow_host_allow_list",
    "UnsupportedWorkflowType",
    "WorkflowDownloadError",
    "WorkflowManager",
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

# Workflow IDs for which input file(s) must be a URL reference.
WORKFLOW_IGNORE_FILE_PATH_INJECTION = frozenset({"vcf_gz"})


def parse_workflow_host_allow_list(allow_list: str | None) -> set[str] | None:
    return {a.strip() for a in (allow_list or "").split(",") if a.strip()} or None


class UnsupportedWorkflowType(Exception):
    pass


class WorkflowDownloadError(Exception):
    pass


class WorkflowManager:
    def __init__(
        self,
        tmp_dir: Path,
        service_base_url: str,
        bento_url: str | None = None,
        logger: logging.Logger | None = None,
        workflow_host_allow_list: str | None = None,
        validate_ssl: bool = True,
        debug: bool = False,
    ):
        self.tmp_dir: Path = tmp_dir
        self.service_base_url: str = service_base_url
        self.bento_url: str | None = bento_url
        self.logger: logging.Logger | None = logger
        self.workflow_host_allow_list: str | None = workflow_host_allow_list
        self._validate_ssl: bool = validate_ssl
        self._debug_mode: bool = debug

        self._debug(f"Instantiating WorkflowManager with debug_mode={self._debug_mode}")

    def _debug(self, message: str):
        if self.logger:
            self.logger.debug(message)

    def _info(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _error(self, message: str):
        if self.logger:
            self.logger.error(message)

    def workflow_path(self, workflow_uri: AnyUrl, workflow_type: WorkflowType) -> Path:
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
        Async version using httpx.AsyncClient. Non-blocking file I/O via asyncio.to_thread.
        """
        workflow_path = self.workflow_path(workflow_uri, workflow_type)

        if workflow_uri.scheme not in ALLOWED_WORKFLOW_REQUEST_SCHEMES:
            await asyncio.to_thread(shutil.copyfile, workflow_uri.path, workflow_path)
            return

        if self.workflow_host_allow_list is not None:
            if workflow_uri.scheme != "file" and workflow_uri.host not in self.workflow_host_allow_list:
                self._error(
                    f"Dis-allowed workflow host: {workflow_uri.host} (allow list: {self.workflow_host_allow_list})"
                )
                return states.STATE_EXECUTOR_ERROR

        self._info(f"Fetching workflow file from {workflow_uri}")

        use_auth_headers: bool = False
        if self.bento_url:
            parsed_bento_url = urlparse(self.bento_url)
            use_auth_headers = all(
                (
                    self.bento_url,
                    parsed_bento_url.scheme == workflow_uri.scheme,
                    parsed_bento_url.netloc == workflow_uri.host,
                    workflow_uri.path.startswith(parsed_bento_url.path),
                )
            )

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
                return
            raise e

        content = await resp.aread()
        if resp.status_code == 200 and len(content) < MAX_WORKFLOW_FILE_BYTES:
            if workflow_path.exists():
                await asyncio.to_thread(workflow_path.unlink)

            await asyncio.to_thread(workflow_path.write_bytes, content)

            self._info("Workflow file downloaded")
        elif not workflow_path.exists():
            self._error(
                f"Error downloading workflow: {workflow_uri} (use_auth_headers={use_auth_headers}, "
                f"wr.status_code={resp.status_code})"
            )
            raise WorkflowDownloadError(f"WorkflowDownloadError: {workflow_path} does not exist")
