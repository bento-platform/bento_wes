import logging

import os
import shutil
import requests

from base64 import urlsafe_b64encode
from pydantic import AnyUrl
from typing import NewType
from urllib.parse import urlparse

from bento_wes import states

__all__ = [
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
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

ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 50000  # 50 KB


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
    def __init__(
        self,
        tmp_dir: str,
        service_base_url: str,
        bento_url: str | None = None,
        logger: logging.Logger | None = None,
        workflow_host_allow_list: str | None = None,
        validate_ssl: bool = True,
        debug: bool = False,
    ):
        self.tmp_dir: str = tmp_dir
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

    def workflow_path(self, workflow_uri: AnyUrl, workflow_type: WorkflowType) -> str:
        """
        Generates a unique filesystem path name for a specified workflow URI.
        """
        if workflow_type not in WES_SUPPORTED_WORKFLOW_TYPES:
            raise UnsupportedWorkflowType(f"Unsupported workflow type: {workflow_type}")

        workflow_name = str(urlsafe_b64encode(bytes(str(workflow_uri), encoding="utf-8")), encoding="utf-8")
        return os.path.join(self.tmp_dir, f"workflow_{workflow_name}.{WORKFLOW_EXTENSIONS[workflow_type]}")

    def download_or_copy_workflow(
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

        workflow_path = self.workflow_path(workflow_uri, workflow_type)

        if workflow_uri.scheme not in ALLOWED_WORKFLOW_REQUEST_SCHEMES:  # file://
            # TODO: Other else cases
            # TODO: Handle exceptions
            shutil.copyfile(workflow_uri.path, workflow_path)
            return

        if self.workflow_host_allow_list is not None:
            # We need to check that the workflow in question is from an
            # allowed set of workflow hosts
            if workflow_uri.scheme != "file" and workflow_uri.netloc not in self.workflow_host_allow_list:
                # Dis-allowed workflow URL
                self._error(
                    f"Dis-allowed workflow host: {workflow_uri.netloc} (allow list: {self.workflow_host_allow_list})")
                return states.STATE_EXECUTOR_ERROR

        self._info(f"Fetching workflow file from {workflow_uri}")

        # SECURITY: We cannot pass our auth token outside the Bento instance. Validate that BENTO_URL is
        # a) a valid URL and b) a prefix of our workflow's URI before downloading.
        # Only bother doing this if BENTO_URL is actually set.
        use_auth_headers: bool = False
        if self.bento_url:
            parsed_bento_url = urlparse(self.bento_url)
            use_auth_headers = all((
                self.bento_url,
                parsed_bento_url.scheme == workflow_uri.scheme,
                parsed_bento_url.netloc == workflow_uri.netloc,
                workflow_uri.path.startswith(parsed_bento_url.path),
            ))

        # TODO: Better auth? May only be allowed to access specific workflows
        try:
            wr = requests.get(
                str(workflow_uri),
                headers={
                    "Host": urlparse(self.service_base_url or "").netloc or "",
                    **(auth_headers if use_auth_headers else {}),
                },
                verify=self._validate_ssl,
            )
        except requests.exceptions.ConnectionError as e:
            if os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                return
            else:
                # Network issues
                raise e

        if wr.status_code == 200 and len(wr.content) < MAX_WORKFLOW_FILE_BYTES:
            if os.path.exists(workflow_path):
                os.remove(workflow_path)

            with open(workflow_path, "wb") as nwf:
                nwf.write(wr.content)

            self._info("Workflow file downloaded")

        elif not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
            # Request issues
            self._error(
                f"Error downloading workflow: {workflow_uri} (use_auth_headers={use_auth_headers}, "
                f"wr.status_code={wr.status_code})")
            raise WorkflowDownloadError(f"WorkflowDownloadError: {workflow_path} does not exist")
