import bento_lib.workflows as w
import os
import shutil
import requests

from base64 import urlsafe_b64encode
from typing import Dict, NewType, Optional, Set
from urllib.parse import urlparse

from bento_wes import states

__all__ = [
    "WorkflowType",
    "WES_WORKFLOW_TYPE_WDL",
    "WES_WORKFLOW_TYPE_CWL",
    "count_bento_workflow_file_outputs",
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

WORKFLOW_EXTENSIONS: Dict[WorkflowType, str] = {
    WES_WORKFLOW_TYPE_WDL: "wdl",
    WES_WORKFLOW_TYPE_CWL: "cwl",
}

ALLOWED_WORKFLOW_URL_SCHEMES = ("http", "https", "file")
ALLOWED_WORKFLOW_REQUEST_SCHEMES = ("http", "https")

MAX_WORKFLOW_FILE_BYTES = 50000  # 50 KB


# TODO: Types for params/metadata
def count_bento_workflow_file_outputs(workflow_id, workflow_params: dict, workflow_metadata: dict) -> int:
    """
    Given a workflow run's parameters and workflow metadata, returns the number
    of files being output for the purposes of generating one-time ingest tokens
    at submission time, to allow the workflow backend to POST the files without
    using a likely-expired OIDC token.
    :param workflow_id: TODO
    :param workflow_params: TODO
    :param workflow_metadata: TODO
    :return: TODO
    """

    output_params = w.make_output_params(workflow_id, workflow_params, workflow_metadata["inputs"])

    n_file_outputs = 0  # Counter for file outputs
    for output in workflow_metadata["outputs"]:
        fo = w.formatted_output(output, output_params)
        # TODO: py3.10: match
        if output["type"] == w.WORKFLOW_TYPE_FILE:
            n_file_outputs += 1  # TODO: Null check?
        elif output["type"] == w.WORKFLOW_TYPE_FILE_ARRAY:
            n_file_outputs += len(fo)

    return n_file_outputs


def parse_workflow_host_allow_list(allow_list: Optional[str]) -> Optional[Set[str]]:
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
    def __init__(self, tmp_dir: str, chord_url: Optional[str] = None, logger: Optional = None,
                 workflow_host_allow_list: Optional[set] = None, validate_ssl: bool = True, debug: bool = False):
        self.tmp_dir = tmp_dir
        self.chord_url = chord_url
        self.logger = logger
        self.workflow_host_allow_list = workflow_host_allow_list
        self._validate_ssl = validate_ssl
        self._debug_mode = debug

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

    def workflow_path(self, workflow_uri: str, workflow_type: WorkflowType) -> str:
        """
        Generates a unique filesystem path name for a specified workflow URI.
        """
        if workflow_type not in WES_SUPPORTED_WORKFLOW_TYPES:
            raise UnsupportedWorkflowType(f"Unsupported workflow type: {workflow_type}")

        workflow_name = str(urlsafe_b64encode(bytes(workflow_uri, encoding="utf-8")), encoding="utf-8")
        return os.path.join(self.tmp_dir, f"workflow_{workflow_name}.{WORKFLOW_EXTENSIONS[workflow_type]}")

    def download_or_copy_workflow(self, workflow_uri: str, workflow_type: WorkflowType, auth_headers: dict) \
            -> Optional[str]:
        """
        Given a URI, downloads the specified workflow via its URI, or copies it over if it's on the local
        file system. # TODO: Local file system = security issue?
        :param workflow_uri: The workflow URI to download/copy
        :param workflow_type: The type of the workflow being downloaded
        :param auth_headers: Authorization headers to pass while requesting the workflow file.
        """

        parsed_workflow_uri = urlparse(workflow_uri)  # TODO: Handle errors, handle references to attachments

        workflow_path = self.workflow_path(workflow_uri, workflow_type)

        # TODO: Better auth? May only be allowed to access specific workflows
        if parsed_workflow_uri.scheme in ALLOWED_WORKFLOW_REQUEST_SCHEMES:
            try:
                if self.workflow_host_allow_list is not None:
                    # We need to check that the workflow in question is from an
                    # allowed set of workflow hosts
                    # TODO: Handle parsing errors
                    parsed_workflow_uri = urlparse(workflow_uri)
                    if (parsed_workflow_uri.scheme != "file" and
                            parsed_workflow_uri.netloc not in self.workflow_host_allow_list):
                        # Dis-allowed workflow URL
                        self._error(f"Dis-allowed workflow host: {parsed_workflow_uri.netloc} "
                                    f"(allow list: {self.workflow_host_allow_list})")
                        return states.STATE_EXECUTOR_ERROR

                self._info(f"Fetching workflow file from {workflow_uri}")

                # SECURITY: We cannot pass our auth token outside the Bento instance.
                # Validate that CHORD_URL is a) a valid URL and b) a prefix of our
                # workflow's URI before downloading. Only bother doing this if CHORD_URL
                # is actually set.
                use_auth_headers: bool = False
                if self.chord_url:
                    parsed_chord_url = urlparse(self.chord_url)
                    use_auth_headers = all((
                        self.chord_url,
                        parsed_chord_url.scheme == parsed_workflow_uri.scheme,
                        parsed_chord_url.netloc == parsed_workflow_uri.netloc,
                        parsed_workflow_uri.path.startswith(parsed_chord_url.path),
                    ))

                wr = requests.get(
                    workflow_uri,
                    headers={
                        "Host": urlparse(self.chord_url or "").netloc or "",
                        **(auth_headers if use_auth_headers else {}),
                    },
                    verify=self._validate_ssl,
                )

                if wr.status_code == 200 and len(wr.content) < MAX_WORKFLOW_FILE_BYTES:
                    if os.path.exists(workflow_path):
                        os.remove(workflow_path)

                    with open(workflow_path, "wb") as nwf:
                        nwf.write(wr.content)

                    self._info("Workflow file downloaded")

                elif not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                    # Request issues
                    self._error(f"Error downloading workflow: {workflow_uri} "
                                f"(use_auth_headers={use_auth_headers}, "
                                f"wr.status_code={wr.status_code})")
                    raise WorkflowDownloadError(f"WorkflowDownloadError: {workflow_path} does not exist")

            except requests.exceptions.ConnectionError as e:
                if not os.path.exists(workflow_path):  # Use cached version if needed, otherwise error
                    # Network issues
                    raise e

        else:  # TODO: Other else cases
            # file://
            # TODO: Handle exceptions
            shutil.copyfile(parsed_workflow_uri.path, workflow_path)
