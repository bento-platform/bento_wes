import os
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from .constants import EXAMPLE_WORKFLOW_DEFINITION, METADATA_URL, TEST_SERVICES


@pytest.fixture(autouse=True)
def settings_env(monkeypatch):
    from bento_wes.config import get_settings

    test_dir = Path(__file__).resolve().parent
    database_path = test_dir / "test.db"

    monkeypatch.setenv("BENTO_AUTHZ_ENABLED", "False")
    monkeypatch.setenv("AUTHZ_ENABLED", "False")
    monkeypatch.setenv("BENTO_AUTHZ_SERVICE_URL", "http://bento-authz.local")
    monkeypatch.setenv("SERVICE_REGISTRY_URL", "http://bento-sr.local")
    monkeypatch.setenv("DATABASE", str(database_path))
    monkeypatch.setenv("TESTING", "True")
    monkeypatch.setenv("WORKFLOW_HOST_ALLOW_LIST", "metadata.local")

    monkeypatch.delenv("WORKFLOW_TIMEOUT", raising=False)

    monkeypatch.setenv("WORKFLOW_TIMEOUT", "172800")  # 48 hours in seconds
    monkeypatch.setenv("INGEST_POST_TIMEOUT", "3600")  # 1 hour in seconds

    get_settings.cache_clear()
    s = get_settings()
    yield s
    get_settings.cache_clear()
    if database_path.exists():
        os.unlink(database_path)


class FakeServiceManager:
    """Stands in for bento_lib's (aiohttp-based) ServiceManager, so tests don't need a service registry."""

    def __init__(self, services: list[dict]):
        self.services = services

    async def fetch_service_list(self, *_args, **_kwargs) -> list[dict]:
        return self.services

    async def get_bento_service_url_by_kind(self, service_kind: str, *_args, **_kwargs) -> str | None:
        return next((s["url"] for s in self.services if s["bento"]["serviceKind"] == service_kind), None)


@pytest.fixture
def service_manager():
    return FakeServiceManager([dict(s) for s in TEST_SERVICES])


@pytest.fixture
def client(settings_env, service_manager):
    from bento_wes.app_factory import create_app
    from bento_wes.config import get_settings
    from bento_wes.service_registry import get_service_manager

    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings_env
    app.dependency_overrides[get_service_manager] = lambda: service_manager
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def mock_workflow_provider(respx_mock, workflow_id: str, definition: dict, provider_url: str = METADATA_URL):
    """Mock a workflow provider serving one workflow's definition and WDL file."""
    workflow_text = Path(__file__).with_name("phenopackets_json.wdl").read_text()
    respx_mock.get(f"{provider_url}/workflows/{workflow_id}").mock(return_value=httpx.Response(200, json=definition))
    respx_mock.get(f"{provider_url}/workflows/{workflow_id}.wdl").mock(
        return_value=httpx.Response(200, text=workflow_text, headers={"Content-Type": "text/plain"})
    )


@pytest.fixture
def _mocked_responses_with_workflow():
    """Fixture that mocks httpx requests and stubs the workflow provider (definition + WDL file)."""
    with respx.mock as respx_mock:
        mock_workflow_provider(respx_mock, "phenopackets_json", EXAMPLE_WORKFLOW_DEFINITION)
        yield respx_mock


@pytest.fixture
def settings():
    from bento_wes.config import get_settings

    return get_settings()


@pytest.fixture
def logger():
    from bento_wes.logger import get_logger

    return get_logger()


@pytest.fixture
def event_bus(logger, settings):
    from bento_wes.events import get_event_bus

    return get_event_bus(logger, settings)


@pytest.fixture
def db_session(settings, logger, event_bus):
    from bento_wes.db import get_db

    gen = get_db(settings, logger, event_bus)
    db = next(gen)
    try:
        yield db
    finally:
        gen.close()
