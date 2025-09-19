import pytest
from fastapi.testclient import TestClient
import respx
import httpx
import os
from pathlib import Path


@pytest.fixture
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

    monkeypatch.setenv("WORKFLOW_TIMEOUT", "48:00:00")
    monkeypatch.setenv("INGEST_POST_TIMEOUT", "01:00:00")

    get_settings.cache_clear()
    s = get_settings()
    yield s
    get_settings.cache_clear()
    if database_path.exists():
        os.unlink(database_path)


@pytest.fixture
def client(settings_env):
    from bento_wes.config import get_settings
    from bento_wes.app_factory import create_app
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings_env
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def _mocked_responses_with_workflow():
    """Fixture that mocks httpx requests and stubs the workflow metadata file."""
    with respx.mock as respx_mock:
        workflow_path = Path(__file__).with_name("phenopackets_json.wdl")
        workflow_text = workflow_path.read_text()

        respx_mock.get("http://metadata.local/workflows/phenopackets_json.wdl").mock(
            return_value=httpx.Response(
                200,
                text=workflow_text,
                headers={"Content-Type": "text/plain"},
            )
        )

        yield respx_mock


@pytest.fixture
def db_session():
    from bento_wes.db import get_db

    gen = get_db()
    db = next(gen)
    try:
        yield db
    finally:
        gen.close()
