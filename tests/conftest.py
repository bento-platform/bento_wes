import pytest
from pytest import MonkeyPatch
from fastapi.testclient import TestClient
import respx
import httpx
import os
from pathlib import Path
import importlib

#--------------------------------------------------------------------------
#                              CONFTEST SETUP
#--------------------------------------------------------------------------

test_dir = Path(__file__).resolve().parent
database_path = test_dir / "test.db"

## has issues if placed inside a fixture
mp = MonkeyPatch()
mp.setenv("BENTO_AUTHZ_ENABLED", "False")
mp.setenv("AUTHZ_ENABLED", "False")
mp.setenv("BENTO_AUTHZ_SERVICE_URL", "http://bento-authz.local")
mp.setenv("SERVICE_REGISTRY_URL", "http://bento-sr.local")
mp.setenv("DATABASE", str(database_path))
mp.setenv("TESTING", "True")
mp.setenv("WORKFLOW_HOST_ALLOW_LIST", "metadata.local") 

import bento_wes.config as cfg
importlib.reload(cfg) 


#--------------------------------------------------------------------------
#                                  FIXTURES
#--------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def cleanup_env():
    yield
    if database_path.exists():
        os.unlink(database_path)

@pytest.fixture()
def app():
    from bento_wes.asgi_main import app as fastapi_app
    yield fastapi_app
    
    

@pytest.fixture
def client(app):
    
    with TestClient(app) as c:
        yield c

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