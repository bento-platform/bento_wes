import pytest
from pytest import MonkeyPatch
from fastapi.testclient import TestClient

from bento_wes.asgi_main import app as fastapi_app

@pytest.fixture(scope="session")
def app():
    mp = MonkeyPatch()
    mp.setenv("AUTHZ_ENABLED", "False")

    return fastapi_app

@pytest.fixture
def client(app):
    client = TestClient(app)
    yield client

