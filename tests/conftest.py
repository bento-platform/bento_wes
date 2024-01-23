import os
import pytest
import responses


@pytest.fixture
def app():
    os.environ["AUTHZ_ENABLED"] = "false"
    os.environ["BENTO_AUTHZ_SERVICE_URL"] = "http://bento-authz.local"
    os.environ["SERVICE_REGISTRY_URL"] = "http://bento-sr.local"

    from bento_wes.app import application
    from bento_wes.db import init_db

    application.config["TESTING"] = True
    application.config["DATABASE"] = ":memory:"
    application.config["OTT_ENDPOINT_NAMESPACE"] = "http://auth.local/ott"

    with application.app_context():
        init_db()
        yield application

    # TODO: Set up SERVICE_TEMP
    # TODO: Specify backend


@pytest.fixture
def client(app):
    yield app.test_client()


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as r:
        yield r
