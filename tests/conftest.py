import os
import pytest
import responses


@pytest.fixture
def app():
    from bento_wes.app import application
    from bento_wes.db import init_db

    application.config["TESTING"] = True
    application.config["DATABASE"] = ":memory:"
    application.config["OTT_ENDPOINT_NAMESPACE"] = ""  # Don't need one-time tokens for testing

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
    with responses.RequestsMock() as r, \
            open(os.path.join(os.path.dirname(__file__), "./phenopackets_json.wdl"), "r") as wf:
        r.add(
            responses.GET,
            "http://metadata.local/workflows/ingest.wdl",
            body=wf.read(),
            status=200,
            content_type="text/plain",
        )
        yield r
