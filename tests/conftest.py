import os
import pytest


os.environ["CHORD_SERVICES"] = os.path.join(os.path.dirname(__file__), "chord_services.json")


@pytest.fixture
def app():
    from chord_wes.app import application
    from chord_wes.db import init_db

    application.config["TESTING"] = True
    application.config["DATABASE"] = ":memory:"

    with application.app_context():
        init_db()
        yield application

    # TODO: Set up SERVICE_TEMP
    # TODO: Specify backend


@pytest.fixture
def client(app):
    client = app.test_client()
    yield client
