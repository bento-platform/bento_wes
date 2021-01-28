import pytest


@pytest.fixture
def app():
    from bento_wes.app import application
    from bento_wes.db import init_db

    application.config["TESTING"] = True
    application.config["DATABASE"] = ":memory:"

    with application.app_context():
        init_db()
        yield application

    # TODO: Set up SERVICE_TEMP
    # TODO: Specify backend


@pytest.fixture
def client(app):
    yield app.test_client()
