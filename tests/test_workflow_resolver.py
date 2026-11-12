import asyncio

import pytest
from bento_lib.service_info.manager import ServiceManagerError

from bento_wes import workflow_resolver as wr

from .constants import TEST_SERVICES, WORKFLOW_PROVIDER_URLS

NEW_PROVIDER_URL = "http://new-provider.local"


@pytest.mark.parametrize(
    "bento, expected",
    [
        ({"workflowProvider": True}, True),
        ({"dataService": True}, True),
        ({"workflowProvider": True, "dataService": True}, True),
        ({"workflowProvider": False, "dataService": False}, False),
        ({}, False),
    ],
)
def test_is_workflow_provider(bento, expected):
    assert wr.is_workflow_provider({"url": "http://svc.local", "bento": bento}) is expected
    assert wr.is_workflow_provider({"url": "http://svc.local"}) is False


def test_provider_urls_include_data_services(resolver):
    # katsu and gohan are data services, reference is a workflow provider, drop-box is neither
    assert asyncio.run(resolver._get_provider_urls()) == WORKFLOW_PROVIDER_URLS


class CountingServiceManager:
    """Fake ServiceManager which counts service list fetches and can be set to fail."""

    def __init__(self, services: list[dict]):
        self.services = services
        self.calls: list[dict] = []
        self.fail = False

    async def fetch_service_list(self, *_args, **kwargs) -> list[dict]:
        self.calls.append(kwargs)
        if self.fail:
            raise ServiceManagerError("registry down")
        return self.services


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(wr.time, "monotonic", c)
    return c


@pytest.fixture
def sm():
    return CountingServiceManager([dict(s) for s in TEST_SERVICES])


@pytest.fixture
def resolver(sm, settings, logger):
    return wr.WorkflowResolver(sm, settings, logger)  # type: ignore


def test_provider_urls_cached_within_ttl(resolver, sm, clock):
    assert asyncio.run(resolver._get_provider_urls()) == WORKFLOW_PROVIDER_URLS
    assert sm.calls == [{"skip_cache": True}]

    clock.now += wr.PROVIDER_LIST_TTL - 1
    sm.services = [*sm.services, {"url": NEW_PROVIDER_URL, "bento": {"workflowProvider": True}}]
    assert asyncio.run(resolver._get_provider_urls()) == WORKFLOW_PROVIDER_URLS
    assert len(sm.calls) == 1


def test_provider_urls_refreshed_after_ttl(resolver, sm, clock):
    asyncio.run(resolver._get_provider_urls())

    clock.now += wr.PROVIDER_LIST_TTL
    sm.services = [*sm.services, {"url": NEW_PROVIDER_URL, "bento": {"workflowProvider": True}}]
    assert asyncio.run(resolver._get_provider_urls()) == [*WORKFLOW_PROVIDER_URLS, NEW_PROVIDER_URL]
    assert sm.calls == [{"skip_cache": True}, {"skip_cache": True}]


def test_provider_urls_stale_fallback_on_registry_error(resolver, sm, clock):
    asyncio.run(resolver._get_provider_urls())

    clock.now += wr.PROVIDER_LIST_TTL
    sm.fail = True
    assert asyncio.run(resolver._get_provider_urls()) == WORKFLOW_PROVIDER_URLS

    # A failed refresh doesn't reset the TTL, so the next call tries the registry again
    sm.fail = False
    asyncio.run(resolver._get_provider_urls())
    assert len(sm.calls) == 3


def test_provider_urls_registry_error_without_previous_list(resolver, sm, clock):
    sm.fail = True
    with pytest.raises(wr.WorkflowResolutionError):
        asyncio.run(resolver._get_provider_urls())
