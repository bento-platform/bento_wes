import chord_lib
from jsonschema import validate


def test_service_info(client):
    rv = client.get("/service-info")
    data = rv.get_json()

    validate(data, chord_lib.schemas.ga4gh.SERVICE_INFO_SCHEMA)
