from jsonschema import validate
from bento_lib.schemas.ga4gh import SERVICE_INFO_SCHEMA

def test_health_ok(client):
    r = client.get("/service-info")
    assert r.status_code == 200
    
    validate(r.json(), SERVICE_INFO_SCHEMA)