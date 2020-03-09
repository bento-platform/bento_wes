import json


def test_run_list(client):
    rv = client.get("/runs")
    assert rv.status_code == 200
    data = rv.get_json()
    assert json.dumps(data) == json.dumps([])

    # TODO: Test with run
    # TODO: Test with_details
