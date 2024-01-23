from bento_wes.workflows import parse_workflow_host_allow_list


def test_parse_allow_list():
    assert parse_workflow_host_allow_list("") is None
    assert parse_workflow_host_allow_list("a") == {"a"}
    assert parse_workflow_host_allow_list("a,") == {"a"}
    assert parse_workflow_host_allow_list("a,a") == {"a"}
    assert parse_workflow_host_allow_list("a, a") == {"a"}
    assert parse_workflow_host_allow_list("a,b") == {"a", "b"}
    assert parse_workflow_host_allow_list("a, b") == {"a", "b"}
