from bento_wes.routers.runs.utils import denest_list


def test_runs_utils_denest_list():
    assert denest_list(5) == [5]
    assert denest_list([1, 2, [3, 4]]) == [1, 2, 3, 4]
    assert denest_list([[1, 2], [3, 4]]) == [1, 2, 3, 4]
    assert denest_list([[1, 2], [[3], 4]]) == [1, 2, 3, 4]
    assert denest_list([[[1, 2]], [[3], [4]]]) == [1, 2, 3, 4]
