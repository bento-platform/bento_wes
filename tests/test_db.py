from bento_wes.db import get_db, Database


def test_db(settings, logger):
    db = next(get_db(settings, logger))
    assert isinstance(db, Database)
