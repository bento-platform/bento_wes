from bento_wes.db import Database, get_db


def test_db(settings, logger, event_bus):
    db = next(get_db(settings, logger, event_bus))
    assert isinstance(db, Database)
