from bento_wes.db import get_db, Database

def test_db():
    db = next(get_db())
    assert isinstance(db, Database)