import sqlite3
from flask import g

import bento_wes.db as db


# noinspection PyUnusedLocal,PyProtectedMember
def test_db(client):
    # Force db init by yielding client

    assert isinstance(db.get_db(), sqlite3.Connection)

    # Test helper util
    assert db._strip_first_slash("/") == ""
    assert db._strip_first_slash("/test/app") == "test/app"
    assert db._strip_first_slash("test/app") == "test/app"
    assert db._strip_first_slash("/test/app/") == "test/app/"
    assert db._strip_first_slash("test/app/") == "test/app/"

    db.close_db(None)
    assert g.get(db, None) is None
