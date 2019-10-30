import sqlite3
from flask import current_app, g

from .states import *


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(_e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    c = db.cursor()

    with current_app.open_resource("schema.sql") as sf:
        c.executescript(sf.read().decode("utf-8"))

    db.commit()


def update_db():
    db = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    # Update all runs that have "stuck" states to have an error state instead on restart. This way, systems don't get
    # stuck checking their status, and if they're in a weird state at boot they should receive an error status anyway.
    c.execute("UPDATE runs SET state = ? WHERE state = ? OR state = ?",
              (STATE_SYSTEM_ERROR, STATE_INITIALIZING, STATE_RUNNING))

    # TODO: Migrations if needed
