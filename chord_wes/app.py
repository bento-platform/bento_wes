import os
import sqlite3

from celery import Celery
from flask import Flask, g, json, request


def make_celery(app):
    c = Celery(app.import_name, backend=app.config["CELERY_RESULT_BACKEND"], broker=app.config["CELERY_BROKER_URL"])
    c.conf.update(app.config)

    class ContextTask(c.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                self.run(*args, **kwargs)

    # noinspection PyPropertyAccess
    c.Task = ContextTask
    return celery


application = Flask(__name__)
application.config.from_mapping(
    CHORD_SERVICES=os.environ.get("CHORD_SERVICES", "chord_services.json"),
    CELERY_RESULT_BACKEND="redis://",  # TODO
    CELERY_BROKER_URL="redis://",  # TODO
    DATABASE=os.environ.get("DATABASE", "chord_wes.db")
)
celery = make_celery(application)


with open(application.config["CHORD_SERVICES"]) as cf:
    SERVICES = json.load(cf)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(application.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(_e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    c = db.cursor()

    with application.open_resource("schema.sql") as sf:
        c.executescript(sf.read().decode("utf-8"))

    db.commit()


def update_db():
    db = get_db()
    c = db.cursor()

    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='runs'")
    if c.fetchone() is None:
        init_db()
        return

    # TODO


application.teardown_appcontext(close_db)

with application.app_context():
    if not os.path.exists(os.path.join(os.getcwd(), application.config["DATABASE"])):
        init_db()
    else:
        update_db()


@celery.task()
def run_workflow():
    pass


@application.route("/runs", methods=["GET", "POST"])
def run_list():
    if request.method == "POST":
        pass

    pass


@application.route("/runs/<uuid:run_id>", methods=["GET"])
def run_detail(run_id):
    pass


@application.route("/runs/<uuid:run_id>/cancel", methods=["POST"])
def run_cancel(run_id):
    pass


@application.route("/runs/<uuid:run_id>/status", methods=["GET"])
def run_status(run_id):
    pass


# TODO: Not compatible with GA4GH WES due to conflict with GA4GH service-info (preferred)
@application.route("/service_info", methods=["GET"])
def service_info():
    pass
