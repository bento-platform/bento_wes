import chord_wes
import os

from chord_lib.responses import flask_errors
from flask import Flask, json, jsonify
from werkzeug.exceptions import BadRequest, NotFound

from .celery import celery
from .constants import SERVICE_ID, SERVICE_NAME, SERVICE_TYPE
from .db import init_db, update_db, close_db
from .events import close_flask_event_bus
from .runs import bp_runs


application = Flask(__name__)
application.config.from_mapping(
    CHORD_SERVICES=os.environ.get("CHORD_SERVICES", "chord_services.json"),
    CHORD_URL=os.environ.get("CHORD_URL", "http://127.0.0.1:5000/"),
    CHORD_HOST=os.environ.get("CHORD_HOST", "127.0.0.1"),
    CELERY_RESULT_BACKEND=os.environ.get("CELERY_RESULT_BACKEND", "redis://"),
    CELERY_BROKER_URL=os.environ.get("CELERY_BROKER_URL", "redis://"),
    DATABASE=os.environ.get("DATABASE", "chord_wes.db"),
    SERVICE_ID=SERVICE_ID,
    SERVICE_TEMP=os.environ.get("SERVICE_TEMP", "tmp"),
    SERVICE_URL_BASE_PATH=os.environ.get("SERVICE_URL_BASE_PATH", "/"),
    WOM_TOOL_LOCATION=os.environ.get("WOM_TOOL_LOCATION", "womtool.jar"),
    WRITE_OUTPUT_TO_DRS=os.environ.get("WRITE_OUTPUT_TO_DRS", "false").lower().strip() == "true",
    DRS_SKIP_TYPES=tuple(t.strip() for t in os.environ.get("DRS_SKIP_TYPES", "").split(",") if t.strip())
)

application.register_blueprint(bp_runs)

# Generic catch-all
application.register_error_handler(
    Exception,
    flask_errors.flask_error_wrap_with_traceback(flask_errors.flask_internal_server_error, service_name=SERVICE_NAME)
)
application.register_error_handler(BadRequest, flask_errors.flask_error_wrap(flask_errors.flask_bad_request_error))
application.register_error_handler(NotFound, flask_errors.flask_error_wrap(flask_errors.flask_not_found_error))


def configure_celery(app):
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                self.run(*args, **kwargs)

    # noinspection PyPropertyAccess
    celery.Task = ContextTask


configure_celery(application)


with open(application.config["CHORD_SERVICES"]) as cf:
    SERVICES = json.load(cf)


application.teardown_appcontext(close_db)
application.teardown_appcontext(close_flask_event_bus)

with application.app_context():
    if not os.path.exists(os.path.join(os.getcwd(), application.config["DATABASE"])):
        init_db()
    else:
        update_db()


# TODO: Not compatible with GA4GH WES due to conflict with GA4GH service-info (preferred)
@application.route("/service-info", methods=["GET"])
def service_info():
    return jsonify({
        "id": application.config["SERVICE_ID"],
        "name": SERVICE_NAME,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Workflow execution service for a CHORD application.",
        "organization": {
            "name": "C3G",
            "url": "http://www.computationalgenomics.ca"
        },
        "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
        "version": chord_wes.__version__
    })
