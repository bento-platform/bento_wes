import bento_wes
import os
import subprocess

from bento_lib.responses import flask_errors
from flask import Flask, jsonify
from werkzeug.exceptions import BadRequest, NotFound

from .celery import celery
from .config import Config
from .constants import SERVICE_NAME, SERVICE_TYPE
from .db import init_db, update_db, close_db
from .events import close_flask_event_bus
from .runs import bp_runs


application = Flask(__name__)
application.config.from_object(Config)

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


application.teardown_appcontext(close_db)
application.teardown_appcontext(close_flask_event_bus)

with application.app_context():  # pragma: no cover
    if not os.path.exists(os.path.join(os.getcwd(), application.config["DATABASE"])):
        init_db()
    else:
        update_db()


# TODO: Not compatible with GA4GH WES due to conflict with GA4GH service-info (preferred)
@application.route("/service-info", methods=["GET"])
def service_info():
    service_info = {
            "id": application.config["SERVICE_ID"],
            "name": SERVICE_NAME,  # TODO: Should be globally unique?
            "type": SERVICE_TYPE,
            "description": "Workflow execution service for a CHORD application.",
            "organization": {
                "name": "C3G",
                "url": "http://www.computationalgenomics.ca"
            },
            "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
            "version": bento_wes.__version__,
            "environment": "prod"
    }
    if not application.config["BENTO_DEBUG"]:
        return jsonify(service_info)

    else:
        service_info["environment"] = "dev"
        try:
            subprocess.run(["git", "config", "--global", "--add", "safe.directory", "./bento_wes"])
            res_tag = subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"])
            if res_tag:
                service_info["git_tag"] = res_tag.decode().rstrip()
            res_branch= subprocess.check_output(["git", "branch", "--show-current"])
            if res_branch:
                service_info["git_branch"] = res_branch.decode().rstrip()
            return jsonify(service_info)

        except:
            return flask_errors.flask_not_found_error("Error in dev-mode retrieving git information")
    
        
# # debugger section
if application.config["BENTO_DEBUG"]:
    try:
        import debugpy
        DEBUGGER_PORT = int(os.environ.get("DEBUGGER_PORT", 5680))
        debugpy.listen(("0.0.0.0", DEBUGGER_PORT))
        print("Debugger Attached")
    except ImportError:
        print("Module debugpy not found. Install to enable debugging with VS-Code")
# # end debugger section
