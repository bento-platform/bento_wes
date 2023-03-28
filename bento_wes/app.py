import bento_wes
import os
import subprocess

from bento_lib.responses import flask_errors
from bento_lib.types import GA4GHServiceInfo
from flask import current_app, Flask, jsonify
from werkzeug.exceptions import BadRequest, NotFound

from .celery import celery
from .config import Config
from .constants import BENTO_SERVICE_KIND, SERVICE_NAME, SERVICE_TYPE
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

    if application.config["IS_RUNNING_DEV"]:
        app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        subprocess.run(["git", "config", "--global", "--add", "safe.directory", str(app_dir)])


# TODO: Not compatible with GA4GH WES due to conflict with GA4GH service-info (preferred)
@application.route("/service-info", methods=["GET"])
def service_info():
    info: GA4GHServiceInfo = {
        "id": application.config["SERVICE_ID"],
        "name": SERVICE_NAME,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Workflow execution service for a CHORD application.",
        "organization": {
            "name": "C3G",
            "url": "https://www.computationalgenomics.ca"
        },
        "contactUrl": "mailto:info@c3g.ca",
        "version": bento_wes.__version__,
        "environment": "prod",
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
            "gitRepository": "https://github.com/bento-platform/bento_wes",
        },
    }

    print(application.config["IS_RUNNING_DEV"])

    if not application.config["IS_RUNNING_DEV"]:
        return jsonify(info)

    info["environment"] = "dev"

    try:
        if res_tag := subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"]):
            res_tag_str = res_tag.decode().rstrip()
            info["git_tag"] = res_tag_str
            # noinspection PyTypeChecker
            info["bento"]["gitTag"] = res_tag_str
        if res_branch := subprocess.check_output(["git", "branch", "--show-current"]):
            res_branch_str = res_branch.decode().rstrip()
            info["git_branch"] = res_branch_str
            # noinspection PyTypeChecker
            info["bento"]["gitBranch"] = res_branch_str
        if res_commit := subprocess.check_output(["git", "rev-parse", "HEAD"]):
            res_commit_str = res_commit.decode().rstrip()
            # noinspection PyTypeChecker
            info["bento"]["gitCommit"] = res_commit_str
    except Exception as e:
        except_name = type(e).__name__
        current_app.logger.info(f"Could not retrieve git information: {str(except_name)}: {e}")

    return jsonify(info)
