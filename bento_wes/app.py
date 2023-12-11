import bento_wes
import os

from asgiref.sync import async_to_sync
from bento_lib.responses import flask_errors
from bento_lib.service_info.constants import SERVICE_ORGANIZATION_C3G
from bento_lib.service_info.helpers import build_service_info
from flask import current_app, Flask, jsonify
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, Forbidden, NotFound

from .authz import authz_middleware
from .celery import celery
from .config import Config
from .constants import BENTO_SERVICE_KIND, SERVICE_NAME, SERVICE_TYPE
from .db import init_db, update_db, close_db
from .events import close_flask_event_bus
from .runs import bp_runs


application = Flask(__name__)

# Load configuration from Config class
application.config.from_object(Config)

# Set up CORS
CORS(application, origins=Config.CORS_ORIGINS)

# Attach authz middleware to Flask instance
authz_middleware.attach(application)

# Mount API routes
application.register_blueprint(bp_runs)

# Register error handlers
#  - generic catch-all:
application.register_error_handler(
    Exception,
    flask_errors.flask_error_wrap_with_traceback(
        flask_errors.flask_internal_server_error,
        service_name=SERVICE_NAME,
        authz=authz_middleware,
    ),
)
application.register_error_handler(
    BadRequest, flask_errors.flask_error_wrap(flask_errors.flask_bad_request_error, authz=authz_middleware))
application.register_error_handler(
    Forbidden, flask_errors.flask_error_wrap(flask_errors.flask_forbidden_error, authz=authz_middleware))
application.register_error_handler(
    NotFound, flask_errors.flask_error_wrap(flask_errors.flask_not_found_error, authz=authz_middleware))


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
@authz_middleware.deco_public_endpoint
def service_info():
    return jsonify(
        async_to_sync(build_service_info)(
            {
                "id": current_app.config["SERVICE_ID"],
                "name": SERVICE_NAME,  # TODO: Should be globally unique?
                "type": SERVICE_TYPE,
                "description": "Workflow execution service for a Bento instance.",
                "organization": SERVICE_ORGANIZATION_C3G,
                "contactUrl": "mailto:info@c3g.ca",
                "version": bento_wes.__version__,
                "bento": {
                    "serviceKind": BENTO_SERVICE_KIND,
                    "gitRepository": "https://github.com/bento-platform/bento_wes",
                },
            },
            debug=current_app.config["BENTO_DEBUG"],
            local=current_app.config["BENTO_CONTAINER_LOCAL"],
            logger=current_app.logger,
        )
    )
