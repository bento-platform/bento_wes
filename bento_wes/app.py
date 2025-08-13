import os

from asgiref.sync import async_to_sync
from bento_lib.responses import flask_errors
from flask import  Flask
from flask_cors import CORS
from werkzeug.exceptions import BadRequest, Forbidden, NotFound

from .authz import authz_middleware_flask
from .celery import celery
from .config import flask_config, config
from .constants import SERVICE_NAME
from .db import init_db, update_db, close_db
from .events import close_flask_event_bus
from .runs import bp_runs

application = Flask(__name__)

# Load configuration from Config class
application.config.from_object(flask_config)

# Set up CORS
CORS(application, origins=config.cors_origins)

# Attach authz middleware to Flask instance
authz_middleware_flask.attach(application)

# Mount API routes
application.register_blueprint(bp_runs)

# Register error handlers
#  - generic catch-all:
application.register_error_handler(
    Exception,
    flask_errors.flask_error_wrap_with_traceback(
        flask_errors.flask_internal_server_error,
        service_name=SERVICE_NAME,
        authz=authz_middleware_flask,
    ),
)
application.register_error_handler(
    BadRequest, flask_errors.flask_error_wrap(flask_errors.flask_bad_request_error, authz=authz_middleware_flask)
)
application.register_error_handler(
    Forbidden, flask_errors.flask_error_wrap(flask_errors.flask_forbidden_error, authz=authz_middleware_flask)
)
application.register_error_handler(
    NotFound, flask_errors.flask_error_wrap(flask_errors.flask_not_found_error, authz=authz_middleware_flask)
)


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
