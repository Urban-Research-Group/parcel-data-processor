from time import time
import logging
from functools import wraps
import os


def configure_logger():
    return logging.getLogger(__name__)


def setup_logger(execution_name: str = None):
    logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(logs_dir, exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(logs_dir, f"{execution_name}.log"),
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )

    return logging.getLogger(__name__)


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        start = time()
        result = f(*args, **kw)
        end = time()
        logging.debug(
            "func:%r args:[%r, %r] took: %2.4f sec"
            % (f.__name__, args, kw, end - start)
        )
        return result

    return wrap
