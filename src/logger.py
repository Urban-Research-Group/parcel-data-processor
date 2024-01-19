from time import time
import logging
from functools import wraps

_execution_name = None


def configure_logger(execution_name: str = None):
    if execution_name:
        global _execution_name
        _execution_name = execution_name

    logging.basicConfig(
        filename=f"{_execution_name}.log",
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
