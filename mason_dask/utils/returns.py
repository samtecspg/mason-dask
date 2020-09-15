from functools import wraps

from mason_dask.utils.exception import message

from mason_dask.jobs.executed import ExecutedJob, InvalidJob
from returns.result import Result, Success, Failure


def safe_job(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Result[ExecutedJob, InvalidJob]:
        try:
            result = func(*args, **kwargs)
            return Success(ExecutedJob(result))
        except Exception as e:
            return Failure(InvalidJob(message(e)))
    return wrapper

def safe_obj(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Result[ExecutedJob, InvalidJob]:
        try:
            return Success(func(*args, **kwargs))
        except Exception as e:
            return Failure(InvalidJob(message(e)))
    return wrapper
