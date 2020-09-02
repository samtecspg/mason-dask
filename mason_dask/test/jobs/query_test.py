from os import path
from shutil import rmtree

import pytest
from returns.result import Result, Success, Failure
from returns.pointfree import bind
from returns.pipeline import flow
from functools import partial

from mason_dask.jobs.executed import InvalidJob
from mason_dask.jobs.query import QueryJob, ValidQueryJob

TMP = "../tmp/"

@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    # clear_tmp()


def clear_tmp():
    if path.exists(TMP):
        rmtree(TMP)

def test_nested_pipe():
    
    class TestClass():
        
        def __init__(self, i: int):
            self.i = i

        def regular_function(self, arg: int) -> float:
            return float(arg)

        def returns_container(self, test: float, arg: float) -> Result[str, ValueError]:
            if arg == test:
                return Success(str(arg))
            else:
                return Failure(ValueError('Wrong arg'))

        def also_returns_container(self, arg: str) -> Result[str, ValueError]:
            if arg != "0":
                return Success(str(arg))
            else:
                return Failure(ValueError('Wrong arg 2'))

        def also_returns_container_2(self, arg: str) -> Result[str, ValueError]:
            return Success(arg + '!')

        def run(self):
            a = flow(
                 self.i,
                 self.regular_function,
                 partial(self.returns_container, 0.0),
                 bind(self.also_returns_container),
                 bind(self.also_returns_container_2)
            ) 
            return a
        
    a = TestClass(0).run()
        

def test_schema():
    in_paths = ["good_input_path", "good_input_path2"]
    in_format = "csv"
    out_path = "good_output_path"
    query_string = "SELECT * FROM table LIMIT 3"

    good_spec = {
        "input_paths": in_paths,
        "input_format": in_format,
        "query_string": query_string,
        "output_path": out_path
    }

    job = QueryJob(good_spec).validate()
    assert (isinstance(job, ValidQueryJob))
    
    bad_spec = {
        "input_paths": "1234",
        "input_format": "avro",
        "query_string": "1234",
        "output_path": "1234",
    }

    for key, value in bad_spec.items():
        good_spec[key] = value
        job = QueryJob(good_spec).validate()
        assert (isinstance(job, InvalidJob))

def test_basic():

    spec = {
        "input_paths": ["../data/test_data.csv"],
        "input_format": "csv",
        "output_path": TMP + "csv/",
        "query_string": "SELECT * from table LIMIT 3"
    }

    job = QueryJob(spec).validate()
    assert (isinstance(job, ValidQueryJob))
    a = job.run()
    print("HERE")

