from os import path
from shutil import rmtree

import pytest

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

