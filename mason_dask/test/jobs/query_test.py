from os import path
from shutil import rmtree

import pytest
from distributed import Client, LocalCluster
from mason_dask.utils.cluster_spec import ClusterSpec

from mason_dask.definitions import from_root
from mason_dask.jobs.executed import InvalidJob
from mason_dask.jobs.query import QueryJob, ValidQueryJob
from dask import dataframe as dd

TMP = "../tmp/"
TEST_DATA = from_root("/test/data/test_data.csv")

@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    clear_tmp()

@pytest.fixture(scope="module")
def client():
    with Client(LocalCluster(n_workers=2)) as client:
        yield(client)

def clear_tmp():
    if path.exists(TMP):
        rmtree(TMP)

def test_schema(client):
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

def test_basic(client):

    cluster_spec = ClusterSpec(client)
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_path": TMP + "query_out/",
        "query_string": "SELECT * from $table LIMIT 3"
    }

    job = QueryJob(spec).validate()
    assert (isinstance(job, ValidQueryJob))
    assert(job.run(cluster_spec).bind(lambda e: e.message) == "Table succesfully formatted as parquet and exported to ../tmp/query_out/") # type: ignore
    df = dd.read_parquet(TMP + f"/query_out/part.0.parquet").compute()

    assert(df.shape[0] == 3)
    assert(sorted(list(df["col_a"])) == [123.0, 123.0, 789.0])
    assert(sorted(list(df["col_b"])) == ["test2", "test2", "test3"])
    assert(sorted(list(df["col_c"])) == [456.0, 456.0, 456.0])

    # where
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_path": TMP + "query_out/",
        "query_string": "SELECT * from $table WHERE col_a <= 123.0"
    }
    job = QueryJob(spec).validate()
    assert (isinstance(job, ValidQueryJob))
    job.run(cluster_spec)

    assert (isinstance(job, ValidQueryJob))
    df = dd.read_parquet(TMP + f"/query_out/part.0.parquet").compute()

    assert (df.shape[0] == 2)
    assert (sorted(list(df["col_a"])) == [123.0, 123.0])
    assert (sorted(list(df["col_b"])) == ["test2", "test2"])
    assert (sorted(list(df["col_c"])) == [456.0, 456.0])




