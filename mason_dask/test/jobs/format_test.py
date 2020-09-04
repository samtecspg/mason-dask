from os import path, listdir
from shutil import rmtree

import pytest
from distributed import Client, LocalCluster

from mason_dask.definitions import from_root
from mason_dask.jobs.executed import InvalidJob
from mason_dask.jobs.format import FormatJob, ValidFormatJob
from dask import dataframe as dd

from mason_dask.utils.cluster_spec import ClusterSpec

TMP = "../tmp/"
TEST_DATA = from_root("/test/data/test_data.csv")

@pytest.fixture(scope="module")
def client():
    with Client(LocalCluster(n_workers=2)) as client:
        yield(client)  

@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    clear_tmp()
        
def clear_tmp():
    if path.exists(TMP):
        rmtree(TMP)

def test_local_client(client):
    spec = {
        "input_paths": ["s3://mason-sample-data/tests/in/csv/sample2.csv"],
        "input_format": "csv",
        "output_format": "csv",
        "output_path": "s3://mason-sample-data/tests/out/csv/",
        "filter_columns": [],
        "partition_columns": [],
        "line_terminator": "\r\n",
        "partitions": "3"
    }

    job = FormatJob(spec).validate()
    assert(isinstance(job, ValidFormatJob))

    cluster_spec = ClusterSpec(client)
    assert(cluster_spec.worker_specs is not None)
    assert (len(cluster_spec.worker_specs) == 2)

def test_schema(client):
    in_paths = ["good_input_path", "good_input_path2"]
    in_format = "csv"
    out_format = "parquet"
    out_path = "good_output_path"
    filter_columns = ["col1", "col2"]
    partition_columns = ["col3", "col4"]

    good_spec = {
        "input_paths": in_paths,
        "input_format": in_format,
        "output_format": out_format,
        "output_path": out_path,
        "filter_columns": filter_columns,
        "partition_columns": partition_columns 
    }
    
    job = FormatJob(good_spec).validate()
    assert(isinstance(job, ValidFormatJob))
    assert(job.input_paths == in_paths)
    assert(job.input_format == in_format)
    assert(job.output_format == out_format)
    assert(job.line_terminator == "\n")
    assert(job.partitions == None)
    assert(job.output_path == out_path + "/")
    assert(job.filter_columns == filter_columns)
    assert(job.partition_columns == partition_columns)

    bad_spec = {
        "input_paths": "1234",
        "input_format": "avro",
        "output_format": "",
        "line_terminator": "",
        "output_path": "1234",
        "filter_columns": "bad stuff",
        "partition_columns": "bad stuff"
    }

    for key, value in bad_spec.items():
        good_spec[key] = value
        job = FormatJob(good_spec).validate()
        assert(isinstance(job, InvalidJob))

def test_csv(client):
    # baseline test
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_format": "csv",
        "output_path": TMP + "csv/",
        "filter_columns": [],
        "partition_columns": [],
    }


    cluster_spec = ClusterSpec(client)
    assert(cluster_spec.worker_specs is not None)
    assert(len(cluster_spec.worker_specs) == 2)

    job = FormatJob(spec).validate()
    assert(isinstance(job, ValidFormatJob))
    job.run(cluster_spec)

    parts = ["0.part", '1.part']
    assert(listdir(TMP + "csv/") == parts)
    for p in parts:
        df = dd.read_csv(TMP + f"/csv/{p}").compute()
        assert(df.shape[0] == 2)
        assert(sorted(list(df["col_a"])) == [123.0, 789.0])

    clear_tmp()

    # one partition column
    spec["partition_columns"] = ["col_a"]
    job = FormatJob(spec).validate()
    assert(isinstance(job, ValidFormatJob))
    job.run(cluster_spec)

    prts = [789.0, 123.0]
    assert(listdir(TMP + "csv/") == list(map(lambda p: f"col_a={p}", prts)))

    for prt in prts:
        df = dd.read_csv(TMP + f"/csv/col_a={prt}/0.part").compute()
        assert(df.shape[0] == 2)
        assert(sorted(list(df["col_a"])) == [prt, prt])

    # two partition columns
    clear_tmp()
    spec["partition_columns"] = ["col_a", "col_b"]
    job = FormatJob(spec).validate()
    assert(isinstance(job, ValidFormatJob))
    job.run(cluster_spec)

    part_dict = {(789.0, "test3"): [789.0], (789.0, "test4"): [789.0], (123.0, "test2"): [123.0,123.0]}
    assert(sorted(listdir(TMP + "csv/")) == sorted(list(map(lambda p: f"col_a={p[0]}&col_b={p[1]}", list(part_dict.keys())))))

    for item in part_dict.items():
        df = dd.read_csv(TMP + f"/csv/col_a={item[0][0]}&col_b={item[0][1]}/0.part").compute()
        assert(df.shape[0] == len(item[1]))
        assert(sorted(list(df["col_a"])) == item[1]) 

    # filter columns
    clear_tmp()
    spec["partition_columns"] = []
    spec["filter_columns"] = ["col_a", "col_b"]

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    job.run(cluster_spec)

    parts = ["0.part", '1.part']
    assert(listdir(TMP + "/csv/") == parts)
    df = dd.read_csv(TMP + f"/csv/*").compute()
    assert(df.shape[0] == 4)
    assert(list(df.columns) == ["col_a", "col_b"])

    # partitions options
    spec["filter_columns"] = []
    spec["partitions"] = "4"

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    job.run(cluster_spec)
    assert(listdir(TMP + "csv/") == ["0.part", "1.part", "2.part", "3.part"])

def test_parquet(client):
    # baseline
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_format": "parquet",
        "output_path": TMP + "parquet/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
        
    cluster_spec = ClusterSpec(client)

    job.run(cluster_spec)

    parts = ["part.0.parquet", 'part.1.parquet']
    assert(list(filter(lambda f: f.endswith(".parquet"),listdir(TMP + "parquet/"))) == parts)
    for p in parts:
        df = dd.read_parquet(TMP + f"/parquet/{p}").compute()
        assert(df.shape[0] == 2)
        assert(sorted(list(df["col_a"])) == [123.0, 789.0])

    # partition_columns
    spec["partition_columns"] = ["col_a"]

    job = FormatJob(spec).validate()
    assert(isinstance(job, ValidFormatJob))
    job.run(cluster_spec)

    prts = [789.0, 123.0]
    assert(list(filter(lambda f: f.endswith(".parquet"), listdir(TMP + "parquet/"))) == ["part.0.parquet", "part.1.parquet"])

    for prt in prts:
        df = dd.read_parquet(TMP + f"parquet/col_a={prt}/part.0.parquet").compute()
        assert(df.shape[0] == 2)
        assert(sorted(list(df["col_a"])) == [prt, prt])


def test_json(client):
    # baseline
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_format": "json",
        "output_path": TMP + "json/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))

    cluster_spec = ClusterSpec(client)

    job.run(cluster_spec)
    
    parts = ["0.part", '1.part']
    assert(listdir(TMP + "/json/") == parts)
    for p in parts:
        df = dd.read_json(TMP + f"/json/{p}").compute()
        assert(df.shape[0] == 2)
        assert(sorted(list(df["col_a"])) == [123, 789])
        
def test_xlsx(client):
    # baseline
    spec = {
        "input_paths": [TEST_DATA],
        "input_format": "csv",
        "output_format": "xlsx",
        "output_path": TMP + "xlsx/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    with Client(LocalCluster(n_workers=2)) as client:
        cluster_spec = ClusterSpec(client)
        job.run(cluster_spec)

        parts = ["part_0.xlsx"]
        assert(sorted(listdir(TMP + "xlsx/")) == parts)


