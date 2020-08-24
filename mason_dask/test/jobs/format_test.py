from os import path, listdir
from shutil import rmtree

import pytest
from distributed import Client, LocalCluster

from mason_dask.jobs.executed import InvalidJob
from mason_dask.jobs.format import FormatJob, ValidFormatJob
from dask import dataframe as dd

from mason_dask.utils.cluster_spec import ClusterSpec

TMP = "../tmp/"


@pytest.fixture(autouse=True)
def run_around_tests():
    yield
    clear_tmp()
        
def clear_tmp():
    if path.exists(TMP):
        rmtree(TMP)


def test_local_client():
    with Client(LocalCluster(n_workers=2)) as client:
        cluster_spec = ClusterSpec(client)
        assert (len(cluster_spec.worker_specs) == 2)


def test_schema():
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
    assert(job.partitions == "auto")
    assert(job.output_path == out_path)
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

def test_csv():
    # baseline test
    spec = {
        "input_paths": ["../data/test_data.csv"],
        "input_format": "csv",
        "output_format": "csv",
        "output_path": TMP + "csv/",
        "filter_columns": [],
        "partition_columns": [],
    }

    with Client(LocalCluster(n_workers=2)) as client:

        cluster_spec = ClusterSpec(client)
        assert(len(cluster_spec.worker_specs) == 2)

        job = FormatJob(spec).validate()
        assert(isinstance(job, ValidFormatJob))
        job.run(client)

        parts = ["0.part", '1.part']
        assert(listdir(TMP + "/csv/"), parts)
        for p in parts:
            df = dd.read_csv(TMP + f"/csv/{p}").compute()
            assert(df.shape[0] == 2)
            assert(sorted(list(df["col_a"])) == [123, 789])

        clear_tmp()

        # one partition column
        spec = {
            "input_paths": ["../data/test_data.csv"],
            "input_format": "csv",
            "output_format": "csv",
            "output_path": TMP + "csv/",
            "filter_columns": [],
            "partition_columns": ["col_a"],
        }


        job = FormatJob(spec).validate()
        assert(isinstance(job, ValidFormatJob))
        job.run(client)

        parts = [789, 123]
        assert(listdir(TMP + "csv/"), list(map(lambda p: f"col_a={p}", parts)))

        for p in parts:
            df = dd.read_csv(TMP + f"/csv/col_a={p}/0.part").compute()
            assert(df.shape[0] == 2)
            assert(sorted(list(df["col_a"])) == [p, p])

        # two partition columns
        clear_tmp()
        spec = {
            "input_paths": ["../data/test_data.csv"],
            "input_format": "csv",
            "output_format": "csv",
            "output_path": TMP + "csv/",
            "filter_columns": [],
            "partition_columns": ["col_a","col_b"],
        }
        job = FormatJob(spec).validate()
        assert(isinstance(job, ValidFormatJob))
        job.run(client)

        parts = {(789, "test3"): [789], (789, "test4"): [789], (123, "test2"): [123,123]}
        assert(listdir(TMP + "csv/"), list(map(lambda p: f"col_a={p[0]}&col_b={p[1]}", list(parts.keys()))))

        for (p, e) in parts.items():
            df = dd.read_csv(TMP + f"/csv/col_a={p[0]}&col_b={p[1]}/0.part").compute()
            assert(df.shape[0] == len(e))
            assert(sorted(list(df["col_a"])) == e)

        # filter columns
        clear_tmp()
        spec = {
            "input_paths": ["../data/test_data.csv"],
            "input_format": "csv",
            "output_format": "csv",
            "output_path": TMP + "csv/",
            "filter_columns": ["col_a", "col_b"],
            "partition_columns": [],
        }

        job = FormatJob(spec).validate()
        assert (isinstance(job, ValidFormatJob))
        job.run(client)

        parts = ["0.part", '1.part']
        assert(listdir(TMP + "/csv/"), parts)
        df = dd.read_csv(TMP + f"/csv/*").compute()
        assert(df.shape[0] == 4)
        assert(list(df.columns) == ["col_a", "col_b"])

def test_parquet():
    # baseline
    spec = {
        "input_paths": ["../data/test_data.csv"],
        "input_format": "csv",
        "output_format": "parquet",
        "output_path": TMP + "parquet/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    with Client(LocalCluster(n_workers=2)) as client:

        job.run(client)

        parts = ["part.0.parquet", 'part.1.parquet']
        assert(listdir(TMP + "/parquet/"), parts)
        for p in parts:
            df = dd.read_parquet(TMP + f"/parquet/{p}").compute()
            assert(df.shape[0] == 2)
            assert(sorted(list(df["col_a"])) == [123, 789])

        # partition_columns
        spec = {
            "input_paths": ["../data/test_data.csv"],
            "input_format": "csv",
            "output_format": "parquet",
            "output_path": TMP + "parquet/",
            "filter_columns": [],
            "partition_columns": ["col_a"],
        }

        job = FormatJob(spec).validate()
        assert(isinstance(job, ValidFormatJob))
        job.run(client)

        parts = [789, 123]
        assert(listdir(TMP + "parquet/"), list(map(lambda p: f"col_a={p}", parts)))

        for p in parts:
            df = dd.read_parquet(TMP + f"parquet/col_a={p}/part.0.parquet").compute()
            assert(df.shape[0] == 2)
            assert(sorted(list(df["col_a"])) == [p, p])


def test_json():
    # baseline
    spec = {
        "input_paths": ["../data/test_data.csv"],
        "input_format": "csv",
        "output_format": "json",
        "output_path": TMP + "json/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    with Client(LocalCluster(n_workers=2)) as client:

        job.run(client)
        
        parts = ["0.part", '1.part']
        assert(listdir(TMP + "/json/"), parts)
        for p in parts:
            df = dd.read_json(TMP + f"/json/{p}").compute()
            assert(df.shape[0] == 2)
            assert(sorted(list(df["col_a"])) == [123, 789])
            
def test_xlsx():
    # baseline
    spec = {
        "input_paths": ["../data/test_data.csv"],
        "input_format": "csv",
        "output_format": "xlsx",
        "output_path": TMP + "xlsx/",
        "filter_columns": [],
        "partition_columns": [],
    }

    job = FormatJob(spec).validate()
    assert (isinstance(job, ValidFormatJob))
    with Client(LocalCluster(n_workers=2)) as client:
        job.run(client)

        parts = ["part_0.xlsx", 'part1.xlsx']
        assert(listdir(TMP + "xlsx/"), parts)


