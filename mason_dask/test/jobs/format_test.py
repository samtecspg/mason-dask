from mason_dask.jobs.executed import InvalidJob
from mason_dask.jobs.format import FormatJob

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
        "line_terminator": "",
        "output_path": out_path,
        "filter_columns": filter_columns,
        "partition_columns": partition_columns 
    }
    
    job = FormatJob(good_spec).validate()
    assert(job.input_paths == in_paths)
    assert(job.input_format == in_format)
    assert(job.output_format == out_format)
    assert(job.line_terminator == "")
    assert(job.output_path == out_path)
    assert(job.filter_columns == filter_columns)
    assert(job.partition_columns == partition_columns)


    bad_spec = {
        "input_paths": "1234",
        "input_format": "avro",
        "output_format": "",
        "line_terminator": "",
        "output_path": out_path,
        "filter_columns": filter_columns,
        "partition_columns": partition_columns
    }

def test_csv():
    format_job = FormatJob({
        "input_paths": "test",
        "input_format": "test",
        "output_format": "test",
        "line_terminator": "test",
        "output_path": "test",
        "partition_columns": "test",
        "filter_columns": "test"
    })
