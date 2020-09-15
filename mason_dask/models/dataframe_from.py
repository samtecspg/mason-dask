from typing import List
from dask.dataframe import DataFrame
from mason_dask.jobs.executed import InvalidJob
from dask import dataframe as dd
from returns.result import Result, Failure

from mason_dask.utils.returns import safe_obj

VALID_TEXT_FORMATS = ["csv", "csv-crlf"]
VALID_JSON_FORMATS = ["json", "jsonl"]
VALID_INPUT_FORMATS = VALID_TEXT_FORMATS + VALID_JSON_FORMATS + ["parquet"]

def df_from(input_paths: List[str], input_format: str, line_terminator: str) -> Result[DataFrame, InvalidJob]:
    df: DataFrame
    if input_format in VALID_TEXT_FORMATS:
        @safe_obj
        def read_csv(input_paths, line_terminator, assume_missing, sample) -> DataFrame:
            return dd.read_csv(input_paths, lineterminator=line_terminator, assume_missing=assume_missing, sample=sample)
        return read_csv(input_paths, line_terminator, True, 25000000)
    elif input_format == "parquet":
        @safe_obj
        def read_parquet(input_paths) -> DataFrame:
            return dd.read_parquet(input_paths)
        return read_parquet(input_paths)
    elif input_format in VALID_JSON_FORMATS:
        @safe_obj
        def read_json(input_paths) -> DataFrame:
            return dd.read_json(input_paths)
        return read_json(input_paths) 
    else:
        return Failure(InvalidJob("Invalid Input Format"))
