from typing import Union, List
from dask.dataframe import DataFrame
from mason_dask.jobs.executed import InvalidJob
from dask import dataframe as dd
from returns.result import Result, Success, Failure, safe

VALID_TEXT_FORMATS = ["csv", "csv-crlf"]
VALID_JSON_FORMATS = ["json", "jsonl"]
VALID_INPUT_FORMATS = VALID_TEXT_FORMATS + VALID_JSON_FORMATS + ["parquet"]

def df_from(input_paths: List[str], input_format: str, line_terminator: str) -> Result[DataFrame, InvalidJob]:
    df: DataFrame
    if input_format in VALID_TEXT_FORMATS:
        return Success(dd.read_csv(input_paths, lineterminator=line_terminator, assume_missing=True, sample=25000000))
    elif input_format == "parquet":
        return Success(dd.read_parquet(input_paths))
    elif input_format in VALID_JSON_FORMATS:
        return Success(dd.read_json(input_paths))
    else:
        return Failure(InvalidJob("Invalid Input Format"))
