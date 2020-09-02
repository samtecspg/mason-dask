from collections import namedtuple
from functools import partial
from typing import Union, List

from dask.dataframe import DataFrame
from returns.result import Result, Success

from mason_dask.jobs.executed import InvalidJob, ExecutedJob
from mason_dask.jobs.format import VALID_INPUT_FORMATS
from schema import Schema, And, Use, SchemaError
from schema import Optional as SOptional

from mason_dask.models.dataframe_from import df_from
from mason_dask.models.dataframe_to import df_to as df_to_b
from mason_dask.utils.exception import message

from returns.pipeline import flow
from returns.pointfree import bind, bind_result

class ValidQueryJob:

    def __init__(self, t):
        self.input_format: str = t.input_format
        self.input_paths: List[str] = t.input_paths
        self.output_path: str = t.output_path
        self.query_string: str = t.query_string
        self.line_terminator: str = t.line_terminator

    def run(self):
        return flow(
            self.df(),
            bind(self.query),
            bind(self.df_to)
        )
        
    def df(self) -> Result[DataFrame, InvalidJob]:
        return df_from(self.input_paths, self.input_format, self.line_terminator)
    
    def query(self, dataframe: DataFrame) -> Result[DataFrame, InvalidJob]:
        return Success(dataframe)
    
    def df_to(self, dataframe: DataFrame) -> Result[ExecutedJob, InvalidJob]:
        return df_to_b(dataframe, self.output_path, "parquet")

class QueryJob:
    def __init__(self, spec: dict):
        self.spec = spec

    def schema(self) -> Schema:
        input_formats = And(Use(str), lambda n: n in VALID_INPUT_FORMATS)

        schema = {
            "input_format": input_formats,
            "input_paths": [Use(str)],
            "output_path": Use(str),
            "query_string": Use(str),
            SOptional("line_terminator", default="\n"): str
        }
        return Schema(schema)

    def validate(self) -> Union[ValidQueryJob, InvalidJob]:
        try:
            d = self.schema().validate(self.spec)
            t = namedtuple("JobAttributes", d.keys())(*d.values())
            return ValidQueryJob(t)
        except SchemaError as e:
            return InvalidJob(f"Invalid Schema: {message(e)}")



