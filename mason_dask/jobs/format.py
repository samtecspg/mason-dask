from collections import namedtuple
from functools import partial
from typing import List, Union, Optional

from dask.dataframe import DataFrame
from dask import dataframe as dd
from returns.result import Result, Success, Failure
from returns.converters import flatten
from returns.pipeline import flow
from returns.pointfree import bind
from schema import Schema, Use, And, SchemaError
from schema import Optional as SOptional
from pandas import DataFrame as PDataFrame

from .executed import InvalidJob, ExecutedJob
from ..models.dataframe_from import VALID_INPUT_FORMATS, df_from
from ..models.dataframe_to import df_to
from ..utils.cluster_spec import ClusterSpec
from ..utils.exception import message

VALID_OUTPUT_FORMATS = ["csv", "json", "xlsx", "parquet"]

class FormatJob:
    
    def __init__(self, spec: dict):
        self.spec = spec

    def schema(self) -> Schema:
        input_formats = And(Use(str), lambda n: n in VALID_INPUT_FORMATS)
        output_formats = And(Use(str), lambda n: n in VALID_OUTPUT_FORMATS)

        schema = {
            "input_paths": [Use(str)],
            "input_format": input_formats,
            "output_format": output_formats,
            "output_path": Use(str),
            "partition_columns": [Use(str)],
            "filter_columns": [Use(str)],
            SOptional("credentials"): Use(dict),
            SOptional("line_terminator", default="\n"): str,
            SOptional("partitions"): Use(int)
        }
        return Schema(schema)

    def validate(self) -> Union['ValidFormatJob', InvalidJob]:
        try:
            d = self.schema().validate(self.spec)
            t = namedtuple("JobAttributes", d.keys())(*d.values())
            return ValidFormatJob(t)
        except SchemaError as e:
            return InvalidJob(f"Invalid Schema: {message(e)}")

class ValidFormatJob:

    def __init__(self, t):
        self.input_paths: List[str] = t.input_paths
        self.input_format: str = t.input_format
        self.output_format: str = t.output_format
        outpath: str = t.output_path
        
        if outpath.endswith("/"):
            self.output_path = outpath
        else:
            self.output_path = outpath + "/"
            
        self.partition_columns: List[str] = t.partition_columns
        self.filter_columns: List[str] = t.filter_columns
        self.line_terminator: str = t.line_terminator
        self.partitions: Optional[int]
        try:
            self.partitions = t.partitions
        except AttributeError:
            self.partitions = None 

    def check_columns(self, df: DataFrame, columns: List[str]) -> Result[DataFrame, InvalidJob]:
        keys = df.dtypes.keys()
        diff = set(columns).difference(set(df.dtypes.keys()))
        if len(diff) == 0:
            return Success(df)
        else:
            return Failure(InvalidJob(f"Filter columns {', '.join(diff)} not a subset of {', '.join(keys)}"))

    def repartition(self, df: DataFrame, cluster_spec: ClusterSpec) -> Result[DataFrame, InvalidJob]:
        size = df.size.compute()
        
        num_partitions = self.partitions

        if cluster_spec.valid():
            cluster_spec_num_workers = cluster_spec.num_workers() 
        else:
            cluster_spec_num_workers = None
            
        num_workers = num_partitions or cluster_spec_num_workers 
        
        if num_workers:
            max_partition_size = cluster_spec.max_partition_size()
            even_partition_size = size / num_workers 
            if max_partition_size:
                if even_partition_size > max_partition_size:
                    return Failure(InvalidJob(f"Partitions too large for workers: (size, max) = ({even_partition_size}, {max_partition_size}).  Add more workers or increase worker memory."))
                else:
                    return Success(df.repartition(partition_size=even_partition_size).repartition(npartitions=num_workers))
            else:
                return Success(df.repartition(partition_size=even_partition_size).repartition(npartitions=num_workers))
        else:
            return Success(df)


    def df_filter_columns(self, cluster_spec: ClusterSpec, df: DataFrame) -> Result[DataFrame, InvalidJob]:
        if len(self.filter_columns) > 0:
            check = self.check_columns(df, self.filter_columns)
            
            def filter_and_repartition(df: DataFrame) -> Result[DataFrame, InvalidJob]:
                filtered = df[self.filter_columns]
                repartitioned = self.repartition(filtered, cluster_spec)
                return repartitioned
                
            return flatten(check.map(lambda b: filter_and_repartition(b)))
        else:
            return Success(df)


    def partition_and_write(self, cluster_spec: ClusterSpec, df: DataFrame) -> Result[ExecutedJob, InvalidJob]:
        if len(self.partition_columns) > 0:
            def write_partitioned(df: PDataFrame) -> Result[ExecutedJob, InvalidJob]:
                ddf = dd.from_pandas(df, npartitions=cluster_spec.num_workers() or self.partitions or 10)
                labels = {}

                for p in self.partition_columns:
                    value = df[p].unique()[0]
                    labels[p] = str(value)

                label = "&".join(list(map(lambda item: "=".join(item), labels.items())))
                result = df_to(ddf, self.output_path, self.output_format, label)

                return result

            def repartition_and_write(df: DataFrame) -> Result[ExecutedJob, InvalidJob]:
                pc = self.partition_columns
                df.groupby(pc).apply(write_partitioned, meta=str).compute()
                return Success(ExecutedJob("GOOD"))
            
            check = self.check_columns(df, self.partition_columns)
            return flatten(check.map(lambda b: repartition_and_write(b)))
        else:
            repartitioned = self.repartition(df, cluster_spec)
            return flatten(repartitioned.map(lambda b: df_to(b, self.output_path, self.output_format)))

    def run(self, cluster_spec: ClusterSpec) -> Result[ExecutedJob, InvalidJob]:
        do_filter_columns = partial(self.df_filter_columns, cluster_spec)
        do_partition_and_write = partial(self.partition_and_write, cluster_spec)

        final = flow(
            df_from(self.input_paths, self.input_format, self.line_terminator),
            bind(do_filter_columns),
            bind(do_partition_and_write)
        )
        
        return final

