from collections import namedtuple
from typing import List, Union, Optional

import dask
from dask.dataframe import DataFrame
from dask import dataframe as dd, delayed
from dask.delayed import Delayed
from fsspec.core import OpenFile, open_files
from pandas import DataFrame as PDataFrame
from pyexcelerate import Workbook
import pandas as pd
from schema import Schema, Use, And, SchemaError
from schema import Optional as SOptional

from .executed import InvalidJob, ExecutedJob
from ..utils.cluster_spec import ClusterSpec
from ..utils.exception import message

VALID_TEXT_FORMATS = ["csv", "csv-crlf"]
VALID_JSON_FORMATS = ["json", "jsonl"]
VALID_INPUT_FORMATS = VALID_TEXT_FORMATS + VALID_JSON_FORMATS + ["parquet"]
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
        self.output_path: str = t.output_path
        self.partition_columns: List[str] = t.partition_columns
        self.filter_columns: List[str] = t.filter_columns
        self.line_terminator: str = t.line_terminator
        self.partitions: Optional[int]
        try:
            self.partitions = t.partitions
        except AttributeError:
            self.partitions = None 
            
        

    def df(self) -> Union[DataFrame, InvalidJob]:
        paths = self.input_paths
        df: DataFrame
        if self.input_format in VALID_TEXT_FORMATS:
            df = dd.read_csv(paths, lineterminator=self.line_terminator, assume_missing=True, sample=25000000)
            final = df
        elif self.input_format == "parquet":
            df = dd.read_parquet(paths)
            final = df
        elif self.input_format in VALID_JSON_FORMATS:
            df = dd.read_json(paths)
            final = df
        else:
            final = InvalidJob("Invalid Input Format")
        return final

    def df_to(self, df: DataFrame, label: Optional[str] = None) -> Union[ExecutedJob, InvalidJob]:

        output_path = self.output_path
        output_format = self.output_format
        
        def _write_excel(df: PDataFrame, fil: OpenFile):
            with fil as f:
                values = [df.columns] + list(df.values)
                wb = Workbook()
                wb.new_sheet('sheet 1', data=values)
                wb.save(f)
            return None

        def good_job():
            return ExecutedJob(f"Table succesfully formatted as {output_format} and exported to {output_path}")

        def to_xlsx(df: DataFrame, output_path: str):
            writer = pd.ExcelWriter('test_out.xlsx', engine='xlsxwriter')
            to_excel_chunk = delayed(_write_excel)

            dfs = df.repartition(partition_size="10MB").to_delayed()

            def name_function(i: int):
                return f"part_{i}.xlsx"

            files = open_files(
                output_path,
                mode="wb",
                num=df.npartitions,
                name_function=name_function
            )

            def replace_path(f: OpenFile) -> OpenFile:
                p = f.path
                f.path = p.replace(".xlsx.part", ".xlsx")
                return f

            files = list(map(lambda f: replace_path(f), files))
            values = [to_excel_chunk(dfs[0], files[0])]
            values.extend(
                [to_excel_chunk(d, f) for d, f in zip(dfs[1:], files[1:])]
            )
            delayed(values).compute()

        final: Union[ExecutedJob, InvalidJob]

        if label:
            output_path = output_path + label + "/"

        if output_format == "csv":
            dd.to_csv(df, output_path, index=False)
            final = good_job()
        elif output_format == "parquet":
            dd.to_parquet(df, output_path)
            final = good_job()
        elif output_format == "json":
            dd.to_json(df, output_path)
            final = good_job()
        elif output_format == "xlsx":
            to_xlsx(df, output_path)
            final = good_job()
        else:
            final = InvalidJob(f"Invalid input format: {self.input_format}")

        return final
    
    def df_to_simple(self, df: DataFrame, output_path: str) -> List[Delayed]:
        a: List[Delayed] = dd.to_csv(df, output_path, index=False)
        return a

    def check_columns(self, df: DataFrame, columns: List[str]) -> Union[bool, InvalidJob]:
        keys = df.dtypes.keys()
        diff = set(columns).difference(set(df.dtypes.keys()))
        if len(diff) == 0:
            return True
        else:
            return InvalidJob(f"Filter columns {', '.join(diff)} not a subset of {', '.join(keys)}")

    def repartition(self, df: DataFrame, cluster_spec: ClusterSpec, partitions: Optional[int]) -> Union[DataFrame, InvalidJob]:
        size = df.size.compute()
        final: Union[DataFrame, InvalidJob]
        
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
                    final = InvalidJob(f"Partitions too large for workers: (size, max) = ({even_partition_size}, {max_partition_size}).  Add more workers or increase worker memory.")
                else:
                    final = df.repartition(partition_size=even_partition_size).repartition(npartitions=num_workers)
            else:
                final = df.repartition(partition_size=even_partition_size).repartition(npartitions=num_workers)
        else:
            final = df

        return final

    def run(self, cluster_spec: ClusterSpec):
        df = self.df()
        
        if isinstance(df, DataFrame):
            def write_partitioned(df: PDataFrame, partition_columns: List[str]):
                # TODO:  Fix default value
                ddf = dd.from_pandas(df, npartitions=cluster_spec.num_workers() or self.partitions or 10)
                labels = {}

                for p in partition_columns:
                    value = df[p].unique()[0]
                    labels[p] = str(value)

                label = "&".join(list(map(lambda item: "=".join(item), labels.items())))
                self.df_to(ddf, label)

                return f"Succefully wrote partition {label}"

            if len(self.filter_columns) > 0:
                check = self.check_columns(df, self.filter_columns)
                if isinstance(check, InvalidJob):
                    df = check
                else:
                    df = df[self.filter_columns]
                    df = self.repartition(df, cluster_spec, self.partitions)

            final: Union[ExecutedJob, InvalidJob]
            if len(self.partition_columns) > 0:
                check = self.check_columns(df, self.partition_columns)
                if isinstance(check, InvalidJob):
                    final = check
                else:
                    pc = self.partition_columns
                    a = df.groupby(pc).apply(write_partitioned, pc, meta=str).compute()
                    results = list(a.values)
                    final = ExecutedJob(", ".join(results))
            else:
                df = self.repartition(df, cluster_spec, self.partitions)
                if isinstance(df, DataFrame):
                    final = self.df_to(df)
                else:
                    final = df

        else:
            final = df
            
        return final

