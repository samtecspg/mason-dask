from typing import List, Union, Optional

from dask.dataframe import DataFrame
from dask import dataframe as dd, delayed
from dask.dataframe.shuffle import set_index
from distributed import Client
from fsspec.core import OpenFile, open_files
from pandas import DataFrame as PDataFrame
from pyexcelerate import Workbook
import pandas as pd
from schema import Schema, Use, Literal, Or, And

from .executed import InvalidJob, ExecutedJob
from .job_schema import JobSchema
from ..utils.cluster_spec import ClusterSpec

class FormatJob(JobSchema):


    # "input_paths": "test",
    # "input_format": "test",
    # "output_format": "test",
    # "line_terminator": "test",
    # "output_path": "test",
    # "partition_columns": "test",
    # "filter_columns": "test"

    def schema(self) -> Schema:
        VALID_TEXT_FORMATS = ["csv", "csv-crlf"]
        VALID_JSON_FORMATS = ["json", "jsonl"]
        VALID_INPUT_FORMATS = VALID_TEXT_FORMATS + VALID_JSON_FORMATS + ["parquet"]
        VALID_OUTPUT_FORMATS = ["csv", "json", "xlsx", "parquet"]
        
        schema = {
            "input_paths": [Use(str)],
            "input_format": And(Use(str), lambda n: n in VALID_INPUT_FORMATS),
            "output_format": And(Use(str), lambda n: n in VALID_OUTPUT_FORMATS),
            "line_terminator": Use(str), 
            "output_path": Use(str),
            "partition_columns": [Use(str)],
            "filter_columns": [Use(str)]
        }
        
        return Schema(schema)

    def _write_excel(df: PDataFrame, fil: OpenFile, *, depend_on=None, **kwargs):
        with fil as f:
            values = [df.columns] + list(df.values)
            wb = Workbook()
            wb.new_sheet('sheet 1', data=values)
            wb.save(f)
        return None

    def df(self) -> Union[DataFrame, InvalidJob]:
        paths = self.input_paths
        df: DataFrame
        if self.input_format in self.VALID_TEXT_FORMATS:
            df = dd.read_csv(paths, lineterminator=self.line_terminator)
            final = df
        elif self.input_format == "parquet":
            df = dd.read_parquet(paths)
            final = df
        elif self.input_format in self.VALID_JSON_FORMATS:
            df = dd.read_json(paths)
            final = df
        else:
            final = InvalidJob(self.invalid_input_format())
        return final

    def df_to(self, df: DataFrame, label: Optional[str]) -> Union[ExecutedJob, InvalidJob]:
        
        output_path = self.output_path
        
        def good_job():
            return ExecutedJob(f"Table succesfully formatted as {self.output_format} and exported to {self.output_path}")

        def to_xlsx(df: DataFrame, output_path: str):
            writer = pd.ExcelWriter('test_out.xlsx', engine='xlsxwriter')
            to_excel_chunk = delayed(self._write_excel, pure=False)

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
            output_path += df[label].unique()[0]

        if self.output_format == "csv":
            dd.to_csv(df, output_path)
            df.to_csv(output_path)
            final = good_job()
        elif self.output_format == "parquet":
            dd.to_parquet(df, output_path)
            final = good_job()
        elif self.output_format == "json":
            dd.to_json(df, output_path)
            final = good_job()
        elif self.output_format == "xlsx":
            to_xlsx(df, output_path)
            final = good_job()
        else:
            final = InvalidJob(self.invalid_output_format())
            
        return final

    def check_columns(self, df: DataFrame, columns: List[str]) -> Union[bool, InvalidJob]:
        keys = df.dtypes.keys()
        diff = set(columns).difference(set(df.dtypes.keys()))
        if len(diff) == 0:
            return True 
        else:
            return InvalidJob(f"Filter columns {', '.join(diff)} not a subset of {', '.join(keys)}")
        
    def repartition(self, df: DataFrame, cluster_spec: ClusterSpec, num_partitions: Optional[int] = None) -> Union[DataFrame, InvalidJob]:
        size = df.size.compute()
        workers = cluster_spec.num_workers()
        parts = num_partitions or workers

        even_worker_partition_size = size / workers
        max_partition_size = cluster_spec.max_partition_size()
        if even_worker_partition_size > max_partition_size:
            return InvalidJob(f"Partitions too large for workers: (size, max) = ({even_worker_partition_size}, {max_partition_size}).  Add more workers or increase worker memory.")
        else:
            return df.repartition(partition_size=even_worker_partition_size).repartition(npartitions=parts)

    def run(self, client: Client) -> Union[ExecutedJob, InvalidJob]:
        
        # if isinstance(df, DataFrame):
        # 
        #     if len(self.partition_columns) > 0:
        #         num_partitions = df["vendor_name"].nunique().compute()
        #         df = set_index(df, "vendor_name", npartitions=num_partitions)
        # 
        #     if len(self.filter_columns) > 0:
        #         check = self.check_columns(df, self.filter_columns, cluster_spec)
        #         if isinstance(check, InvalidJob):
        #             df = check
        #         else:
        #             #  filter df
        #             df = self.repartition(df, cluster_spec) # Want to repartition AFTER filtering in this case
        #     
        # 
        # if isinstance(df, DataFrame):
        #     final: Union[ExecutedJob, InvalidJob] = self.df_to(df)
        # else:
        #     final = df

        df = self.df()
        def write_file(grp, label):
            pc = grp[label].unique()[0]
            grp.to_csv(f"{self.output_path}{label}={pc}.csv")
            return ExecutedJob("GOOD")

        col = self.partition_columns[0]
        df.groupby(col).apply(self.df_to, col, meta=ExecutedJob).compute()

        return None
