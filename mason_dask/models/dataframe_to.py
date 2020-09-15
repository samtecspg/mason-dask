from typing import Optional

from dask.dataframe import DataFrame
from returns.result import Result, Failure

from mason_dask.jobs.executed import ExecutedJob, InvalidJob
from pandas import DataFrame as PDataFrame

from fsspec.core import OpenFile, open_files
from pyexcelerate import Workbook
from dask import dataframe as dd, delayed
import pandas as pd

from mason_dask.utils.returns import safe_job


def df_to(df: DataFrame, output_path: str, output_format: str, label: Optional[str] = None) -> Result[ExecutedJob, InvalidJob]:
    
    def _write_excel(df: PDataFrame, fil: OpenFile):
        with fil as f:
            values = [df.columns] + list(df.values)
            wb = Workbook()
            wb.new_sheet('sheet 1', data=values)
            wb.save(f)
        return None



    if label:
        output_path = output_path + label + "/"

    success_message = f"Table succesfully formatted as {output_format} and exported to {output_path}"
    if output_format == "csv":
        @safe_job
        def to_csv(df, output_path):
            dd.to_csv(df, output_path, index=False)
            return success_message
        return to_csv(df, output_path)
    elif output_format == "parquet":
        @safe_job
        def to_parquet(df, output_path):
            dd.to_parquet(df, output_path)
            return success_message
        return to_parquet(df, output_path)
    elif output_format == "json":
        @safe_job
        def to_json(df, output_path):
            dd.to_json(df, output_path)
            return success_message
        return to_json(df, output_path) 
    elif output_format == "xlsx":
        @safe_job
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
        return to_xlsx(df, output_path)
    else:
        return Failure(InvalidJob(f"Invalid output format: {output_format}"))


