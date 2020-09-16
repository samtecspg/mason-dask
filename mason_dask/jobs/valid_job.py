from abc import abstractmethod

from mason_dask.jobs.executed import ExecutedJob, InvalidJob
from returns.result import Result
from mason_dask.utils.cluster_spec import ClusterSpec


class ValidJob:
    
    @abstractmethod
    def run(self, cluster_spec: ClusterSpec) -> Result[ExecutedJob, InvalidJob]:
        raise NotImplementedError("run method not implemented")
