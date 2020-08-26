from typing import List, Tuple, Optional

from distributed import Client
from distributed.scheduler import WorkerState

from mason_dask.utils.worker_spec import WorkerSpec

class ClusterSpec:
    
    def __init__(self, client: Client, num_workers: Optional[int] = None, desired_spec: Optional[WorkerSpec] = None):
        self.worker_specs = None
        if client.cluster and client.cluster.scheduler:
            workers_dict: dict = client.cluster.scheduler.workers
            workers: List[WorkerState] = list({k: workers_dict[k] for k in workers_dict.keys() if isinstance(workers_dict[k], WorkerState)}.values())
            worker_specs: List[Tuple[int, int]] = list(map(lambda w: (w.memory_limit, w.ncores), workers))
            self.worker_specs = list(map(lambda s: WorkerSpec(*s), worker_specs))
        elif num_workers and desired_spec:
            assert(desired_spec is not None)
            assert(num_workers is not None)
            self.worker_specs = [desired_spec] * num_workers 
            
    def valid(self) -> bool:
        if self.worker_specs:
            return True
        else:
            return False

    def max_partition_size(self) -> Optional[int]:
        if self.worker_specs:
            return min(list(map(lambda s: s.memory, self.worker_specs)))
        else:
            return None
    
    def num_workers(self) -> Optional[int]:
        if self.worker_specs:
            return len(self.worker_specs)
        else:
            return None
    
