from typing import List, Tuple

from distributed import Client
from distributed.scheduler import WorkerState

from mason_dask.utils.worker_spec import WorkerSpec

class ClusterSpec:
    
    def __init__(self, client: Client):
        workers_dict: dict = client.cluster.scheduler.workers
        workers: List[WorkerState] = list({k: workers_dict[k] for k in workers_dict.keys() if isinstance(workers_dict[k], WorkerState)}.values())
        worker_specs: List[Tuple[int, int]] = list(map(lambda w: (w.memory_limit, w.ncores), workers))
        
        self.worker_specs: List[WorkerSpec] = list(map(lambda s: WorkerSpec(*s), worker_specs))
        
    def max_partition_size(self) -> int:
        return min(list(map(lambda s: s.memory, self.worker_specs)))
    
    def num_workers(self) -> int:
        return len(self.worker_specs)
    
