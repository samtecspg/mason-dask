from abc import abstractmethod
from typing import Union

from mason_dask.jobs.executed import InvalidJob

from schema import Schema, SchemaError
from collections import namedtuple

from mason_dask.utils.exception import message

class JobSchema:
    
    def __init__(self, spec: dict):
        self.spec = spec
    
    @abstractmethod 
    def schema(self) -> Schema:
        raise NotImplementedError("Schema not Implemented")
        
    def validate(self) -> Union['JobSchema', InvalidJob]:
        final: Union[dict, InvalidJob]
        try:
            d = self.schema().validate(self.spec)
            self.attributes = namedtuple("JobAttributes", d.keys())(*d.values())
            return self
        except SchemaError as e:
            final = InvalidJob(f"Invalid job spec: {message(e)}")
        return final

    def __getattr__(self, name):
        def _missing():
            return self.attributes.__getattribute__(name)

        return _missing()
