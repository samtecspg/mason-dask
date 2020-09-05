from mason_dask.utils.exception import message
from returns.result import Result, Success, Failure
from returns.pointfree import bind
from returns.pipeline import flow
from functools import partial

def test_nested_pipe():
    class TestClass():

        def __init__(self, i: int):
            self.i = i

        def regular_function(self, arg: int) -> float:
            return float(arg)

        def returns_container(self, test: float, arg: float) -> Result[str, ValueError]:
            if arg == test:
                return Success(str(arg))
            else:
                return Failure(ValueError('Wrong arg'))

        def also_returns_container(self, arg: str) -> Result[str, ValueError]:
            if arg != "0":
                return Success(str(arg))
            else:
                return Failure(ValueError('Wrong arg 2'))

        def also_returns_container_2(self, arg: str) -> Result[str, ValueError]:
            return Success(arg + '!')

        def run(self):
            a = flow(
                self.i,
                self.regular_function,
                partial(self.returns_container, 0.0),
                bind(self.also_returns_container),
                bind(self.also_returns_container_2)
            )
            return a

    assert(TestClass(0).run()._inner_value == "0.0!")
    assert(message(TestClass(1).run()._inner_value) == "ValueError: Wrong arg\n")


