from pyspark import AccumulatorParam
from enum import Enum


class DictAccumulatorMethod(Enum):
    # REPLACE either adds a new key value or replace the value of existing key
    REPLACE = lambda d, key, value: \
        d.update({key: value})
    # KEEP either adds a new key value or keeps the value of existing key
    KEEP = lambda d, key, value: \
        d.update({key: d.get(key, value)})
    # ADD adds the value to existing value or add new key value
    ADD = lambda d, key, value: \
        d.update({key: (d[key] + value) if key in d else value})
    # LIST add new value to the list pertaining to given key
    LIST = lambda d, key, value: \
        d.update({key: d.get(key, []) + value})
    # SET add new value to the set pertaining to given key
    SET = lambda d, key, value: \
        d.update({key: list(set(d.get(key, set([]))).union(value))})


class DictAccumulator(AccumulatorParam):

    def __init__(self, dict_accumulator_method=DictAccumulatorMethod.REPLACE):
        """
        Initialize accumulator with specific type
        :param dict_accumulator_method: method which defines combining values based on type
        """
        self.method = dict_accumulator_method

    def zero(self, init_value: dict):
        return init_value

    def addInPlace(self, v1: dict, v2: dict):
        for key, value in v2.items():
            self.method(v1, key, value)
        return v1


class SetAccumulator(AccumulatorParam):

    def zero(self, init_value: set):
        return init_value

    def addInPlace(self, v1: set, v2: set):
        return v1.union(v2)


class ListAccumulator(AccumulatorParam):

    def zero(self, init_value: list):
        return init_value

    def addInPlace(self, v1: list, v2: list):
        return v1 + v2
