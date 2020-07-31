import unittest
from pyspark.sql import SparkSession
import CustomAccumulators as ca


class AccumulatorManager:

    def __init__(self, acc):
        self.acc = acc

    def accumulator(self):
        return self.acc


def get_spark():
    return SparkSession \
        .builder \
        .appName("Accumulator Test") \
        .getOrCreate()


class CustomAccumulatorsTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = get_spark()
        self.input_data = [i for i in range(10)]
        self.rdd = self.spark.sparkContext.parallelize(self.input_data)

    def test_dict_accumulator_list(self):
        acc = self.spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.LIST))
        acc_manager = AccumulatorManager(acc)

        def odd_even(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {x % 2: [x]}

        self.rdd.foreach(lambda x: odd_even(x, acc_manager))
        out = acc.value
        odd_even_data = {}
        for value in self.input_data:
            key = value % 2
            if key in odd_even_data:
                odd_even_data[key].append(value)
            else:
                odd_even_data[key] = [value]
        for key, value in odd_even_data.items():
            self.assertTrue(key in out)
            self.assertEqual(sorted(value), sorted(out[key]))

    def test_dict_accumulator_set(self):
        acc = self.spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.SET))
        acc_manager = AccumulatorManager(acc)

        def odd_even(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {"key": set([x % 2])}

        self.rdd.foreach(lambda x: odd_even(x, acc_manager))
        out = acc.value
        self.assertEqual([0, 1], sorted(out["key"]))

    def test_dict_accumulator_keep(self):
        acc = self.spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.KEEP))
        acc_manager = AccumulatorManager(acc)

        def double(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {x: x*2}

        self.rdd.foreach(lambda x: double(x, acc_manager))
        out = acc.value
        data = {}
        for value in self.input_data:
            data[value] = value*2
        for key, value in data.items():
            self.assertTrue(key in out)
            self.assertEqual(value, out[key])

        def same(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {x: x}

        self.rdd.foreach(lambda x: same(x, acc_manager))
        out = acc.value
        for key, value in data.items():
            self.assertTrue(key in out)
            self.assertEqual(value, out[key])

    def test_dict_accumulator_replace(self):
        acc = self.spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.REPLACE))
        acc_manager = AccumulatorManager(acc)

        def process(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {x: x * 2}
            accumulator += {x: x}

        self.rdd.foreach(lambda x: process(x, acc_manager))
        out = acc.value
        data = {}
        for value in self.input_data:
            data[value] = value
        for key, value in data.items():
            self.assertTrue(key in out)
            self.assertEqual(value, out[key])

    def test_dict_accumulator_add(self):
        acc = self.spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.ADD))
        acc_manager = AccumulatorManager(acc)

        def count(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += {"count": x}

        self.rdd.foreach(lambda x: count(x, acc_manager))
        out = acc.value
        self.assertEqual(sum(self.input_data), out["count"])

    def test_set_accumulator(self):
        acc = self.spark.sparkContext.accumulator(set(), ca.SetAccumulator())
        acc_manager = AccumulatorManager(acc)

        def odd_even(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += set([x % 2])

        self.rdd.foreach(lambda x: odd_even(x, acc_manager))
        out = acc.value
        self.assertEqual([0, 1], sorted(out))

    def test_list_accumulator(self):
        acc = self.spark.sparkContext.accumulator(list(), ca.ListAccumulator())
        acc_manager = AccumulatorManager(acc)

        def double(x, acc_manager_inst):
            accumulator = acc_manager_inst.accumulator()
            accumulator += [x*2]

        self.rdd.foreach(lambda x: double(x, acc_manager))
        out = acc.value
        data = [value*2 for value in self.input_data]
        self.assertEqual(sorted(data), sorted(out))


if __name__ == '__main__':
    unittest.main()
