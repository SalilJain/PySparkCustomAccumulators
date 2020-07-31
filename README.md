# PySparkCustomAccumulators

Spark, by default, provides accumulators that are int/float that supports the commutative and associative operations. 
Though spark also provides a class AccumulatorParam to inherit from to support different types of accumulators. One just needs to implement two methods zero and addInPlace. zero defines zero value of the accumulator type and addInPlace defines how two values of accumulator type are added together.

This repository exposes three different types of custom accumulators.

## DictAccumulator
There are five different types of DictAccumulator supported:

REPLACE: replaces the current value with the new value if the key exists otherwise adds the key, value to the dictionary

KEEP: keeps the old value if the key exists otherwise adds the key, value to the dictionary

ADD: add the new value to existing if the key exists otherwise adds the key, value to the dictionary

LIST: adds the new list to existing if the key exists otherwise adds the key and the list as value to the dictionary

SET: union the new set (converted from new list) to existing set value (converted from existing set as value) if the key exists otherwise adds the key and the list as value to the dictionary

```python
import CustomAccumulators as ca
acc = spark.sparkContext.accumulator(dict(),
                                                  ca.DictAccumulator(ca.DictAccumulatorMethod.LIST))
def odd_even(x):
    global acc
    acc += {x % 2: [x]}

rdd.foreach(odd_even)
```

## List Accumulator

```python
import CustomAccumulators as ca
acc = spark.sparkContext.accumulator(list(), ca.ListAccumulator())

def odd_even(x):
    global acc
    acc += [x*2]

rdd.foreach(odd_even)
```

## SetAccumulator
```python
import CustomAccumulators as ca
acc = spark.sparkContext.accumulator(set(), ca.SetAccumulator())

def odd_even(x):
    global acc
    acc += set([x % 2])

rdd.foreach(odd_even)
```
