# python metaflow/plugins/argo/test_jsontype.py run
from metaflow import JSONType


# a user-made class for testing
class ChrisVan:

    def __init__(self):
        self.hello = "Hi, this is Chris Van, but you can call me Chris"

    def say_hi(self):
        print(self.hello)


# testing string inputs

# input: str, output: dict
x = JSONType('{"a": [1, 2, 3], "b": 4}')
print(f"type: {type(x)}, data: {x}")

# input: str, output: list
x = JSONType('[{"a": [1, 2, 3], "b": 4}, 5, "99"]')
print(f"type: {type(x)}, data: {x}")

# input: str, output: set
y = {"a", 123, "b", 5, "99"}
try:
    x = JSONType(str(y))
    print(f"type: {type(x)}, data: {x}")
except:
    print(f"JSONType does not support conversion to {type(y)} objects")

# input: str, output: ChrisVan
y = ChrisVan()
try:
    x = JSONType(str(y))
    print(f"type: {type(x)}, data: {x}")
except:
    print(f"JSONType does not support conversion to {type(y)} objects")


# testing other types as inputs
print("")

# input: dict, output: dict
x = JSONType({"a": [1, 2, 3], "b": 4})
print(f"type: {type(x)}, data: {x}")

# input: list, output: list
x = JSONType([{"a": [1, 2, 3], "b": 4}, 5, "99"])
print(f"type: {type(x)}, data: {x}")

# input: set, output: set
y = {"a", 123, "b", 5, "99"}
x = JSONType(y)
print(f"type: {type(x)}, data: {x}")

# input: ChrisVan, output: ChrisVan
y = ChrisVan()
y.say_hi()
x = JSONType(y)
print(f"type: {type(x)}, data: {x}")


"""
OUTPUT:
type: <class 'dict'>, data: {'a': [1, 2, 3], 'b': 4}
type: <class 'list'>, data: [{'a': [1, 2, 3], 'b': 4}, 5, '99']
JSONType does not support conversion to <class 'set'> objects
JSONType does not support conversion to <class '__main__.ChrisVan'> objects

type: <class 'dict'>, data: {'a': [1, 2, 3], 'b': 4}
type: <class 'list'>, data: [{'a': [1, 2, 3], 'b': 4}, 5, '99']
type: <class 'set'>, data: {'a', 'b', 5, 123, '99'}
Hi, this is Chris Van, but you can call me Chris
type: <class '__main__.ChrisVan'>, data: <__main__.ChrisVan object at 0x10c5f9fd0>
"""
