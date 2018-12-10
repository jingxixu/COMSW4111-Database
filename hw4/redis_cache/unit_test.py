from redis_cache import data_cache
from utils import utils as ut

ut.set_debug_mode(True)

t = {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}
r = data_cache.compute_key("people", {"playerID": "willite01", "nameLast": "Williams", "bats": "R"}, \
                           ['nameLast', "birthCity"])


def test1():
    data_cache.add_to_cache(r, t)


def test2():
    result = data_cache.get_from_cache(r)
    print("Result = ", result)

def test_cache4():
    print("test add_to_cache")
    data_cache.add_to_cache(r, t)
    print("test get_from_cache")
    result = data_cache.get_from_cache(r)
    print("Result = ", result)

# test1()
# test2()
test_cache4()

