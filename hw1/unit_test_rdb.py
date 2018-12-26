
import RDBDataTable

import json

connect_info = {
    "host": "localhost",
    "user": "dbuser",
    "pw": "dbuser",
    "db": "lahman2017"
}

def test1():
    rt = RDBDataTable.RDBDataTable("Foo", 'batting', None, connect_info, debug_messages=False)
    print(rt)


def test2():
    rt = RDBDataTable.RDBDataTable("Foo", 'batting', None, connect_info, debug_messages=False)
    result = rt.find_by_primary_key(['willite01', 'BOS', '1960', '1'], ['playerID', 'H', 'HR'])
    print("test2: Result = ", json.dumps(result, indent=2))

def test3():
    rt = RDBDataTable.RDBDataTable("Foo", 'people', None, connect_info, debug_messages=False)
    tmp = {"nameLast": "Williams", "throws": "R"}
    result = rt.find_by_template(tmp, ['playerID', 'nameLast', 'throws'], 3, 10)
    print("test3: Result = ", json.dumps(result, indent=2))

def test4():
    rt = RDBDataTable.RDBDataTable("Foo", 'people', None, connect_info, debug_messages=False)
    row = {"playerID": "dff1a", "nameLast": "Ferguson", "throws": "R"}
    result = rt.insert(row)
    print("test4: Result = ", json.dumps(result, indent=2))

def test5():
    rt = RDBDataTable.RDBDataTable("Foo", 'people', None, connect_info, debug_messages=False)
    tmp = {"playerID": "dff1a", "nameLast": "Ferguson", "throws": "R"}
    result = rt.delete(tmp)
    print("test3: Result = ", json.dumps(result, indent=2))

test5()