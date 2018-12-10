from dbservice import dataservice
import utils.utils as ut
import json

ut.set_debug_mode(False)
dataservice.set_config()




def test_get_resource():
    template = {
        "nameLast": "Williams",
        "nameFirst": "Ted"
    }

    fields = ['playerID', 'nameFirst', 'bats', 'birthCity']

    result = dataservice.retrieve_by_template("people", template, fields)
    print("Result = ", json.dumps(result, indent=2))

def test_cache1():
    # first miss then hit
    template = {
        "nameLast": "Williams",
        "nameFirst": "Ted"
    }
    fields = ['playerID', 'nameFirst', 'birthCity']

    result = dataservice.retrieve_by_template("people", template, fields)
    print("Result = ", json.dumps(result, indent=2))
    result = dataservice.retrieve_by_template("people", template, fields)
    print("Result = ", json.dumps(result, indent=2))

def test_cache2():
    # cache miss
    template = {
        "nameLast": "Williams",
        "nameFirst": "Ted"
    }

    fields = ['playerID', 'nameFirst', 'bats', 'birthCity']
    result = dataservice.retrieve_by_template("people", template, fields)
    print("Result = ", json.dumps(result, indent=2))

def test_cache3():
    # this depends on test_cache2
    # cache hit
    template = {
        "nameLast": "Williams",
        "nameFirst": "Ted"
    }

    fields = ['playerID', 'nameFirst', 'bats', 'birthCity']
    result = dataservice.retrieve_by_template("people", template, fields)
    print("Result = ", json.dumps(result, indent=2))




# test_get_resource()
# test_cache1()
# test_cache2()
test_cache3()

