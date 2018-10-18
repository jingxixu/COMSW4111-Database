# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)

def parse_and_print_args():
    """
    :return:
        in_args: {'teamid': ['ana'], 'lgid': ['al']}
        fields: ['yearid, playerid, GS']
        limit: int
        offset: int
    """

    fields = None
    in_args = None
    limit = None
    offset = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        fields = copy.copy(in_args.get('fields', None))
        limit = copy.copy(in_args.get('limit', None))
        offset = copy.copy(in_args.get('offset', None))
        if fields:
            del(in_args['fields'])
        if limit:
            limit = int(limit[0])
            del(in_args['limit'])
        if offset:
            offset = int(offset[0])
            del(in_args['offset'])

    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None

    limit_def = 10
    offset_def = 0
    if limit is None or limit > limit_def:
        limit = limit_def
    if offset is None:
        offset = offset_def

    # print("Request.args : ", json.dumps(in_args))
    print("in_args", in_args)
    print("fields", fields)
    print("body", body)
    print("limit", limit)
    print("offset", offset)
    return in_args, fields, body, limit, offset

def get_current_link(url, limit, offset):
    if '?' in url:
        ss = url.split("&")
        current_url = ''
        for s in ss:
            if s[0:5] != 'limit' and s[0:6] != 'offset':
                current_url+=s
                current_url+='&'
        # ...?offset=
        if 'offset' in current_url:
            current_url = current_url.split('offset', 1)[0]
        current_url+='offset='+str(offset)+'&limit='+str(limit)
    else:
        current_url = url
        current_url += '?offset=' + str(offset) + '&limit=' + str(limit)
    return current_url

def get_next_link(url, limit, offset):
    if '?' in url:
        ss = url.split("&")
        current_url = ''
        for s in ss:
            if s[0:5] != 'limit' and s[0:6] != 'offset':
                current_url+=s
                current_url+='&'
        # ...?offset=
        if 'offset' in current_url:
            current_url = current_url.split('offset', 1)[0]
        current_url+='offset='+str(offset+limit)+'&limit='+str(limit)
    else:
        current_url=url
        current_url += '?offset=' + str(offset + limit) + '&limit=' + str(limit)
    return current_url

def get_prev_link(url, limit, offset):
    if '?' in url:
        ss = url.split("&")
        current_url = ''
        for s in ss:
            if s[0:5] != 'limit' and s[0:6] != 'offset':
                current_url+=s
                current_url+='&'
        # ...?offset=
        if 'offset' in current_url:
            current_url = current_url.split('offset', 1)[0]
        current_url+='offset='+str(offset-limit if offset-limit>=0 else 0)+'&limit='+str(limit)
    else:
        current_url=url
        current_url += '?offset=' + str(offset - limit if offset - limit >= 0 else 0) + '&limit=' + str(limit)
    return current_url

### NOTE base resourse
@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource_b(resource):
    # in_args: {'nameLast': ['Williams'], 'nameFirst': ['woody']}
    in_args, fields, body, limit, offset = parse_and_print_args()
    if request.method == 'GET':
        data = SimpleBO.find_by_template(resource,
                                           in_args, fields, limit, offset)
        current_l = {'current': get_current_link(request.url, limit, offset)}
        prev_l = {'previous': get_prev_link(request.url, limit, offset)}
        next_l = {'next': get_next_link(request.url, limit, offset)}
        links = [prev_l, current_l, next_l]
        result = dict()
        result['data'] = data
        result['links'] = links
        # add link to result
        return json.dumps(result), 200, \
               {"content-type": "application/json; charset: utf-8"}
    elif request.method == 'POST':
        # print("This would be a really good place to call insert()")
        # print("on table ", resource)
        # print("with row ", body)
        # print("But there has to be some HW not written in class.")
        SimpleBO.insert(resource, body)

        return "POST successfully", 201
        # return "Method " + request.method + " on resource " + resource + \
        #     " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}
    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}


### NOTE specific resource
@app.route('/api/<resource>/<primary_key>', methods=['GET', 'DELETE', 'PUT'])
def get_resource_p(resource, primary_key):
    """
    resource is just the table name
    """
    in_args, fields, body, limit, offset = parse_and_print_args()

    if request.method == 'GET':
        result = SimpleBO.find_by_primary_key(resource, primary_key, fields)
        if result:
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "NOT FOUND", 404
    elif request.method == 'DELETE':
        SimpleBO.delete_by_primary_key(resource, primary_key)
        return "DELETE successfully"
    elif request.method == 'PUT':
        SimpleBO.update_by_primary_key(resource, body, primary_key)
        return "UPDATE successfully"
    else:
        return "{} method not implemented".format(request.method)

### NOTE related resource
@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', 'POST'])
def get_resource_r(resource, primary_key, related_resource):
    in_args, fields, body, limit, offset = parse_and_print_args()

    if request.method == 'GET':
        data = SimpleBO.find_related(resource, primary_key, related_resource, in_args, fields, limit, offset)
        current_l = {'current': get_current_link(request.url, limit, offset)}
        prev_l = {'previous': get_prev_link(request.url, limit, offset)}
        next_l = {'next': get_next_link(request.url, limit, offset)}
        links = [prev_l, current_l, next_l]
        result = dict()
        result['data'] = data
        result['links'] = links
        if result:
            return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
        else:
            return "NOT FOUND", 404
    elif request.method == 'POST':
        SimpleBO.insert_related(resource, primary_key, related_resource, body)
        return "POST successfully"
    else:
        return "{} method not implemented".format(request.method)

@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_resource_custom_teammates(playerid):
    # just to get limit and offset
    in_args, fields, body, limit, offset = parse_and_print_args()

    if request.method == 'GET':
        data = SimpleBO.get_teammates(playerid, limit, offset)
        current_l = {'current': get_current_link(request.url, limit, offset)}
        prev_l = {'previous': get_prev_link(request.url, limit, offset)}
        next_l = {'next': get_next_link(request.url, limit, offset)}
        links = [prev_l, current_l, next_l]
        result = dict()
        result['data'] = data
        result['links'] = links
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
    else:
        return "{} method not implemented".format(request.method)

@app.route('/api/people/<playerid>/career_stats', methods=['GET'])
def get_resource_careerstats(playerid):
    # just to get limit and offset
    in_args, fields, body, limit, offset = parse_and_print_args()

    if request.method == 'GET':
        data = SimpleBO.get_career_stats(playerid, limit, offset)
        current_l = {'current': get_current_link(request.url, limit, offset)}
        prev_l = {'previous': get_prev_link(request.url, limit, offset)}
        next_l = {'next': get_next_link(request.url, limit, offset)}
        links = [prev_l, current_l, next_l]
        result = dict()
        result['data'] = data
        result['links'] = links
        return json.dumps(result), 200, {'Content-Type': 'application/json; charset=utf-8'}
    else:
        return "{} method not implemented".format(request.method)


if __name__ == '__main__':
    app.run()

