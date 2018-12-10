import redis
from utils import utils as ut
from operator import itemgetter
import json

"""
Connect to local Redis server. StrictRedis complies more closely with standard than
simple Redis client in this package. decode_responses specifies whether or not to convert
bytes to UTF8 character string or treat as raw bytes, e.g. an image, audio stream, etc.
"""
r = redis.StrictRedis(
    host='localhost',
    port=6379,
    charset="utf-8", decode_responses=True)


def add_to_cache(key, value):
    """

    :param key: A valid Redis key string.
    :param value: A Python dictionary to add to cache.
    :return: None
    """
    k = key
    data = json.dumps(value)
    r.set(k, data)
    return k


def get_from_cache(key):
    """

    :param key: A valid Redis key.
    :return: The "map object" associated with the key.
    """
    result = r.get(key)
    if result is not None:
        result = json.loads(result)
    return result


def compute_key(resource, template, fields):
    """

    :param resource: The name of a resource, i.e. database table name.
    :param template: A query template for finding a resource in a table.
    :param fields: List of fields to retrieve, e.g. project clause.
    :return: A valid Redis key that for storing/retrieving a map from the Redis cache.
    """
    ut.debug_message("Resource = ", resource)
    ut.debug_message("Template = ", template)
    ut.debug_message("Fields = ", fields)

    t = None
    f = None

    if template is not None:
        """
        Convert the query template to a string form of p1=v1,p2=v2, ...
        """
        print("Items = ", template.items())
        t = template.items()
        print("t = ", t)
        t = tuple(t)
        print("t = ", t)
        sorted(t, key=itemgetter(1))
        ts = [str(e[0]) + "=" + str(e[1]) for e in t]
        t = ",".join(ts)

    if fields is not None:
        """
        Convert a fields list into a string of the form f=f1,f2,...
        """
        f = sorted(fields)
        f = "f=" + (",".join(f))

    if f is not None or t is not None:
        """
        There is a convention in Redis that a collection of resources if of the form 
        collection_name:key_value. If there is a sub-key representing the template or fields selector,
        add a ':' to the end of the resource name.
        """
        result = resource + ":"
    else:
        result = resource

    # Add template string to key if it exists.
    if t is not None:
        result += t

    # If there is a fields list, add it to the key. Add a "," to separate from template string
    # if there is a template string.
    if f is not None and t is not None:
        result += "," + f
    elif f is not None and t is None:
        result += f

    return result


def check_query_cache(resource, template, fields):
    """

    :param resource: The name of a resource collection (table), e.g. 'people'
    :param template: A selection template, e.g. {'nameLast': 'Smith', 'bats': 'R'}
    :param fields: A list of fields to return, e.g. ['nameLast', 'nameFirst', 'throws', 'birthCity']
    :return: Returns a cached value from the Redis result cache if one exists.
    """
    key = compute_key(resource, template, fields)
    result = get_from_cache(key)
    return result



def add_to_query_cache(resource, template, fields, query_result):
    """
    Stores a query result in the cache.

    :param resource: The name of a resource collection (table), e.g. 'people'
    :param template: A selection template, e.g. {'nameLast': 'Smith', 'bats': 'R'}
    :param fields: A list of fields to return, e.g. ['nameLast', 'nameFirst', 'throws', 'birthCity']
    :param query_result: The value returned from the data service as a result of the query.
    :return: key value for cached resource.
    """
    key = compute_key(resource, template, fields)
    result = add_to_cache(key, query_result)
    return result

