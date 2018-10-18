import pymysql
import json
from operator import itemgetter

# cnx = pymysql.connect(host='localhost',
#                               user='root',
#                               password='xixi1995',
#                               db='lahman2017raw_new',
#                               charset='utf8mb4',
#                               cursorclass=pymysql.cursors.DictCursor)

cnx = pymysql.connect(host='localhost',
                              user='dbuser',
                              password='dbuser',
                              db='lahman2017raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result

def template_to_where_clause(t):
    s = ""

    if t is None:
        return s

    for (k, v) in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v[0] + "'"

    if s != "":
        s = "WHERE " + s

    return s

def body_to_update_clause(body):
    uc = ' set '
    s = []
    for k, v in body.items():
        s.append(k + " = '" + v + "'")
    uc += ', '.join(s)
    return uc

def get_key_columns(table, key_name):
    """
    :return: a list of key_name keys, e.g. ['yearID', 'teamID']
    """
    # This is MySQL specific and relies on the fact that MySQL returns the keys in
    # based on seq_in_index
    q = "show keys from " + table
    result = run_q(q, None, True)
    keys = [(r['Column_name'], r['Seq_in_index']) for r in result if r['Key_name'] == key_name]
    keys = sorted(keys, key=itemgetter(1))
    keys = [k[0] for k in keys]
    return keys

def get_all_fields(table):
    """
    :return: a list of all fields/columns, e.g. ['playerID', 'birthYear', 'birthMonth', 'birthDay']
    """
    q = "show columns from " + table
    result = run_q(q, None, True)
    fields = [d['Field'] for d in result]
    return fields


def find_by_template(table, template, fields=None, limit=None, offset=None):
    """
    :param template: {'nameLast': ['Williams'], 'nameFirst': ['woody']}
    """
    wc = template_to_where_clause(template)

    if fields is None:
        q = 'select * ' + ' from ' + table + ' ' + wc
    else:
        q = "select " + fields[0] + " from " + table + " " + wc

    if limit is not None and offset is not None:
        q = q + " limit " + str(limit) + " offset " + str(offset)
    result = run_q(q, None, True)
    return result

def parse_primary_keys(pks):
    """
    parse primary_key

    'willite01_1960_1_BOS' --> [['willite01'], ['1960'], ['1'], ['BOS']]
    """
    parsed_keys = []
    p = 0
    for i, s in enumerate(pks):
        if s == '_':
            parsed_keys.append([pks[p:i]])
            p = i+1
    parsed_keys.append([pks[p:]])
    return parsed_keys

def find_by_primary_key(resource, primary_key, fields=None):
    ### parse primary_key here 'willite01_1960_1_BOS' --> [['willite01'], ['1960'], ['1'], ['BOS']]
    parsed_keys = []
    p = 0
    for i, s in enumerate(primary_key):
        if s == '_':
            parsed_keys.append([primary_key[p:i]])
            p = i+1
    parsed_keys.append([primary_key[p:]])
    ### parse finished

    key_columns = get_key_columns(resource, 'PRIMARY')
    tmp = dict(zip(key_columns, parsed_keys))
    result = find_by_template(resource, tmp, fields)
    return result


def insert(table, body):
    keys = body.keys()
    q = "INSERT into " + table + " "
    s1 = list(keys)
    s1 = ",".join(s1)
    q += "(" + s1 + ")"
    v = ["%s"] * len(keys)
    v = ",".join(v)
    q += "values(" + v + ")"
    params = tuple(body.values())
    result = run_q(q, params, False)


def delete(table, template):
    wc = template_to_where_clause(template)
    q = "delete from " + table + " " + wc + ";"
    run_q(q, None, False)

def delete_by_primary_key(resource, primary_key):
    parsed_keys = parse_primary_keys(primary_key)
    key_columns = get_key_columns(resource, 'PRIMARY')
    tmp = dict(zip(key_columns, parsed_keys))
    delete(resource, tmp)

def update(table, template, body):
    """ body: {'lgID': 'fff', 'G': 'yeah'} """
    wc = template_to_where_clause(template)
    uc = body_to_update_clause(body)
    q = "update " + table + uc + ' ' + wc + ';'
    run_q(q, None, False)

def update_by_primary_key(resource, body, primary_key):
    parsed_keys = parse_primary_keys(primary_key)
    key_columns = get_key_columns(resource, 'PRIMARY')
    tmp = dict(zip(key_columns, parsed_keys))
    update(resource, tmp, body)

def find_related(resource, primary_key, related_resource, in_args, fields, limit, offset):
    kc1 = get_key_columns(resource, 'PRIMARY')
    kc2 = get_all_fields(related_resource)
    parsed_keys = parse_primary_keys(primary_key)
    key_columns = get_key_columns(resource, 'PRIMARY')
    shared_fields = set(kc1) & set(kc2)
    tmp = dict(zip(key_columns, parsed_keys))
    tmp = {k:tmp[k] for k in shared_fields}
    tmp = {**tmp, **in_args}

    return find_by_template(related_resource, tmp, fields, limit, offset)

def insert_related(resource, primary_key, related_resource, body):
    kc1 = get_key_columns(resource, 'PRIMARY')
    kc2 = get_all_fields(related_resource)
    parsed_keys = parse_primary_keys(primary_key)
    key_columns = get_key_columns(resource, 'PRIMARY')
    shared_fields = set(kc1) & set(kc2)
    tmp = dict(zip(key_columns, parsed_keys))
    tmp2body = {k:tmp[k][0] for k in shared_fields}
    body = {**tmp2body, **body}
    insert(related_resource, body)

def get_teammates(playerid, limit, offset):
    q = "select b.playerID, p.namefirst as first_name, p.namelast as last_name, \
            min(b.yearid) as first_year, max(b.yearid) as last_year, \
            count(b.playerid) as count_of_seasons \
            from (Appearances a,  Appearances b) inner join people p on p.playerid = b.playerid \
            where a.teamid=b.teamid and a.yearid=b.yearid and a.playerid=%s \
            group by b.playerid \
            order by b.playerid "
    q = q + " limit " + str(limit) + " offset " + str(offset)
    result = run_q(q, playerid, True)
    return result

def get_career_stats(playerid, limit, offset):
    q = "select b.playerid, b.teamid, b.yearid, a.g_all, sum(b.h) as hit, sum(b.ab) as ABs, sum(f2.A) as Assists, sum(f2.E) as errors\
          from (batting as b inner join appearances as a \
          on b.playerid = a.playerid and b.teamid = a.teamid and b.yearid = a.yearid) \
          inner join (select f.playerid, f.yearid, f.teamid, sum(f.A) as A, sum(f.E) as E \
          from fielding as f \
          group by f.playerid, f.teamid, f.yearid) as f2 \
          on b.playerid = f2.playerid and b.teamid = f2.teamid and b.yearid =f2.yearid\
          where b.playerid = %s \
          group by b.playerid, b.teamid, b.yearid "
    q = q + " limit " + str(limit) + " offset " + str(offset)
    result = run_q(q, playerid, True)
    return result




