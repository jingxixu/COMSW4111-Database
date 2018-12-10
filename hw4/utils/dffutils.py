import json
import pymysql
#from . import DataTableExceptions               # Exceptions for the solution
from utils import utils as  ut

pymysql_exceptions = (
    pymysql.err.IntegrityError,
    pymysql.err.MySQLError,
    pymysql.err.ProgrammingError,
    pymysql.err.InternalError,
    pymysql.err.DatabaseError,
    pymysql.err.DataError,
    pymysql.err.InterfaceError,
    pymysql.err.NotSupportedError,
    pymysql.err.OperationalError)

default_db_params = {
    "dbhost": "localhost",                    # Changeable defaults in constructor
    "port": 3306,
    "dbname": "lahman2017",
    "dbuser": "dbuser",
    "dbpw": "dbuser",
    "cursorClass": pymysql.cursors.DictCursor,        # Default setting for DB connections
    "charset":  'utf8mb4'                             # Do not change
}

def get_new_connection(params=default_db_params):
    cnx = pymysql.connect(
        host=params["dbhost"],
        port=params["port"],
        user=params["dbuser"],
        password=params["dbpw"],
        db=params["dbname"],
        charset=params["charset"],
        cursorclass=params["cursorClass"])
    return cnx

def insert(cnx, table_name, columns, values, commit=True):
    """
    This is a helper method to perform an insert.
    :param table_name: The RDB table for the insert. This is a table in the catalog,
    not one of the CSV table names.
    :param columns: Columns names.
    :param values: Matching values.
    :return: Return value from insert statement
    """
    q = "insert into " + table_name + " "
    column_count = len(columns)
    column_list = ",".join(columns)
    column_list = "(" + column_list + ")"
    v = ["%s"] * column_count
    v = ",".join(v)
    v = " values (" + v + ")"
    q += " " + column_list + " " + v
    rr = run_q(cnx, q, values, False, commit=commit)
    return rr


def update(cnx, table_name, row, where_clause, commit=True):
    """
    This is a helper method to perform an insert.
    :param table_name: The RDB table for the insert. This is a table in the catalog,
    not one of the CSV table names.
    :param columns: Columns names.
    :param values: Matching values.
    :return: Return value from insert statement
    """
    q = "update " + table_name + " set "

    terms = []
    keys = list(row.keys())
    values = list(row.values())

    for k in keys:
        terms.append(str(k) + "=%s")

    q += ",".join(terms) + " " + where_clause

    rr = run_q(cnx, q, values, fetch=False, commit=commit)
    return rr


def commit_cnx(cnx):
    cnx.commit()
    cnx.close()


def abort_cnx(cnx):
    cnx.close()

def run_q(cnx, q, args, fetch=False, commit=True):
    """
    :param cnx: The database connection to use.
    :param q: The query string to run.
    :param args: Parameters to insert into query template if q is a template.
    :param fetch: True if this query produces a result and the function should perform and return fetchall()
    :return:
    """
    #debug_message("run_q: q = " + q)
    ut.debug_message("Q = " + q)
    ut.debug_message("Args = ", args)

    result = None

    try:
        cursor = cnx.cursor()
        result = cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        if commit:
            cnx.commit()
    except pymysql_exceptions as original_e:
        #print("dffutils.run_q got exception = ", original_e)
        raise(original_e)

    return result


def json_to_s(obj):
    if obj is not None:
        try:
            s = json.dumps(obj, indent=2)
        except json.JSONDecodeError:
            s = str(obj)
    else:
        s = ""

    return s


def debug_message(msg, obj=None):

    obj_msg = None
    out_msg = msg

    # Some related objects can be converted to JSON.
    if obj:
        try:
            obj_msg = json.dumps(obj, indent=2)
        except TypeError:
            # Just use string format.
            obj_msg = str(obj)

        out_msg += ", object = " + obj_msg

    print(out_msg)

def debug_messages(msg, obj=None):
    pass