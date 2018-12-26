import pymysql
import copy         # Copy data structures.
import pymysql.cursors
import json
from operator import itemgetter


# Maximum number of data rows to display in the __str__().
max_rows_to_print = 10

cursorClass = pymysql.cursors.DictCursor
charset = 'utf8mb4'


def debug_messages(msg, obj=None):

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


class RDBDataTable:


    def __init__(self, t_name, t_file, key_columns, connect_info, debug_messages=False):
        self.table_name = t_name
        self.table_file = t_file
        self.key_columns = key_columns
        self.columns = None
        self.rows = None
        self.derived = False
        self.debug_msgs = debug_messages

        if connect_info is not None:
            self.cnx = pymysql.connect(host=connect_info['host'],
                                  user=connect_info['user'],
                                  password=connect_info['pw'],
                                  db=connect_info['db'],
                                  charset=charset,
                                  cursorclass=pymysql.cursors.DictCursor)
        else:
            self.derived = True


    def __debug_message__(self, msg, obj=None):
        if self.debug_msgs:
            debug_messages(msg, obj)


    def run_q(self, q, args, fetch=False):
        """

        :param q: The query string to run.
        :param fetch: True if this query produces a result and the function should perform and return fetchall()
        :return:
        """
        self.__debug_message__("run_q: q = " + q)

        cursor = self.cnx.cursor()
        cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
            return result
        self.cnx.commit()

    # Get the names of the columns
    def get_column_names(self):
        q = "show columns from " + self.table_file
        result = self.run_q(q, None, True)
        result = [r['Field'] for r in result]
        return list(result)

    def get_no_of_rows(self):
        q = "select count(*) as count from " + self.table_file
        result = self.run_q(q, None, True)
        result = result[0]['count']
        return result

    def get_key_columns(self):
        # This is MySQL specific and relies on the fact that MySQL returns the keys in
        # based on seq_in_index
        q = "show keys from " + self.table_file
        result = self.run_q(q, None, True)
        keys = [(r['Column_name'], r['Seq_in_index']) for r in result]
        keys = sorted(keys, key=itemgetter(1))
        keys = [k[0] for k in keys]
        return keys

    def __str__(self):
        result = "Table name: {}, File name: {}, No of rows: {}, Key columns: {}"
        row_count = None
        columns = None
        key_names = None

        # Some of the values are not defined for a derived table. We will implement support for
        # derived tables later.
        if self.table_file:
            row_count = self.get_no_of_rows()
            columns = self.get_column_names()
            key_names = self.get_key_columns()
        else:
            row_count = "DERIVED"
            columns = "DERIVED"
            key_names = "DERIVED"

        if self.table_name is None:
            self.table_name = "DERIVED"

        result = result.format(self.table_name, self.table_file, row_count, key_names) + "\n"
        result += "Column names: " + str(columns)

        q_result = []
        if row_count != "DERIVED":
            if row_count <= max_rows_to_print:
                q_result = self.find_by_template(None, fields=None, limit=None, offset=None)
            else:
                q_result = self.find_by_template(None, fields=None, limit=max_rows_to_print)

            result += "\n First few rows: \n"
            for r in q_result:
                result += str(r) + "\n"

        return result


    def template_to_where_clause(self, t):
        s = ""

        if t is None:
            return s

        for (k, v) in t.items():
            if s != "":
                s += " AND "
            s += k + "='" + v + "'"

        if s != "":
            s = "WHERE " + s;

        return s


    def find_by_template(self, t, fields=None, limit=None, offset=None):
        w = self.template_to_where_clause(t)
        cursor = self.cnx.cursor()
        if fields is None:
            fields = ['*']
        q = "SELECT " + ",".join(fields) + " FROM " + self.table_file + " " + w
        if limit is not None:
            q += " limit " + str(limit)
        if offset is not None:
            q += " offset " + str(offset)

        print("Query = ", q)
        cursor.execute(q);
        r = cursor.fetchall()
        result = r
        # print("Query result = ", r)
        return result

    def find_by_primary_key(self, key, fields):
        key_columns = self.get_key_columns()
        tmp = dict(zip(key_columns, key))
        result = self.find_by_template(tmp, fields, None, None)
        return result


    def delete(self, template):

        # I did not call run_q() because it commits after each statement.
        # I run the second query to get row_count, then commit.
        # I should move some of this logic into run_q to handle getting
        # row count, running multiple statements, etc.
        where_clause = self.template_to_where_clause(template)
        q1 = "delete from " + self.table_file + " " + where_clause + ";"
        q2 = "select row_count() as no_of_rows_deleted;"
        cursor = self.cnx.cursor()
        cursor.execute(q1)
        cursor.execute(q2)
        result = cursor.fetchone()
        self.cnx.commit()
        return result

    def insert(self, row):
        keys = row.keys()
        q = "INSERT into " + self.table_file + " "
        s1 = list(keys)
        s1 = ",".join(s1)

        q += "(" + s1 + ") "

        v = ["%s"] * len(keys)
        v = ",".join(v)

        q += "values(" + v + ")"

        params = tuple(row.values())

        result = self.run_q(q, params, False)










