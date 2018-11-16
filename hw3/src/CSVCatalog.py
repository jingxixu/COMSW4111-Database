import pymysql
import csv
import logging

import src.DataTableExceptions as de

### After conda install pymysql, python -m stops working

class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """

        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        self.column_name = column_name
        self.column_type = column_type
        self.not_null = not_null

    def __str__(self):
        pass

    def to_json(self):
        """

        :return: A JSON object, not a string, representing the column and it's properties.
        """
        res = dict()
        res["column_name"] = self.column_name
        res["column_type"] = self.column_type
        res["not_null"] = self.not_null
        return res


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, columns):
        """

        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        ## TODO index type cannot be something other than the three
        self.index_name = index_name
        self.index_type = index_type
        self.columns = columns

    def to_tuple(self):
        key = self.index_name
        value = dict()
        value["index_name"] = self.index_name
        value["columns"] = self.columns
        value["kind"] = self.index_type
        return key, value

class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None, load=False):
        """

        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        :param load: construct a table definition based on existing metadat in db
        """
        assert t_name is not None
        assert csv_f is not None

        if not load:
            self.table_name = t_name
            self.csv_f = csv_f
            self.column_definitions = column_definitions if column_definitions is not None else []
            self.index_definitions = index_definitions if index_definitions is not None else []
            self.cnx = cnx


            # construct valid columns
            csv.register_dialect('myDialect',
                                 delimiter=',',
                                 skipinitialspace=True)
            with open(self.csv_f, 'r') as csvFile:
                reader = csv.reader(csvFile, dialect='myDialect')
                for row in reader:
                    self.valid_columns = row
                    break

            # test if table_name already exists
            q = "select table_name from catalog_tables"
            result = run_q(self.cnx, q, None, True)
            existed_tbnms = [r['table_name'] for r in result]
            if self.table_name in existed_tbnms:
                raise de.DataTableException(code=-101, message="Table name {} is duplicate".format(self.table_name))

            ## add table metadata
            q = "insert into catalog_tables values (%s, %s)"
            args = [t_name, csv_f]
            run_q(self.cnx, q, args, False)

            ## add column metadata
            if len(column_definitions) > 0:
                for c in column_definitions:
                    self.__add_column_definition(c)

            ## add index metadata
            pass

        else:
            q = "select table_name from catalog_tables"
            result = run_q(self.cnx, q, None, True)
            existed_tbnms = [r['table_name'] for r in result]
            if self.table_name not in existed_tbnms:
                raise de.DataTableException(message="Table name {} does not exists".format(self.table_name))


    def __str__(self):
        pass

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """

        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        # get file path
        q = "select * from catalog_tables where table_name = %s"
        args = [table_name]
        res = run_q(cnx, q, args, True)
        csv_f = res[0]['data_path']

        # get column definitions
        cds = []
        q = "select * from catalog_columns where table_name = %s"
        args = [table_name]
        res = run_q(cnx, q, args, True)
        for d in res:
            cds.append(ColumnDefinition(d["column_name"], d["type"], d["is_nullable"]))

        # get index definitions
        ## TODO
        ids = []
        q = "select * from catalog_indices where table_name = %s"
        args = [table_name]
        res = run_q(cnx, q, args, True)
        print(res)
        ids = get_ids(res)

        # return cls()

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        ## TODO test this
        self.__add_column_definition(c)
        self.column_definitions.append(c)

    def __add_column_definition(self, c):
        """
        Private version of add_column_definition, which does not add c to self.column_definitions.
        Used by constructor only.

        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        if c.column_name not in self.valid_columns:
            raise de.DataTableException(code=-100, message="Column {} definition is invald".format(c.column_name))
        q = "insert into catalog_columns values (%s, %s, %s, %s)"
        is_nullable = "yes" if c.not_null else "no" # convert python boolean to str for mysql
        args = [self.table_name, c.column_name, is_nullable, c.column_type]
        run_q(self.cnx, q, args, False)

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        pass

    def to_json(self):
        """

        :return: A JSON representation of the table and it's elements.
        """
        res = dict()
        res["definition"] = {"name": self.table_name, "path": self.csv_f}
        columns = [cd.to_json() for cd in self.column_definitions] if self.column_definitions is not None else []
        res["columns"] = columns
        res["index"] = {ind_d.to_tuple()[0]: ind_d.to_tuple()[1] for ind_d in self.index_definitions}
        return res

    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        table_columns = [cd.column_name for cd in self.column_definitions]
        if not set(columns).issubset(set(table_columns)):
            raise de.DataTableException(code=-1000, message="Primary key references an undefined column")
        self.define_index("PRIMARY", columns, "PRIMARY")


    def define_index(self, index_name, columns, kind="INDEX"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        """

        i = 0
        for c_n in columns:
            q = "insert into catalog_indices values (%s, %s, %s, %s, %s)" # arg can be int
            args = [index_name, self.table_name, c_n, i, kind]
            run_q(self.cnx, q, args, False)
            i = i + 1
        self.index_definitions.append(IndexDefinition(index_name, kind, columns))

    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        pass

    def get_index_selectivity(self, index_name):
        """

        :param index_name: Do not implement for now. Will cover in class.
        :return:
        """

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.

        example: {"definition": {"name": "people", "path": "../Data/core/People.csv"}, "columns": [], "indexes": {}}
        """
        return self.to_json()



class CSVCatalog:

    def __init__(self, dbhost='localhost', dbname='CSVCatalog', dbuser='dbuser', dbpw='dbuser'):
        self.cnx = pymysql.connect(host=dbhost,
                              user=dbuser,
                              password=dbpw,
                              db=dbname,
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):
        """

        :return: a TableDefinition object

        CSVCatalog.create_table creates the table by creating a TableDefinition, which in turn causes __init__ to occur.
        """
        return TableDefinition(t_name=table_name, csv_f=file_name,
                               column_definitions=column_definitions, cnx=self.cnx)


    def drop_table(self, table_name):
        """
        delete table information from catalog_tables, catalog_columns and catalog_indices
        """
        q = "delete from catalog_tables where table_name = %s"
        args = [table_name]
        run_q(self.cnx, q, args, False)
        q = "delete from catalog_columns where table_name = %s"
        args = [table_name]
        run_q(self.cnx, q, args, False)
        q = "delete from catalog_indices where table_name = %s"
        args = [table_name]
        run_q(self.cnx, q, args, False)

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        return TableDefinition.load_table_definition(cnx=self.cnx, table_name=table_name)


##### for manipulating mysql database
def run_q(cnx, q, args, fetch=False):
    cursor = cnx.cursor()
    try:
        cursor.execute(q, args)
    except Exception as e:
        raise de.DataTableException(ex=e)
    # cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result

def get_ids(cnx, res, table_name):
    """
    Given the result returned by run_q, parse them to return a list of index definitions.

    :param res:
    :return:
    """
    ids = []
    indices = [d["index_name"] for d in res]
    indices = list(set(indices))
    i = 0
    for d in res:
        q = "select column_name from catalog_indices where table_name=%s and index_name=%s"
        args = [table_name, indices[i]]
        run_q(cnx, q, args, True)
        i += 1















