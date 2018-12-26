import csv          # Python package for reading and writing CSV files.
import copy         # Copy data structures.
import os           # Use for some directory functions

# Some custom exceptions that convert technology specific exceptions to
# more problem specific and intuitive exceptions.
import DataTableExceptions


class CSVTable:

    # There can be many, many rows in a data table. We do not want to convert all to a string
    # This is the maximum we convert to string.
    max_rows_to_print = 10

    # The directory path containing data files.
    data_dir = os.path.dirname(os.path.realpath(__file__)) + "/Data/"

    # Width to use for formatting a row, column in __str__
    str_column_width = 15

    def __init__(self, t_name, t_file, key_columns):
        """
        :param t_name: An arbitrary name for the relation. This currently does not have
                    any function. This is a placeholder for the aliasing/renaming concept.
        :param t_file: For a CSV file, this is the name of the file, e.g. Batting.csv
        :param key_columns: A list of the columns from the file that form the primary key.
                    For Batting.csv, for example, this would be ['playerID', 'teamID', 'yearID', 'stint']
        """
        self.table_name = t_name
        self.table_file = t_file
        self.key_columns = key_columns

        # Most student implementations will use a list/array. I used a dictionary to enable support
        # for simple indexes. The numerical position of a tuple in a list can change if there are
        # deletes. So, I needed something stable.
        #
        # A dictionary hold the rows in the table. An auto-increment integer is the key. This is similar to
        # an auto-increment field being used for a primary key.
        self.rows = {}              # Store the loaded rows.
        self.next_row_id = 0        # Auto-increment integer for key.

        self.headers = None         # Need to remember the original file column headers.

        # The table may have several indexes. The self.indexes entry is a dictionary of indexes, each of which
        # is a dictionary of row IDs.
        self.indexes = None         # A dictionary of indexes for speeding up data access.

        # True if the table is derived. Cannot modify  a derived table.
        self.derived = False


    def __col_list_valid(self, col_list):
        """

        :param col_list: List of column names.
        :return: Returns True if the table contains the columns in the list, and a list of invalid columns otherwise.
        """
        headers_set = set(self.headers)
        col_set = set(col_list)

        # The passed columns must be a subset of the column headers.
        if col_set.issubset(headers_set):
            result = True
        else:
            # Return the list of invalid column names.
            result = col_set.difference(headers_set)

        return result

    # I have been pretty sloppy about methods, static methods and class methods.
    # This is not an OO Python course and I am lazy.
    @staticmethod
    def __format_odict(l):
        """
        Use to help print a row from the tale.

        :param l: An ordered dictionary representing a row in the table.
        :return: A string with the row printed in equally spaced columns
        """
        result = ""
        temp = list(l)
        template = "{:<" + str(CSVTable.str_column_width) + "}"
        for i in range(0, len(temp)):
            result += template.format(temp[i])
        return result

    # Simply returns the fully qualified name of the file to read or write.
    def __get_file_name(self):
        fn = CSVTable.data_dir + self.table_file
        return fn

    def __get_index_values(self, row, index_name):
        """

        :param row: A row (dictionary) from the table.
        :param index_name: The name of an index.
        :return: Returns the string of column values delimited by "_" that are the row's values for the index key
        """
        result = {}
        columns = index_name.split("_")     # Index names is the columns in order delimited by "_"
        for c in columns:
            result[c] = row[c]

        return str(result)


    def __get_index_name(self, tmp):
        """
        Returns first index matching the set of keys in the template.

        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the name.
        :param tmp: Query template.
        :return: Index or None
        """

        if self.indexes is None:
            return None

        # Get a set containing the key's columns.
        tmp_set = set(tmp.keys())

        # Examine each of the indexes.
        for name, bucket in self.indexes.items():
            # Convert the name string to a set of column names.
            # Using sets allows more flexibility with the order of columns in the row.
            cols = set(name.split("_"))

            if cols == tmp_set:
                return name
        else:
            return None

    def __str__(self):
        """
        This is some pretty icky, hacked code to print out the tale. I only did this because
        I told the students not to use Pandas.
        :return: String representation of the table.
        """
        result = "Table name: {}, File name: {}, No of rows: {}, Key columns: {}"

        if self.rows is None:
            l = 0
        else:
            l = len(self.rows)

        # If there is no file containing the data, the table is derived.
        if self.table_file is None:
            self.table_name = "DERIVED"

        result = result.format(self.table_name, self.table_file, l, self.key_columns)

        tmp = list(self.rows.values())
        l = len(tmp)

        # If there are rows, print them.
        if l > 0:

            # If there are a lot of rows, just print the first few and last few.
            n = min(l, CSVTable.max_rows_to_print)
            if n < CSVTable.max_rows_to_print:
                first_n = n
                second_n = 0
            else:
                first_n = int(CSVTable.max_rows_to_print/2)
                second_n = l - first_n

            if first_n == 0:
                first_n = 1

            result += "\n"

            # Start by printing the column values at the top of the "table."
            result += self.__format_odict(self.headers)
            result += "\n"

            # Print the first few.
            for i in range(0, first_n):
                temp_r = list(tmp[i].values())
                result += self.__format_odict(temp_r) + "\n"

            # Print the last few.
            if second_n > 0:

                # Print "... ... ..." between the last few and the first few.
                middle_row = ["..."] * len(self.headers)

                result += self.__format_odict(middle_row) + "\n"
                for i in range(l-1, second_n-1, -1):
                    temp_r = list(tmp[i].values())
                    result += self.__format_odict(temp_r) + "\n"
        else:
            result += "No rows"

        return result

    # Returns True if the set of primary keys associated with table is valid.
    # Currently only checks that they keys are a subset of columns.
    def __primary_keys_valid(self):
        keys = set(self.key_columns)
        cols = set(self.headers)

        if not keys.issubset(cols):
            return False
        else:
            return True

    # Returns a list of the rows. Does not return the auto-increment ID key.
    # A more complete solution would be to make this class support slices and iteration but sometimes
    # you have to stop the madness.
    def get_row_list(self):
        result = None
        if self.rows:
            result = self.rows.values()
            return list(result)
        else:
            return None

    # Load from a file and creates the table and data.
    def load(self):

        fn = None
        try:
            self.derived = False                    # Reading a file means table is not derived.
            fn = self.__get_file_name()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    if self.headers is None:        # Just sets the header if not set.
                        self.headers = r.keys()     # The keys for any row,  contain column headers.

                        if not self.__primary_keys_valid():  # The columns in the file do not contain the named keys.
                            raise DataTableExceptions.DataTableException(-1,
                                         "Mismatch between primary key fields and columns in the file.")

                    # Auto-increment the row ID and add to dictionary.
                    self.next_row_id += 1
                    self.rows[self.next_row_id] = r     # Add the loaded dict to the dict of rows.

        except IOError as e:
            print("Got an I/O error = ", e)
            # In case I started to read, reset incomplete information.
            self.rows = None
            self.headers = None
            raise DataTableExceptions(-2, "Could not read file = ", fn)

    def save(self):
        """
        Writes the data back to the file.
        :return: None
        """
        fn = self.__get_file_name()
        try:
            with open(fn, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.headers)
                writer.writeheader()
                for r in self.rows.values():       # Need to convert to a list without the generated IDs.
                    writer.writerow(r)
                csvfile.close()
        except Exception as e:
            raise DataTableExceptions.DataTableException(-3,
                         "Could not write data. Original exception was ", e)

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.

        I did it this way because delete and finds both need to compare rows to templates.
        """

        # Basically, this means there is no where clause. A row always matches the None where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())            # Get the column names from the template.
            for n in c_names:                   # For every column in the rows that is in the key
                if row[n] != t[n]:              # The column does not match the template.
                    return False
            else:
                return True
        except Exception as e:
            raise(DataTableExceptions(-4, "Some kind of problem with keys/column names"))

    def project(self, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:              # If there is not project clause, return the base table
                return self                 # Should really return a new, identical table but am lazy.
            else:
                # Derived tables do not have names unless you alias/rename them.
                # Just generate a name for the new table.
                result = CSVTable("PROJECT_" + self.table_name, None, None)
                result.derived = False      # Temporarily set derived to false to enable inserts.
                result.headers = fields

                for k,v in self.rows.items():             # For every row in the table.
                    tmp = {}                    # Not sure why I am using range.
                    for f in fields:         # Make a new row with just the requested columns/fields.
                        new_v = v[f]
                        tmp[f]=new_v
                    else:
                        result.insert(tmp)                  # Insert into new table when done.

                result.derived = True           # Completed table is derived.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-5, "Invalid field in project")

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-6, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.rows is not None:

            invc = self.__col_list_valid(t)
            if invc != True and len(invc) > 0:
                raise DataTableExceptions.DataTableException(-7, "Invalid columns in template.")

            # Determine if we can use an index. If we can, access via the index.
            index_name = self.__get_index_name(t)
            if index_name is not None:
                return self.__get_by_index(t, index_name, fields)

            # Continue with scan based selection.


            # Derived tables do not have names. Make the name SELECTED_ plus the base table name.
            # There are not keys or columns.
            result = CSVTable('SELECTED_' + self.table_name, None, None)
            result.derived = False              # Temporarily set derived to false to allow inserts.

            # Add the rows that match the template to the newly created table.
            for k in self.rows:
                r = self.rows[k]
                if self.matches_template(r, t):
                    result.insert(r)

            # Apply project if there are project fields.
            result = result.project(fields)

            # If there ARE result rows, the keys in a dictionary  of the first row define the columns.
            if result.rows and result.headers is None:
                if len(result.rows) > 0:
                    for k in result.rows:
                        result.headers = list(result.rows[k].keys())
                        break
            result.derived = True
        else:
            result = None

        return result

    # Given a row, determine the template for a primary key based on key_columns and values.
    def get_key(self, r):

        if self.key_columns is None:
            return None

        result = {}

        try:
            # Look at every key in the list of key columns.
            for k in self.key_columns:
                # Get the value for the key column from the input row and add to
                # template that we will return.
                result[k] = r[k]

                # This is technically not correct but is in the code to handle possible
                # empty fields for columns from the Lahman 2017 tables.
                if result[k] == "":
                    raise ValueError("Key field " + k + " is empty.")
        except KeyError as ke:
            raise DataTableExceptions.DataTableException(-8, "Key is missing attribute " + str(ke))

        return result

    def insert(self, r):
        """
        Inserts a row into the table.
        :param r: A row to insert into the table.
        :return: None
        """

        try:
            # Cannot insert into derived tables.
            if self.derived:
                raise DataTableExceptions.DataTableException(-10,
                                     "Cannot modify a derived table.")

            if self.rows is None:
                self.rows = {}

            keys = r.keys()

            # If there are no defined columns. The first insert defines the columns.
            if self.headers is None:
                self.next_row_id += 1
                self.rows[self.next_row_id] = r
                self.headers = keys
            else:
                # Are there any invalid columns?
                invc = self.__col_list_valid(r)

                if invc != True and len(invc) > 0:
                    raise DataTableExceptions.DataTableException(-11, "Invalid columns " + str(invc))
                else:
                    pk = self.get_key(r)                            # Form a template for the primary key.
                    if pk is not None:
                        for k in pk.keys():                         # This checking should be separate function.
                            if pk[k] is None:
                                raise DataTableExceptions.DataTableException(-12, "Null primary key column")

                        # The key is valid. Now determine if there is an entry with this key.
                        t = self.find_by_template(pk)

                        # Does the result have rows, and the length is not empty.
                        if t.rows is not None:
                            rows = t.rows
                            keys = list(t.rows.keys())
                            l = len(list(keys))
                            print("l = ", l)
                            print("")
                            if l > 0:
                                print("Hello")
                                raise DataTableExceptions.DataTableException(-12, "Duplicate primary key")
                            else:
                                print("Adding")
                                # Add to dictionary using auto-increment ID.
                                self.next_row_id += 1
                                self.rows[self.next_row_id] = r
                    else:
                        self.next_row_id += 1
                        self.rows[self.next_row_id] = r

                # Do not automatically index the derived table.
                # User must explicitly call create index.
        except Exception as e:
            error = str(e)
            raise DataTableExceptions.DataTableException(-501,"Unknown error in insert(). Original e = " + error)

    def delete(self, t):
        """
        Deletes all rows that match a template.
        :param t:
        :return: None
        """

        if self.derived:
            raise DataTableExceptions.DataTableException(-20,
                                 "Cannot modify a derived table.")

        try:
            new_rows = {}

            # I make a new list with the rows that should not be deleted.
            # Deleting elements in a list while iterating through the list freaks me out.
            for k,v in self.rows.items():
                if not self.matches_template(v, t):
                   new_rows[k] = v
            else:
                self.rows = new_rows
        except Exception as e:
            raise DataTableExceptions(-31,"Deleted failed. Original exception = " + e)

    def find_by_primary_key(self, key_values, fields=None):
        template = dict(zip(self.key_columns,key_values))
        #print("template = ", template)
        result = self.find_by_template(template, fields)
        if result.rows is not None:
            return result.rows[1]           # There will be a single row.
        else:
            return None

    def create_index(self, columns):
        """
        Creates a new index for the table. Columns is a list of column names to form the index.
        The column does not need to be unique.
        :param columns: Column name.
        :return: None. Creates the index on the table.
        """

        # Raise an exception if there is
        l = self.__col_list_valid(columns)
        if  l != True:
            raise DataTableExceptions.DataTableException(-501, "Invalid columns in index definition = " +
                                                         str(l))

        if self.indexes is None:
            self.indexes = {}

        # Index name is columns separated by "_". Would be bad if there were "_" in column names.
        index_name = "_".join(columns)
        idx = self.indexes.get(index_name, None)
        if idx is not None:
            raise DataTableExceptions.DataTableException(-502, "Duplicate index definition." +
                                                         str(l))

        # Create the place to hold the index information.
        self.indexes[index_name] = {}
        index = self.indexes[index_name]

        # Put every row in the index. The entry is of the form {index key value: row id}
        for (k, r) in self.rows.items():

            # Get the index value from the current row.
            key = self.__get_index_values(r, index_name)

            # Find the "bucket," which is the list of IDs matching the index value.
            bucket = index.get(key, None)
            if bucket is None:
                bucket = {}
                index[key] = bucket
            bucket[k] = r

    # Find a row by index. Called by find_by_template, which does initial error checking.
    def __get_by_index(self, template, index_name, fields=None):
        idx = self.indexes[index_name]                          # Get the index using name.
        key = self.__get_index_values(template, index_name)     # Compute index key from row columns.
        result = idx.get(key, None)                             # Get matching rows from index.
        name = "SELECT_" + self.table_name                      # This is technically a select
        result = self.table_from_rows(name, None, result)       # Create table from rows found.
        result = result = result.project(fields)                # Perform PROJECT on fields.

        return result

    # Create a new table from a list of rows.
    @staticmethod
    def table_from_rows(name, keys, rows):
        result = CSVTable(name, None, None)                     # Create tge table.
        if rows is None:                                        # Add a copy of rows if it exists.
            return result
        result.rows = copy.copy(rows)
        keys = result.rows.keys()                               # Get the keys (row IDs)
        for k in keys:
            r1 = result.rows[k]                                 # Just get a row to compute headers.
            break
        result.headers = list(r1.keys())
        return result












