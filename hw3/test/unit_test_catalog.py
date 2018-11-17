import src.CSVCatalog as CSVCatalog

import time
import json

def cleanup():
    """
    Deletes previously created information to enable re-running tests.
    :return: None
    """
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("teams")

def print_start(test_name, info):
    print_test_separator("Starting {}".format(test_name))
    print_info(info)

def print_end(test_name, expect):
    print_expect(expect)
    print_test_separator("Complete {}".format(test_name))


def print_test_separator(msg):
    print("\n")
    lot_of_stars = 20*'*'
    print(lot_of_stars, '  ', msg, '  ', lot_of_stars)
    print("\n")

def print_expect(s):
    print("\n##### Expected result:{}".format(s))

def print_info(s):
    print("##### {}\n".format(s))

def test_create_table_1():
    """
    Simple create of table definition. No columns or indexes.
    :return:
    """
    cleanup()
    print_test_separator("Starting test_create_table_1")
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table(
        "people",
        "data/People.csv")
    print("People table", json.dumps(t.describe_table()))
    print_test_separator("Complete test_create_table_1")


def test_create_table_2_fail():
    """
    Creates a table, and then attempts to create a table with the same name. Second create should fail.
    :return:
    """
    print_test_separator("Starting test_create_table_2_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()
    t = cat.create_table("people",
     "data/People.csv")

    try:
        t = cat.create_table("people", "data/People.csv")
    except Exception as e:
        print("Second created failed with e = ", e)
        print("Second create should fail.")
        print_test_separator("Successful end for  test_create_table_2_fail")
        return

    print_test_separator("INCORRECT end for  test_create_table_2_fail")



def test_create_table_3():
    """
    Creates a table that includes several column definitions.
    :return:
    """
    print_test_separator("Starting test_create_table_3")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))

    t = cat.create_table("people", "data/People.csv", cds)
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_3")


def test_create_table_3_fail():
    """
    Creates a table that includes several column definitions. This test should fail because one of the defined
    columns is not in the underlying CSV file.
    :return:
    """
    print_test_separator("Starting test_create_table_3_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))
    cds.append(CSVCatalog.ColumnDefinition("canary"))

    try:
        t = cat.create_table("people", "data/People.csv", cds)
        print_test_separator("FAILURE test_create_table_3")
        print("People table", json.dumps(t.describe_table(), indent=2))
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("Complete test_create_table_3_fail successfully")

def test_create_table_4():
    """
        Creates a table that includes several column definitions.
        :return:
        """
    print_test_separator("Starting test_create_table_4")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))


    t = cat.create_table("batting", "data/Batting.csv", cds)

    t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
    print("People table", json.dumps(t.describe_table(), indent=2))
    print_test_separator("Complete test_create_table_4")

def test_create_table_4_fail():
    """
    Creates a table that includes several column definitions and a primary key.
    The primary key references an undefined column, which is an error.

    NOTE: You should check for other errors. You do not need to check in the CSV file for uniqueness but
    should test other possible failures.
    :return:
    """
    print_test_separator("Starting test_create_table_4_fail")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))


    t = cat.create_table("batting", "data/Batting.csv", cds)
    try:
        t.define_primary_key(['playerID', 'teamID', 'yearID', 'HR'])
        print("Batting table", json.dumps(t.describe_table(), indent=2))
        print_test_separator("FAILURES test_create_table_4_fail")
    except Exception as e:
        print("Exception e = ", e)
        print_test_separator("SUCCESS test_create_table_4_fail should fail.")


def test_create_table_5_prep():
    """
    Creates a table that includes several column definitions and a primary key.
    :return:
    """
    print_test_separator("Starting test_create_table_5_prep")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("H", column_type="number", not_null=False))
    cds.append(CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False))

    t = cat.create_table("batting", "data/Batting.csv", cds)

    t.define_primary_key(['playerID', 'teamID', 'yearID', 'stint'])
    print("Batting table", json.dumps(t.describe_table(), indent=2))

    print_test_separator("Completed test_create_table_5_prep")

def test_create_table_5():
    """
    Modifies a preexisting/precreated table definition.

    NOTE: this depends on test_create_table_5_prep.
    :return:
    """
    print_test_separator("Starting test_create_table_5")

    # DO NOT CALL CLEANUP. Want to access preexisting table.
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    print("Initial status of table = \n", json.dumps(t.describe_table(), indent=2))
    t.add_column_definition(CSVCatalog.ColumnDefinition("HR", "number"))
    t.add_column_definition(CSVCatalog.ColumnDefinition("G", "number"))
    t.define_index("team_year_idx", ['teamID', 'yearID'], "INDEX")
    print("Modified status of table = \n", json.dumps(t.describe_table(), indent=2))
    print("\n##### Expected result: pass")
    print_test_separator("Complete test_create_table_5")

def test_to_json():
    try:
        print("\n*********** Testing to JSON. *******************\n")
        cat = CSVCatalog.CSVCatalog()
        cat.drop_table("teams")

        cds = []
        cds.append(CSVCatalog.ColumnDefinition('teamID', 'text', True))
        cds.append(CSVCatalog.ColumnDefinition('yearID', 'text', True))
        cds.append(CSVCatalog.ColumnDefinition('W', column_type='number'))

        tbl = CSVCatalog.TableDefinition(
            "teams",
            "data/Teams.csv",
            column_definitions=cds,
            cnx=cat.cnx) # maybe this is inproper?
        r = json.dumps(tbl.to_json(), indent=2)
        print("Teams definition = \n", r)
        with open("unit_tests_catalog_json.txt", "w") as result_file:
            result_file.write(r)

        print("\n\n")
    except Exception as e:
        print("My implementation throws a custom exception. You can print any meaningful error you want.")
        print("Could not create table. Exception = ", e)
        print("\n*********** Testing to JSON. *******************\n")

def my_test_1():
    """
    Test add_column_definition. Should pass.

    :return:
    """
    print_start("my_test_1", "Test add_column_definition. Should pass.")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("teamID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("yearID", column_type="text", not_null=True))
    cds.append(CSVCatalog.ColumnDefinition("stint", column_type="number", not_null=True))

    t = cat.create_table("batting", "data/Batting.csv", cds)
    print("Before adding another column:\n", json.dumps(t.describe_table(), indent=2))
    new_cd = CSVCatalog.ColumnDefinition("AB", column_type="number", not_null=False)
    t.add_column_definition(new_cd)
    print("\nAfter adding another column:\n", json.dumps(t.describe_table(), indent=2))
    print_end("my_test_1", "pass")

def my_test_4():
    """
    Try add_column_definition if the column already exists. This should fail.

    :return:
    """
    print_test_separator("Starting my_test_4")
    cleanup()
    cat = CSVCatalog.CSVCatalog()

    cds = []
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))

    t = cat.create_table("people", "data/People.csv", cds)
    print("People table", json.dumps(t.describe_table(), indent=2))
    print("\nTry to add a duplicate column playerID:")
    try:
        c = CSVCatalog.ColumnDefinition("playerID")
        t.add_column_definition(c)
    except Exception as e:
        print("Exception e = ", e)
    print("\n##### Expected result: fail")
    print_test_separator("Complete my_test_4")

def my_test_2():
    """
    Try loading a table which does not exist. Should fail.

    :return:
    """
    pass

def my_test_3():
    """
    Try passing multiple index definitions in table definition. Should pass.
    """
    print_test_separator("Starting my_test_3")
    print("##### Try passing multiple index definitions in table definition. Should pass.\n")
    cleanup()

    cat = CSVCatalog.CSVCatalog()
    cds =[]
    cds.append(CSVCatalog.ColumnDefinition("playerID", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameLast", "text", True))
    cds.append(CSVCatalog.ColumnDefinition("nameFirst", column_type="text"))

    ids = []
    ## TODO test this?
    # ids.append(CSVCatalog.IndexDefinition("PRIMARY", "PRIMARY", ["playerID"]))
    # ids.append(CSVCatalog.IndexDefinition("my_idx", "INDEX", ["playerID"]))
    ids.append(CSVCatalog.IndexDefinition("PRIMARY", "PRIMARY", ["playerID"]))
    ids.append(CSVCatalog.IndexDefinition("my_idx", "INDEX", ["nameLast", "nameFirst"]))
    t = cat.create_table("people", "data/People.csv", cds, ids)
    print("People table", json.dumps(t.describe_table(), indent=2))
    print("\n##### Expected result: pass")
    print_test_separator("Complete my_test_3")

if __name__ == "__main__":
    test_create_table_1()
    test_create_table_2_fail()
    test_create_table_3()
    test_create_table_3_fail()
    test_create_table_4()
    test_create_table_4_fail()
    # test_to_json() # do not worry about this yet
    test_create_table_5_prep()
    test_create_table_5()
    my_test_1()
    my_test_4()
    my_test_3()