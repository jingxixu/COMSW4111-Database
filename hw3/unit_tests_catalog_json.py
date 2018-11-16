from Homeworks.HW3.src  import CSVCatalog
import json


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
            "/Users/donaldferguson/Dropbox/ColumbiaCourse/Courses/Fall2018/W4111/Homeworks/HW3/Data/core/Teams.csv",
            column_definitions=cds)
        r = json.dumps(tbl.to_json(), indent=2)
        print("Teams definition = \n", r)
        with open("unit_tests_catalog_json.txt", "w") as result_file:
            result_file.write(r)

        print("\n\n")
    except Exception as e:
        print("My implementation throws a custom exception. You can print any meaningful error you want.")
        print("Could not create table. Exception = ", e)


test_to_json()