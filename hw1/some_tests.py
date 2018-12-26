import CSVDataTable
import json
import time


def test1():
    print("Test 1")
    csvt = CSVDataTable.CSVTable("People", "People.csv", ["playerID"])
    csvt.load()
    print("Table = ", csvt)


def test2(t, fields=None):
    print("test 2: ")
    print("Testing template = ", t)
    if fields is not None:
        print("Field list = ", fields)

    csvt = CSVDataTable.CSVTable("People", "People.csv", ["playerID"])
    csvt.load()

    r = csvt.find_by_template(t, fields)
    print(r)

def test3():

    csvt = CSVDataTable.CSVTable("Batting", "Batting.csv", ["playerID", "teamID", "yearID", "stint"])
    csvt.load()
    t = csvt.find_by_primary_key(['willite01', 'BOS', '1960', '1'],
                                 fields=["playerID", "H", "AB", "HR"])
    print(t)


def testA():
    csvt = CSVDataTable.CSVTable("People", "PeopleSmall.csv", ["playerID"])
    csvt.load()
    csvt.insert({"playerID": "DFF1",
                 "nameLast": "Ferguson", "nameFirst": "Donald"})
    csvt.save()

def testB():
    csvt = CSVDataTable.CSVTable("People", "PeopleSmall.csv", ["playerID"])
    csvt.load()
    csvt.delete({"playerID": "DFF1"}),
    csvt.save()

def testC():
    csvt = CSVDataTable.CSVTable("People", "PeopleSmall.csv", ["playerID"])
    csvt.load()
    csvt.insert({"cat": "DFF1",
                 "nameLast": "Ferguson", "nameFirst": "Donald"})
    csvt.save()


def testD():
    try:
        # Create the DataTable and load it.
        csvt = CSVDataTable.CSVTable("People", "People.csv", ["playerID"])
        csvt.load()

        # Print some information about the table.
        print("Initial table is:")
        print(csvt)

        # Define the template and fields to select.
        t2 = {"nameLast": "Williams", "throws": "R"}
        fields2 = ['nameLast', 'nameFirst', 'birthCountry', 'throws', 'bats']
        print("Testing template ", t2, " on table", "People")
        print("With field list = ", fields2)

        # Run and print query. Notice the result is also a Data Table
        result = csvt.find_by_template(t2, fields2)
        print("\n\nQuery result is ")
        print(result)

        print("\n\nNow querying the derived table.")
        t3 = { "nameFirst": "Ted"}
        print("New template is ", str(t3))
        result2 = result.find_by_template(t3, None)
        print("\n\nQuery2 result is ")
        print(result2)
    except Exception as e:
        print("Got exception = ", str(e))


def test11():
    csvt = CSVDataTable.CSVTable("People", "People.csv", ["playerID"])
    csvt.load()

    t1 = {"iq": "9", "nameLast": "Williams"}
    result = csvt.find_by_template(t1, None)


def test12(no):
    print("Starting a lot of lookups.")
    csvt = CSVDataTable.CSVTable("People", "People.csv", ["playerID"])
    csvt.load()
    start_time = time.time()
    t = {"playerID": "willite01"}
    for i in range(0, no):
        r = csvt.find_by_template(t)
    end_time = time.time()
    print("Without an index, lookups = ", no, "Elapsed time = ", end_time-start_time)

    csvt.create_index(['playerID'])
    start_time = time.time()
    for i in range(0, no):
        r = csvt.find_by_template(t)
    end_time = time.time()
    print("WITH an index, lookups = ", no, "Elapsed time = ", end_time - start_time)




def run_tests():

    #test1()
    #test2 ({"nameLast": "Williams", "throws": "R"},
    #            fields=['playerID', 'nameLast', 'nameFirst', 'throws', 'birthCity'])
    #test3()

    #testA()
    #testB()
    #testC()
    #testD()
    test12(1000)

run_tests()