import json
import requests

def test2():
    r = requests.get('http://127.0.0.1:5000/api/people?nameLast=Williams&nameFirst=woody&fields=nameFirst,nameLast,throws,playerid')
    print("People with names like the greatest hitter of all time are: ", json.dumps(r.json(), indent=2))

# compound primary keys
def test3():
    r = requests.get('http://localhost:5000/api/batting/willite01_1960_1_BOS?fields=playerID, G,AB,H')
    print(json.dumps(r.json(), indent=2))

def test4():
    r = requests.get('http://localhost:5000/api/people/willite01')
    print(json.dumps(r.json(), indent=2))

def test5():
    # might not exist
    r = requests.get('http://localhost:5000/api/people/jx2324')
    print(json.dumps(r.json(), indent=2))

def test6():
    r = requests.get('http://127.0.0.1:5000/api/batting/willite01_1960_1_BOS/people')
    print(json.dumps(r.json(), indent=2))

def test7():
    r = requests.get('http://127.0.0.1:5000/api/people/willite01/batting')
    print(json.dumps(r.json(), indent=2))

def test8():
    r = requests.get('http://127.0.0.1:5000/api/appearances/BOS_willite01_1960/people?fields=nameLast,nameFirst,playerID,birthCity,throws')
    print(json.dumps(r.json(), indent=2))

def test9():
    r = requests.get('http://127.0.0.1:5000/api/appearances?teamid=ana&lgid=al&fields=teamid,lgid,yearid,playerid,GS&offset=103&limit=2')
    print(json.dumps(r.json(), indent=2))

# custom teammates
def test10():
    r = requests.get('http://127.0.0.1:5000/api/teammates/willite01?offset=199&limit=10')
    print(json.dumps(r.json(), indent=2))

# custom career_stas
def test11():
    r = requests.get('http://127.0.0.1:5000/api/people/willite01/career_stats')
    print(json.dumps(r.json(), indent=2))

# custom roster
def test14():
    r = requests.get('http://127.0.0.1:5000/api/roster?teamid=BOS&yearid=2004')
    print(json.dumps(r.json(), indent=2))

def test12():
    r = requests.get('http://localhost:5000/api/people?nameLast=Smith&fields=nameLast,playerID')
    print(json.dumps(r.json(), indent=2))

def test13():
    r = requests.get('http://127.0.0.1:5000/api/teammates/willite01')
    print(json.dumps(r.json(), indent=2))

if __name__ == "__main__":
    test14()