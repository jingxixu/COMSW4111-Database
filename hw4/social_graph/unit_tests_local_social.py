#from social_graph import fan_comment
#from utils import utils as ut

import social_graph.fan_comment_template as fct
import utils.utils as ut


import json
import py2neo
import pymysql

cnx = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuser',
                             db='lahman2017raw_new',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

fg = fct.FanGraph(auth=('neo4j','xixi1995'),
                              host="localhost",
                              port=7687,
                              secure=False)

ut.set_debug_mode(True)


def load_players():

    q = "SELECT playerID, nameLast, nameFirst FROM People where  " + \
        "exists (select * from appearances where appearances.playerID = people.playerID and yearID >= 2017)"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        r = curs.fetchone()
        if r is not None:
            p = fg.create_player(player_id=r['playerID'], last_name=r['nameLast'], first_name=r['nameFirst'])
            print("Created player = ", p)

    print("Loaded ", cnt, "records.")


def load_teams():

    q = "SELECT teamid, name from teams where yearid >= 2017"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        # TODO bug, move it to the end
        r = curs.fetchone()
        if r is not None:
            p = fg.create_team(team_id=r['teamid'], team_name=r['name'])
            print("Created team = ", p)

    print("Loaded ", cnt, "records.")


def load_appearances():

    q = "SELECT distinct playerid, teamid, g_all as games from appearances where yearid >= 2017"

    curs = cnx.cursor()
    curs.execute(q)

    r = curs.fetchone()
    cnt = 0
    while r is not None:
        print(r)
        cnt += 1
        r = curs.fetchone()
        if r is not None:
            try:
                p = fg.create_appearance(team_id=r['teamid'], player_id=r['playerid'])
                print("Created appearances = ", p)
            except Exception as e:
                print("Could not create.")

    print("Loaded ", cnt, "records.")


def load_follows_fans():
    fg.create_fan(uni="js1", last_name="Smith", first_name="John")
    fg.create_fan(uni="ja1", last_name="Adams", first_name="John")
    fg.create_fan(uni="tj1", last_name="Jefferson", first_name="Thomas")
    fg.create_fan(uni="gw1", last_name="Washing", first_name="George")
    fg.create_fan(uni="jm1", last_name="Monroe", first_name="James")
    fg.create_fan(uni="al1", last_name="Lincoln", first_name="Abraham")

    fg.create_follows(follower="gw1", followed="js1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="ja1", followed="gw1")
    fg.create_follows(follower="jm1", followed="gw1")
    fg.create_follows(follower="tj1", followed="gw1")
    fg.create_follows(follower="al1", followed="jm1")


def create_supports():

    fg.create_supports("gw1", "WAS")
    fg.create_supports("ja1", "BOS")
    fg.create_supports("tj1", "WAS")
    fg.create_supports("jm1", "NYA")
    fg.create_supports("al1", "CHA")
    fg.create_supports("al1", "CHN")



# load_players()
# load_teams()
# load_appearances()
# load_follows_fans()
# create_supports()


def test_create_comment1():
    t = fg.get_team('BOS')
    f = fg.get_fan('al1')
    p = fg.get_player('pedrodu01')
    c = "Awesome"
    pid = p['player_id']
    tid = t['team_id']
    fid = f['uni']
    c = fg.create_comment(fid, c, tid, pid)
    print(c)

def test_create_sub_comment2():
    # This depends on test_create_comment1
    c = fg.get_comment("72d5ac97-16dc-477e-aa63-b8ce57cd6974")
    m = "Totally agree!"
    r = fg.create_sub_comment('al1', "72d5ac97-16dc-477e-aa63-b8ce57cd6974", m)
    print(r)

def test_create_comment3():
    t = fg.get_team('PHI')
    f = fg.get_fan('tj1')
    p = fg.get_player('garcilu03')
    c = "Please give me A plus!"
    pid = p['player_id']
    tid = t['team_id']
    fid = f['uni']
    c = fg.create_comment(fid, c, tid, pid)
    print(c)

def test_create_sub_comment4():
    # This depends on test_create_comment3
    c = fg.get_comment("d0b98bc5-fc6b-4d1c-8d82-ea201e348494")
    m = "Yeah Baby Yeah!"
    r = fg.create_sub_comment('gw1', "d0b98bc5-fc6b-4d1c-8d82-ea201e348494", m)
    print(r)

def test_create_sub_comment5():
    # This depends on test_create_comment3
    c = fg.get_comment("d0b98bc5-fc6b-4d1c-8d82-ea201e348494")
    m = "Also give me A plus!"
    r = fg.create_sub_comment('jm1', "d0b98bc5-fc6b-4d1c-8d82-ea201e348494", m)
    print(r)

def test_create_sub_comment6():
    # This depends on test_create_sub_comment4, a comment on sub comment!
    c = fg.get_comment("08337863-ac70-4424-a5b2-2116ad6daebd")
    m = "Why do you say that?"
    r = fg.create_sub_comment('tj1', "08337863-ac70-4424-a5b2-2116ad6daebd", m)
    print(r)

# test_create_comment1()
# test_create_sub_comment2()
# test_create_comment3()
# test_create_sub_comment4()
# test_create_sub_comment5()
test_create_sub_comment6()







