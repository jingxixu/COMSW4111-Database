use lahman2017raw_new;

-- select p.playerid, h.category, p.namefirst, p.namelast
-- from people as p inner join halloffame as h;


-- create view people_test as
-- select playerid, birthyear, birthmonth
-- from people

-- create view appearances_test as
-- select playerid, yearid, teamid from appearances
-- order by playerid;

-- select * from people_test p left join appearances_test a on p.playerid = a.playerid where p.playerid = 'aardsda01';


-- Q1 --------------------------------------------------------------------
-- select a.c from (select playerid, category, yearid from halloffame group by playerid, category, yearid) as a where a.c > 1;

-- select count(*), playerid, category, yearid from halloffame
-- group by playerid, category, yearid  order by playerid;

-- select p.playerid, h.category, p.namefirst, p.namelast
-- from people as p 
-- inner join 
-- (select playerid, category from halloffame group by playerid, category) as h
-- on p.playerid = h.playerid
-- where p.playerid not in (select playerid from managers order by playerid)
-- and p.playerid not in (select playerid from appearances order by playerid)
-- order by p.playerid;



-- Q2 --------------------------------------------------------------------
-- select p.playerid, p.nameLast, p.nameFirst, stats.total_hits, stats.total_abs, stats.career_average, stats.total_hrs, stats.total_wins from people as p
-- inner join
-- (select a.playerid, b.total_hits, b.total_abs, b.career_average, b.total_hrs, a.total_wins from (select playerid, sum(W) as total_wins from pitching group by playerid having sum(W) > 350) a
-- left join (select playerid, sum(h) as total_hits, sum(ab) as total_abs, sum(hr) as total_hrs, sum(h)/sum(ab) as career_average from batting group by playerid having sum(h)/sum(ab) > 0.340 and sum(hr) > 500) as b
-- on a.playerid = b.playerid 
-- union
-- select b.playerid, b.total_hits, b.total_abs, b.career_average, b.total_hrs, a.total_wins from (select playerid, sum(W) as total_wins from pitching group by playerid having sum(W) > 350) as a 
-- right join (select playerid, sum(h) as total_hits, sum(ab) as total_abs, sum(hr) as total_hrs, sum(h)/sum(ab) as career_average from batting group by playerid having sum(h)/sum(ab) > 0.340 and sum(hr) > 500) as b
-- on a.playerid = b.playerid) as stats
-- on p.playerid = stats.playerid
-- order by p.playerid;



-- Q8 --------------------------------------------------------------------

-- drop table if exists AllStarFullFixed;
-- CREATE TABLE `AllStarFullFixed` (
--   `playerID` varchar(12) NOT NULL,
--   `yearID` char(4) NOT NULL,
--   `gameNum` int(11) DEFAULT NULL,
--   `gameID` varchar(32) NOT NULL,
--   `teamID` varchar(4) NOT NULL,
--   `lgID` enum('NL','AL') NOT NULL,
--   `GP` varchar(4) DEFAULT NULL,
--   `startingPos` varchar(4) DEFAULT NULL,
--   PRIMARY KEY (`playerID`,`gameID`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- drop trigger if exists ins_pos_year;
-- delimiter //
-- create trigger ins_pos_year before insert on AllStarFullFixed
-- for each row 
-- 	begin
-- 		declare x varchar(4);
--         declare flag int;
--         set x = 1;
--         set flag = 0;
--         while x <= 10 do
-- 			if x = new.startingPos then
-- 				set flag = 1;
-- 			end if;
--             set x = x+1;
-- 		end while;
--         if not flag = 1 then
-- 			SIGNAL SQLSTATE '45001'
-- 				SET MESSAGE_TEXT = 'Jingxi Xu (jx2324): invalid startingPos inserted';
-- 		end if;
--         if new.yearID<1933 or new.yearID>2018 then
-- 			SIGNAL SQLSTATE '45001'
-- 				SET MESSAGE_TEXT = 'Jingxi Xu (jx2324) invalid yearID inserted';
-- 		end if;
-- 	end //
-- delimiter ;

-- insert into lahman2017raw_new.AllStarFullFixed values('aaronha01', '1960', 1, 'foo', 'ML1', 'NL', '1', 'a');
-- insert into lahman2017raw_new.AllStarFullFixed values('aaronha01', '2020', 1, 'foo', 'ML1', 'NL', '1', '10')



-- Q9 --------------------------------------------------------------------
-- drop procedure if exists update_batting_h_ab;
-- delimiter //
-- create procedure update_batting_h_ab(in playerid_in varchar(32), in teamid_in varchar(32), 
-- 	in yearid_in varchar(32), in stint_in varchar(32), in h_in int, in ab_in int)
-- begin
-- 	declare th int;
--     declare tab int;
--     declare a float;
-- 	update batting set h=h_in, ab=ab_in where playerid=playerid_in and teamid=teamid_in and yearid=yearid_in and stint=stint_in;
-- 	select sum(h), sum(ab), sum(h)/sum(ab) into th, tab, a from batting
-- 		where playerid=playerid_in and teamid=teamid_in and yearid=yearid_in
-- 		group by playerid, teamid, yearid;
-- 	update copy_tables_are_awesome set total_hits=th, total_abs=tab, average=a
-- 		where playerid=playerid_in and teamid=teamid_in and yearid=yearid_in;
-- end //
-- delimiter ;


-- Q7 --------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS `HallOfFame` (
--   `playerID` varchar(12) NOT NULL,
--   `yearid` varchar(4) NOT NULL,
--   `votedBy` varchar(32) NOT NULL,
--   `ballots`  INT DEFAULT NULL,
--   `needed` INT DEFAULT NULL,
--   `votes` INT DEFAULT NULL,
--   `inducted` ENUM ('Y', 'N') DEFAULT NULL,
--   `category` ENUM ('Player', 'Manager') DEFAULT NULL,
--   `needed_note` varchar(32) DEFAULT NULL,
--   PRIMARY KEY (`playerID`,`yearid`,`votedBy`),
--   CONSTRAINT `people` FOREIGN KEY (`playerID`) REFERENCES `people` (`playerid`)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Q3 --------------------------------------------------------------------
-- drop view if exists batting_summary;
-- create view batting_summary as
-- select playerid, teamid, yearid, sum(h) as hits, sum(ab) as at_bats, sum(hr) as home_runs, sum(rbi) as RBIs from batting
-- group by playerid, teamid, yearid;

-- drop view if exists pitching_summary;
-- create view pitching_summary as
-- select playerid, teamid, yearid, sum(w) as wins, sum(l) as loses, sum(g) as games, sum(IPOuts) as outs_pitched from pitching
-- group by playerid, teamid, yearid;

-- drop view if exists people_summary;
-- create view people_summary as
-- select playerid, nameLast, nameFirst, throws, bats from people;

-- drop view if exists appearances_summary;
-- create view appearances_summary as
-- select playerid, teamid, yearid, g_all, g_batting, g_defense, g_p as g_pitching from appearances
-- group by playerid, teamid, yearid;

-- drop view if exists fielding_summary;
-- create view fielding_summary as
-- select playerid, teamid, yearid, sum(po) as put_outs, sum(a) as assists, sum(e) as errors from fielding
-- group by playerid, teamid, yearid;

select * from halloffame;








