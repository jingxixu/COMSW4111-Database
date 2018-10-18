### PRIMARY key
# appearances
ALTER TABLE `lahman2017raw`.`Appearances`
CHANGE COLUMN `yearID` `yearID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `teamID` `teamID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `playerID` `playerID` VARCHAR(32) NOT NULL ,
ADD PRIMARY KEY (`teamID`, `playerID`, `yearID`);

# batting
ALTER TABLE `lahman2017raw`.`Batting`
CHANGE COLUMN `playerID` `playerID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `yearID` `yearID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `stint` `stint` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `teamID` `teamID` VARCHAR(32) NOT NULL ,
ADD PRIMARY KEY (`playerID`, `yearID`, `stint`, `teamID`);

# people
ALTER TABLE `lahman2017raw`.`People`
CHANGE COLUMN `playerID` `playerID` VARCHAR(32) NOT NULL ,
ADD PRIMARY KEY (`playerID`);

# teams
ALTER TABLE `lahman2017raw`.`Teams`
CHANGE COLUMN `yearID` `yearID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `teamID` `teamID` VARCHAR(32) NOT NULL ,
ADD PRIMARY KEY (`yearID`, `teamID`);

# fielding
ALTER TABLE `lahman2017raw`.`Fielding`
CHANGE COLUMN `playerID` `playerID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `yearID` `yearID` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `stint` `stint` VARCHAR(32) NOT NULL ,
CHANGE COLUMN `POS` `POS` VARCHAR(32) NOT NULL ,
ADD PRIMARY KEY (`playerID`, `yearID`, `POS`, `stint`);

### FOREIGN key
# appearances
ALTER TABLE `lahman2017raw`.`Appearances`
ADD CONSTRAINT `a2t`
  FOREIGN KEY (`yearID` , `teamID`)
  REFERENCES `lahman2017raw`.`Teams` (`yearID` , `teamID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;
  
# appearances
ALTER TABLE `lahman2017raw`.`Appearances`
ADD CONSTRAINT `a2p`
  FOREIGN KEY (`playerID`)
  REFERENCES `lahman2017raw`.`People` (`playerID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;
  
# batting
ALTER TABLE `lahman2017raw`.`Batting`
ADD CONSTRAINT `fpeople`
  FOREIGN KEY (`playerID`)
  REFERENCES `lahman2017raw`.`People` (`playerID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;
  
# fielding
ALTER TABLE `lahman2017raw`.`Fielding`
ADD CONSTRAINT `f2p`
  FOREIGN KEY (`playerID`)
  REFERENCES `lahman2017raw`.`People` (`playerID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;
  
### View
# roster
drop view if exists roster;
create view roster as
select p.namelast, p.namefirst, b.playerid, b.teamid, b.yearid, a.g_all, sum(b.h) as hit, sum(b.ab) as ABs, sum(f2.A) as Assists, sum(f2.E) as errors
from batting as b inner join appearances as a
on b.playerid = a.playerid and b.teamid = a.teamid and b.yearid = a.yearid
inner join 
(select f.playerid, f.yearid, f.teamid, sum(f.A) as A, sum(f.E) as E
from fielding as f
group by f.playerid, f.teamid, f.yearid) as f2
on b.playerid = f2.playerid and b.teamid = f2.teamid and b.yearid =f2.yearid
inner join people as p on b.playerid = p.playerid
group by b.playerid, b.teamid, b.yearid;