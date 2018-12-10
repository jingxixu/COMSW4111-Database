create (f:Fan { last_name: "Ferguson", first_name: "Donald", uni: "dff9"}) return f
CREATE CONSTRAINT ON (f:Fan) ASSERT f.uni IS UNIQUE
CREATE CONSTRAINT ON (p:Player) ASSERT p.player_id IS UNIQUE