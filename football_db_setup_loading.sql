-- CREATION OF DATABASE
CREATE DATABASE football_relationalDB CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- CREATION OF USER ACCOUNTS
CREATE USER 'app_read'@'%' IDENTIFIED BY 'passwordread'; GRANT SELECT ON football_relationalDB.* TO 'app_read'@'%';

CREATE USER 'app_write'@'%' IDENTIFIED BY 'passwordwrite'; GRANT SELECT, INSERT, UPDATE, DELETE, EXECUTE ON football_relationalDB.* TO 'app_write'@'%';

CREATE USER 'app_admin'@'%' IDENTIFIED BY 'passwordadmin'; GRANT ALL PRIVILEGES ON football_relationalDB.* TO 'app_admin'@'%'; FLUSH PRIVILEGES;

-- CREATION OF 7 PRODUCTION TABLES
CREATE TABLE club (
  club_id INT PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  domestic_competition_id VARCHAR(8),
  squad_size INT,
  average_age DECIMAL(3,1),
  foreigners_number INT,
  foreigners_percentage DECIMAL(4,1),
  national_team_players INT,
  stadium_name VARCHAR(120),
  stadium_seats INT,
  net_transfer_record VARCHAR(16),
  last_season INT,
  CONSTRAINT uq_club_name_comp UNIQUE (name, domestic_competition_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE player (
  player_id INT PRIMARY KEY,
  first_name VARCHAR(80),
  last_name VARCHAR(80),
  name VARCHAR(120),
  position VARCHAR(40),
  sub_position VARCHAR(40),
  current_club_id INT,
  market_value_eur BIGINT,
  highest_market_value_eur BIGINT,
  last_season INT,
  CONSTRAINT fk_player_club FOREIGN KEY (current_club_id)
    REFERENCES club(club_id)
    ON DELETE SET NULL
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE player_bio (
  player_id INT PRIMARY KEY,
  height_in_cm INT,
  dob DATE,
  country_of_citizenship VARCHAR(80), 
  foot ENUM('left','right','both'),
  city_of_birth VARCHAR(80),
  country_of_birth VARCHAR(80),
  image_url VARCHAR(255),
  agent_name VARCHAR(120),
  contract_expiration_date DATE,
  CONSTRAINT fk_bio_player FOREIGN KEY (player_id)
    REFERENCES player(player_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE game (
  game_id INT PRIMARY KEY,
  competition_id VARCHAR(8),
  season VARCHAR(16),
  round VARCHAR(32),
  date DATE,
  home_club_id INT,
  away_club_id INT,
  home_club_goals INT CHECK (home_club_goals >= 0),
  away_club_goals INT CHECK (away_club_goals >= 0),
  home_club_position INT,
  away_club_position INT,
  home_club_manager_name VARCHAR(120),
  away_club_manager_name VARCHAR(120),
  stadium VARCHAR(120),
  attendance INT,
  referee VARCHAR(120),
  home_club_formation VARCHAR(32),
  away_club_formation VARCHAR(32),
  match_time TIME,
  competition_type VARCHAR(32),
  CONSTRAINT fk_game_home FOREIGN KEY (home_club_id)
    REFERENCES club(club_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_game_away FOREIGN KEY (away_club_id)
    REFERENCES club(club_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT uq_game_natural UNIQUE (competition_id, season, home_club_id, away_club_id, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE appearance (
  appearance_id VARCHAR(30) PRIMARY KEY,
  game_id INT,
  player_id INT,
  player_club_id INT,
  player_current_club_id INT,
  date DATE,
  yellow_cards INT CHECK (yellow_cards BETWEEN 0 AND 2),
  red_cards INT CHECK (red_cards BETWEEN 0 AND 1),
  goals INT DEFAULT 0 CHECK (goals >= 0),
  assists INT DEFAULT 0 CHECK (assists >= 0),
  minutes_played INT,
  CONSTRAINT fk_app_game FOREIGN KEY (game_id)
    REFERENCES game(game_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_app_player FOREIGN KEY (player_id)
    REFERENCES player(player_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_app_player_club FOREIGN KEY (player_club_id)
    REFERENCES club(club_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_app_player_curr_club FOREIGN KEY (player_current_club_id)
    REFERENCES club(club_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  INDEX ix_app_player_game (player_id, game_id),
  INDEX ix_app_game_player (game_id, player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE game_events (
  game_event_id VARCHAR(64) PRIMARY KEY NOT NULL,
  game_id INT NOT NULL,
  minute INT,
  type ENUM('Cards', 'Goals', 'Shootout', 'Substitutions') NOT NULL,
  club_id INT,
  player_id INT,
  description VARCHAR(128),
  player_in_id INT,
  player_assist_id INT,
  CONSTRAINT fk_event_game FOREIGN KEY (game_id)
    REFERENCES game(game_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_event_club FOREIGN KEY (club_id)
    REFERENCES club(club_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_event_player FOREIGN KEY (player_id)
    REFERENCES player(player_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_event_player_in FOREIGN KEY (player_in_id)
    REFERENCES player(player_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_event_player_assist FOREIGN KEY (player_assist_id)
    REFERENCES player(player_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  INDEX ix_event_game_minute (game_id, minute)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE transfer (
  transfer_id INT PRIMARY KEY,
  player_id INT NOT NULL,
  transfer_date DATE NOT NULL,
  transfer_season VARCHAR(16),
  from_club_id INT,
  to_club_id INT,
  transfer_fee BIGINT,
  market_value_in_eur BIGINT CHECK (market_value_in_eur >= 0),
  CONSTRAINT fk_tr_player FOREIGN KEY (player_id)
    REFERENCES player(player_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_tr_from FOREIGN KEY (from_club_id)
    REFERENCES club(club_id)
    ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_tr_to FOREIGN KEY (to_club_id)
    REFERENCES club(club_id)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  INDEX ix_tr_player_date (player_id, transfer_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE competition (
	competition_id VARCHAR(8),
    name VARCHAR(64),
    type VARCHAR(64),
    country_name VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET = utf8mb4;

-- CREATION OF STAGING TABLES

CREATE TABLE stg_club (
  club_id VARCHAR(120),
  name VARCHAR(120),
  domestic_competition_id VARCHAR(32),
  squad_size VARCHAR(32),
  average_age VARCHAR(32),
  foreigners_number VARCHAR(32),
  foreigners_percentage VARCHAR(32),
  national_team_players VARCHAR(32),
  stadium_name VARCHAR(120),
  stadium_seats VARCHAR(32),
  net_transfer_record VARCHAR(32),
  last_season VARCHAR(16)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\clubs.csv'
INTO TABLE stg_club
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(club_id, name, domestic_competition_id, squad_size, average_age, foreigners_number, foreigners_percentage, national_team_players,
 stadium_name, stadium_seats, net_transfer_record, last_season);

COMMIT;


CREATE TABLE stg_player (
  player_id VARCHAR(80),
  first_name VARCHAR(80),
  last_name VARCHAR(80),
  name VARCHAR(120),
  position VARCHAR(40),
  sub_position VARCHAR(40),
  current_club_id VARCHAR(40),
  market_value_eur VARCHAR(40),
  highest_market_value_eur VARCHAR(40),
  last_season VARCHAR(40)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\players.csv'
INTO TABLE stg_player
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(player_id, first_name, last_name, name, position, sub_position, current_club_id, market_value_eur,
 highest_market_value_eur, last_season);

COMMIT;

CREATE TABLE stg_player_bio (
  player_id VARCHAR(80),
  height_cm VARCHAR(80),
  dob VARCHAR(80),
  country_of_citizenship VARCHAR(80), 
  foot VARCHAR(80),
  city_of_birth VARCHAR(80),
  country_of_birth VARCHAR(80),
  image_url VARCHAR(255),
  agent_name VARCHAR(120),
  contract_expiration_date VARCHAR(80)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\player_bio.csv'
INTO TABLE stg_player_bio
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(player_id, height_cm, dob, country_of_citizenship, foot, city_of_birth, country_of_birth,
 image_url, agent_name,contract_expiration_date);

COMMIT;

CREATE TABLE stg_game (
  game_id VARCHAR(16),
  competition_id VARCHAR(16),
  season VARCHAR(16),
  round VARCHAR(32),
  date VARCHAR(32),
  home_club_id VARCHAR(16),
  away_club_id VARCHAR(16),
  home_club_goals VARCHAR(16),
  away_club_goals VARCHAR(16),
  home_club_position VARCHAR(16),
  away_club_position VARCHAR(16),
  home_club_manager_name VARCHAR(120),
  away_club_manager_name VARCHAR(120),
  stadium VARCHAR(120),
  attendance VARCHAR(16),
  referee VARCHAR(120),
  home_club_formation VARCHAR(32),
  away_club_formation VARCHAR(32),
  match_time VARCHAR(32),
  competition_type VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\games.csv'
INTO TABLE stg_game
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(game_id, competition_id, season, round, date, home_club_id, away_club_id, home_club_goals, away_club_goals, home_club_position, away_club_position, home_club_manager_name, away_club_manager_name, stadium, attendance, referee,
 home_club_formation, away_club_formation, match_time, competition_type);

COMMIT;

CREATE TABLE stg_appearance (
  appearance_id VARCHAR(32),
  game_id VARCHAR(32),
  player_id VARCHAR(32),
  player_club_id VARCHAR(32),
  player_current_club_id VARCHAR(32),
  date VARCHAR(32),
  yellow_cards VARCHAR(32),
  red_cards VARCHAR(32),
  goals VARCHAR(32),
  assists VARCHAR(32),
  minutes_played VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\appearances.csv'
INTO TABLE stg_appearance
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(appearance_id, game_id, player_id, player_club_id, player_current_club_id, date, yellow_cards, red_cards, goals, assists, minutes_played);

COMMIT;

CREATE TABLE stg_game_events (
  game_event_id VARCHAR(32),
  game_id VARCHAR(32),
  minute VARCHAR(32),
  type VARCHAR(32),
  club_id VARCHAR(32),
  player_id VARCHAR(32),
  description VARCHAR(128),
  player_in_id VARCHAR(32),
  player_assist_id VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\game_events.csv'
INTO TABLE stg_game_events
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(game_event_id, game_id, minute, type, club_id, player_id, description, player_in_id, player_assist_id);

COMMIT;


CREATE TABLE stg_transfer (
  transfer_id VARCHAR(32),
  player_id VARCHAR(32),
  transfer_date VARCHAR(32),
  transfer_season VARCHAR(32),
  from_club_id VARCHAR(32),
  to_club_id VARCHAR(32),
  transfer_fee VARCHAR(32),
  market_value_in_eur VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\transfer.csv'
INTO TABLE stg_transfer
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(transfer_id, player_id, transfer_date, transfer_season, from_club_id, to_club_id, transfer_fee, market_value_in_eur);

COMMIT;


START TRANSACTION;

LOAD DATA LOCAL INFILE 'C:\\Users\\qusya\\OneDrive\\Desktop\\Y2T1\\Database Systems\\football_db\\competitions.csv'
INTO TABLE competition
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(competition_id, name, type, country_name);

COMMIT;

-- CLEANING & TRANSFORMATION TO INSERT INTO PRODUCTION TABLE

INSERT INTO club
(club_id, name, domestic_competition_id, squad_size, average_age, foreigners_number, foreigners_percentage, national_team_players,
 stadium_name, stadium_seats, net_transfer_record, last_season)
SELECT DISTINCT
  CAST(TRIM(club_id) AS UNSIGNED),
  NULLIF(TRIM(name),''),
  NULLIF(TRIM(domestic_competition_id),''),
  CAST(NULLIF(TRIM(squad_size),'') AS SIGNED),
  CAST(NULLIF(TRIM(average_age),'') AS DECIMAL(3,1)),
  CAST(NULLIF(TRIM(foreigners_number),'') AS SIGNED),
  CAST(NULLIF(TRIM(foreigners_percentage),'') AS DECIMAL(4,1)),
  CAST(NULLIF(TRIM(national_team_players),'') AS SIGNED),
  NULLIF(TRIM(stadium_name),''),
  CAST(NULLIF(TRIM(stadium_seats),'') AS SIGNED),
  NULLIF(TRIM(net_transfer_record),''),
  CAST(NULLIF(REGEXP_REPLACE(last_season, '[^0-9]', ''), '') AS SIGNED)
FROM stg_club sc;

INSERT INTO player
(player_id, first_name, last_name, name, position, sub_position,
 current_club_id, market_value_eur, highest_market_value_eur, last_season)
SELECT
  CAST(NULLIF(TRIM(sp.player_id),'') AS UNSIGNED),
  NULLIF(TRIM(sp.first_name),''),
  NULLIF(TRIM(sp.last_name),''),
  NULLIF(TRIM(sp.name),''),
  NULLIF(TRIM(sp.position),''),
  NULLIF(TRIM(sp.sub_position),''),
  c.club_id,
  CAST(NULLIF(TRIM(sp.market_value_eur),'') AS UNSIGNED),
  CAST(NULLIF(TRIM(sp.highest_market_value_eur),'') AS UNSIGNED),
  CAST(NULLIF(REGEXP_REPLACE(sp.last_season, '[^0-9]', ''), '') AS SIGNED)
FROM stg_player sp
JOIN club c
  ON c.club_id = CAST(NULLIF(TRIM(sp.current_club_id),'') AS UNSIGNED);
 
 INSERT INTO player_bio
(player_id, height_in_cm, dob, country_of_citizenship, foot, city_of_birth,
 country_of_birth, image_url, agent_name, contract_expiration_date)
SELECT
  CAST(NULLIF(TRIM(player_id),'') AS UNSIGNED),
  CAST(NULLIF(TRIM(height_cm),'') AS UNSIGNED),
  STR_TO_DATE(NULLIF(SUBSTRING_INDEX(dob, ' ', 1), ''), '%m/%d/%Y'),
  NULLIF(TRIM(country_of_citizenship),''),
  NULLIF(TRIM(foot),''),
  NULLIF(TRIM(city_of_birth),''),
  NULLIF(TRIM(country_of_birth),''),
  NULLIF(TRIM(image_url),''),
  NULLIF(TRIM(agent_name),''),
  STR_TO_DATE(NULLIF(SUBSTRING_INDEX(contract_expiration_date, ' ', 1), ''), '%m/%d/%Y')
FROM stg_player_bio spb;

INSERT INTO game (
  game_id, competition_id, season, round, date,
  home_club_id, away_club_id, home_club_goals, away_club_goals,
  home_club_position, away_club_position,
  home_club_manager_name, away_club_manager_name,
  stadium, attendance, referee,
  home_club_formation, away_club_formation, match_time, competition_type
)
SELECT
  CAST(NULLIF(TRIM(sg.game_id),'') AS UNSIGNED),
  NULLIF(TRIM(sg.competition_id),'') ,
  NULLIF(TRIM(sg.season),''),
  NULLIF(TRIM(sg.round),''),
  STR_TO_DATE(date, '%c/%e/%Y'),
  CAST(NULLIF(TRIM(sg.home_club_id),'') AS UNSIGNED),
  CAST(NULLIF(TRIM(sg.away_club_id),'') AS UNSIGNED),
  CAST(NULLIF(TRIM(sg.home_club_goals),'') AS SIGNED),
  CAST(NULLIF(TRIM(sg.away_club_goals),'') AS SIGNED),
  CAST(NULLIF(TRIM(sg.home_club_position),'') AS SIGNED),
  CAST(NULLIF(TRIM(sg.away_club_position),'') AS SIGNED),
  NULLIF(TRIM(sg.home_club_manager_name),''),
  NULLIF(TRIM(sg.away_club_manager_name),''),
  NULLIF(TRIM(sg.stadium),''),
  CAST(NULLIF(TRIM(sg.attendance),'') AS SIGNED),
  NULLIF(TRIM(sg.referee),''),
  NULLIF(TRIM(sg.home_club_formation),''),
  NULLIF(TRIM(sg.away_club_formation),''),
  STR_TO_DATE(match_time, '%r'),
  NULLIF(TRIM(sg.competition_type),'')
FROM stg_game sg
JOIN club hc ON hc.club_id = CAST(NULLIF(TRIM(sg.home_club_id),'') AS UNSIGNED)
JOIN club ac ON ac.club_id = CAST(NULLIF(TRIM(sg.away_club_id),'') AS UNSIGNED);

INSERT INTO appearance
(appearance_id, game_id, player_id, player_club_id, player_current_club_id, date,
 yellow_cards, red_cards, goals, assists, minutes_played)
SELECT
  NULLIF(TRIM(sa.appearance_id), ''),
  CAST(NULLIF(TRIM(sa.game_id), '') AS UNSIGNED),
  CAST(NULLIF(TRIM(sa.player_id), '') AS UNSIGNED),
  CAST(NULLIF(TRIM(sa.player_club_id), '') AS UNSIGNED),
  CAST(NULLIF(TRIM(sa.player_current_club_id), '') AS UNSIGNED),
  STR_TO_DATE(NULLIF(TRIM(sa.date), ''), '%c/%e/%Y'),
  LEAST(GREATEST(CAST(NULLIF(TRIM(sa.yellow_cards),'0') AS UNSIGNED),0),2),
  LEAST(GREATEST(CAST(NULLIF(TRIM(sa.red_cards),'0') AS UNSIGNED),0),1),
  GREATEST(CAST(NULLIF(TRIM(sa.goals),'0') AS UNSIGNED),0),
  GREATEST(CAST(NULLIF(TRIM(sa.assists),'0') AS UNSIGNED),0),
  CAST(NULLIF(REGEXP_REPLACE(minutes_played, '[^0-9]', ''), '') AS UNSIGNED)
FROM stg_appearance sa
JOIN game   g ON g.game_id   = CAST(NULLIF(TRIM(sa.game_id), '') AS UNSIGNED)
JOIN player p ON p.player_id = CAST(NULLIF(TRIM(sa.player_id), '') AS UNSIGNED);

INSERT INTO game_events
(game_event_id, game_id, minute, type, club_id, player_id, description, player_in_id, player_assist_id)
SELECT
  TRIM(sge.game_event_id),
  g.game_id,
  CAST(NULLIF(TRIM(sge.minute), '') AS SIGNED),
  CASE TRIM(sge.type)
    WHEN 'Cards' THEN 'Cards'
    WHEN 'Goals' THEN 'Goals'
    WHEN 'Shootout' THEN 'Shootout'
    WHEN 'Substitutions' THEN 'Substitutions'
    ELSE NULL
  END,
  CAST(NULLIF(TRIM(sge.club_id), '') AS UNSIGNED),
  p_main.player_id,
  NULLIF(TRIM(sge.description), ''),
  p_in.player_id,
  p_ast.player_id
FROM stg_game_events sge
JOIN game g
  ON g.game_id = CAST(NULLIF(TRIM(sge.game_id), '') AS UNSIGNED)
LEFT JOIN player p_main ON p_main.player_id = CAST(NULLIF(TRIM(sge.player_id), '') AS UNSIGNED)
LEFT JOIN player p_in   ON p_in.player_id   = CAST(NULLIF(TRIM(sge.player_in_id), '') AS UNSIGNED)
LEFT JOIN player p_ast  ON p_ast.player_id  = CAST(NULLIF(REGEXP_REPLACE(TRIM(sge.player_assist_id), '[^0-9]', ''), '') AS UNSIGNED);

INSERT INTO transfer
(transfer_id, player_id, transfer_date, transfer_season, from_club_id, to_club_id, transfer_fee, market_value_in_eur)
SELECT
  CAST(NULLIF(TRIM(st.transfer_id), '') AS UNSIGNED),
  p.player_id,
  STR_TO_DATE(NULLIF(TRIM(st.transfer_date), ''), '%c/%e/%Y'),
  NULLIF(TRIM(st.transfer_season), ''),
  fc.club_id,                                            
  tc.club_id,
  NULLIF(REGEXP_REPLACE(TRIM(st.transfer_fee), '[^0-9-]', ''), ''),
  CAST(NULLIF(REGEXP_REPLACE(TRIM(st.market_value_in_eur), '[^0-9-]', ''), '') AS SIGNED)
FROM stg_transfer st
JOIN player p
  ON p.player_id = CAST(NULLIF(TRIM(st.player_id), '') AS UNSIGNED)
LEFT JOIN club fc
  ON (TRIM(st.from_club_id) = '' AND 1=1)
   OR fc.club_id = CAST(NULLIF(TRIM(st.from_club_id), '') AS UNSIGNED)
JOIN club tc
  ON tc.club_id = CAST(NULLIF(TRIM(st.to_club_id), '') AS UNSIGNED);


