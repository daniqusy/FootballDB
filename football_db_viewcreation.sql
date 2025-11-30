-- CREATION OF VIEWS

CREATE OR REPLACE VIEW view_player_season_stats AS
SELECT
  a.player_id,
  g.competition_id,
  g.season,
  COUNT(*)                           AS apps,
  SUM(a.minutes_played)              AS minutes,
  SUM(a.goals)                       AS goals,
  SUM(a.assists)                     AS assists,
  ROUND( (SUM(a.goals)+SUM(a.assists)) / NULLIF(SUM(a.minutes_played),0) * 90, 3) AS g_a_per90
FROM appearance a
JOIN game g ON g.game_id = a.game_id
GROUP BY a.player_id, g.competition_id, g.season;

CREATE OR REPLACE VIEW view_club_transfer_roi AS
SELECT
  t.to_club_id                                  AS club_id,
  t.player_id,
  t.transfer_season,
  t.transfer_fee,
  t.market_value_in_eur,
  SUM(a.minutes_played)                         AS post_minutes,
  SUM(a.goals)                                  AS post_goals,
  SUM(a.assists)                                AS post_assists,
  ROUND(NULLIF(t.transfer_fee,0) / SUM(a.minutes_played) , 2) AS eur_per_minutes,
  ROUND(NULLIF(t.transfer_fee,0) / (SUM(a.goals)+SUM(a.assists)), 2) AS eur_per_contrib
FROM transfer t
LEFT JOIN appearance a
  ON a.player_id = t.player_id
 AND a.date >= t.transfer_date
GROUP BY t.to_club_id, t.player_id, t.transfer_season, t.transfer_fee, t.market_value_in_eur;
