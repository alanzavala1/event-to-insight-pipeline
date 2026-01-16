-- Unified mart view for analysis queries
CREATE OR REPLACE VIEW mart_team_week AS
SELECT
  m.season,
  m.week,
  m.team,
  m.opponent,
  m.team_total_yards,
  m.team_pass_yards,
  m.team_rush_yards,
  i.pass_share,
  i.top_player_name,
  i.top_player_yards,
  i.top_player_share,
  v.rolling_std_total_yards
FROM core_team_week_metrics m
LEFT JOIN mart_team_week_insights i
  ON i.season = m.season AND i.week = m.week AND i.team = m.team
LEFT JOIN mart_team_week_volatility v
  ON v.season = m.season AND v.week = m.week AND v.team = m.team;
