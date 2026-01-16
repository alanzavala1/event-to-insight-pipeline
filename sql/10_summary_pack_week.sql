-- One query that returns a compact "week summary" table for downstream reporting
SELECT
  team,
  opponent,
  team_total_yards,
  pass_share,
  top_player_name,
  top_player_share,
  rolling_std_total_yards
FROM mart_team_week
WHERE season = 2025 AND week = 3
ORDER BY team_total_yards DESC;

