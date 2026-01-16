-- Biggest week-over-week changes in team_total_yards (week vs prior week)
WITH w AS (
  SELECT
    season,
    week,
    team,
    team_total_yards,
    LAG(team_total_yards) OVER (PARTITION BY season, team ORDER BY week) AS prev_week_yards
  FROM mart_team_week
  WHERE season = 2025
)
SELECT
  team,
  week,
  team_total_yards,
  prev_week_yards,
  (team_total_yards - prev_week_yards) AS wow_change
FROM w
WHERE prev_week_yards IS NOT NULL
ORDER BY ABS(wow_change) DESC
LIMIT 20;
