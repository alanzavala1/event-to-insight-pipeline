-- Top offenses by total yards for a given season/week
-- Params: set season/week at the bottom
SELECT team, opponent, team_total_yards
FROM mart_team_week
WHERE season = 2025 AND week = 3
ORDER BY team_total_yards DESC
LIMIT 10;
