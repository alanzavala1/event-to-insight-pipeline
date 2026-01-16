-- Most volatile teams this week (highest rolling stddev)
SELECT team, rolling_std_total_yards
FROM mart_team_week
WHERE season = 2025 AND week = 3 AND rolling_std_total_yards IS NOT NULL
ORDER BY rolling_std_total_yards DESC
LIMIT 10;
