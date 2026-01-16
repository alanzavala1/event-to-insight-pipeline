-- Most consistent teams this week (lowest rolling stddev)
SELECT team, rolling_std_total_yards
FROM mart_team_week
WHERE season = 2025 AND week = 3 AND rolling_std_total_yards IS NOT NULL
ORDER BY rolling_std_total_yards ASC
LIMIT 10;
