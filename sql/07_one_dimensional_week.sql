-- Most one-dimensional offenses this week (pass_share farthest from 0.50)
SELECT team, opponent, pass_share
FROM mart_team_week
WHERE season = 2025 AND week = 3 AND pass_share IS NOT NULL
ORDER BY ABS(pass_share - 0.50) DESC
LIMIT 10;
