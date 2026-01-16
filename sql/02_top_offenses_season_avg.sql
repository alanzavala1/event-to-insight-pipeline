-- Season average total yards per team
SELECT
  team,
  AVG(team_total_yards) AS avg_total_yards
FROM mart_team_week
WHERE season = 2025
GROUP BY team
ORDER BY avg_total_yards DESC
LIMIT 10;
