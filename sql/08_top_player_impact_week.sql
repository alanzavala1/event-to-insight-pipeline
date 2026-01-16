-- Top player impact this week (same as dependency, but explicit framing)
SELECT team, opponent, top_player_name, top_player_share
FROM mart_team_week
WHERE season = 2025 AND week = 3 AND top_player_share IS NOT NULL
ORDER BY top_player_share DESC
LIMIT 10;
