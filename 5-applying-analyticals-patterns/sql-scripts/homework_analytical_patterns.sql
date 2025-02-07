-- The homework this week will be using the players, players_scd, and player_seasons tables from week 1

-- 1. A query that does state change tracking for players
    -- A player entering the league should be New
    -- A player leaving the league should be Retired
    -- A player staying in the league should be Continued Playing
    -- A player that comes out of retirement should be Returned from Retirement
    -- A player that stays out of the league should be Stayed Retired

-- Create a table to track the growth and state changes of players over the years
-- This table will store the player's name, the current year, the first and last active years,
-- the player's state (e.g., New, Retired), and an array of years the player was active

CREATE TABLE players_growth_accounting (
    player_name TEXT, -- Name of the player
    current_year INT, -- The current year being tracked
    first_active_year INT, -- The first year the player was active
    last_active_year INT, -- The last year the player was active
    player_state TEXT, -- The state of the player (New, Retired, etc.)
    years_active INT[], -- Array of years the player was active
    PRIMARY KEY (player_name, current_year) -- Primary key constraint
);

-- This SQL script inserts data into the players_growth_accounting table by comparing player records
-- from the previous year (2000) and the current year (2001). It determines the player's state 
-- (e.g., New, Retired, Continued Playing, Returned from Retirement, Stayed Retired) and updates 
-- the years active array accordingly. The script uses common table expressions (CTEs) to separate 
-- the data for yesterday and today, and then performs a full outer join to combine and analyze the data.

INSERT INTO players_growth_accounting
WITH
    yesterday AS (
        SELECT *
        FROM players_growth_accounting
        WHERE current_year = 2000 -- Select records from the previous year
    ),
    today AS (
        SELECT 
            player_name,
            season AS current_year
        FROM player_seasons
        WHERE season = 2001 -- Select records for the current year
    )
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name, -- Use player name from today or yesterday
    COALESCE(t.current_year, y.current_year + 1) AS current_year, -- Use current year from today or increment yesterday's year
    COALESCE(y.first_active_year, t.current_year) AS first_active_year, -- Use first active year from yesterday or current year
    COALESCE(t.current_year, y.last_active_year) AS last_active_year, -- Use last active year from today or yesterday
    CASE
        WHEN y.player_name IS NULL THEN 'New' -- Player is new if not in yesterday's data
        WHEN t.player_name IS NULL THEN 'Retired' -- Player is retired if not in today's data
        WHEN t.current_year - y.last_active_year = 1 THEN 'Continued Playing' -- Player continued playing if active in consecutive years
        WHEN t.current_year - y.last_active_year > 1 THEN 'Returned from Retirement' -- Player returned from retirement if gap in active years
        WHEN t.player_name IS NULL AND y.current_year <> y.last_active_year THEN 'Stayed Retired' -- Player stayed retired if not active this year
    END AS player_state, -- Determine the player's state
    CASE
        WHEN t.current_year IS NULL THEN y.years_active -- Use years active from yesterday if not active today
        WHEN y.years_active IS NULL THEN ARRAY[t.current_year] -- Initialize years active array if not present
        ELSE years_active || ARRAY[t.current_year] -- Append current year to years active array
    END AS years_active -- Update the years active array
FROM yesterday y
FULL OUTER JOIN today t
    ON y.player_name = t.player_name; -- Join yesterday's and today's data on player name

-- 2. A query that uses GROUPING SETS to do efficient aggregations of game_details data
    -- Aggregate this dataset along the following dimensions
    -- player and team
        -- Answer questions like who scored the most points playing for one team?
    -- player and season
        -- Answer questions like who scored the most points in one season?
    -- team
        -- Answer questions like which team has won the most games?

-- This SQL script performs data aggregation on game statistics.
-- It first creates a common table expression (CTE) named 'games_augmented' 
-- that joins 'games' and 'game_details' tables to include player points.
-- Then, it selects and groups data by different levels (player-team, player-season, and team)
-- to calculate the number of games played and total points scored, 
-- while also indicating the aggregation level for each group.
WITH 
    games_augmented AS (
        SELECT
            g.game_id, -- Game ID
            g.season, -- Season of the game
            gd.player_name, -- Name of the player
            gd.team_id, -- Team ID
            gd.pts -- Points scored by the player
        FROM games g
        LEFT JOIN game_details AS gd
            ON g.game_id = gd.game_id -- Join games and game_details on game_id
    )
SELECT
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_id) = 0
            THEN 'player_team' -- Aggregation level for player and team
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0
            THEN 'player_season' -- Aggregation level for player and season
        WHEN GROUPING(team_id) = 0 THEN 'team' -- Aggregation level for team
    END AS aggregation_level, -- Determine the aggregation level
    player_name, -- Name of the player
    team_id, -- Team ID
    season, -- Season of the game
    COUNT(1) AS game_played, -- Count of games played
    SUM(pts) AS total_points -- Sum of points scored
FROM games_augmented
GROUP BY GROUPING SETS (
    (player_name, team_id), -- Group by player and team
    (player_name, season), -- Group by player and season
    (team_id) -- Group by team
);

-- 3. A query that uses window functions on game_details to find out the following things:
    -- What is the most games a team has won in a 90 game stretch?
    -- How many games in a row did LeBron James score over 10 points a game?

-- This query calculates the longest streak of games where LeBron James scored more than 10 points.
-- It first creates a temporary table 'streaks' to identify streak groups based on consecutive games with points over 10.
-- Then, it selects the longest streak by counting the number of games in each streak group and orders the result in descending order.
WITH
    streaks AS (
        SELECT
            g.game_date_est, -- Game date
            g.game_id, -- Game ID
            player_name, -- Player name
            pts, -- Points scored
            CASE
                WHEN pts > 10 THEN 1 ELSE 0 -- Check if points scored is greater than 10
            END AS over_10_pts, -- Flag for points over 10
            ROW_NUMBER() OVER (
                PARTITION BY player_name 
                ORDER BY game_date_est
                ) - ROW_NUMBER () OVER(
                    PARTITION BY 
                        player_name,
                        CASE
                            WHEN pts > 10 THEN 1
                            ELSE 0
                        END
                    ORDER BY game_date_est
                ) AS streak_group -- Calculate streak group
            FROM game_details gd
            LEFT JOIN games g
                ON gd.game_id = g.game_id -- Join game_details and games on game_id
            WHERE gd.player_name = 'LeBron James' -- Filter for LeBron James
    )
SELECT
    player_name, -- Player name
    COUNT(*) AS longest_streak -- Count of longest streak
FROM streaks
WHERE over_10_pts = 1 -- Filter for points over 10
GROUP BY player_name, streak_group -- Group by player name and streak group
ORDER BY longest_streak DESC -- Order by longest streak in descending order
LIMIT 1; -- Limit to the top result
