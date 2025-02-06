-- 1. Create a Flink job that sessionizes the input data by IP address and host
-- 2. Use a 5 minute gap
-- 3. Answer these questions
-- 		1. What is the average number of web events of a session from a user on Tech Creator?
-- 		2. Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)

-- Creating processed_events_ip_aggregated table that will
-- store the aggregated data.
CREATE TABLE processed_events_ip_aggregated (
	event_window_timestamp TIMESTAMP(3),
	host VARCHAR,
	ip VARCHAR,
	num_hits BIGINT
);

-- Q1.: What is the average number of web events of a session from a user on Tech Creator?  
SELECT
	ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host LIKE '%techcreator.io'
GROUP BY ip, host, event_window_timestamp
ORDER BY avg_num_hits DESC
-- Answer: On Average the tech creator website gets a web events of a session from range of 1 to 38, with multiple users(IP).
-- Where maximum are in the low range of 1 and few user being in the high range.
-- Sample data for processed_events_ip_aggregated table
-- | ip              | host                    | event_window_timestamp | num_hits |
-- |-----------------|-------------------------|------------------------|----------|
-- | 197.59.215.51   | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 38       |
-- | 212.123.149.167 | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 18       |
-- | 49.204.16.80    | bootcamp.techcreator.io | 2025-02-06 16:35:00    | 12       |
-- | 79.116.19.141   | bootcamp.techcreator.io | 2025-02-06 16:55:00    | 11       |
-- | 103.201.125.203 | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 11       |
-- | 38.183.11.31    | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 10       |
-- | 212.123.149.167 | bootcamp.techcreator.io | 2025-02-06 17:00:00    | 10       |
-- | 38.183.11.31    | bootcamp.techcreator.io | 2025-02-06 17:00:00    | 10       |
-- | 103.201.125.203 | bootcamp.techcreator.io | 2025-02-06 16:35:00    | 9        |
-- | 49.37.114.88    | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 9        |
-- | 181.237.77.90   | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 7        |
-- | 103.72.212.125  | bootcamp.techcreator.io | 2025-02-06 17:00:00    | 6        |
-- | 197.59.215.51   | bootcamp.techcreator.io | 2025-02-06 17:00:00    | 6        |
-- | 103.201.125.203 | bootcamp.techcreator.io | 2025-02-06 16:40:00    | 6        |
-- | 46.155.65.71    | bootcamp.techcreator.io | 2025-02-06 17:05:00    | 6        |


-- Q2.: Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT
    ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host = 'www.dataexpert.io'
GROUP BY ip, host, event_window_timestamp;

SELECT
    ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host = 'zachwilson.techcreator.io'
GROUP BY ip, host, event_window_timestamp;

SELECT
    ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host = 'zachwilson.tech'
GROUP BY ip, host, event_window_timestamp;

SELECT
    ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host = 'lulu.techcreator.io'
GROUP BY ip, host, event_window_timestamp;

--  Consolidating all the above queries into one query for better comparison
SELECT
    ip,
	host,
    event_window_timestamp,
	CAST(AVG(num_hits) AS INTEGER) AS avg_num_hits
FROM processed_events_ip_aggregated
WHERE host IN ('www.dataexpert.io', 'zachwilson.techcreator.io', 'lulu.techcreator.io', 'zachwilson.tech')
GROUP BY ip, host, event_window_timestamp;
-- Answer: All three website have shown low range events result, but still among the three host the most is the www.dataexpert.io website.
-- Sample data for processed_events_ip_aggregated table
-- | ip              | host              | event_window_timestamp | num_hits |
-- |-----------------|-------------------|------------------------|----------|
-- | 73.58.221.159   | www.dataexpert.io | 2025-02-06 16:55:00    | 1        |
-- | 94.245.87.19    | www.dataexpert.io | 2025-02-06 16:55:00    | 1        |
-- | 119.155.197.194 | www.dataexpert.io | 2025-02-06 16:55:00    | 1        |
-- | 102.89.47.154   | www.dataexpert.io | 2025-02-06 17:00:00    | 6        |
-- | 202.179.66.185  | www.dataexpert.io | 2025-02-06 16:45:00    | 7        |
-- | 100.16.139.117  | www.dataexpert.io | 2025-02-06 17:00:00    | 2        |
-- | 66.249.66.23    | www.dataexpert.io | 2025-02-06 16:55:00    | 1        |
-- | 119.155.197.194 | www.dataexpert.io | 2025-02-06 16:30:00    | 1        |
-- | 66.44.23.132    | www.dataexpert.io | 2025-02-06 16:40:00    | 3        |
-- | 108.6.16.247    | www.dataexpert.io | 2025-02-06 16:50:00    | 6        |
-- | 73.58.221.159   | zachwilson.techcreator.io | 2025-02-06 16:55:00    | 1        |
-- | 94.245.87.19    | zachwilson.tech           | 2025-02-06 16:55:00    | 1        |
-- | 119.155.197.194 | lulu.techcreator.io       | 2025-02-06 16:55:00    | 1        |
-- | 102.89.47.154   | zachwilson.techcreator.io | 2025-02-06 17:00:00    | 6        |
-- | 202.179.66.185  | zachwilson.tech           | 2025-02-06 16:45:00    | 7        |
-- | 100.16.139.117  | lulu.techcreator.io       | 2025-02-06 17:00:00    | 2        |
-- | 66.249.66.23    | zachwilson.techcreator.io | 2025-02-06 16:55:00    | 1        |
