SELECT
    DATE(created_at) AS date,
    region,
    user_tier,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events
FROM raw.events
WHERE DATE(created_at) = @partition_date
GROUP BY 1, 2, 3
