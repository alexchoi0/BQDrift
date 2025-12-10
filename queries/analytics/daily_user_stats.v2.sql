SELECT
    DATE(e.created_at) AS date,
    e.region,
    u.tier AS user_tier,
    COUNT(DISTINCT e.user_id) AS unique_users,
    COUNT(*) AS total_events
FROM raw.events e
LEFT JOIN raw.users u ON e.user_id = u.id
WHERE DATE(e.created_at) = @partition_date
GROUP BY 1, 2, 3
