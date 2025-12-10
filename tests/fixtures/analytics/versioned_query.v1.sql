SELECT
    DATE(created_at) AS date,
    user_id,
    COUNT(*) AS events
FROM raw.events
WHERE DATE(created_at) = @partition_date
GROUP BY 1, 2
