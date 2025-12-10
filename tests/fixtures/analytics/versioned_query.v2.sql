SELECT
    DATE(e.created_at) AS date,
    e.user_id,
    COUNT(*) AS events
FROM raw.events e
WHERE DATE(e.created_at) = @partition_date
GROUP BY 1, 2
