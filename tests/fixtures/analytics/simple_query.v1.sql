SELECT
    DATE(created_at) AS date,
    region,
    COUNT(*) AS count
FROM raw.events
WHERE DATE(created_at) = @partition_date
GROUP BY 1, 2
