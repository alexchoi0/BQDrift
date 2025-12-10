SELECT
    DATE(e.created_at) AS date,
    COALESCE(e.user_id, 'unknown') AS user_id,
    COUNT(*) AS events,
    COUNT(DISTINCT e.session_id) AS session_count
FROM raw.events e
WHERE DATE(e.created_at) = @partition_date
GROUP BY 1, 2
